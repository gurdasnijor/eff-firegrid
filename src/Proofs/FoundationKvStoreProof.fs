namespace Eff.Proofs

open System
open Eff
open Eff.Foundation

module FoundationKvStoreProof =
    type TextKey(value: string, explode: bool) =
        member _.Value = value
        member _.Explode = explode

        interface IComparable with
            member this.CompareTo(other: obj) =
                match other with
                | :? TextKey as otherKey ->
                    if this.Explode || otherKey.Explode then
                        failwith "kv local apply comparison failed"
                    else
                        compare this.Value otherKey.Value
                | _ -> invalidArg "other" "expected TextKey"

        override this.Equals(other: obj) =
            match other with
            | :? TextKey as otherKey -> this.Value = otherKey.Value && this.Explode = otherKey.Explode
            | _ -> false

        override this.GetHashCode() = hash (this.Value, this.Explode)
        override this.ToString() = this.Value

    type KvStoreProofResult =
        { PutAck: bool
          StrongGetAfterPut: bool
          EventualGetAfterPut: bool
          DeleteAck: bool
          StrongGetAfterDelete: bool
          StrongFollowerCatchUp: bool
          WriteAckBeforeLocalApply: bool
          StrongReadFailsAfterApplyFailure: bool
          EventualReadFailsAfterApplyFailure: bool }

    module private TextKeys =
        let normal value = TextKey(value, false)
        let explosive value = TextKey(value, true)

        let encode (key: TextKey) =
            if key.Explode then
                "explode:" + key.Value
            else
                "key:" + key.Value

        let decode (text: string) =
            if text.StartsWith("explode:", StringComparison.Ordinal) then
                Ok(explosive (text.Substring "explode:".Length))
            elif text.StartsWith("key:", StringComparison.Ordinal) then
                Ok(normal (text.Substring "key:".Length))
            else
                Error("bad key: " + text)

    module private KvCodec =
        let private field (value: string) = string value.Length + ":" + value

        let private readField (text: string) (index: int) =
            let colon = text.IndexOf(':', index)

            if colon < 0 then
                Error "missing field length separator"
            else
                let lengthText = text.Substring(index, colon - index)

                match Int32.TryParse lengthText with
                | false, _ -> Error("bad field length: " + lengthText)
                | true, length ->
                    let start = colon + 1
                    let finish = start + length

                    if finish > text.Length then
                        Error "field length exceeds record body"
                    else
                        Ok(text.Substring(start, length), finish)

        let encode event =
            match event with
            | Put(key, value) -> "put|" + field (TextKeys.encode key) + field (string value)
            | Delete key -> "delete|" + field (TextKeys.encode key)

        let decode (body: string) =
            if body.StartsWith("put|", StringComparison.Ordinal) then
                readField body 4
                |> Result.bind (fun (keyText, next) ->
                    readField body next
                    |> Result.bind (fun (valueText, finish) ->
                        if finish <> body.Length then
                            Error "trailing put data"
                        else
                            TextKeys.decode keyText
                            |> Result.bind (fun key ->
                                match Int32.TryParse valueText with
                                | true, value -> Ok(Put(key, value))
                                | false, _ -> Error("bad value: " + valueText))))
            elif body.StartsWith("delete|", StringComparison.Ordinal) then
                readField body 7
                |> Result.bind (fun (keyText, finish) ->
                    if finish <> body.Length then
                        Error "trailing delete data"
                    else
                        TextKeys.decode keyText |> Result.map Delete)
            else
                Error("unknown kv event body: " + body)

        let codec: SubjectHistory.Codec<KvEvent<TextKey, int>> =
            { Encode = encode; Decode = decode }

    let private failsWith (expected: string) work =
        async {
            try
                let! _ = work
                return false
            with e ->
                return e.Message.Contains(expected)
        }

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "foundation.kv_store"
            "foundation-kv-store"
            { ProofOperationOptions.empty with
                Key = Some "foundation-kv-store" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "fnd-kv-store-" + suffix
                let subjectName = "subject-" + suffix
                let subject = SubjectHistory.SubjectId subjectName
                let key name = TextKeys.normal name

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                do! basin |> S2.createStream subjectName

                let! store = KvStore.start basin KvCodec.codec subject (SubjectHistory.Seq 0L)

                let! putVersion = KvStore.put (key "alpha") 1 store
                let! strongPutVersion, strongPutValue = KvStore.get Strong (key "alpha") store
                let! eventualPutVersion, eventualPutValue = KvStore.get Eventual (key "alpha") store

                let! deleteVersion = KvStore.delete (key "alpha") store
                let! strongDeleteVersion, strongDeleteValue = KvStore.get Strong (key "alpha") store

                let! externalVersion = SubjectHistory.append basin KvCodec.codec subject [ Put(key "beta", 2) ]
                let! externalReadVersion, externalValue = KvStore.get Strong (key "beta") store

                let! normalVersion = KvStore.put (key "stable") 10 store
                let! stableVersion, stableValue = KvStore.get Strong (key "stable") store

                let! failingVersion = KvStore.put (TextKeys.explosive "bad") 99 store

                let! strongFailure =
                    KvStore.get Strong (key "stable") store
                    |> failsWith "kv local apply comparison failed"

                let! eventualFailure =
                    KvStore.get Eventual (key "stable") store
                    |> failsWith "kv local apply comparison failed"

                do! KvStore.stop store
                do! basin |> S2.deleteStream subjectName

                let result =
                    { PutAck = putVersion = SubjectHistory.Version 1L
                      StrongGetAfterPut = strongPutVersion = putVersion && strongPutValue = Some 1
                      EventualGetAfterPut = eventualPutVersion = putVersion && eventualPutValue = Some 1
                      DeleteAck = deleteVersion = SubjectHistory.Version 2L
                      StrongGetAfterDelete = strongDeleteVersion = deleteVersion && strongDeleteValue = None
                      StrongFollowerCatchUp = externalReadVersion = externalVersion && externalValue = Some 2
                      WriteAckBeforeLocalApply =
                        stableVersion = normalVersion
                        && stableValue = Some 10
                        && failingVersion = SubjectHistory.Version(SubjectHistory.versionNumber normalVersion + 1L)
                      StrongReadFailsAfterApplyFailure = strongFailure
                      EventualReadFailsAfterApplyFailure = eventualFailure }

                do!
                    ctx.EmitSpan
                        "proof.foundation.kv_store.completed"
                        [ "proof.property", "foundation.kv-store"
                          "foundation.put", string result.StrongGetAfterPut
                          "foundation.delete", string result.StrongGetAfterDelete
                          "foundation.write_ack_window", string result.WriteAckBeforeLocalApply ]

                return result
            })

    let kvStoreProperty =
        property "foundation.kv-store" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "put returns durable append version" (fun result -> result.PutAck)
                  v.Expect.Workload "strong get observes put" (fun result -> result.StrongGetAfterPut)
                  v.Expect.Workload "eventual get observes put" (fun result -> result.EventualGetAfterPut)
                  v.Expect.Workload "delete returns durable append version" (fun result -> result.DeleteAck)
                  v.Expect.Workload "strong get observes delete" (fun result -> result.StrongGetAfterDelete)
                  v.Expect.Workload "strong get catches up to another writer" (fun result ->
                      result.StrongFollowerCatchUp)
                  v.Expect.Workload "write ack precedes local apply success" (fun result ->
                      result.WriteAckBeforeLocalApply)
                  v.Expect.Workload "strong read fails after local apply failure" (fun result ->
                      result.StrongReadFailsAfterApplyFailure)
                  v.Expect.Workload "eventual read fails after local apply failure" (fun result ->
                      result.EventualReadFailsAfterApplyFailure)
                  v.Trace.SpanExists
                      "foundation KvStore proof span emitted"
                      "proof.foundation.kv_store.completed"
                      [ "proof.property", "foundation.kv-store" ]
                  v.Trace.Operation
                      "foundation KvStore operation was recorded"
                      ({ TraceOperationMatch.named "foundation.kv_store" with
                          Status = Some "ok"
                          OutputContains = [ "WriteAckBeforeLocalApply"; "StrongFollowerCatchUp" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "foundation.kv-store" {
            describedAs "KvStore put/delete, eventual and strong reads, follower catch-up, and write-ack invariants."
            property kvStoreProperty
        }
