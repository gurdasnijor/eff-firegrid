// Capability C3: KvStore
//
// Script-first proof for the concrete S2 KV-demo-style store over StateView.
//
// Run:
//   dotnet fable scripts/foundation-02-kv-store.fsx --outDir build_kv_store --runScript

#r "nuget: Fable.Core, 5.1.0"
#load "../src/S2/Interop.fs"
#load "../src/S2/Errors.fs"
#load "../src/S2/Client.fs"
#load "../src/S2/Cli.fs"
#load "../src/Foundation/SubjectHistory.fs"
#load "../src/Foundation/StateView.fs"
#load "../src/Foundation/KvStore.fs"

open System
open Fable.Core
open Eff
open Eff.Foundation

[<Emit("process.exit($0)")>]
let exit (code: int) : unit = jsNative

[<Emit("Date.now()")>]
let now () : float = jsNative

module Proof =
    let mutable passed = 0
    let mutable failed = 0
    let mutable failures: string list = []
    let mutable counter = 0

    let uniq (prefix: string) : string =
        counter <- counter + 1
        sprintf "%s-%d-%d" prefix (int64 (now ())) counter

    let check name condition =
        if condition then
            passed <- passed + 1
            printfn "  ok  %s" name
        else
            failed <- failed + 1
            failures <- name :: failures
            printfn "  bad %s" name

    let checkFails name (expected: string) (body: Async<'a>) =
        async {
            try
                let! _ = body
                check name false
            with e ->
                check name (e.Message.Contains expected)
        }

    let section name body =
        async {
            printfn "\n== %s ==" name

            try
                do! body
            with e ->
                failed <- failed + 1
                failures <- (name + " threw: " + e.Message) :: failures
                printfn "  bad threw: %s" e.Message
        }

    let finish () =
        printfn "\n%d passed, %d failed" passed failed

        if failed > 0 then
            failures |> List.rev |> List.iter (printfn "  - %s")
            exit 1
        else
            exit 0

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

module TextKeys =
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

module KvCodec =
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

let basinName = "test-basin-885234"

let main =
    async {
        let s2 = S2Cli.connect ()
        let basin = s2 |> S2.basin basinName
        let subjectName = Proof.uniq "foundation-kv-store"
        let subject = SubjectHistory.SubjectId subjectName
        let mutable created = false
        let mutable store: KvStore<TextKey, int> option = None

        let key name = TextKeys.normal name

        printfn "kv-store subject stream: %s/%s" basinName subjectName

        let cleanup () =
            async {
                match store with
                | Some running ->
                    try
                        do! KvStore.stop running
                    with e ->
                        printfn "store cleanup failed: %s" e.Message

                    store <- None
                | None -> ()

                if created then
                    do! basin |> S2.deleteStream subjectName
                    created <- false
            }

        try
            try
                do! basin |> S2.createStream subjectName
                created <- true

                let! running = KvStore.start basin KvCodec.codec subject (SubjectHistory.Seq 0L)

                store <- Some running

                do!
                    Proof.section
                        "put and get"
                        (async {
                            let! putVersion = KvStore.put (key "alpha") 1 running
                            Proof.check "put returns durable append version" (putVersion = SubjectHistory.Version 1L)

                            let! strongVersion, strongValue = KvStore.get Strong (key "alpha") running

                            Proof.check
                                "strong get catches up to put"
                                (strongVersion = putVersion && strongValue = Some 1)

                            let! eventualVersion, eventualValue = KvStore.get Eventual (key "alpha") running

                            Proof.check
                                "eventual get observes current local map"
                                (eventualVersion = putVersion && eventualValue = Some 1)
                        })

                do!
                    Proof.section
                        "delete"
                        (async {
                            let! deleteVersion = KvStore.delete (key "alpha") running

                            Proof.check
                                "delete returns durable append version"
                                (deleteVersion = SubjectHistory.Version 2L)

                            let! strongVersion, strongValue = KvStore.get Strong (key "alpha") running

                            Proof.check
                                "strong get observes delete"
                                (strongVersion = deleteVersion && strongValue = None)
                        })

                do!
                    Proof.section
                        "second writer catch-up"
                        (async {
                            let! externalVersion =
                                SubjectHistory.append basin KvCodec.codec subject [ Put(key "beta", 2) ]

                            let! strongVersion, strongValue = KvStore.get Strong (key "beta") running

                            Proof.check
                                "strong get catches up to append from another writer"
                                (strongVersion = externalVersion && strongValue = Some 2)
                        })

                do!
                    Proof.section
                        "write ack before local apply"
                        (async {
                            let! normalVersion = KvStore.put (key "stable") 10 running
                            let! stableVersion, stableValue = KvStore.get Strong (key "stable") running

                            Proof.check
                                "precondition stable key is applied"
                                (stableVersion = normalVersion && stableValue = Some 10)

                            let! failingVersion = KvStore.put (TextKeys.explosive "bad") 99 running

                            Proof.check
                                "put returns durable version before local apply succeeds"
                                (failingVersion = SubjectHistory.Version(
                                    SubjectHistory.versionNumber normalVersion + 1L
                                ))

                            do!
                                Proof.checkFails
                                    "strong get surfaces failed local apply after acknowledged write"
                                    "kv local apply comparison failed"
                                    (KvStore.get Strong (key "stable") running)

                            do!
                                Proof.checkFails
                                    "eventual get also fails after local apply failure"
                                    "kv local apply comparison failed"
                                    (KvStore.get Eventual (key "stable") running)
                        })

                do! cleanup ()
                Proof.finish ()
            with e ->
                do! cleanup ()
                return raise e
        with e ->
            printfn "\nfatal: %s" e.Message
            Proof.failed <- Proof.failed + 1
            Proof.failures <- ("fatal: " + e.Message) :: Proof.failures
            Proof.finish ()
    }

main |> Async.StartImmediate
