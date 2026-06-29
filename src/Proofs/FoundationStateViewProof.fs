namespace Eff.Proofs

open Eff
open Eff.Foundation

module FoundationStateViewProof =
    type CounterRecord =
        | Add of amount: int
        | Mark of label: string

    type CounterState = { Total: int; Labels: string list }

    type StateViewProofResult =
        { StrongSeededTail: bool
          StrongSeededState: bool
          EventualSnapshot: bool
          StrongFollowerCatchUp: bool
          StrongFollowerState: bool
          PumpErrorFailsStrongRead: bool
          PumpErrorFailsEventualRead: bool
          StopAfterPumpError: bool }

    module private CounterRecord =
        let encode record =
            match record with
            | Add amount -> "add|" + string amount
            | Mark label -> "mark|" + label

        let decode (body: string) =
            let parts = body.Split('|') |> Array.toList

            match parts with
            | [ "add"; amount ] ->
                match System.Int32.TryParse amount with
                | true, value -> Ok(Add value)
                | false, _ -> Error("bad add amount: " + amount)
            | [ "mark"; label ] -> Ok(Mark label)
            | _ -> Error("unknown record body: " + body)

        let codec: SubjectHistory.Codec<CounterRecord> =
            { Encode = encode; Decode = decode }

    module private RawRecord =
        let codec: SubjectHistory.Codec<string> = { Encode = id; Decode = Ok }

    module private CounterState =
        let empty = { Total = 0; Labels = [] }

        let apply state (record: SubjectHistory.StoredRecord<CounterRecord>) =
            match record.Body with
            | Add amount ->
                { state with
                    Total = state.Total + amount }
            | Mark label ->
                { state with
                    Labels = state.Labels @ [ label ] }

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
            "foundation.state_view"
            "foundation-state-view"
            { ProofOperationOptions.empty with
                Key = Some "foundation-state-view" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "fnd-state-view-" + suffix
                let subjectName = "subject-" + suffix
                let failedSubjectName = "subject-failure-" + suffix
                let subject = SubjectHistory.SubjectId subjectName
                let failedSubject = SubjectHistory.SubjectId failedSubjectName

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                do! basin |> S2.createStream subjectName
                do! basin |> S2.createStream failedSubjectName

                let! seeded = SubjectHistory.append basin CounterRecord.codec subject [ Add 3; Mark "seeded" ]

                let! view =
                    StateView.start
                        basin
                        CounterRecord.codec
                        subject
                        (SubjectHistory.Seq 0L)
                        CounterState.empty
                        CounterState.apply

                let! seededStrong = StateView.read Strong view
                let! eventual = StateView.read Eventual view

                let! appended = SubjectHistory.append basin CounterRecord.codec subject [ Add 4; Mark "after-start" ]

                let! followerStrong = StateView.read Strong view
                do! StateView.stop view

                let! _ = SubjectHistory.append basin RawRecord.codec failedSubject [ "corrupt" ]

                let! failedView =
                    StateView.start
                        basin
                        CounterRecord.codec
                        failedSubject
                        (SubjectHistory.Seq 0L)
                        CounterState.empty
                        CounterState.apply

                let! pumpErrorFailsStrongRead = StateView.read Strong failedView |> failsWith "decode failed"
                let! pumpErrorFailsEventualRead = StateView.read Eventual failedView |> failsWith "decode failed"

                let! stopAfterPumpError =
                    async {
                        try
                            do! StateView.stop failedView
                            return true
                        with _ ->
                            return false
                    }

                do! basin |> S2.deleteStream failedSubjectName
                do! basin |> S2.deleteStream subjectName

                let result =
                    { StrongSeededTail = seededStrong.AppliedTail = seeded
                      StrongSeededState = seededStrong.State.Total = 3 && seededStrong.State.Labels = [ "seeded" ]
                      EventualSnapshot =
                        eventual.State.Total = 3
                        && eventual.State.Labels = [ "seeded" ]
                        && eventual.AppliedTail = SubjectHistory.Version 2L
                      StrongFollowerCatchUp = followerStrong.AppliedTail = appended
                      StrongFollowerState =
                        followerStrong.State.Total = 7
                        && followerStrong.State.Labels = [ "seeded"; "after-start" ]
                      PumpErrorFailsStrongRead = pumpErrorFailsStrongRead
                      PumpErrorFailsEventualRead = pumpErrorFailsEventualRead
                      StopAfterPumpError = stopAfterPumpError }

                do!
                    ctx.EmitSpan
                        "proof.foundation.state_view.completed"
                        [ "proof.property", "foundation.state-view"
                          "foundation.seeded", string result.StrongSeededState
                          "foundation.catch_up", string result.StrongFollowerCatchUp
                          "foundation.pump_error", string result.PumpErrorFailsStrongRead ]

                return result
            })

    let stateViewProperty =
        property "foundation.state-view" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "strong read catches up to seeded tail" (fun result -> result.StrongSeededTail)
                  v.Expect.Workload "strong read folds seeded state" (fun result -> result.StrongSeededState)
                  v.Expect.Workload "eventual read returns local snapshot" (fun result -> result.EventualSnapshot)
                  v.Expect.Workload "strong read catches follower append" (fun result -> result.StrongFollowerCatchUp)
                  v.Expect.Workload "strong read returns follower state" (fun result -> result.StrongFollowerState)
                  v.Expect.Workload "pump error fails strong read" (fun result -> result.PumpErrorFailsStrongRead)
                  v.Expect.Workload "pump error fails eventual read" (fun result -> result.PumpErrorFailsEventualRead)
                  v.Expect.Workload "stop completes after pump error" (fun result -> result.StopAfterPumpError)
                  v.Trace.SpanExists
                      "foundation StateView proof span emitted"
                      "proof.foundation.state_view.completed"
                      [ "proof.property", "foundation.state-view" ]
                  v.Trace.Operation
                      "foundation StateView operation was recorded"
                      ({ TraceOperationMatch.named "foundation.state_view" with
                          Status = Some "ok"
                          OutputContains = [ "StrongFollowerCatchUp"; "PumpErrorFailsStrongRead" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "foundation.state-view" {
            describedAs "StateView fold, eventual read, strong read, follower catch-up, and terminal error invariants."
            property stateViewProperty
        }
