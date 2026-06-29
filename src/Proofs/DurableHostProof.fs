namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable

module DurableHostProof =
    type DurableHostResult =
        { FirstActivityCommitsOnce: bool
          SecondRunWaitingNotDuplicateCommit: bool
          CompletedActivityAdvancesWorkflow: bool
          StaleFenceReturnsDeposed: bool }

    let private forceDecoded entries =
        entries
        |> List.map (fun (_, decoded) ->
            match decoded with
            | Ok entry -> entry
            | Error error -> failwith error)

    let private activityCommandCount activity entries =
        entries
        |> List.filter (function
            | Outgoing(Command(CallActivity(_, called))) when called = activity -> true
            | _ -> false)
        |> List.length

    let private hasActivityCalled opId activity entries =
        entries
        |> List.exists (function
            | Incoming(HistoryEvent(ActivityCalled(id, called))) when id = opId && called = activity -> true
            | _ -> false)

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.host"
            "durable-host"
            { ProofOperationOptions.empty with
                Key = Some "durable-host" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-host-" + suffix
                let key = StorageKey("workflow-" + suffix)
                let staleKey = StorageKey("stale-" + suffix)

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                do! S2Substrate.ensureStreams basin key
                do! S2Substrate.ensureStreams basin staleKey

                let pair = S2Substrate.streams basin key
                let stalePair = S2Substrate.streams basin staleKey

                let! owner = S2Substrate.claimWith (FenceToken "host-loop/fence-1") pair

                let reserve = Activities.create "reserve" "order-1"
                let charge = Activities.create "charge" "reserved"

                let program =
                    durable {
                        let! reserved = Workflow.call reserve.Name reserve.Input
                        let! charged = Workflow.call charge.Name reserved
                        return charged
                    }

                let! first = DurableHost.runOnce StepRecordCodec.encode StepRecordCodec.decode 100L owner program

                let! afterFirstRaw = S2Substrate.readLogText StepRecordCodec.decode owner
                let afterFirst = forceDecoded afterFirstRaw

                let firstActivityCommitsOnce =
                    match first with
                    | DurableHostStatus.Committed ack ->
                        ack.Start.SeqNum = 1L
                        && ack.End.SeqNum = 3L
                        && hasActivityCalled (OpId 0) reserve afterFirst
                        && activityCommandCount reserve afterFirst = 1
                    | _ -> false

                let! second = DurableHost.runOnce StepRecordCodec.encode StepRecordCodec.decode 101L owner program

                let! afterSecondRaw = S2Substrate.readLogText StepRecordCodec.decode owner
                let afterSecond = forceDecoded afterSecondRaw

                let secondRunWaitingNotDuplicateCommit =
                    match second with
                    | DurableHostStatus.Waiting(OpId 0, NeedsActivity blocked) ->
                        blocked = reserve
                        && afterSecond = afterFirst
                        && activityCommandCount reserve afterSecond = 1
                    | _ -> false

                let! completion =
                    S2Substrate.commitText
                        StepRecordCodec.encode
                        [ Incoming(HistoryEvent(ActivityCompleted(OpId 0, "reserved"))) ]
                        owner

                let completionCommitted =
                    match completion with
                    | Committed _ -> true
                    | _ -> false

                let! third = DurableHost.runOnce StepRecordCodec.encode StepRecordCodec.decode 102L owner program

                let! afterThirdRaw = S2Substrate.readLogText StepRecordCodec.decode owner
                let afterThird = forceDecoded afterThirdRaw

                let completedActivityAdvancesWorkflow =
                    completionCommitted
                    && match third with
                       | DurableHostStatus.Committed _ ->
                           hasActivityCalled (OpId 1) charge afterThird
                           && activityCommandCount charge afterThird = 1
                       | _ -> false

                let! staleOwner = S2Substrate.claimWith (FenceToken "host-loop/stale") stalePair
                let! _freshOwner = S2Substrate.claimWith (FenceToken "host-loop/fresh") stalePair

                let! stale =
                    DurableHost.runOnce
                        StepRecordCodec.encode
                        StepRecordCodec.decode
                        200L
                        staleOwner
                        (Workflow.call "ship" "order-1")

                let staleFenceReturnsDeposed =
                    match stale with
                    | DurableHostStatus.Deposed expected -> expected = "host-loop/fresh"
                    | _ -> false

                do! basin |> S2.deleteStream (StorageKey.inboxStreamName staleKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName staleKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
                do! basin |> S2.deleteStream (StorageKey.logStreamName key)

                let result =
                    { FirstActivityCommitsOnce = firstActivityCommitsOnce
                      SecondRunWaitingNotDuplicateCommit = secondRunWaitingNotDuplicateCommit
                      CompletedActivityAdvancesWorkflow = completedActivityAdvancesWorkflow
                      StaleFenceReturnsDeposed = staleFenceReturnsDeposed }

                do!
                    ctx.EmitSpan
                        "proof.durable_host.completed"
                        [ "proof.property", "durable-host"
                          "host.first_commit_once", string result.FirstActivityCommitsOnce
                          "host.waiting_no_duplicate", string result.SecondRunWaitingNotDuplicateCommit
                          "host.completed_advances", string result.CompletedActivityAdvancesWorkflow
                          "host.deposed", string result.StaleFenceReturnsDeposed ]

                return result
            })

    let hostProperty =
        property "durable-host" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "first activity step commits exactly once" (fun result ->
                      result.FirstActivityCommitsOnce)
                  v.Expect.Workload "second run returns Waiting without duplicate commit" (fun result ->
                      result.SecondRunWaitingNotDuplicateCommit)
                  v.Expect.Workload "completed activity advances workflow to next need" (fun result ->
                      result.CompletedActivityAdvancesWorkflow)
                  v.Expect.Workload "stale fence returns Deposed cleanly" (fun result ->
                      result.StaleFenceReturnsDeposed)
                  v.Trace.SpanExists
                      "durable host proof span emitted"
                      "proof.durable_host.completed"
                      [ "proof.property", "durable-host" ]
                  v.Trace.Operation
                      "durable host operation was recorded"
                      ({ TraceOperationMatch.named "durable.host" with
                          Status = Some "ok"
                          OutputContains =
                              [ "FirstActivityCommitsOnce"
                                "SecondRunWaitingNotDuplicateCommit"
                                "StaleFenceReturnsDeposed" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-host" {
            describedAs
                "A single DurableHost run reads the durable log, plans one step, and commits only under the owner fence."

            property hostProperty
        }
