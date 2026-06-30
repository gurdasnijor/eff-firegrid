namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable

module DurableInboxFoldProof =
    type DurableInboxFoldResult =
        { ActivityCompletionFoldedIntoLog: bool
          CursorReconstructedAfterRestart: bool
          DuplicateSourceMessageSkipped: bool
          MalformedInboxFailsClosed: bool
          StaleOwnerCannotCommitInboxProgress: bool
          InboxEnvelopeCodecRoundTrips: bool }

    let private forceDecoded entries =
        entries
        |> List.map (fun (seqNum, decoded) ->
            match decoded with
            | Ok entry -> seqNum, entry
            | Error error -> failwith error)

    let private envelope source sourceSeqNum message =
        { Source = source
          SourceSeqNum = sourceSeqNum
          Message = message }

    let private appendInbox pair envelope =
        S2Substrate.appendInboxText [] (InboxEnvelopeCodec.encode envelope) pair

    let private hasAccepted expected entries =
        entries
        |> List.exists (function
            | _, Incoming(InboxMessageAccepted actual) -> actual = expected
            | _ -> false)

    let private hasHighwater source nextSeqNum entries =
        entries
        |> List.exists (function
            | _, Incoming(InboxSourceHighwater(actualSource, actualNext)) ->
                actualSource = source && actualNext = nextSeqNum
            | _ -> false)

    let private hasInboxCheckpoint nextSeqNum entries =
        entries
        |> List.exists (function
            | _, Incoming(InboxCheckpoint actualNext) -> actualNext = nextSeqNum
            | _ -> false)

    let private activityCompletedCount entries =
        entries
        |> List.filter (function
            | _, Incoming(HistoryEvent(ActivityCompleted(OpId 0, "reserved"))) -> true
            | _ -> false)
        |> List.length

    let private checkInboxEnvelopeCodecRoundTrips () =
        [ envelope "activity-worker" 7L (CompleteActivity(OpId 0, "reserved"))
          envelope "timer" 9L (FireTimer(OpId 1, 500L))
          envelope "client" 2L (RaiseSignal("approved", "yes"))
          envelope "starter" 0L (StartWorkflow(WorkflowName "checkout", "order-1")) ]
        |> List.forall (fun value -> InboxEnvelopeCodec.decode (InboxEnvelopeCodec.encode value) = Ok value)
        && InboxEnvelopeCodec.decode "3:bad2:-1" |> Result.isError

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.inbox_fold"
            "durable-inbox-fold"
            { ProofOperationOptions.empty with
                Key = Some "durable-inbox-fold" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-inbox-fold-" + suffix
                let key = StorageKey("workflow-" + suffix)
                let badKey = StorageKey("bad-" + suffix)
                let staleKey = StorageKey("stale-" + suffix)

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                do! S2Substrate.ensureStreams basin key
                do! S2Substrate.ensureStreams basin badKey
                do! S2Substrate.ensureStreams basin staleKey

                let pair = S2Substrate.streams basin key
                let badPair = S2Substrate.streams basin badKey
                let stalePair = S2Substrate.streams basin staleKey

                let completion =
                    envelope "activity-worker" 0L (CompleteActivity(OpId 0, "reserved"))

                let! _inboxAck = appendInbox pair completion
                let! owner = S2Substrate.claimWith (FenceToken "inbox-fold/fence-1") pair

                let! first =
                    InboxFold.runOnce StepRecordCodec.encode StepRecordCodec.decode InboxEnvelopeCodec.decode 10 owner

                let! afterFirstRaw = S2Substrate.readLogText StepRecordCodec.decode owner
                let afterFirst = forceDecoded afterFirstRaw

                let activityCompletionFoldedIntoLog =
                    match first with
                    | InboxFoldStatus.Folded report ->
                        report.FromSeqNum = 0L
                        && report.NextSeqNum = 1L
                        && report.Scanned = 1
                        && report.Duplicates = 0
                        && (report.Accepted |> List.map (fun folded -> folded.Envelope)) = [ completion ]
                        && hasAccepted completion afterFirst
                        && hasHighwater "activity-worker" 1L afterFirst
                        && hasInboxCheckpoint 1L afterFirst
                        && activityCompletedCount afterFirst = 1
                    | _ -> false

                let! restartedOwner = S2Substrate.claimWith (FenceToken "inbox-fold/fence-2") pair

                let! afterRestart =
                    InboxFold.runOnce
                        StepRecordCodec.encode
                        StepRecordCodec.decode
                        InboxEnvelopeCodec.decode
                        10
                        restartedOwner

                let cursorReconstructedAfterRestart =
                    match afterRestart with
                    | InboxFoldStatus.Folded report ->
                        report.FromSeqNum = 1L
                        && report.NextSeqNum = 1L
                        && report.Scanned = 0
                        && List.isEmpty report.Accepted
                        && report.Commit.IsNone
                    | _ -> false

                let duplicate = envelope "activity-worker" 0L (CompleteActivity(OpId 0, "reserved"))
                let next = envelope "activity-worker" 1L (CompleteActivity(OpId 1, "charged"))

                let! _dupAck = appendInbox pair duplicate
                let! _nextAck = appendInbox pair next

                let! afterDuplicate =
                    InboxFold.runOnce
                        StepRecordCodec.encode
                        StepRecordCodec.decode
                        InboxEnvelopeCodec.decode
                        10
                        restartedOwner

                let! afterDuplicateRaw = S2Substrate.readLogText StepRecordCodec.decode restartedOwner
                let afterDuplicateLog = forceDecoded afterDuplicateRaw

                let duplicateSourceMessageSkipped =
                    match afterDuplicate with
                    | InboxFoldStatus.Folded report ->
                        report.FromSeqNum = 1L
                        && report.NextSeqNum = 3L
                        && report.Scanned = 2
                        && report.Duplicates = 1
                        && (report.Accepted |> List.map (fun folded -> folded.Envelope)) = [ next ]
                        && hasHighwater "activity-worker" 2L afterDuplicateLog
                        && hasInboxCheckpoint 3L afterDuplicateLog
                    | _ -> false

                let! _badAck = S2Substrate.appendInboxText [] "not-an-envelope" badPair
                let! badOwner = S2Substrate.claimWith (FenceToken "inbox-fold/bad") badPair

                let! bad =
                    InboxFold.runOnce
                        StepRecordCodec.encode
                        StepRecordCodec.decode
                        InboxEnvelopeCodec.decode
                        10
                        badOwner

                let! afterBadRaw = S2Substrate.readLogText StepRecordCodec.decode badOwner
                let afterBad = forceDecoded afterBadRaw

                let malformedInboxFailsClosed =
                    match bad with
                    | InboxFoldStatus.Failed(InboxFoldFailure.InboxDecodeFailed(0L, _)) -> List.isEmpty afterBad
                    | _ -> false

                let staleEnvelope =
                    envelope "activity-worker" 0L (CompleteActivity(OpId 0, "reserved"))

                let! _staleInboxAck = appendInbox stalePair staleEnvelope
                let! staleOwner = S2Substrate.claimWith (FenceToken "inbox-fold/stale") stalePair
                let! _freshOwner = S2Substrate.claimWith (FenceToken "inbox-fold/fresh") stalePair

                let! stale =
                    InboxFold.runOnce
                        StepRecordCodec.encode
                        StepRecordCodec.decode
                        InboxEnvelopeCodec.decode
                        10
                        staleOwner

                let staleOwnerCannotCommitInboxProgress =
                    match stale with
                    | InboxFoldStatus.Deposed expected -> expected = "inbox-fold/fresh"
                    | _ -> false

                do! basin |> S2.deleteStream (StorageKey.inboxStreamName staleKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName staleKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName badKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName badKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
                do! basin |> S2.deleteStream (StorageKey.logStreamName key)

                let result =
                    { ActivityCompletionFoldedIntoLog = activityCompletionFoldedIntoLog
                      CursorReconstructedAfterRestart = cursorReconstructedAfterRestart
                      DuplicateSourceMessageSkipped = duplicateSourceMessageSkipped
                      MalformedInboxFailsClosed = malformedInboxFailsClosed
                      StaleOwnerCannotCommitInboxProgress = staleOwnerCannotCommitInboxProgress
                      InboxEnvelopeCodecRoundTrips = checkInboxEnvelopeCodecRoundTrips () }

                do!
                    ctx.EmitSpan
                        "proof.durable_inbox_fold.completed"
                        [ "proof.property", "durable-inbox-fold"
                          "inbox.activity_completion", string result.ActivityCompletionFoldedIntoLog
                          "inbox.cursor_restart", string result.CursorReconstructedAfterRestart
                          "inbox.dedup", string result.DuplicateSourceMessageSkipped
                          "inbox.stale", string result.StaleOwnerCannotCommitInboxProgress ]

                return result
            })

    let inboxFoldProperty =
        property "durable-inbox-fold" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "activity completion is folded into the log" (fun result ->
                      result.ActivityCompletionFoldedIntoLog)
                  v.Expect.Workload "inbox cursor is reconstructed after restart" (fun result ->
                      result.CursorReconstructedAfterRestart)
                  v.Expect.Workload "duplicate source message is skipped" (fun result ->
                      result.DuplicateSourceMessageSkipped)
                  v.Expect.Workload "malformed inbox message fails closed" (fun result ->
                      result.MalformedInboxFailsClosed)
                  v.Expect.Workload "stale owner cannot commit inbox progress" (fun result ->
                      result.StaleOwnerCannotCommitInboxProgress)
                  v.Expect.Workload "inbox envelope codec round-trips" (fun result ->
                      result.InboxEnvelopeCodecRoundTrips)
                  v.Trace.SpanExists
                      "durable inbox fold proof span emitted"
                      "proof.durable_inbox_fold.completed"
                      [ "proof.property", "durable-inbox-fold" ]
                  v.Trace.Operation
                      "durable inbox fold operation was recorded"
                      ({ TraceOperationMatch.named "durable.inbox_fold" with
                          Status = Some "ok"
                          OutputContains =
                              [ "ActivityCompletionFoldedIntoLog"
                                "CursorReconstructedAfterRestart"
                                "DuplicateSourceMessageSkipped" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-inbox-fold" {
            describedAs "Inbox arrivals are admitted to the fenced log with replayable cursor and source dedup."
            property inboxFoldProperty
        }
