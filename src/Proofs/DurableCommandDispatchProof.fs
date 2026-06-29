namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable

module DurableCommandDispatchProof =
    type DurableCommandDispatchResult =
        { PureSelectionFindsCommandsOnly: bool
          PureSelectionHonorsBatchLimit: bool
          PureSelectionFailsClosedOnDecodeError: bool
          CodecRoundTripsCheckpoint: bool
          S2CheckpointSuppressesDuplicateDispatch: bool
          S2StaleOwnerCannotCheckpoint: bool }

    let private activity = Activities.create "reserve" "order-1"

    let private callCommand = CallActivity(OpId 0, activity)

    let private logCommand = WriteLog(OpId 1, "reserved")

    let private entries =
        [ 1L, Incoming(HistoryEvent(ActivityCalled(OpId 0, activity)))
          2L, Outgoing(Command callCommand)
          3L, Incoming(HistoryEvent(LogEmitted(OpId 1, "reserved")))
          4L, Outgoing(Command logCommand) ]

    let private checkPureSelectionFindsCommandsOnly () =
        let batch = DurableCommandDispatch.selectFromDecoded 10 entries

        DispatchBatch.fromSeqNum batch = 0L
        && DispatchBatch.nextSeqNum batch = 5L
        && DispatchBatch.scanned batch = 4
        && DispatchBatch.commands batch = [ { SourceSeqNum = 2L
                                              Command = callCommand }
                                            { SourceSeqNum = 4L
                                              Command = logCommand } ]

    let private checkPureSelectionHonorsBatchLimit () =
        let batch = DurableCommandDispatch.selectFromDecoded 2 entries

        DispatchBatch.fromSeqNum batch = 0L
        && DispatchBatch.nextSeqNum batch = 3L
        && DispatchBatch.scanned batch = 2
        && DispatchBatch.commands batch = [ { SourceSeqNum = 2L
                                              Command = callCommand } ]

    let private checkPureSelectionFailsClosedOnDecodeError () =
        let decoded =
            [ 1L, Ok(Incoming(HistoryEvent(ActivityCalled(OpId 0, activity))))
              2L, Error "bad command body"
              3L, Ok(Outgoing(Command logCommand)) ]

        match DurableCommandDispatch.trySelect 10 decoded with
        | Error(CommandDispatchFailure.DecodeFailed(2L, "bad command body")) -> true
        | _ -> false

    let private checkCodecRoundTripsCheckpoint () =
        let checkpoint = Incoming(CommandDispatchCheckpoint 42L)

        StepRecordCodec.decode (StepRecordCodec.encode checkpoint) = Ok checkpoint
        && StepRecordCodec.decode "in|dispatch.checkpoint|2:-1" |> Result.isError

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.command_dispatch"
            "durable-command-dispatch"
            { ProofOperationOptions.empty with
                Key = Some "durable-command-dispatch" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-command-dispatch-" + suffix
                let key = StorageKey("workflow-" + suffix)
                let staleKey = StorageKey("stale-" + suffix)

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                do! S2Substrate.ensureStreams basin key
                do! S2Substrate.ensureStreams basin staleKey

                let pair = S2Substrate.streams basin key
                let stalePair = S2Substrate.streams basin staleKey

                let! owner = S2Substrate.claimWith (FenceToken "command-dispatch/fence-1") pair

                let! seed = S2Substrate.commitText StepRecordCodec.encode (entries |> List.map snd) owner

                let! pending = DurableCommandDispatch.readPending StepRecordCodec.decode 10 owner

                let! checkpoint =
                    match pending with
                    | Ok batch -> DurableCommandDispatch.checkpoint StepRecordCodec.encode owner batch
                    | Error _ -> async.Return CommandDispatchCheckpointResult.NotRequired

                let! afterCheckpoint = DurableCommandDispatch.readPending StepRecordCodec.decode 10 owner

                let s2CheckpointSuppressesDuplicateDispatch =
                    match seed, pending, checkpoint, afterCheckpoint with
                    | Committed seedAck, Ok before, CommandDispatchCheckpointResult.Checkpointed checkpointAck, Ok after ->
                        seedAck.Start.SeqNum = 1L
                        && seedAck.End.SeqNum = 5L
                        && DispatchBatch.commands before = [ { SourceSeqNum = 2L
                                                               Command = callCommand }
                                                             { SourceSeqNum = 4L
                                                               Command = logCommand } ]
                        && DispatchBatch.nextSeqNum before = 5L
                        && checkpointAck.Start.SeqNum = 5L
                        && checkpointAck.End.SeqNum = 6L
                        && DispatchBatch.fromSeqNum after = 6L
                        && DispatchBatch.nextSeqNum after = 6L
                        && List.isEmpty (DispatchBatch.commands after)
                    | _ -> false

                let! staleOwner = S2Substrate.claimWith (FenceToken "command-dispatch/stale") stalePair
                let! _freshOwner = S2Substrate.claimWith (FenceToken "command-dispatch/fresh") stalePair

                let staleBatch =
                    DurableCommandDispatch.selectFromDecoded
                        10
                        [ 0L, Incoming(HistoryEvent(CurrentTimeRecorded(OpId 0, 100L))) ]

                let! staleCheckpoint = DurableCommandDispatch.checkpoint StepRecordCodec.encode staleOwner staleBatch

                let s2StaleOwnerCannotCheckpoint =
                    match staleCheckpoint with
                    | CommandDispatchCheckpointResult.Deposed expected -> expected = "command-dispatch/fresh"
                    | _ -> false

                do! basin |> S2.deleteStream (StorageKey.inboxStreamName staleKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName staleKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
                do! basin |> S2.deleteStream (StorageKey.logStreamName key)

                let result =
                    { PureSelectionFindsCommandsOnly = checkPureSelectionFindsCommandsOnly ()
                      PureSelectionHonorsBatchLimit = checkPureSelectionHonorsBatchLimit ()
                      PureSelectionFailsClosedOnDecodeError = checkPureSelectionFailsClosedOnDecodeError ()
                      CodecRoundTripsCheckpoint = checkCodecRoundTripsCheckpoint ()
                      S2CheckpointSuppressesDuplicateDispatch = s2CheckpointSuppressesDuplicateDispatch
                      S2StaleOwnerCannotCheckpoint = s2StaleOwnerCannotCheckpoint }

                do!
                    ctx.EmitSpan
                        "proof.durable_command_dispatch.completed"
                        [ "proof.property", "durable-command-dispatch"
                          "dispatch.selection", string result.PureSelectionFindsCommandsOnly
                          "dispatch.decode_fail_closed", string result.PureSelectionFailsClosedOnDecodeError
                          "dispatch.checkpoint", string result.S2CheckpointSuppressesDuplicateDispatch
                          "dispatch.deposed", string result.S2StaleOwnerCannotCheckpoint ]

                return result
            })

    let commandDispatchProperty =
        property "durable-command-dispatch" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "pure selection finds only outgoing commands" (fun result ->
                      result.PureSelectionFindsCommandsOnly)
                  v.Expect.Workload "pure selection honors the scan batch limit" (fun result ->
                      result.PureSelectionHonorsBatchLimit)
                  v.Expect.Workload "pure selection fails closed on decode errors" (fun result ->
                      result.PureSelectionFailsClosedOnDecodeError)
                  v.Expect.Workload "checkpoint records round-trip through the shared codec" (fun result ->
                      result.CodecRoundTripsCheckpoint)
                  v.Expect.Workload "S2 checkpoint suppresses duplicate dispatch" (fun result ->
                      result.S2CheckpointSuppressesDuplicateDispatch)
                  v.Expect.Workload "stale owner cannot checkpoint dispatch progress" (fun result ->
                      result.S2StaleOwnerCannotCheckpoint)
                  v.Trace.SpanExists
                      "durable command dispatch proof span emitted"
                      "proof.durable_command_dispatch.completed"
                      [ "proof.property", "durable-command-dispatch" ]
                  v.Trace.Operation
                      "durable command dispatch operation was recorded"
                      ({ TraceOperationMatch.named "durable.command_dispatch" with
                          Status = Some "ok"
                          OutputContains =
                              [ "PureSelectionFindsCommandsOnly"
                                "PureSelectionFailsClosedOnDecodeError"
                                "S2CheckpointSuppressesDuplicateDispatch" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-command-dispatch" {
            describedAs
                "Committed durable commands are selected from the log with a durable cursor and fenced checkpoint."

            property commandDispatchProperty
        }
