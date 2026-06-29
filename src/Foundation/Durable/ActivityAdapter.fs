namespace Eff.Foundation.Durable

open Eff

type ActivityCommandCompletion =
    { SourceSeqNum: int64
      OpId: OpId
      Activity: Activity
      Value: Payload }

type ActivityCommandAdapterReport =
    { Batch: DispatchBatch
      Completed: ActivityCommandCompletion list
      AlreadyCompleted: int
      Ignored: int
      Checkpoint: CommandDispatchCheckpointResult }

[<RequireQualifiedAccess>]
type ActivityCommandAdapterFailure =
    | LogReadFailed of string
    | DecodeFailed of seqNum: int64 * error: string
    | MissingHandler of ActivityName
    | HandlerFailed of ActivityName * error: string
    | CompletionCommitFailed of S2Errors.S2Failure
    | CheckpointFailed of CommandDispatchFailure

[<RequireQualifiedAccess>]
type ActivityCommandAdapterStatus =
    | Processed of ActivityCommandAdapterReport
    | Deposed of expectedFence: string
    | Failed of ActivityCommandAdapterFailure

[<RequireQualifiedAccess>]
module ActivityCommandAdapter =
    let dispatcher = "activity"

    let private decodeLog decoded =
        let rec loop records =
            function
            | [] -> Ok(List.rev records)
            | (seqNum, Ok record) :: rest -> loop ((seqNum, record) :: records) rest
            | (seqNum, Error error) :: _ -> Error(ActivityCommandAdapterFailure.DecodeFailed(seqNum, error))

        loop [] decoded

    let private readLog decode owned =
        async {
            try
                let! decoded = S2Substrate.readLogText decode owned
                return decodeLog decoded
            with error ->
                return Error(ActivityCommandAdapterFailure.LogReadFailed error.Message)
        }

    let private invokeHandler activity handler =
        async {
            try
                let! result = handler activity.Input
                return Ok result
            with error ->
                return Error(ActivityCommandAdapterFailure.HandlerFailed(ActivityName activity.Name, error.Message))
        }

    let private commitCompletion encode owned opId value =
        let event = ActivityCompleted(opId, value)

        async {
            let! result = S2Substrate.commitText encode [ Incoming(HistoryEvent event) ] owned

            return
                match result with
                | Committed ack -> Ok(ack, event)
                | Deposed expected -> Error(ActivityCommandAdapterStatus.Deposed expected)
                | CommitFailed failure ->
                    Error(
                        ActivityCommandAdapterStatus.Failed(
                            ActivityCommandAdapterFailure.CompletionCommitFailed failure
                        )
                    )
        }

    let runOnce encode decode maxRecords registry owned =
        async {
            let! log = readLog decode owned

            match log with
            | Error failure -> return ActivityCommandAdapterStatus.Failed failure
            | Ok decoded ->
                let batch = DurableCommandDispatch.selectFromDecoded dispatcher maxRecords decoded
                let history = decoded |> List.map snd |> DurableStepper.historyFromRecords
                let commands = DispatchBatch.commands batch

                let rec loop history completed alreadyCompleted ignored remaining =
                    async {
                        match remaining with
                        | [] ->
                            let! checkpoint = DurableCommandDispatch.checkpoint encode dispatcher owned batch

                            return
                                match checkpoint with
                                | CommandDispatchCheckpointResult.Deposed expected ->
                                    ActivityCommandAdapterStatus.Deposed expected
                                | CommandDispatchCheckpointResult.Failed failure ->
                                    ActivityCommandAdapterStatus.Failed(
                                        ActivityCommandAdapterFailure.CheckpointFailed failure
                                    )
                                | CommandDispatchCheckpointResult.Checkpointed _
                                | CommandDispatchCheckpointResult.NotRequired ->
                                    ActivityCommandAdapterStatus.Processed
                                        { Batch = batch
                                          Completed = List.rev completed
                                          AlreadyCompleted = alreadyCompleted
                                          Ignored = ignored
                                          Checkpoint = checkpoint }
                        | command :: rest ->
                            match command.Command with
                            | CallActivity(opId, activity) ->
                                match History.completed opId history with
                                | Some _ -> return! loop history completed (alreadyCompleted + 1) ignored rest
                                | None ->
                                    match ActivityRegistry.require activity.Name registry with
                                    | Error(DurableRegistryError.ActivityNotFound name) ->
                                        return
                                            ActivityCommandAdapterStatus.Failed(
                                                ActivityCommandAdapterFailure.MissingHandler name
                                            )
                                    | Error error ->
                                        return
                                            ActivityCommandAdapterStatus.Failed(
                                                ActivityCommandAdapterFailure.HandlerFailed(
                                                    ActivityName activity.Name,
                                                    string error
                                                )
                                            )
                                    | Ok handler ->
                                        let! handled = invokeHandler activity handler

                                        match handled with
                                        | Error failure -> return ActivityCommandAdapterStatus.Failed failure
                                        | Ok value ->
                                            let! committed = commitCompletion encode owned opId value

                                            match committed with
                                            | Error status -> return status
                                            | Ok(_, event) ->
                                                let completion =
                                                    { SourceSeqNum = command.SourceSeqNum
                                                      OpId = opId
                                                      Activity = activity
                                                      Value = value }

                                                return!
                                                    loop
                                                        (History.append event history)
                                                        (completion :: completed)
                                                        alreadyCompleted
                                                        ignored
                                                        rest
                            | ScheduleTimer _
                            | CancelTimer _
                            | WriteLog _ -> return! loop history completed alreadyCompleted (ignored + 1) rest
                    }

                return! loop history [] 0 0 commands
        }
