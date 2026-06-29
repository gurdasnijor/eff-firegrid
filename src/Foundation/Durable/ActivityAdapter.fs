namespace Eff.Foundation.Durable

open Eff

type ActivityCommandCompletion =
    { SourceSeqNum: int64
      InboxSeqNum: int64
      OpId: OpId
      Activity: Activity
      Value: Payload }

type ActivityCommandAdapterReport =
    { Batch: DispatchBatch
      Completed: ActivityCommandCompletion list
      AlreadyCompleted: int
      AlreadyPublished: int
      Ignored: int
      Checkpoint: CommandDispatchCheckpointResult }

[<RequireQualifiedAccess>]
type ActivityCommandAdapterFailure =
    | LogReadFailed of string
    | DecodeFailed of seqNum: int64 * error: string
    | InboxReadFailed of string
    | InboxDecodeFailed of seqNum: int64 * error: string
    | MissingHandler of ActivityName
    | HandlerFailed of ActivityName * error: string
    | CompletionPublishFailed of string
    | CheckpointFailed of CommandDispatchFailure

[<RequireQualifiedAccess>]
type ActivityCommandAdapterStatus =
    | Processed of ActivityCommandAdapterReport
    | Deposed of expectedFence: string
    | Failed of ActivityCommandAdapterFailure

[<RequireQualifiedAccess>]
module ActivityCommandAdapter =
    let dispatcher = "activity"

    let completionSource = "activity"

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

    let private readPublishedCompletions decode owned =
        async {
            let pageSize = 100

            let rec readPage from seen =
                async {
                    try
                        let! records = S2Substrate.readInbox from pageSize owned

                        let rec decodeRecords nextSeqNum state =
                            function
                            | [] -> Ok(nextSeqNum, state)
                            | (record: S2.ReadRecord) :: rest ->
                                match decode record.Body with
                                | Ok envelope ->
                                    let state =
                                        match envelope.Message with
                                        | CompleteActivity _ when envelope.Source = completionSource ->
                                            state |> Set.add envelope.SourceSeqNum
                                        | StartWorkflow _
                                        | RaiseSignal _
                                        | CompleteActivity _ -> state

                                    decodeRecords (record.SeqNum + 1L) state rest
                                | Error error ->
                                    Error(ActivityCommandAdapterFailure.InboxDecodeFailed(record.SeqNum, error))

                        match decodeRecords from seen records with
                        | Error failure -> return Error failure
                        | Ok(nextSeqNum, state) ->
                            if List.isEmpty records then
                                return Ok state
                            else
                                return! readPage nextSeqNum state
                    with error ->
                        match S2Errors.classify error with
                        | S2Errors.RangeNotSatisfiable _ -> return Ok seen
                        | _ -> return Error(ActivityCommandAdapterFailure.InboxReadFailed error.Message)
                }

            return! readPage 0L Set.empty
        }

    let private publishCompletion owned sourceSeqNum opId value =
        async {
            let envelope =
                { Source = completionSource
                  SourceSeqNum = sourceSeqNum
                  Message = CompleteActivity(opId, value) }

            try
                let! ack =
                    owned.Inbox
                    |> S2.append
                        [ S2.Record.textWith
                              [ "src", completionSource; "seq", string sourceSeqNum ]
                              (InboxEnvelopeCodec.encode envelope) ]

                return Ok(ack, envelope)
            with error ->
                return
                    Error(
                        ActivityCommandAdapterStatus.Failed(
                            ActivityCommandAdapterFailure.CompletionPublishFailed error.Message
                        )
                    )
        }

    let runOnce encode decode maxRecords registry owned =
        async {
            let! log = readLog decode owned

            match log with
            | Error failure -> return ActivityCommandAdapterStatus.Failed failure
            | Ok decoded ->
                let! published = readPublishedCompletions InboxEnvelopeCodec.decode owned

                match published with
                | Error failure -> return ActivityCommandAdapterStatus.Failed failure
                | Ok publishedSourceSeqNums ->
                    let batch = DurableCommandDispatch.selectFromDecoded dispatcher maxRecords decoded
                    let history = decoded |> List.map snd |> DurableStepper.historyFromRecords
                    let commands = DispatchBatch.commands batch

                    let rec loop published completed alreadyCompleted alreadyPublished ignored remaining =
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
                                              AlreadyPublished = alreadyPublished
                                              Ignored = ignored
                                              Checkpoint = checkpoint }
                            | command :: rest ->
                                match command.Command with
                                | CallActivity(opId, activity) ->
                                    if History.completed opId history |> Option.isSome then
                                        return!
                                            loop
                                                published
                                                completed
                                                (alreadyCompleted + 1)
                                                alreadyPublished
                                                ignored
                                                rest
                                    elif published |> Set.contains command.SourceSeqNum then
                                        return!
                                            loop
                                                published
                                                completed
                                                alreadyCompleted
                                                (alreadyPublished + 1)
                                                ignored
                                                rest
                                    else
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
                                                let! publishedCompletion =
                                                    publishCompletion owned command.SourceSeqNum opId value

                                                match publishedCompletion with
                                                | Error status -> return status
                                                | Ok(ack, _) ->
                                                    let completion =
                                                        { SourceSeqNum = command.SourceSeqNum
                                                          InboxSeqNum = ack.Start.SeqNum
                                                          OpId = opId
                                                          Activity = activity
                                                          Value = value }

                                                    return!
                                                        loop
                                                            (published |> Set.add command.SourceSeqNum)
                                                            (completion :: completed)
                                                            alreadyCompleted
                                                            alreadyPublished
                                                            ignored
                                                            rest
                                | ScheduleTimer _
                                | CancelTimer _
                                | WriteLog _ ->
                                    return!
                                        loop published completed alreadyCompleted alreadyPublished (ignored + 1) rest
                        }

                    return! loop publishedSourceSeqNums [] 0 0 0 commands
        }
