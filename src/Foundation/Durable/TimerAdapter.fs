namespace Eff.Foundation.Durable

open Eff

type TimerPublication =
    { SourceSeqNum: int64
      InboxSeqNum: int64
      OpId: OpId
      Deadline: int64 }

type PendingTimer =
    { SourceSeqNum: int64
      OpId: OpId
      Deadline: int64 }

type TimerCommandAdapterReport =
    { FromSeqNum: int64
      NextSeqNum: int64
      Scanned: int
      Published: TimerPublication list
      AlreadyPublished: int
      Canceled: int
      NotDue: PendingTimer option
      Ignored: int
      Checkpoint: CommandDispatchCheckpointResult }

[<RequireQualifiedAccess>]
type TimerCommandAdapterFailure =
    | LogReadFailed of string
    | DecodeFailed of seqNum: int64 * error: string
    | InboxReadFailed of string
    | InboxDecodeFailed of seqNum: int64 * error: string
    | PublishFailed of string
    | CheckpointFailed of CommandDispatchFailure

[<RequireQualifiedAccess>]
type TimerCommandAdapterStatus =
    | Processed of TimerCommandAdapterReport
    | Deposed of expectedFence: string
    | Failed of TimerCommandAdapterFailure

[<RequireQualifiedAccess>]
module TimerCommandAdapter =
    let dispatcher = "timer"

    let timerSource = "timer"

    let private decodeLog decoded =
        let rec loop records =
            function
            | [] -> Ok(List.rev records)
            | (seqNum, Ok record) :: rest -> loop ((seqNum, record) :: records) rest
            | (seqNum, Error error) :: _ -> Error(TimerCommandAdapterFailure.DecodeFailed(seqNum, error))

        loop [] decoded

    let private readLog decode owned =
        async {
            try
                let! decoded = S2Substrate.readLogText decode owned
                return decodeLog decoded
            with error ->
                return Error(TimerCommandAdapterFailure.LogReadFailed error.Message)
        }

    let private dispatchCursor decoded =
        decoded
        |> List.choose (function
            | _, Incoming(CommandDispatchCheckpoint(checkpointDispatcher, nextSeqNum)) when
                checkpointDispatcher = dispatcher
                ->
                Some nextSeqNum
            | _ -> None)
        |> List.fold max 0L

    let private readPublishedTimers decode owned =
        async {
            let pageSize = 100

            let rec readPage from seen =
                async {
                    try
                        let! records = S2Substrate.readInbox from pageSize owned

                        let rec decodeRecords nextSeqNum (state: Set<int64>) =
                            function
                            | [] -> Ok(nextSeqNum, state)
                            | (record: S2.ReadRecord) :: rest ->
                                match decode record.Body with
                                | Ok envelope ->
                                    let state =
                                        match envelope.Message with
                                        | FireTimer _ when envelope.Source = timerSource ->
                                            state |> Set.add envelope.SourceSeqNum
                                        | StartWorkflow _
                                        | RaiseSignal _
                                        | CompleteActivity _
                                        | FireTimer _ -> state

                                    decodeRecords (record.SeqNum + 1L) state rest
                                | Error error ->
                                    Error(TimerCommandAdapterFailure.InboxDecodeFailed(record.SeqNum, error))

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
                        | _ -> return Error(TimerCommandAdapterFailure.InboxReadFailed error.Message)
                }

            return! readPage 0L Set.empty
        }

    let private canceledTimers decoded =
        decoded
        |> List.choose (function
            | _, Incoming(HistoryEvent(TimerCanceled opId)) -> Some opId
            | _, Outgoing(Command(CancelTimer opId)) -> Some opId
            | _ -> None)
        |> Set.ofList

    let private firedTimers decoded =
        decoded
        |> List.choose (function
            | _, Incoming(HistoryEvent(TimerFired opId)) -> Some opId
            | _ -> None)
        |> Set.ofList

    let private publishTimer owned sourceSeqNum opId deadline =
        async {
            let envelope =
                { Source = timerSource
                  SourceSeqNum = sourceSeqNum
                  Message = FireTimer(opId, deadline) }

            try
                let! ack =
                    owned.Inbox
                    |> S2.append
                        [ S2.Record.textWith
                              [ "src", timerSource; "seq", string sourceSeqNum; "deadline", string deadline ]
                              (InboxEnvelopeCodec.encode envelope) ]

                return
                    Ok
                        { SourceSeqNum = sourceSeqNum
                          InboxSeqNum = ack.Start.SeqNum
                          OpId = opId
                          Deadline = deadline }
            with error ->
                return Error(TimerCommandAdapterFailure.PublishFailed error.Message)
        }

    let private checkpoint nextSeqNum fromSeqNum owned =
        async {
            if nextSeqNum <= fromSeqNum then
                return CommandDispatchCheckpointResult.NotRequired
            else
                let! result =
                    S2Substrate.commitText
                        StepRecordCodec.encode
                        [ Incoming(CommandDispatchCheckpoint(dispatcher, nextSeqNum)) ]
                        owned

                return
                    match result with
                    | Committed ack -> CommandDispatchCheckpointResult.Checkpointed ack
                    | Deposed expected -> CommandDispatchCheckpointResult.Deposed expected
                    | CommitFailed failure ->
                        CommandDispatchCheckpointResult.Failed(CommandDispatchFailure.CheckpointCommitFailed failure)
        }

    let runOnce decode now maxRecords owned =
        async {
            if maxRecords <= 0 then
                invalidArg (nameof maxRecords) "maxRecords must be positive"

            let! log = readLog decode owned

            match log with
            | Error failure -> return TimerCommandAdapterStatus.Failed failure
            | Ok decoded ->
                let! published = readPublishedTimers InboxEnvelopeCodec.decode owned

                match published with
                | Error failure -> return TimerCommandAdapterStatus.Failed failure
                | Ok publishedSourceSeqNums ->
                    let fromSeqNum = dispatchCursor decoded
                    let canceled = canceledTimers decoded
                    let fired = firedTimers decoded

                    let records =
                        decoded
                        |> List.filter (fun (seqNum, _) -> seqNum >= fromSeqNum)
                        |> List.truncate maxRecords

                    let rec loop
                        nextSeqNum
                        scanned
                        publishedTimers
                        alreadyPublished
                        canceledCount
                        ignored
                        notDue
                        remaining
                        =
                        async {
                            match remaining with
                            | [] ->
                                let! checkpoint = checkpoint nextSeqNum fromSeqNum owned

                                return
                                    match checkpoint with
                                    | CommandDispatchCheckpointResult.Deposed expected ->
                                        TimerCommandAdapterStatus.Deposed expected
                                    | CommandDispatchCheckpointResult.Failed failure ->
                                        TimerCommandAdapterStatus.Failed(
                                            TimerCommandAdapterFailure.CheckpointFailed failure
                                        )
                                    | CommandDispatchCheckpointResult.Checkpointed _
                                    | CommandDispatchCheckpointResult.NotRequired ->
                                        TimerCommandAdapterStatus.Processed
                                            { FromSeqNum = fromSeqNum
                                              NextSeqNum = nextSeqNum
                                              Scanned = scanned
                                              Published = List.rev publishedTimers
                                              AlreadyPublished = alreadyPublished
                                              Canceled = canceledCount
                                              NotDue = notDue
                                              Ignored = ignored
                                              Checkpoint = checkpoint }
                            | (seqNum, record) :: rest ->
                                match record with
                                | Outgoing(Command(ScheduleTimer(opId, deadline))) ->
                                    if fired |> Set.contains opId then
                                        return!
                                            loop
                                                (seqNum + 1L)
                                                (scanned + 1)
                                                publishedTimers
                                                (alreadyPublished + 1)
                                                canceledCount
                                                ignored
                                                notDue
                                                rest
                                    elif publishedSourceSeqNums |> Set.contains seqNum then
                                        return!
                                            loop
                                                (seqNum + 1L)
                                                (scanned + 1)
                                                publishedTimers
                                                (alreadyPublished + 1)
                                                canceledCount
                                                ignored
                                                notDue
                                                rest
                                    elif canceled |> Set.contains opId then
                                        return!
                                            loop
                                                (seqNum + 1L)
                                                (scanned + 1)
                                                publishedTimers
                                                alreadyPublished
                                                (canceledCount + 1)
                                                ignored
                                                notDue
                                                rest
                                    elif now < deadline then
                                        let pending =
                                            { SourceSeqNum = seqNum
                                              OpId = opId
                                              Deadline = deadline }

                                        return!
                                            loop
                                                nextSeqNum
                                                (scanned + 1)
                                                publishedTimers
                                                alreadyPublished
                                                canceledCount
                                                ignored
                                                (Some pending)
                                                []
                                    else
                                        let! publishedTimer = publishTimer owned seqNum opId deadline

                                        match publishedTimer with
                                        | Error failure -> return TimerCommandAdapterStatus.Failed failure
                                        | Ok publication ->
                                            return!
                                                loop
                                                    (seqNum + 1L)
                                                    (scanned + 1)
                                                    (publication :: publishedTimers)
                                                    alreadyPublished
                                                    canceledCount
                                                    ignored
                                                    notDue
                                                    rest
                                | Outgoing(Command(CancelTimer _))
                                | Outgoing(Command(CallActivity _))
                                | Outgoing(Command(WriteLog _)) ->
                                    return!
                                        loop
                                            (seqNum + 1L)
                                            (scanned + 1)
                                            publishedTimers
                                            alreadyPublished
                                            canceledCount
                                            (ignored + 1)
                                            notDue
                                            rest
                                | Outgoing _ ->
                                    return!
                                        loop
                                            (seqNum + 1L)
                                            (scanned + 1)
                                            publishedTimers
                                            alreadyPublished
                                            canceledCount
                                            ignored
                                            notDue
                                            rest
                                | Incoming _ ->
                                    return!
                                        loop
                                            (seqNum + 1L)
                                            (scanned + 1)
                                            publishedTimers
                                            alreadyPublished
                                            canceledCount
                                            ignored
                                            notDue
                                            rest
                        }

                    return! loop fromSeqNum 0 [] 0 0 0 None records
        }
