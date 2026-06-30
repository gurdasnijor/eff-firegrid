namespace Eff.Foundation.Durable

open Eff

type InboxFoldedMessage =
    { InboxSeqNum: int64
      Envelope: InboxEnvelope
      Events: Event list }

type InboxFoldReport =
    { FromSeqNum: int64
      NextSeqNum: int64
      Scanned: int
      Accepted: InboxFoldedMessage list
      Duplicates: int
      Commit: CommitResult option }

[<RequireQualifiedAccess>]
type InboxFoldFailure =
    | LogReadFailed of string
    | LogDecodeFailed of seqNum: int64 * error: string
    | InboxReadFailed of string
    | InboxDecodeFailed of seqNum: int64 * error: string
    | CommitFailed of S2Errors.S2Failure

[<RequireQualifiedAccess>]
type InboxFoldStatus =
    | Folded of InboxFoldReport
    | Deposed of expectedFence: string
    | Failed of InboxFoldFailure

[<RequireQualifiedAccess>]
module InboxFold =
    let private decodeLog decoded =
        let rec loop records =
            function
            | [] -> Ok(List.rev records)
            | (seqNum, Ok record) :: rest -> loop ((seqNum, record) :: records) rest
            | (seqNum, Error error) :: _ -> Error(InboxFoldFailure.LogDecodeFailed(seqNum, error))

        loop [] decoded

    let private readLog decode owned =
        async {
            try
                let! decoded = S2Substrate.readLogText decode owned
                return decodeLog decoded
            with error ->
                return Error(InboxFoldFailure.LogReadFailed error.Message)
        }

    let private cursor decoded =
        decoded
        |> List.choose (function
            | _, Incoming(InboxCheckpoint nextSeqNum) -> Some nextSeqNum
            | _ -> None)
        |> List.fold max 0L

    let private highwater decoded =
        decoded
        |> List.choose (function
            | _, Incoming(InboxSourceHighwater(source, nextSeqNum)) -> Some(source, nextSeqNum)
            | _ -> None)
        |> List.fold
            (fun state (source, nextSeqNum) ->
                let current = state |> Map.tryFind source |> Option.defaultValue 0L
                state |> Map.add source (max current nextSeqNum))
            Map.empty

    let private readInbox (decode: string -> Result<InboxEnvelope, string>) from count owned =
        async {
            try
                let! readRecords = S2Substrate.readInbox from count owned
                let records: S2.ReadRecord list = readRecords

                let rec loop (acc: (int64 * InboxEnvelope) list) (remaining: S2.ReadRecord list) =
                    match remaining with
                    | [] -> Ok(List.rev acc)
                    | (record: S2.ReadRecord) :: rest ->
                        match decode record.Body with
                        | Ok envelope -> loop ((record.SeqNum, envelope) :: acc) rest
                        | Error error -> Error(InboxFoldFailure.InboxDecodeFailed(record.SeqNum, error))

                return loop [] records
            with error ->
                match S2Errors.classify error with
                | S2Errors.RangeNotSatisfiable _ -> return Ok []
                | _ -> return Error(InboxFoldFailure.InboxReadFailed error.Message)
        }

    let private eventsFor =
        function
        | CompleteActivity(opId, value) -> [ ActivityCompleted(opId, value) ]
        | FireTimer(opId, _) -> [ TimerFired opId ]
        | StartWorkflow _
        | RaiseSignal _ -> []

    let private recordsFor inboxSeqNum envelope =
        let events = eventsFor envelope.Message

        let eventRecords = events |> List.map (HistoryEvent >> Incoming)

        let accepted =
            { InboxSeqNum = inboxSeqNum
              Envelope = envelope
              Events = events }

        accepted,
        [ yield Incoming(InboxMessageAccepted envelope)
          yield! eventRecords
          yield Incoming(InboxSourceHighwater(envelope.Source, envelope.SourceSeqNum + 1L)) ]

    let private selectFresh highwater inboxRecords =
        let rec loop state accepted duplicates records =
            function
            | [] -> accepted, duplicates, records
            | (inboxSeqNum, envelope) :: rest ->
                let nextSeen = state |> Map.tryFind envelope.Source |> Option.defaultValue 0L

                if envelope.SourceSeqNum < nextSeen then
                    loop state accepted (duplicates + 1) records rest
                else
                    let acceptedMessage, committedRecords = recordsFor inboxSeqNum envelope

                    loop
                        (state |> Map.add envelope.Source (envelope.SourceSeqNum + 1L))
                        (accepted @ [ acceptedMessage ])
                        duplicates
                        (records @ committedRecords)
                        rest

        loop highwater [] 0 [] inboxRecords

    let runOnce encode decodeLogEntry decodeInboxEnvelope maxRecords owned =
        async {
            let! log = readLog decodeLogEntry owned

            match log with
            | Error failure -> return InboxFoldStatus.Failed failure
            | Ok decoded ->
                let fromSeqNum = cursor decoded
                let! inbox = readInbox decodeInboxEnvelope fromSeqNum maxRecords owned

                match inbox with
                | Error failure -> return InboxFoldStatus.Failed failure
                | Ok inboxRecords ->
                    let nextSeqNum =
                        match List.rev inboxRecords with
                        | (seqNum, _) :: _ -> seqNum + 1L
                        | [] -> fromSeqNum

                    if nextSeqNum = fromSeqNum then
                        return
                            InboxFoldStatus.Folded
                                { FromSeqNum = fromSeqNum
                                  NextSeqNum = nextSeqNum
                                  Scanned = 0
                                  Accepted = []
                                  Duplicates = 0
                                  Commit = None }
                    else
                        let accepted, duplicates, records = selectFresh (highwater decoded) inboxRecords

                        let records = records @ [ Incoming(InboxCheckpoint nextSeqNum) ]
                        let! commit = S2Substrate.commitText encode records owned

                        return
                            match commit with
                            | Committed _ ->
                                InboxFoldStatus.Folded
                                    { FromSeqNum = fromSeqNum
                                      NextSeqNum = nextSeqNum
                                      Scanned = List.length inboxRecords
                                      Accepted = accepted
                                      Duplicates = duplicates
                                      Commit = Some commit }
                            | Deposed expected -> InboxFoldStatus.Deposed expected
                            | CommitFailed failure -> InboxFoldStatus.Failed(InboxFoldFailure.CommitFailed failure)
        }
