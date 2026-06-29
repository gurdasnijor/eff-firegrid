namespace Eff.Foundation.Durable

open System
open Eff

[<RequireQualifiedAccess>]
module InboxEnvelopeCodec =
    let private opText (OpId value) = string value

    let private parseOp (text: string) =
        match Int32.TryParse text with
        | true, value -> Ok(OpId value)
        | false, _ -> Error("bad op id: " + text)

    let private parseInt64 name (text: string) =
        match Int64.TryParse text with
        | true, value -> Ok value
        | false, _ -> Error("bad " + name + ": " + text)

    let private field (value: string) = string value.Length + ":" + value

    let private fields values =
        values |> List.map field |> String.concat ""

    let private readField (text: string) (index: int) =
        let colon = text.IndexOf(':', index)

        if colon < 0 then
            Error "missing field length separator"
        else
            let lengthText = text.Substring(index, colon - index)

            match Int32.TryParse lengthText with
            | false, _ -> Error("bad field length: " + lengthText)
            | true, length ->
                if length < 0 then
                    Error("negative field length: " + lengthText)
                else
                    let start = colon + 1
                    let finish = start + length

                    if finish > text.Length then
                        Error "field length exceeds record body"
                    else
                        Ok(text.Substring(start, length), finish)

    let private readFields count text index =
        let rec loop remaining next acc =
            if remaining = 0 then
                if next = String.length text then
                    Ok(List.rev acc)
                else
                    Error "trailing envelope data"
            else
                match readField text next with
                | Ok(value, finish) -> loop (remaining - 1) finish (value :: acc)
                | Error error -> Error error

        loop count index []

    let private workflowNameText (WorkflowName name) = name

    let private messageFields =
        function
        | StartWorkflow(name, input) -> [ "start"; workflowNameText name; input ]
        | RaiseSignal(name, payload) -> [ "signal"; name; payload ]
        | CompleteActivity(opId, value) -> [ "activity-completed"; opText opId; value ]

    let encode envelope =
        fields (
            envelope.Source
            :: string envelope.SourceSeqNum
            :: messageFields envelope.Message
        )

    let private decodeMessage =
        function
        | "start" :: workflowName :: input :: [] -> Ok(StartWorkflow(WorkflowName workflowName, input))
        | "signal" :: name :: payload :: [] -> Ok(RaiseSignal(name, payload))
        | "activity-completed" :: opId :: value :: [] ->
            parseOp opId |> Result.map (fun id -> CompleteActivity(id, value))
        | tag :: _ -> Error("unknown inbox message tag: " + tag)
        | [] -> Error "missing inbox message tag"

    let decode body =
        readFields 5 body 0
        |> Result.bind (function
            | source :: sourceSeqNum :: messageFields ->
                parseInt64 "source seq num" sourceSeqNum
                |> Result.bind (fun seqNum ->
                    if seqNum < 0L then
                        Error("bad source seq num: " + sourceSeqNum)
                    else
                        decodeMessage messageFields
                        |> Result.map (fun message ->
                            { Source = source
                              SourceSeqNum = seqNum
                              Message = message }))
            | _ -> Error "bad inbox envelope field count")

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
