namespace Eff.Foundation.Durable

open System

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

[<RequireQualifiedAccess>]
module StepRecordCodec =
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
                    Error "trailing record data"
            else
                match readField text next with
                | Ok(value, finish) -> loop (remaining - 1) finish (value :: acc)
                | Error error -> Error error

        loop count index []

    let private prefixed prefix fields = prefix + "|" + fields

    let private eventPrefix =
        function
        | ActivityCalled(opId, activity) ->
            prefixed "event.activity-called" (fields [ opText opId; activity.Name; activity.Input ])
        | ActivityCompleted(opId, value) -> prefixed "event.activity-completed" (fields [ opText opId; value ])
        | CurrentTimeRecorded(opId, timestamp) ->
            prefixed "event.current-time" (fields [ opText opId; string timestamp ])
        | LogEmitted(opId, message) -> prefixed "event.log" (fields [ opText opId; message ])
        | TimerCreated(opId, deadline) -> prefixed "event.timer-created" (fields [ opText opId; string deadline ])
        | TimerFired opId -> prefixed "event.timer-fired" (fields [ opText opId ])
        | TimerCanceled opId -> prefixed "event.timer-canceled" (fields [ opText opId ])
        | SignalReceived(opId, name, payload) -> prefixed "event.signal" (fields [ opText opId; name; payload ])

    let private commandPrefix =
        function
        | CallActivity(opId, activity) ->
            prefixed "command.activity" (fields [ opText opId; activity.Name; activity.Input ])
        | ScheduleTimer(opId, deadline) -> prefixed "command.timer" (fields [ opText opId; string deadline ])
        | CancelTimer opId -> prefixed "command.cancel-timer" (fields [ opText opId ])
        | WriteLog(opId, message) -> prefixed "command.log" (fields [ opText opId; message ])

    let private workflowNameText (WorkflowName name) = name

    let private inboxMessageFields =
        function
        | StartWorkflow(name, input) -> "start" :: [ workflowNameText name; input ]
        | RaiseSignal(name, payload) -> "signal" :: [ name; payload ]
        | CompleteActivity(opId, value) -> "activity-completed" :: [ opText opId; value ]

    let private inboxEnvelopeFields envelope =
        envelope.Source
        :: string envelope.SourceSeqNum
        :: inboxMessageFields envelope.Message

    let private inboxEnvelopePrefix envelope =
        prefixed "inbox.accepted" (fields (inboxEnvelopeFields envelope))

    let private encodeStepRecord =
        function
        | HistoryEvent event -> eventPrefix event
        | Command command -> commandPrefix command
        | CommandDispatchCheckpoint(dispatcher, nextSeqNum) ->
            prefixed "dispatch.checkpoint" (fields [ dispatcher; string nextSeqNum ])
        | InboxCheckpoint nextSeqNum -> prefixed "inbox.checkpoint" (fields [ string nextSeqNum ])
        | InboxSourceHighwater(source, nextSeqNum) -> prefixed "inbox.highwater" (fields [ source; string nextSeqNum ])
        | InboxMessageAccepted envelope -> inboxEnvelopePrefix envelope

    let encode =
        function
        | Incoming record -> "in|" + encodeStepRecord record
        | Outgoing record -> "out|" + encodeStepRecord record

    let private splitPrefix (text: string) =
        let bar = text.IndexOf('|')

        if bar < 0 then
            Error "missing record prefix separator"
        else
            Ok(text.Substring(0, bar), text.Substring(bar + 1), bar + 1)

    let private decodeActivity fields =
        match fields with
        | [ opId; name; input ] -> parseOp opId |> Result.map (fun id -> id, { Name = name; Input = input })
        | _ -> Error "bad activity field count"

    let private decodeEvent prefix body start =
        match prefix with
        | "event.activity-called" ->
            readFields 3 body start
            |> Result.bind decodeActivity
            |> Result.map ActivityCalled
        | "event.activity-completed" ->
            readFields 2 body start
            |> Result.bind (function
                | [ opId; value ] -> parseOp opId |> Result.map (fun id -> ActivityCompleted(id, value))
                | _ -> Error "bad activity-completed field count")
        | "event.current-time" ->
            readFields 2 body start
            |> Result.bind (function
                | [ opId; timestamp ] ->
                    parseOp opId
                    |> Result.bind (fun id ->
                        parseInt64 "timestamp" timestamp
                        |> Result.map (fun value -> CurrentTimeRecorded(id, value)))
                | _ -> Error "bad current-time field count")
        | "event.log" ->
            readFields 2 body start
            |> Result.bind (function
                | [ opId; message ] -> parseOp opId |> Result.map (fun id -> LogEmitted(id, message))
                | _ -> Error "bad log field count")
        | "event.timer-created" ->
            readFields 2 body start
            |> Result.bind (function
                | [ opId; deadline ] ->
                    parseOp opId
                    |> Result.bind (fun id ->
                        parseInt64 "deadline" deadline
                        |> Result.map (fun value -> TimerCreated(id, value)))
                | _ -> Error "bad timer-created field count")
        | "event.timer-fired" ->
            readFields 1 body start
            |> Result.bind (function
                | [ opId ] -> parseOp opId |> Result.map TimerFired
                | _ -> Error "bad timer-fired field count")
        | "event.timer-canceled" ->
            readFields 1 body start
            |> Result.bind (function
                | [ opId ] -> parseOp opId |> Result.map TimerCanceled
                | _ -> Error "bad timer-canceled field count")
        | "event.signal" ->
            readFields 3 body start
            |> Result.bind (function
                | [ opId; name; payload ] -> parseOp opId |> Result.map (fun id -> SignalReceived(id, name, payload))
                | _ -> Error "bad signal field count")
        | _ -> Error("unknown event prefix: " + prefix)

    let private decodeCommand prefix body start =
        match prefix with
        | "command.activity" -> readFields 3 body start |> Result.bind decodeActivity |> Result.map CallActivity
        | "command.timer" ->
            readFields 2 body start
            |> Result.bind (function
                | [ opId; deadline ] ->
                    parseOp opId
                    |> Result.bind (fun id ->
                        parseInt64 "deadline" deadline
                        |> Result.map (fun value -> ScheduleTimer(id, value)))
                | _ -> Error "bad timer field count")
        | "command.cancel-timer" ->
            readFields 1 body start
            |> Result.bind (function
                | [ opId ] -> parseOp opId |> Result.map CancelTimer
                | _ -> Error "bad cancel-timer field count")
        | "command.log" ->
            readFields 2 body start
            |> Result.bind (function
                | [ opId; message ] -> parseOp opId |> Result.map (fun id -> WriteLog(id, message))
                | _ -> Error "bad command log field count")
        | _ -> Error("unknown command prefix: " + prefix)

    let private decodeInboxMessage fields =
        match fields with
        | "start" :: workflowName :: input :: [] -> Ok(StartWorkflow(WorkflowName workflowName, input))
        | "signal" :: name :: payload :: [] -> Ok(RaiseSignal(name, payload))
        | "activity-completed" :: opId :: value :: [] ->
            parseOp opId |> Result.map (fun id -> CompleteActivity(id, value))
        | tag :: _ -> Error("unknown inbox message tag: " + tag)
        | [] -> Error "missing inbox message tag"

    let private decodeInboxEnvelope body start =
        readFields 5 body start
        |> Result.bind (function
            | source :: sourceSeqNum :: messageFields ->
                parseInt64 "source seq num" sourceSeqNum
                |> Result.bind (fun seqNum ->
                    if seqNum < 0L then
                        Error("bad source seq num: " + sourceSeqNum)
                    else
                        decodeInboxMessage messageFields
                        |> Result.map (fun message ->
                            { Source = source
                              SourceSeqNum = seqNum
                              Message = message }))
            | _ -> Error "bad inbox envelope field count")

    let private decodeStepRecord text =
        match splitPrefix text with
        | Error error -> Error error
        | Ok(prefix, _, start) when prefix.StartsWith("event.", StringComparison.Ordinal) ->
            decodeEvent prefix text start |> Result.map HistoryEvent
        | Ok(prefix, _, start) when prefix.StartsWith("command.", StringComparison.Ordinal) ->
            decodeCommand prefix text start |> Result.map Command
        | Ok(prefix, _, start) when prefix = "dispatch.checkpoint" ->
            readFields 2 text start
            |> Result.bind (function
                | [ dispatcher; nextSeqNum ] ->
                    parseInt64 "next seq num" nextSeqNum
                    |> Result.bind (fun value ->
                        if value < 0L then
                            Error("bad next seq num: " + nextSeqNum)
                        else
                            Ok(CommandDispatchCheckpoint(dispatcher, value)))
                | _ -> Error "bad dispatch checkpoint field count")
        | Ok(prefix, _, start) when prefix = "inbox.checkpoint" ->
            readFields 1 text start
            |> Result.bind (function
                | [ nextSeqNum ] ->
                    parseInt64 "next seq num" nextSeqNum
                    |> Result.bind (fun value ->
                        if value < 0L then
                            Error("bad next seq num: " + nextSeqNum)
                        else
                            Ok(InboxCheckpoint value))
                | _ -> Error "bad inbox checkpoint field count")
        | Ok(prefix, _, start) when prefix = "inbox.highwater" ->
            readFields 2 text start
            |> Result.bind (function
                | [ source; nextSeqNum ] ->
                    parseInt64 "next seq num" nextSeqNum
                    |> Result.bind (fun value ->
                        if value < 0L then
                            Error("bad next seq num: " + nextSeqNum)
                        else
                            Ok(InboxSourceHighwater(source, value)))
                | _ -> Error "bad inbox highwater field count")
        | Ok(prefix, _, start) when prefix = "inbox.accepted" ->
            decodeInboxEnvelope text start |> Result.map InboxMessageAccepted
        | Ok(prefix, _, _) -> Error("unknown step record prefix: " + prefix)

    let decode (body: string) =
        if body.StartsWith("in|", StringComparison.Ordinal) then
            decodeStepRecord (body.Substring 3) |> Result.map Incoming
        elif body.StartsWith("out|", StringComparison.Ordinal) then
            decodeStepRecord (body.Substring 4) |> Result.map Outgoing
        else
            Error("bad step history wrapper: " + body)
