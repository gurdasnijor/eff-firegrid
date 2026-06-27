namespace Eff.Foundation

open Eff

module StreamLog =
    type StreamId = StreamId of string
    type SeqNum = SeqNum of int64
    type StreamVersion = StreamVersion of int64

    type LogRecord<'event> = { SeqNum: SeqNum; Event: 'event }

    type AppendConflict =
        { Expected: StreamVersion
          Actual: StreamVersion }

    type EventCodec<'event> =
        { Encode: 'event -> string
          Decode: string -> Result<'event, string> }

    type AppendRange =
        { Start: SeqNum
          EndExclusive: StreamVersion
          Tail: StreamVersion }

    type AppendTicket = { Ack: Async<AppendRange> }

    type TypedAppendSession<'event> =
        { Codec: EventCodec<'event>
          Session: S2.AppendSession }

    type TypedReadSession<'event> =
        { Codec: EventCodec<'event>
          Session: S2.ReadSession }

    type TypedReadCursor<'event> =
        { Codec: EventCodec<'event>
          Cursor: S2.ReadCursor }

    let private name (StreamId value) = value

    let stream (basin: S2.Basin) streamId = basin |> S2.stream (name streamId)

    let toRange (ack: S2.AppendAck) =
        { Start = SeqNum ack.Start.SeqNum
          EndExclusive = StreamVersion ack.End.SeqNum
          Tail = StreamVersion ack.Tail.SeqNum }

    let private encodeRecords codec events =
        events |> List.map (codec.Encode >> S2.Record.text)

    let private decodeRecord codec (record: S2.ReadRecord) =
        match codec.Decode record.Body with
        | Ok event ->
            Ok
                { SeqNum = SeqNum record.SeqNum
                  Event = event }
        | Error error -> Error(sprintf "decode failed at seq %d: %s" record.SeqNum error)

    let private firstError results =
        results
        |> List.tryPick (function
            | Error error -> Some error
            | Ok _ -> None)

    let append basin codec streamId events =
        async {
            let! ack = stream basin streamId |> S2.append (encodeRecords codec events)
            return toRange ack
        }

    let appendExpected basin codec streamId (StreamVersion expected) events =
        async {
            let opts = S2.AppendOptions.none |> S2.AppendOptions.matchSeqNum expected
            let s = stream basin streamId

            try
                let! ack = s |> S2.appendWith opts (encodeRecords codec events)
                return Ok(toRange ack)
            with e ->
                match S2Errors.classify e with
                | S2Errors.SeqNumMismatch actual ->
                    return
                        Error
                            { Expected = StreamVersion expected
                              Actual = StreamVersion actual }
                | _ -> return (raise e)
        }

    let readFrom basin codec streamId (SeqNum from) =
        async {
            let s = stream basin streamId
            let! tail = s |> S2.checkTail
            let tailSeq = tail.SeqNum
            let chunkSize = 1000

            let rec loop next acc =
                async {
                    if next >= tailSeq then
                        return Ok(List.rev acc)
                    else
                        let count = min chunkSize (int (tailSeq - next))
                        let! records = s |> S2.read (S2.FromSeqNum next) count
                        let decoded = records |> List.map (decodeRecord codec)

                        match firstError decoded with
                        | Some error -> return Error error
                        | None ->
                            let events = decoded |> List.choose Result.toOption

                            if List.isEmpty events then
                                return Error(sprintf "read made no progress before tail %d" tailSeq)
                            else
                                return! loop (next + int64 events.Length) (List.rev events @ acc)
                }

            return! loop from []
        }

    let checkTail basin streamId =
        async {
            let! tail = stream basin streamId |> S2.checkTail
            return StreamVersion tail.SeqNum
        }

    let openAppendSession basin codec streamId : Async<TypedAppendSession<'event>> =
        async {
            let! session = stream basin streamId |> S2.appendSession
            return { Codec = codec; Session = session }
        }

    let submit events (typed: TypedAppendSession<'event>) =
        async {
            let! ticket = typed.Session |> S2.submit (encodeRecords typed.Codec events)

            return
                { Ack =
                    async {
                        let! ack = ticket |> S2.ack
                        return toRange ack
                    } }
        }

    let closeAppendSession (typed: TypedAppendSession<'event>) = typed.Session |> S2.closeAppendSession

    let openReadSession basin codec streamId (SeqNum from) : Async<TypedReadSession<'event>> =
        async {
            let! session =
                stream basin streamId
                |> S2.readSession
                    { S2.ReadOptions.empty with
                        Start = Some(S2.FromSeqNum from) }

            return { Codec = codec; Session = session }
        }

    let openReadCursor basin codec streamId (SeqNum from) : Async<TypedReadCursor<'event>> =
        async {
            let! cursor =
                stream basin streamId
                |> S2.readCursor
                    { S2.ReadOptions.empty with
                        Start = Some(S2.FromSeqNum from) }

            return { Codec = codec; Cursor = cursor }
        }

    let take n (typed: TypedReadSession<'event>) =
        async {
            let! records = typed.Session |> S2.take n
            let decoded = records |> List.map (decodeRecord typed.Codec)

            match firstError decoded with
            | Some error -> return Error error
            | None -> return Ok(decoded |> List.choose Result.toOption)
        }

    let tryNext (typed: TypedReadCursor<'event>) =
        async {
            let! record = typed.Cursor |> S2.tryNext

            match record with
            | None -> return Ok None
            | Some record ->
                match decodeRecord typed.Codec record with
                | Ok decoded -> return Ok(Some decoded)
                | Error error -> return Error error
        }

    let closeReadSession (typed: TypedReadSession<'event>) = typed.Session |> S2.closeReadSession

    let closeReadCursor (typed: TypedReadCursor<'event>) = typed.Cursor |> S2.closeReadCursor
