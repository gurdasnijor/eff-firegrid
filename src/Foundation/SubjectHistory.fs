namespace Eff.Foundation

open System
open Eff

module SubjectHistory =
    type SubjectId = SubjectId of string
    type Seq = Seq of int64
    type Version = Version of int64

    type Codec<'record> =
        { Encode: 'record -> string
          Decode: string -> Result<'record, string> }

    type StoredRecord<'record> = { Seq: Seq; Body: 'record }

    type AppendConflict<'record> =
        { Expected: Version
          Actual: Version
          Conflicting: StoredRecord<'record> option }

    type AppendOutcome<'record> =
        | Appended of next: Version
        | IdempotentRetry of alreadyAt: Version * existing: StoredRecord<'record>
        | Conflict of AppendConflict<'record>

    type Cursor<'record> =
        { Codec: Codec<'record>
          Cursor: S2.ReadCursor }

    let private streamName (SubjectId value) = value

    let private stream (basin: S2.Basin) subject = basin |> S2.stream (streamName subject)

    let versionNumber (Version value) = value

    let seqNumber (Seq value) = value

    let private encodeRecords codec records =
        records |> List.map (codec.Encode >> S2.Record.text)

    let private decodeRecord codec (record: S2.ReadRecord) =
        match codec.Decode record.Body with
        | Ok body -> Ok { Seq = Seq record.SeqNum; Body = body }
        | Error error -> Error(sprintf "decode failed at seq %d: %s" record.SeqNum error)

    let tail basin subject =
        async {
            let! pos = stream basin subject |> S2.checkTail
            return Version pos.SeqNum
        }

    let private tryReadOne basin codec subject (Seq seq) =
        async {
            try
                let! records = stream basin subject |> S2.read (S2.FromSeqNum seq) 1

                match records with
                | [] -> return Ok None
                | first :: _ ->
                    match decodeRecord codec first with
                    | Ok record -> return Ok(Some record)
                    | Error error -> return Error error
            with e ->
                match S2Errors.classify e with
                | S2Errors.RangeNotSatisfiable _ -> return Ok None
                | _ -> return Error e.Message
        }

    let appendExpected basin codec subject (Version expected) records =
        async {
            let opts = S2.AppendOptions.none |> S2.AppendOptions.matchSeqNum expected
            let! appended = stream basin subject |> S2.tryAppendWith opts (encodeRecords codec records)

            match appended with
            | Ok ack -> return Ok(Version ack.End.SeqNum)
            | Error(S2Errors.SeqNumMismatch actual) ->
                let! conflicting = tryReadOne basin codec subject (Seq expected)

                return
                    Error
                        { Expected = Version expected
                          Actual = Version actual
                          Conflicting =
                            match conflicting with
                            | Ok record -> record
                            | Error _ -> None }
            | Error error -> return raise (Exception(sprintf "%A" error))
        }

    let appendIdempotent basin codec subject expected records =
        async {
            let! result = appendExpected basin codec subject expected records

            match result with
            | Ok next -> return Appended next
            | Error conflict ->
                match records, conflict.Conflicting with
                | [ attempted ], Some existing when attempted = existing.Body ->
                    return IdempotentRetry(conflict.Actual, existing)
                | _ -> return Conflict conflict
        }

    let append basin codec subject records =
        async {
            let! ack = stream basin subject |> S2.append (encodeRecords codec records)
            return Version ack.End.SeqNum
        }

    let openCursor basin codec subject (Seq from) =
        async {
            let! cursor =
                stream basin subject
                |> S2.readCursor
                    { S2.ReadOptions.empty with
                        Start = Some(S2.FromSeqNum from) }

            return { Codec = codec; Cursor = cursor }
        }

    let tryNext cursor =
        async {
            let! record = cursor.Cursor |> S2.tryNext

            match record with
            | None -> return Ok None
            | Some record ->
                match decodeRecord cursor.Codec record with
                | Ok decoded -> return Ok(Some decoded)
                | Error error -> return Error error
        }

    let closeCursor cursor = cursor.Cursor |> S2.closeReadCursor

    let foldTo basin codec subject from until initial apply =
        async {
            let! cursor = openCursor basin codec subject from
            let mutable closing = false

            try
                let mutable state = initial
                let mutable next = seqNumber from
                let target = versionNumber until

                while next < target do
                    let! item = tryNext cursor

                    match item with
                    | Error error -> failwith error
                    | Ok None -> failwithf "cursor ended at %d before target %d" next target
                    | Ok(Some record) ->
                        state <- apply state record
                        next <- seqNumber record.Seq + 1L

                let result = state, Version next
                closing <- true
                do! closeCursor cursor
                return result
            with e ->
                if not closing then
                    do! closeCursor cursor

                return raise e
        }
