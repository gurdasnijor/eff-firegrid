namespace Eff.Foundation

open Eff

module SubjectHistory =
    type SubjectId = SubjectId of string
    type Seq = Seq of int64
    type Version = Version of int64

    type Codec<'record> =
        { Encode: 'record -> string
          Decode: string -> Result<'record, string> }

    type StoredRecord<'record> = { Seq: Seq; Body: 'record }

    [<RequireQualifiedAccess>]
    type ConflictRecord<'record> =
        | Found of StoredRecord<'record>
        | Unavailable
        | LookupFailed of message: string

    type AppendConflict<'record> =
        { Expected: Version
          Actual: Version
          Conflicting: ConflictRecord<'record> }

    [<RequireQualifiedAccess>]
    type AppendFailure<'record> =
        | Conflict of AppendConflict<'record>
        | Failed of S2Errors.S2Failure

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
                | [] -> return Ok ConflictRecord.Unavailable
                | first :: _ when first.SeqNum = seq ->
                    match decodeRecord codec first with
                    | Ok record -> return Ok(ConflictRecord.Found record)
                    | Error error -> return Error error
                | _ -> return Ok ConflictRecord.Unavailable
            with e ->
                match S2Errors.classify e with
                | S2Errors.RangeNotSatisfiable _ -> return Ok ConflictRecord.Unavailable
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
                    Error(
                        AppendFailure.Conflict
                            { Expected = Version expected
                              Actual = Version actual
                              Conflicting =
                                match conflicting with
                                | Ok record -> record
                                | Error message -> ConflictRecord.LookupFailed message }
                    )
            | Error error -> return Error(AppendFailure.Failed error)
        }

    let append basin codec subject records =
        async {
            let! ack = stream basin subject |> S2.append (encodeRecords codec records)
            return Version ack.End.SeqNum
        }

    let openCursorWithWait waitSecs basin codec subject (Seq from) =
        async {
            let! cursor =
                stream basin subject
                |> S2.readCursor
                    { S2.ReadOptions.empty with
                        Start = Some(S2.FromSeqNum from)
                        WaitSecs = waitSecs }

            return { Codec = codec; Cursor = cursor }
        }

    let openCursor basin codec subject from =
        openCursorWithWait None basin codec subject from

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

                if target < next then
                    failwithf "target version %d is before start seq %d" target next

                while next < target do
                    let! item = tryNext cursor

                    match item with
                    | Error error -> failwith error
                    | Ok None -> failwithf "cursor ended at %d before target %d" next target
                    | Ok(Some record) ->
                        let actual = seqNumber record.Seq

                        if actual <> next then
                            failwithf "cursor returned seq %d while folding seq %d" actual next

                        state <- apply state record
                        next <- actual + 1L

                let result = state, Version next
                closing <- true
                do! closeCursor cursor
                return result
            with e ->
                if not closing then
                    do! closeCursor cursor

                return raise e
        }
