// Capability C1: StreamLog
//
// Script-first proof for the typed StreamLog boundary over real S2 streams.
//
// Run:
//   dotnet fable scripts/foundation-00-stream-log.fsx --outDir build_script --runScript

#r "nuget: Fable.Core, 5.1.0"
#load "../src/S2/Interop.fs"
#load "../src/S2/Errors.fs"
#load "../src/S2/Client.fs"
#load "../src/S2/Cli.fs"
#load "../src/S2/Patterns.fs"
#load "../src/Foundation/StreamLog.fs"

open Fable.Core
open Eff
open Eff.Foundation

[<Emit("process.exit($0)")>]
let exit (code: int) : unit = jsNative

[<Emit("Date.now()")>]
let now () : float = jsNative

module Proof =
    let mutable passed = 0
    let mutable failed = 0
    let mutable failures: string list = []
    let mutable counter = 0

    let uniq (prefix: string) : string =
        counter <- counter + 1
        sprintf "%s-%d-%d" prefix (int64 (now ())) counter

    let check name condition =
        if condition then
            passed <- passed + 1
            printfn "  ok  %s" name
        else
            failed <- failed + 1
            failures <- name :: failures
            printfn "  bad %s" name

    let section name body =
        async {
            printfn "\n== %s ==" name

            try
                do! body
            with e ->
                failed <- failed + 1
                failures <- (name + " threw: " + e.Message) :: failures
                printfn "  bad threw: %s" e.Message
        }

    let finish () =
        printfn "\n%d passed, %d failed" passed failed

        if failed > 0 then
            failures |> List.rev |> List.iter (printfn "  - %s")

            exit 1
        else
            exit 0

type DemoEvent = Said of string

module DemoEvent =
    let codec: StreamLog.EventCodec<DemoEvent> =
        { Encode =
            fun event ->
                match event with
                | Said text -> "said:" + text
          Decode =
            fun body ->
                if body.StartsWith "said:" then
                    Ok(Said(body.Substring "said:".Length))
                else
                    Error(sprintf "unknown event body %s" body) }

let basinName = "test-basin-885234"

let main =
    async {
        let s2 = S2Cli.connect ()
        let basin = s2 |> S2.basin basinName
        let streamName = Proof.uniq "foundation-stream-log"
        let streamId = StreamLog.StreamId streamName
        let mutable created = false

        printfn "stream: %s/%s" basinName streamName

        try
            do! basin |> S2.createStream streamName
            created <- true

            do!
                Proof.section
                    "append/read/checkTail"
                    (async {
                        let! appended = StreamLog.append basin DemoEvent.codec streamId [ Said "a"; Said "b" ]

                        Proof.check "append start is 0" (appended.Start = StreamLog.SeqNum 0L)
                        Proof.check "append end is 2" (appended.EndExclusive = StreamLog.StreamVersion 2L)

                        let! tail = StreamLog.checkTail basin streamId
                        Proof.check "tail is next seq" (tail = StreamLog.StreamVersion 2L)

                        let! read = StreamLog.readFrom basin DemoEvent.codec streamId (StreamLog.SeqNum 0L)

                        match read with
                        | Error error -> Proof.check ("read decodes: " + error) false
                        | Ok records ->
                            Proof.check "read count is 2" (records.Length = 2)

                            Proof.check
                                "read seqs are 0,1"
                                ((records |> List.map _.SeqNum) = [ StreamLog.SeqNum 0L; StreamLog.SeqNum 1L ])

                            Proof.check
                                "read events round-trip"
                                ((records |> List.map _.Event) = [ Said "a"; Said "b" ])
                    })

            do!
                Proof.section
                    "appendExpected conflict"
                    (async {
                        let! good =
                            StreamLog.appendExpected
                                basin
                                DemoEvent.codec
                                streamId
                                (StreamLog.StreamVersion 2L)
                                [ Said "c" ]

                        Proof.check "appendExpected at tail succeeds" (Result.isOk good)

                        let! bad =
                            StreamLog.appendExpected
                                basin
                                DemoEvent.codec
                                streamId
                                (StreamLog.StreamVersion 2L)
                                [ Said "stale" ]

                        match bad with
                        | Error conflict ->
                            Proof.check "stale expected recorded" (conflict.Expected = StreamLog.StreamVersion 2L)
                            Proof.check "actual tail recorded" (conflict.Actual = StreamLog.StreamVersion 3L)
                        | Ok _ -> Proof.check "stale append conflicts" false
                    })

            do!
                Proof.section
                    "append session ack"
                    (async {
                        let! session = StreamLog.openAppendSession basin DemoEvent.codec streamId
                        let! ticket = session |> StreamLog.submit [ Said "session-a"; Said "session-b" ]
                        let! ack = ticket.Ack
                        Proof.check "session ack starts at 3" (ack.Start = StreamLog.SeqNum 3L)
                        Proof.check "session ack ends at 5" (ack.EndExclusive = StreamLog.StreamVersion 5L)
                        do! session |> StreamLog.closeAppendSession
                    })

            do!
                Proof.section
                    "read session tails typed records"
                    (async {
                        let! tail = StreamLog.checkTail basin streamId
                        let (StreamLog.StreamVersion start) = tail
                        let! reader = StreamLog.openReadSession basin DemoEvent.codec streamId (StreamLog.SeqNum start)

                        let! writer = StreamLog.openAppendSession basin DemoEvent.codec streamId
                        let! ticket = writer |> StreamLog.submit [ Said "live-a"; Said "live-b" ]
                        let! _ = ticket.Ack

                        let! taken = reader |> StreamLog.take 2

                        match taken with
                        | Error error -> Proof.check ("read session decodes: " + error) false
                        | Ok records ->
                            Proof.check "read session took 2" (records.Length = 2)

                            Proof.check
                                "read session events"
                                ((records |> List.map _.Event) = [ Said "live-a"; Said "live-b" ])

                        do! reader |> StreamLog.closeReadSession
                        do! writer |> StreamLog.closeAppendSession
                    })

            do!
                Proof.section
                    "read cursor pulls one record at a time"
                    (async {
                        let! tail = StreamLog.checkTail basin streamId
                        let (StreamLog.StreamVersion start) = tail
                        let! cursor = StreamLog.openReadCursor basin DemoEvent.codec streamId (StreamLog.SeqNum start)

                        let! writer = StreamLog.openAppendSession basin DemoEvent.codec streamId
                        let! ticket = writer |> StreamLog.submit [ Said "cursor-a"; Said "cursor-b" ]
                        let! _ = ticket.Ack

                        let! first = cursor |> StreamLog.tryNext
                        let! second = cursor |> StreamLog.tryNext

                        match first, second with
                        | Ok(Some a), Ok(Some b) ->
                            Proof.check "cursor first event" (a.Event = Said "cursor-a")
                            Proof.check "cursor second event" (b.Event = Said "cursor-b")

                            Proof.check
                                "cursor seqs advance"
                                ((a.SeqNum, b.SeqNum) = (StreamLog.SeqNum start, StreamLog.SeqNum(start + 1L)))
                        | other -> Proof.check (sprintf "cursor returned records: %A" other) false

                        do! cursor |> StreamLog.closeReadCursor
                        do! writer |> StreamLog.closeAppendSession
                    })

            do! basin |> S2.deleteStream streamName
            created <- false
            Proof.finish ()
        with e ->
            printfn "\nfatal: %s" e.Message

            if created then
                try
                    do! basin |> S2.deleteStream streamName
                    printfn "deleted %s after failure" streamName
                with cleanup ->
                    printfn "cleanup failed: %s" cleanup.Message

            Proof.failed <- Proof.failed + 1
            Proof.failures <- ("fatal: " + e.Message) :: Proof.failures
            Proof.finish ()
    }

main |> Async.StartImmediate
