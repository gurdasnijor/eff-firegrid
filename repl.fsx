// ============================================================================
// S2 playground — a guided tour of the whole F# surface (base client + patterns).
//
// Auth is zero-config (reads ~/.config/s2/config.toml via S2Cli).
//
//   Run once:        npm run play
//   Edit + rerun:    npm run watch     (restart the watcher after adding a #load!)
//
// It uses an ephemeral stream (unique name) and deletes it at the end, so it's
// safe to rerun. Each section is wrapped — one failure won't abort the rest.
// Comment out sections you don't care about.
// ============================================================================

#r "nuget: Fable.Core, 5.1.0"
// Load order mirrors the <Compile> order in eff-firegrid.fsproj.
#load "src/S2/Interop.fs"
#load "src/S2/Errors.fs"
#load "src/S2/Client.fs"
#load "src/S2/Cli.fs"
#load "src/S2/Patterns.fs"

open Fable.Core
open Eff

[<Emit("Date.now()")>]
let now () : float = jsNative

/// A typed message for the patterns section.
type Msg = { Id: int; Text: string }

let s2 = S2Cli.connect ()
let basinName = "test-basin-885234"

/// Run a named section; print errors but keep going.
let section (name: string) (body: Async<unit>) =
    async {
        printfn "\n── %s ──" name

        try
            do! body
        with e ->
            printfn "  ✗ %s" e.Message
    }

let tour =
    async {
        // ---- account survey (read-only) -------------------------------------
        do!
            section
                "account"
                (async {
                    let! basins = s2 |> S2.listBasins
                    printfn "  basins: %s" (basins |> List.map (fun b -> b.Name) |> String.concat ", ")
                    let! filtered = s2 |> S2.listBasinsWith "test"
                    printfn "  basins matching 'test': %d" (List.length filtered)
                    let! cfg = s2 |> S2.getBasinConfig basinName
                    printfn "  basin config: createOnAppend=%A cipher=%A" cfg.CreateStreamOnAppend cfg.StreamCipher
                })

        do!
            section
                "locations"
                (async {
                    let! locs = s2 |> S2.listLocations
                    printfn "  %d location(s)" (List.length locs)
                    let! def = s2 |> S2.getDefaultLocation
                    printfn "  default: %s (private=%b)" def.Name def.IsPrivate
                })

        do!
            section
                "metrics"
                (async {
                    let endT = System.DateTime.UtcNow
                    let startT = endT.AddDays(-1.0)
                    let! acct = s2 |> S2.accountMetrics S2.ActiveBasins startT endT
                    printfn "  account ActiveBasins: %d metric(s)" (List.length acct)
                    let! bas = s2 |> S2.basinMetrics S2.BasinStorage basinName startT endT
                    printfn "  basin Storage: %d metric(s)" (List.length bas)
                })

        do!
            section
                "access tokens"
                (async {
                    let! toks = s2 |> S2.listTokens
                    printfn "  %d token(s)" (List.length toks)
                })

        do!
            section
                "connectWith (custom options)"
                (async {
                    let s2b =
                        S2.connectWith
                            { S2.ConnectOptions.create (S2Cli.accessToken ()) with
                                Compression = Some S2.Gzip }

                    let! basins = s2b |> S2.listBasins
                    printfn "  gzip client sees %d basin(s)" (List.length basins)
                })

        // ---- ephemeral stream for the data-plane tour -----------------------
        let basin = s2 |> S2.basin basinName
        let sname = sprintf "fsharp-repl-%d" (int64 (now ()))
        do! basin |> S2.ensureStream sname
        let stream = basin |> S2.stream sname
        printfn "\n[using ephemeral stream %s/%s]" basinName sname

        do!
            section
                "streams: list / config"
                (async {
                    let! streams = basin |> S2.listStreamsWith sname
                    printfn "  listStreamsWith finds it: %b" (streams |> List.exists (fun s -> s.Name = sname))

                    let! _ =
                        basin
                        |> S2.reconfigureStream
                            { S2.StreamConfig.empty with
                                RetentionPolicy = Some(S2.RetainForSecs 3600) }
                            sname

                    let! cfg = basin |> S2.getStreamConfig sname
                    printfn "  retention after reconfigure: %A" cfg.RetentionPolicy
                    let! r = basin |> S2.ensureStreamWith S2.StreamConfig.empty sname
                    printfn "  ensure result: %A" r
                })

        do!
            section
                "append: records, conditions, commands"
                (async {
                    let! ack = stream |> S2.appendStrings [ "hello"; "world" ]
                    printfn "  appendStrings -> #%d..#%d" ack.Start.SeqNum ack.End.SeqNum
                    let! _ = stream |> S2.append [ S2.Record.textWith [ ("k", "v") ] "with-header" ]

                    let! _ =
                        stream
                        |> S2.append [ S2.Record.bytes (System.Text.Encoding.UTF8.GetBytes "binary") ]

                    printfn "  appended header + bytes records"
                    let! tail = stream |> S2.checkTail
                    let! _ = stream |> S2.appendIfSeqNum tail.SeqNum [ S2.Record.text "conditional" ]
                    printfn "  appendIfSeqNum ok"
                    let! _ = stream |> S2.append [ S2.Record.fence "writer-token" ]

                    let! bad =
                        stream
                        |> S2.tryAppendWith
                            (S2.AppendOptions.none |> S2.AppendOptions.fencingToken "wrong")
                            [ S2.Record.text "x" ]

                    match bad with
                    | Error(S2Errors.FencingTokenMismatch _) ->
                        printfn "  wrong fencing token -> typed FencingTokenMismatch"
                    | other -> printfn "  unexpected: %A" other
                })

        do!
            section
                "read: read / readWith / readLast"
                (async {
                    let! recs = stream |> S2.read (S2.FromSeqNum 0L) 100
                    printfn "  read from 0: %d records" (List.length recs)

                    let! filtered =
                        stream
                        |> S2.readWith
                            { S2.ReadOptions.empty with
                                Start = Some(S2.FromSeqNum 0L)
                                Count = Some 50
                                IgnoreCommandRecords = true }

                    printfn "  readWith (ignoreCommandRecords): %d records" (List.length filtered)
                    let! last = stream |> S2.readLast 3
                    printfn "  readLast 3: %s" (last |> List.map (fun r -> r.Body) |> String.concat ", ")
                })

        do!
            section
                "sessions: append + read"
                (async {
                    let! sess = stream |> S2.appendSession
                    let! _ = sess |> S2.submitAck [ S2.Record.text "sess-1" ]
                    let! _ = sess |> S2.submitAck [ S2.Record.text "sess-2" ]
                    printfn "  last acked: %A" (S2.lastAcked sess |> Option.map (fun a -> a.End.SeqNum))
                    do! sess |> S2.closeAppendSession

                    let! rsess =
                        stream
                        |> S2.readSession
                            { S2.ReadOptions.empty with
                                Start = Some(S2.FromSeqNum 0L)
                                WaitSecs = Some 0 }

                    let mutable count = 0
                    do! rsess |> S2.iter (fun _ -> async { count <- count + 1 })
                    printfn "  read session iterated %d records" count
                })

        do!
            section
                "patterns: typed messaging"
                (async {
                    let! tail = stream |> S2.checkTail
                    let! p = stream |> S2Patterns.producer S2Patterns.Json.serialize
                    let! r1 = p |> S2Patterns.submit { Id = 1; Text = "typed-a" }
                    let! _ = p |> S2Patterns.submit { Id = 2; Text = "typed-b" }
                    printfn "  produced message 1 at #%d..#%d" r1.Start r1.End
                    do! p |> S2Patterns.closeProducer

                    let! c =
                        stream
                        |> S2Patterns.consumer S2Patterns.Json.deserialize (S2.FromSeqNum tail.SeqNum)

                    let! msgs = c |> S2Patterns.take 2

                    printfn
                        "  consumed %d typed message(s): %s"
                        (List.length msgs)
                        (msgs |> List.map (fun m -> m.Text) |> String.concat ", ")
                })

        // ---- cleanup --------------------------------------------------------
        do! basin |> S2.deleteStream sname
        printfn "\n[deleted %s]" sname
    }

async {
    printfn "S2 F# playground — full surface tour"

    try
        do! tour
    with e ->
        printfn "✗ fatal: %s" e.Message
}
|> Async.StartImmediate
