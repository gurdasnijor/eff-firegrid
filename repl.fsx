#r "nuget: Fable.Core, 5.1.0"
#load "src/S2/Interop.fs"
#load "src/S2/Errors.fs"
#load "src/S2/Client.fs"
#load "src/S2/Cli.fs"

open Eff

let s2 = S2Cli.connect ()

let playground =
    async {
        // Read-only: list basins on the account.
        let! basins = s2 |> S2.listBasins
        printfn "→ %d basin(s):" (List.length basins)
        for b in basins do
            printfn "    %s" b.Name

        // --- scope to a basin (read-only) ---
        let basin = s2 |> S2.basin "test-basin-885234"
        let! streams = basin |> S2.listStreams
        printfn "→ %d stream(s):" (List.length streams)
        for s in streams do printfn "    %s" s.Name
        //
        let stream = basin |> S2.stream "fsharp-coverage"
        let! tail = stream |> S2.checkTail
        printfn "→ tail @ #%d" tail.SeqNum
        let! recent = stream |> S2.readLast 10
        printfn "→ last %d record(s):" (List.length recent)
        for r in recent do printfn "    #%d  %s" r.SeqNum r.Body

        // --- MUTATING (runs on every save while uncommented) ---
        do! basin |> S2.ensureStream "events"
        let! ack = stream |> S2.appendStrings [ "hello from the playground" ]
        printfn "→ appended #%d..#%d" ack.Start.SeqNum ack.End.SeqNum
    }

// --- run --------------------------------------------------------------------
async {
    try
        do! playground
    with e ->
        printfn "✗ %s" e.Message
}
|> Async.StartImmediate
