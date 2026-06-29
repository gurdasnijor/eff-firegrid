namespace Eff.Proofs

open Fable.Core

module Runner =
    [<Emit("process.exit($0)")>]
    let private processExit (_code: int) : unit = jsNative

    [<Emit("(process.env[$0] != null ? process.env[$0] : '')")>]
    let private envOr (_name: string) : string = jsNative

    let private defaultRoot = ".eff-firegrid/proofs"

    let private envFlag name =
        match envOr name with
        | ""
        | "0"
        | "false" -> false
        | _ -> true

    let private envOption name =
        match envOr name with
        | "" -> None
        | value -> Some value

    let defaultConfig =
        { Root = defaultRoot
          ProofFilter = envOption "PROOF"
          TrialId = None
          Preserve = envFlag "PRESERVE"
          Seed = 1 }

    let private matchesFilter (filter: string option) (proof: ProofSpec) =
        match filter with
        | None -> true
        | Some(value: string) ->
            proof.Name.Contains value
            || proof.Properties |> List.exists (fun property -> property.Name.Contains value)

    let listProofs (proofs: ProofSpec list) =
        for proof in proofs do
            printfn "%s" proof.Name

            for property in proof.Properties do
                printfn "  %s" property.Name

    let run (config: RunnerConfig) (proofs: ProofSpec list) =
        async {
            let selected = proofs |> List.filter (matchesFilter config.ProofFilter)
            let reports = ResizeArray<PropertyReport>()

            if List.isEmpty selected then
                printfn "no proofs matched filter"
                processExit 1

            for proof in selected do
                printfn "\n### %s" proof.Name

                for property in proof.Properties do
                    printfn "\n== %s ==" property.Name
                    let! report = property.RunProperty config proof.Name
                    reports.Add report

                    for check in report.Checks do
                        if check.Passed then
                            printfn "  ✓ %s" check.Name
                        else
                            printfn "  ✗ %s" check.Name

                            match check.Message with
                            | Some message -> printfn "      %s" message
                            | None -> ()

                    for control in report.NegativeControls do
                        if control.Passed then
                            printfn "  ✓ negative: %s" control.Name
                        else
                            printfn "  ✗ negative: %s" control.Name

                            match control.Message with
                            | Some message -> printfn "      %s" message
                            | None -> ()

                    printfn "  report: %s" report.ReportPath

            let passed = reports |> Seq.filter (fun report -> report.Passed) |> Seq.length
            let failed = reports.Count - passed

            printfn "\n%d properties passed, %d failed" passed failed
            processExit (if failed = 0 then 0 else 1)
        }
        |> Async.StartImmediate

    let replay reportPath proofs =
        let replay = Reports.readReplaySpec reportPath

        printfn "replay report: %s" replay.ReportPath
        printfn "replay command: %s" replay.ReplayCommand

        run
            { defaultConfig with
                ProofFilter = Some replay.PropertyName
                TrialId = Some replay.TrialId
                Preserve = true }
            proofs
