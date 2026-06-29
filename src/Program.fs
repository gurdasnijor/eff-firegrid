namespace Eff

open Eff.Proofs

module Program =
    let private usage () =
        printfn "eff-firegrid"
        printfn ""
        printfn "Commands:"
        printfn "  proofs list               List registered proof names"
        printfn "  proofs run [--proof name] Run the compiled validation proof suite"
        printfn "  proofs replay <report>    Re-run the proof property named by report.json"

    let private listProofs () = Runner.listProofs Registry.all

    let rec private parseRunArgs config args =
        match args with
        | [] -> config
        | "--proof" :: value :: rest -> parseRunArgs { config with ProofFilter = Some value } rest
        | "--trial-id" :: value :: rest -> parseRunArgs { config with TrialId = Some value } rest
        | "--preserve" :: rest -> parseRunArgs { config with Preserve = true } rest
        | "--seed" :: value :: rest -> parseRunArgs { config with Seed = int value } rest
        | _ :: rest -> parseRunArgs config rest

    [<EntryPoint>]
    let main argv =
        match argv |> Array.toList with
        | "proofs" :: "list" :: _ ->
            listProofs ()
            0
        | "proofs" :: "replay" :: reportPath :: _ ->
            Runner.replay reportPath Registry.all
            0
        | "proofs" :: "run" :: args ->
            Runner.run (parseRunArgs Runner.defaultConfig args) Registry.all
            0
        | "proofs" :: args ->
            Runner.run (parseRunArgs Runner.defaultConfig args) Registry.all
            0
        | _ ->
            usage ()
            0
