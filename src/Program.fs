namespace Eff

open Eff.Proofs

module Program =
    let private usage () =
        printfn "eff-firegrid"
        printfn ""
        printfn "Commands:"
        printfn "  proofs       Run the compiled validation proof suite"
        printfn "  proofs list  List registered proof names"

    let private listProofs () =
        for proof in Registry.all do
            printfn "%s" proof.Name

    [<EntryPoint>]
    let main argv =
        match argv |> Array.toList with
        | "proofs" :: "list" :: _ ->
            listProofs ()
            0
        | "proofs" :: _ ->
            Harness.runProofs Registry.all
            0
        | _ ->
            usage ()
            0
