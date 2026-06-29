namespace Eff.Proofs

module Proof =
    type ProofBuilder(name: string) =
        member _.Yield(_) : ProofDraft = { Description = None; Properties = [] }

        [<CustomOperation("describedAs")>]
        member _.DescribedAs(state: ProofDraft, description) =
            { state with
                Description = Some description }

        [<CustomOperation("property")>]
        member _.Property(state: ProofDraft, property) =
            { state with
                Properties = state.Properties @ [ property ] }

        member _.Run(state: ProofDraft) : ProofSpec =
            if List.isEmpty state.Properties then
                failwithf "proof '%s' must declare at least one property" name

            { Name = name
              Description = state.Description
              Properties = state.Properties }

    let proof name = ProofBuilder name

[<AutoOpen>]
module ProofSyntax =
    let proof name = Proof.proof name
