namespace Eff.Proofs

module Registry =
    let all: ProofSpec list =
        [ FoundationSubjectHistoryProof.proof
          FoundationStateViewProof.proof
          DurableSemanticsProof.proof ]
