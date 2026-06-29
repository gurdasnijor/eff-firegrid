namespace Eff.Proofs

module Registry =
    let all: ProofSpec list =
        [ FoundationSubjectHistoryProof.proof
          FoundationStateViewProof.proof
          FoundationKvStoreProof.proof
          DurableSemanticsProof.proof
          DurableS2SubstrateProof.proof
          DurableStepperProof.proof
          DurableHostProof.proof ]
