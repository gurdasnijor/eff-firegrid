namespace Eff.Proofs

module Registry =
    let all: ProofSpec list =
        [ FoundationSubjectHistoryProof.proof
          FoundationStateViewProof.proof
          FoundationKvStoreProof.proof
          DurableSemanticsProof.proof
          DurableS2SubstrateProof.proof
          DurableStepperProof.proof
          DurableHostProof.proof
          DurableCommandDispatchProof.proof
          DurableRegistryProof.proof
          DurableActivityAdapterProof.proof
          DurableInboxFoldProof.proof
          DurableActivityInboxProof.proof
          DurableHostTickProof.proof
          DurableTimerAdapterProof.proof
          DurableClientAdmissionProof.proof
          DurableSignalAdmissionProof.proof
          DurableStatusProof.proof
          DurableRuntimeProof.proof
          DurableAppFacadeProof.proof
          DurableTestHostProof.proof
          DurableEnvironmentBootstrapProof.proof
          DurableWorkerLoopProof.proof
          DurableTypedStatusProof.proof
          DurableRaceFacadeProof.proof ]
