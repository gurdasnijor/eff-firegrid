namespace Eff.Proofs

/// The proof registry. Every compiled proof is listed here once; the runner
/// (and the PROOF filter) work off this list. Adding a proof is one line.
module Registry =
    let all: Harness.Proof list = [ SubjectHistoryProof.proof ]
