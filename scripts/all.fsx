// scripts/all.fsx
//
// Runs every committed foundation proof through the shared harness and prints
// one combined summary. This is the single entrypoint that replaces the
// self-running .fsx proof scratchpads.
//
//   npm run scripts                        # every proof
//   PROOF=foundation-00 npm run scripts    # only proofs whose name contains the substring
//   PRESERVE=1 npm run scripts             # keep resources of failed proofs for inspection
//
// Each foundation proof file is a definition module that exposes `proof`; add a
// new one here as the suite grows.

#load "_prelude.fsx"
#load "foundation-00-subject-history.fsx"

Prelude.runProofs [ FoundationSubjectHistory.proof ]
