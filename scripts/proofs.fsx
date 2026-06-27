// scripts/proofs.fsx
//
// The single entrypoint for the proof suite. It is a thin Fable launcher: it
// loads the SAME compiled modules that eff-firegrid.fsproj type-checks, then
// runs the registry. There is no proof logic here — proofs live in src/Proofs
// so the build and linter enforce them.
//
//   npm run proofs                         # every proof
//   PROOF=foundation-00 npm run proofs     # only proofs whose name contains the substring
//   PRESERVE=1 npm run proofs              # keep failed proofs' resources for inspection
//   S2_BASIN=my-basin npm run proofs       # override the default basin

#r "nuget: Fable.Core, 5.1.0"
#load "../src/S2/Interop.fs"
#load "../src/S2/Errors.fs"
#load "../src/S2/Client.fs"
#load "../src/S2/Cli.fs"
#load "../src/Foundation/SubjectHistory.fs"
#load "../src/Proofs/Harness.fs"
#load "../src/Proofs/SubjectHistoryProof.fs"
#load "../src/Proofs/Registry.fs"

open Eff.Proofs

Harness.runProofs Registry.all
