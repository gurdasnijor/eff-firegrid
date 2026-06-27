namespace EffFiregrid.Build

open System
open System.Collections.Concurrent
open System.Diagnostics
open System.IO
open Fake.Core
open Fake.Core.TargetOperators

module Program =
    let rec findRoot (dir: DirectoryInfo) =
        if File.Exists(Path.Combine(dir.FullName, "eff-firegrid.fsproj")) then
            dir.FullName
        elif isNull dir.Parent then
            failwith "Could not locate repository root containing eff-firegrid.fsproj"
        else
            findRoot dir.Parent

    let root = AppContext.BaseDirectory |> DirectoryInfo |> findRoot

    let run (fileName: string) (args: string list) =
        let psi = ProcessStartInfo()
        psi.FileName <- fileName

        for arg in args do
            psi.ArgumentList.Add(arg)

        psi.WorkingDirectory <- root
        psi.UseShellExecute <- false

        use proc = Process.Start(psi)
        proc.WaitForExit()

        if proc.ExitCode <> 0 then
            failwithf "Command failed (%d): %s %s" proc.ExitCode fileName (String.concat " " args)

    let deleteDir relative =
        let path = Path.Combine(root, relative)

        if Directory.Exists(path) then
            Directory.Delete(path, true)

    let generatedDirs =
        [ "bin"
          "obj"
          "build"
          "build_test"
          "build_bench"
          "build_script"
          "build_scripts"
          "build_subject_history"
          "tools/repo/bin"
          "tools/repo/obj" ]

    let selectedTarget (argv: string array) =
        let rec loop index =
            if index >= argv.Length then
                "Check"
            else
                match argv.[index] with
                | "--target"
                | "-t" when index + 1 < argv.Length -> argv.[index + 1]
                | value when value.StartsWith("--target=", StringComparison.Ordinal) ->
                    value.Substring("--target=".Length)
                | _ -> loop (index + 1)

        loop 0

    let clean () =
        for dir in generatedDirs do
            deleteDir dir

    let cleanNode () = deleteDir "node_modules"

    let cleanAll () =
        clean ()
        cleanNode ()

    let restoreTools () = run "dotnet" [ "tool"; "restore" ]

    let install () =
        run "npm" [ "ci"; "--no-fund"; "--no-audit" ]

    let build () =
        run "dotnet" [ "build"; "eff-firegrid.fsproj"; "-clp:ErrorsOnly" ]

    let format () =
        run "dotnet" [ "fantomas"; "src"; "tests"; "benchmarks"; "scripts"; "repl.fsx" ]

    let formatCheck () =
        run "dotnet" [ "fantomas"; "--check"; "src"; "tests"; "benchmarks"; "scripts"; "repl.fsx" ]

    let lint () =
        run
            "dotnet"
            [ "fsharplint"
              "lint"
              "--lint-config"
              "fsharplint.json"
              "eff-firegrid.fsproj" ]

    let fableSmoke () =
        run "dotnet" [ "fable"; "repl.fsx"; "--outDir"; "build"; "--noCache" ]

    let play () =
        run "dotnet" [ "fable"; "repl.fsx"; "--outDir"; "build"; "--runScript" ]

    let scriptSubjectHistory () =
        run
            "dotnet"
            [ "fable"
              "scripts/foundation-00-subject-history.fsx"
              "--outDir"
              "build_script"
              "--runScript" ]

    let runFableScript (relPath: string) =
        run "dotnet" [ "fable"; relPath; "--outDir"; "build_script"; "--runScript" ]

    let actorOwnership () = runFableScript "scripts/actor-01-ownership.fsx"
    let actorDurableSteps () = runFableScript "scripts/actor-02-durable-steps.fsx"
    let actorSuspension () = runFableScript "scripts/actor-03-suspension.fsx"
    let actorRuntime () = runFableScript "scripts/actor-04-runtime.fsx"
    let actorErgonomic () = runFableScript "scripts/actor-05-ergonomic.fsx"

    let actors () =
        actorOwnership ()
        actorDurableSteps ()
        actorSuspension ()
        actorRuntime ()
        actorErgonomic ()

    let test () =
        run "dotnet" [ "fable"; "tests/Suite.fsx"; "--outDir"; "build_test" ]
        run "node" [ "build_test/Suite.js" ]

    let bench () =
        run "dotnet" [ "fable"; "benchmarks/Micro.fsx"; "--outDir"; "build_bench" ]
        run "node" [ "build_bench/Micro.js" ]

    let benchE2E () =
        run "dotnet" [ "fable"; "benchmarks/Throughput.fsx"; "--outDir"; "build_bench" ]
        run "node" [ "build_bench/Throughput.js" ]

    let check () =
        formatCheck ()
        build ()
        lint ()
        fableSmoke ()
        test ()

    [<EntryPoint>]
    let main argv =
        let fakeContext: Context.FakeExecutionContext =
            { IsCached = false
              Context = ConcurrentDictionary<string, obj>()
              ScriptFile = "tools/repo/Program.fs"
              Arguments = [] }

        Context.setExecutionContext (Context.RuntimeContext.Fake fakeContext)

        Target.create "Clean" (fun _ -> clean ())
        Target.create "CleanNode" (fun _ -> cleanNode ())
        Target.create "CleanAll" (fun _ -> cleanAll ())
        Target.create "RestoreTools" (fun _ -> restoreTools ())
        Target.create "Install" (fun _ -> install ())
        Target.create "Build" (fun _ -> build ())
        Target.create "Format" (fun _ -> format ())
        Target.create "FormatCheck" (fun _ -> formatCheck ())
        Target.create "Lint" (fun _ -> lint ())
        Target.create "FableSmoke" (fun _ -> fableSmoke ())
        Target.create "Play" (fun _ -> play ())
        Target.create "ScriptSubjectHistory" (fun _ -> scriptSubjectHistory ())
        Target.create "ActorOwnership" (fun _ -> actorOwnership ())
        Target.create "ActorDurableSteps" (fun _ -> actorDurableSteps ())
        Target.create "ActorSuspension" (fun _ -> actorSuspension ())
        Target.create "ActorRuntime" (fun _ -> actorRuntime ())
        Target.create "ActorErgonomic" (fun _ -> actorErgonomic ())
        Target.create "Actors" (fun _ -> actors ())
        Target.create "Test" (fun _ -> test ())
        Target.create "Bench" (fun _ -> bench ())
        Target.create "BenchE2E" (fun _ -> benchE2E ())
        Target.create "Check" (fun _ -> check ())

        "Clean" ==> "CleanAll" |> ignore
        "RestoreTools" ==> "Build" |> ignore
        "RestoreTools" ==> "Format" |> ignore
        "RestoreTools" ==> "FormatCheck" |> ignore
        "RestoreTools" ==> "Lint" |> ignore
        "RestoreTools" ==> "FableSmoke" |> ignore
        "RestoreTools" ==> "Play" |> ignore
        "RestoreTools" ==> "ScriptSubjectHistory" |> ignore
        "RestoreTools" ==> "ActorOwnership" |> ignore
        "RestoreTools" ==> "ActorDurableSteps" |> ignore
        "RestoreTools" ==> "ActorSuspension" |> ignore
        "RestoreTools" ==> "ActorRuntime" |> ignore
        "RestoreTools" ==> "ActorErgonomic" |> ignore
        "RestoreTools" ==> "Actors" |> ignore
        "RestoreTools" ==> "Test" |> ignore
        "RestoreTools" ==> "Bench" |> ignore
        "RestoreTools" ==> "BenchE2E" |> ignore
        "RestoreTools" ==> "Check" |> ignore

        Target.runOrDefault (selectedTarget argv)
        0
