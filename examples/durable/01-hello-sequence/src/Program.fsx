// Durable hello sequence.

#r "nuget: Fable.Core, 5.1.0"

#load "../../../../src/S2/Interop.fs"
#load "../../../../src/S2/Errors.fs"
#load "../../../../src/S2/Client.fs"
#load "../../../../src/S2/Cli.fs"
#load "../../../../src/Foundation/Durable/Semantics.fs"
#load "../../../../src/Foundation/Durable/Api.fs"
#load "../../../../src/Foundation/Durable/Registry.fs"
#load "../../../../src/Foundation/Durable/S2Substrate.fs"
#load "../../../../src/Foundation/Durable/Stepper.fs"
#load "../../../../src/Foundation/Durable/StepRecordCodec.fs"
#load "../../../../src/Foundation/Durable/Client.fs"
#load "../../../../src/Foundation/Durable/CommandDispatch.fs"
#load "../../../../src/Foundation/Durable/ActivityAdapter.fs"
#load "../../../../src/Foundation/Durable/InboxFold.fs"
#load "../../../../src/Foundation/Durable/TimerAdapter.fs"
#load "../../../../src/Foundation/Durable/Host.fs"
#load "../../../../src/Foundation/Durable/Runtime.fs"
#load "../../../../src/Foundation/Durable/App/Storage.fs"
#load "../../../../src/Foundation/Durable/App/Definitions.fs"
#load "../../../../src/Foundation/Durable/App/DurableFacade.fs"
#load "../../../../src/Foundation/Durable/App/Environment.fs"
#load "../../../../src/Foundation/Durable/App/Client.fs"
#load "../../../../src/Foundation/Durable/App/Worker.fs"
#load "../../../../src/Foundation/Durable/App/Builder.fs"

open Eff.Foundation.Durable
open Eff.Foundation.Durable.App

module D = Eff.Foundation.Durable.App.Durable

let greet =
    Activity.define "hello.greet" (fun name -> async { return "Hello, " + name })

let punctuate =
    Activity.define "hello.punctuate" (fun message -> async { return message + "!" })

let helloSequence =
    Workflow.define "hello.sequence" (fun name ->
        durable {
            let! greeting = D.call greet name
            return! D.call punctuate greeting
        })

let app =
    durableApp {
        activity greet
        activity punctuate
        workflow helloSequence
    }

let startOrFail =
    function
    | DurableAppStartResult.Started instanceId -> instanceId
    | DurableAppStartResult.Rejected failure -> failwith ("start failed: " + string failure)

let run () =
    async {
        let client =
            app
            |> DurableApp.client
                { Environment = "durable"
                  BasinName = None }

        let worker =
            app
            |> DurableApp.worker
                { Environment = "durable"
                  BasinName = None
                  HostId = "hello"
                  MaxRunUntilIdleTicks = Some 10 }

        let! start = client.start helloSequence "Ada"
        let instanceId = startOrFail start
        let! _ = worker.runUntilIdle instanceId
        let! status = client.statusOf helloSequence instanceId
        printfn "%A" status
    }

Async.StartImmediate(run ())
