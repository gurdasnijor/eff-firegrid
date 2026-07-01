// Durable app test-host pattern.

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
#load "../../../../src/Proofs/Proof.fs"
#load "../../../../src/Proofs/Reports.fs"
#load "../../../../src/Proofs/DurableTestHost.fs"

open Eff.Foundation.Durable
open Eff.Foundation.Durable.App
open Eff.Proofs

module D = Eff.Foundation.Durable.App.Durable

let greet =
    Activity.define "test-host.greet" (fun name -> async { return "Hello, " + name + "!" })

let greeting =
    Workflow.define "test-host.greeting" (fun name -> durable { return! D.call greet name })

let app =
    durableApp {
        activity greet
        workflow greeting
    }

let runWithTestHost ctx =
    async {
        let! host = DurableTestHost.start ctx app
        let! completed = DurableTestHost.runUntilCompleted host greeting "Ada" "Hello, Ada!"
        do! host.cleanup ()
        return completed
    }

printfn "durable test-host example loaded"
