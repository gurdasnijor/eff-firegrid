// Durable external signal flow.

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

let approved = Signal.define "approval.approved"

let approval =
    Workflow.define "approval.wait" (fun requestId ->
        durable {
            let! approver = D.waitForSignal approved
            return requestId + ":approved-by:" + approver
        })

let app = durableApp { workflow approval }

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
                  HostId = "approval"
                  MaxRunUntilIdleTicks = Some 10 }

        let! start = client.start approval "request-7"
        let instanceId = startOrFail start
        let! _ = worker.runUntilIdle instanceId
        let! waiting = client.statusOf approval instanceId
        printfn "%A" waiting

        let! _ = client.signal instanceId approved "alice"
        let! _ = worker.runUntilIdle instanceId
        let! completed = client.statusOf approval instanceId
        printfn "%A" completed
    }

Async.StartImmediate(run ())
