// Durable checkout flow.

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

let reserveInventory =
    Activity.define "checkout.reserve-inventory" (fun orderId -> async { return "reservation:" + orderId })

let chargePayment =
    Activity.define "checkout.charge-payment" (fun reservation -> async { return "charge:" + reservation })

let sendReceipt =
    Activity.define "checkout.send-receipt" (fun chargeId -> async { return "receipt:" + chargeId })

let checkout =
    Workflow.define "checkout.success" (fun orderId ->
        durable {
            let! reservation = D.call reserveInventory orderId
            let! chargeId = D.call chargePayment reservation
            return! D.call sendReceipt chargeId
        })

let app =
    durableApp {
        activity reserveInventory
        activity chargePayment
        activity sendReceipt
        workflow checkout
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
                  HostId = "checkout"
                  MaxRunUntilIdleTicks = Some 10 }

        let! start = client.start checkout "order-1001"
        let instanceId = startOrFail start
        let! _ = worker.runUntilIdle instanceId
        let! status = client.statusOf checkout instanceId
        printfn "%A" status
    }

Async.StartImmediate(run ())
