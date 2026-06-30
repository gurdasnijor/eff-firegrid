// Durable runtime tutorial.
//
// Run with:
//   EFF_FIREGRID_BASIN=test-basin-885234 \
//     dotnet fable examples/durable-tutorial/src/Tutorial.fsx --outDir build_examples --runScript

#r "nuget: Fable.Core, 5.1.0"

#load "../../../src/S2/Interop.fs"
#load "../../../src/S2/Errors.fs"
#load "../../../src/S2/Client.fs"
#load "../../../src/S2/Cli.fs"
#load "../../../src/Foundation/Durable/Semantics.fs"
#load "../../../src/Foundation/Durable/Api.fs"
#load "../../../src/Foundation/Durable/Registry.fs"
#load "../../../src/Foundation/Durable/S2Substrate.fs"
#load "../../../src/Foundation/Durable/Stepper.fs"
#load "../../../src/Foundation/Durable/StepRecordCodec.fs"
#load "../../../src/Foundation/Durable/Client.fs"
#load "../../../src/Foundation/Durable/CommandDispatch.fs"
#load "../../../src/Foundation/Durable/ActivityAdapter.fs"
#load "../../../src/Foundation/Durable/InboxFold.fs"
#load "../../../src/Foundation/Durable/TimerAdapter.fs"
#load "../../../src/Foundation/Durable/Host.fs"
#load "../../../src/Foundation/Durable/Runtime.fs"
#load "../../../src/Foundation/Durable/App.fs"

open Eff
open Eff.Foundation.Durable
open Eff.Foundation.Durable.App
open Fable.Core

[<Emit("Date.now()")>]
let nowMillis () : int64 = jsNative

[<Emit("process.env[$0] || $1")>]
let envOr (_name: string) (_fallback: string) : string = jsNative

let workflowText (WorkflowName name) = name

let instanceText (instanceId: InstanceId) = InstanceId.value instanceId

let needText =
    function
    | NeedsActivity activity -> "activity:" + activity.Name
    | NeedsActivities activities -> "activities:" + string (List.length activities)
    | NeedsEvent(Timer deadline) -> "timer:" + string deadline
    | NeedsEvent(Signal name) -> "signal:" + name
    | NeedsRace tasks -> "race:" + string (List.length tasks)
    | NeedsTimerCancellation timers -> "cancel-timers:" + string (List.length timers)
    | NeedsCurrentTime -> "current-time"
    | NeedsLog message -> "log:" + message

let statusText =
    function
    | DurableClientStatusRead.Succeeded InstanceNotFound -> "not-found"
    | DurableClientStatusRead.Succeeded(InstanceRunning workflow) -> "running:" + workflowText workflow
    | DurableClientStatusRead.Succeeded(InstanceWaiting(workflow, _, need)) ->
        "waiting:" + workflowText workflow + ":" + needText need
    | DurableClientStatusRead.Succeeded(InstanceCompleted(workflow, payload)) ->
        "completed:" + workflowText workflow + ":" + payload
    | DurableClientStatusRead.Failed failure -> "failed:" + string failure

let startInstance =
    function
    | DurableClientStartStatus.Accepted ack -> ack.InstanceId
    | DurableClientStartStatus.Failed failure -> failwith ("start failed: " + string failure)

module D = Eff.Foundation.Durable.App.Durable
module T = Eff.Foundation.Durable.App.DurableTask

let reserve =
    Activity.define "reserve" (fun orderId -> async { return "reserved:" + orderId })

let charge =
    Activity.define "charge" (fun reservation -> async { return "charged:" + reservation })

let approved = Signal.define "approved"

let checkout =
    Workflow.define "checkout" (fun orderId ->
        durable {
            let! reserved = D.call reserve orderId
            let! charged = D.call charge reserved
            return charged
        })

let approval =
    Workflow.define "approval" (fun orderId ->
        durable {
            let! approvedBy = D.waitForSignal approved
            return orderId + ":approved-by:" + approvedBy
        })

let approvalOrTimeout =
    Workflow.define "approval-or-timeout" (fun deadlineText ->
        durable {
            let deadline = int64 deadlineText

            let! winner = D.any [ T.signal approved; T.timer deadline ]

            match winner with
            | EventWon(_, Signal "approved", approver) -> return "approved:" + approver
            | EventWon(_, Timer _, _) -> return "timed-out"
            | ActivityWon _ -> return "unexpected"
            | EventWon(_, Signal name, _) -> return "unexpected-signal:" + name
        })

let app =
    durableApp {
        activity reserve
        activity charge
        workflow checkout
        workflow approval
        workflow approvalOrTimeout
    }

let deleteInstance basin instanceId =
    async {
        let key = DurableClient.instanceKey instanceId
        do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
        do! basin |> S2.deleteStream (StorageKey.logStreamName key)
    }

let tutorial =
    async {
        let basinName = envOr "EFF_FIREGRID_BASIN" "test-basin-885234"
        let s2 = S2Cli.connect ()
        let basin = s2 |> S2.basin basinName

        let storage = DurableStorage.s2 basin

        let client = app |> DurableApp.clientWith { Storage = storage }

        let worker =
            app
            |> DurableApp.workerWith
                { Storage = storage
                  HostId = "tutorial"
                  MaxRunUntilIdleTicks = Some 10 }

        printfn "using basin: %s" basinName

        let! checkoutStart = client.start checkout "order-1"
        let checkoutId = startInstance checkoutStart
        printfn "checkout instance: %s" (instanceText checkoutId)

        let! checkoutTicks = worker.runUntilIdle checkoutId
        printfn "checkout ticks: %d" (List.length checkoutTicks)

        let! checkoutStatus = client.status checkoutId
        printfn "checkout status: %s" (statusText checkoutStatus)

        let approvalId = InstanceId.create ("approval-" + string (nowMillis ()))
        let! _ = client.startWith approvalId approval "order-2"
        let! _ = worker.runUntilIdle approvalId

        let! beforeApproval = client.status approvalId
        printfn "approval before signal: %s" (statusText beforeApproval)

        let! _ = client.signal approvalId approved "alice"
        let! _ = worker.runUntilIdle approvalId

        let! afterApproval = client.status approvalId
        printfn "approval after signal: %s" (statusText afterApproval)

        let timeoutId = InstanceId.create ("approval-timeout-" + string (nowMillis ()))
        let pastDeadline = nowMillis () - 1L
        let! _ = client.startWith timeoutId approvalOrTimeout (string pastDeadline)
        let! _ = worker.runUntilIdle timeoutId

        let! timeoutStatus = client.status timeoutId
        printfn "timeout status: %s" (statusText timeoutStatus)

        do! deleteInstance basin timeoutId
        do! deleteInstance basin approvalId
        do! deleteInstance basin checkoutId
    }

Async.StartImmediate tutorial
