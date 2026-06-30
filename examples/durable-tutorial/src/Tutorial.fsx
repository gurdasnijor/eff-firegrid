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

open Eff
open Eff.Foundation.Durable
open Fable.Core

[<Emit("Date.now()")>]
let nowMillis () : int64 = jsNative

[<Emit("process.env[$0] || $1")>]
let envOr (_name: string) (_fallback: string) : string = jsNative

let requireOk =
    function
    | Ok value -> value
    | Error error -> failwith (string error)

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

let workflows =
    WorkflowRegistry.empty
    |> WorkflowRegistry.register "checkout" (fun orderId ->
        durable {
            let! reserved = Workflow.call "reserve" orderId
            let! charged = Workflow.call "charge" reserved
            return charged
        })
    |> Result.bind (
        WorkflowRegistry.register "approval" (fun orderId ->
            durable {
                let! approvedBy = Workflow.waitForSignal "approved"
                return orderId + ":approved-by:" + approvedBy
            })
    )
    |> Result.bind (
        WorkflowRegistry.register "approval-or-timeout" (fun deadlineText ->
            durable {
                let deadline = int64 deadlineText

                let! winner = Workflow.any [ DurableTask.signal "approved"; DurableTask.timer deadline ]

                match winner with
                | EventWon(_, Signal "approved", approver) -> return "approved:" + approver
                | EventWon(_, Timer _, _) -> return "timed-out"
                | ActivityWon _ -> return "unexpected"
                | EventWon(_, Signal name, _) -> return "unexpected-signal:" + name
            })
    )
    |> requireOk

let activities =
    ActivityRegistry.empty
    |> ActivityRegistry.register "reserve" (fun orderId -> async { return "reserved:" + orderId })
    |> Result.bind (ActivityRegistry.register "charge" (fun reservation -> async { return "charged:" + reservation }))
    |> requireOk

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

        let runtime =
            DurableRuntime.create
                { DurableRuntimeOptions.create "tutorial-host" with
                    MaxRunUntilIdleTicks = 10 }
                basin
                workflows
                activities

        printfn "using basin: %s" basinName

        let! checkoutStart = runtime.Client.Start (WorkflowName.create "checkout") "order-1"
        let checkoutId = startInstance checkoutStart
        printfn "checkout instance: %s" (instanceText checkoutId)

        let! checkoutTicks = runtime.Host.RunUntilIdle checkoutId
        printfn "checkout ticks: %d" (List.length checkoutTicks)

        let! checkoutStatus = runtime.Client.GetStatus checkoutId
        printfn "checkout status: %s" (statusText checkoutStatus)

        let approvalId = InstanceId.create ("approval-" + string (nowMillis ()))
        let! _ = runtime.Client.StartWith approvalId (WorkflowName.create "approval") "order-2"
        let! _ = runtime.Host.RunUntilIdle approvalId

        let! beforeApproval = runtime.Client.GetStatus approvalId
        printfn "approval before signal: %s" (statusText beforeApproval)

        let! _ = runtime.Client.RaiseSignal approvalId "approved" "alice"
        let! _ = runtime.Host.RunUntilIdle approvalId

        let! afterApproval = runtime.Client.GetStatus approvalId
        printfn "approval after signal: %s" (statusText afterApproval)

        let timeoutId = InstanceId.create ("approval-timeout-" + string (nowMillis ()))
        let pastDeadline = nowMillis () - 1L
        let! _ = runtime.Client.StartWith timeoutId (WorkflowName.create "approval-or-timeout") (string pastDeadline)
        let! _ = runtime.Host.RunUntilIdle timeoutId

        let! timeoutStatus = runtime.Client.GetStatus timeoutId
        printfn "timeout status: %s" (statusText timeoutStatus)

        do! deleteInstance basin timeoutId
        do! deleteInstance basin approvalId
        do! deleteInstance basin checkoutId
    }

Async.StartImmediate tutorial
