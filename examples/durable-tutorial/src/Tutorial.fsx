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
#load "../../../src/Foundation/Durable/App/Storage.fs"
#load "../../../src/Foundation/Durable/App/Definitions.fs"
#load "../../../src/Foundation/Durable/App/DurableFacade.fs"
#load "../../../src/Foundation/Durable/App/Environment.fs"
#load "../../../src/Foundation/Durable/App/Client.fs"
#load "../../../src/Foundation/Durable/App/Worker.fs"
#load "../../../src/Foundation/Durable/App/Builder.fs"

open Eff
open Eff.Foundation.Durable
open Eff.Foundation.Durable.App
open Fable.Core

[<Emit("Date.now()")>]
let nowMillis () : int64 = jsNative

[<Emit("process.env[$0] || ''")>]
let env (_name: string) : string = jsNative

let isBlank value = System.String.IsNullOrWhiteSpace value

let requireEnv name =
    let value = env name

    if isBlank value then
        failwith ("missing required environment variable: " + name)
    else
        value

let instanceText (instanceId: InstanceId) = InstanceId.value instanceId

let needText =
    function
    | DurableAppNeed.Activity name -> "activity:" + name
    | DurableAppNeed.Activities names -> "activities:" + string (List.length names)
    | DurableAppNeed.Timer deadline -> "timer:" + string deadline
    | DurableAppNeed.Signal name -> "signal:" + name
    | DurableAppNeed.Race contenders -> "race:" + string (List.length contenders)
    | DurableAppNeed.TimerCancellation count -> "cancel-timers:" + string count
    | DurableAppNeed.CurrentTime -> "current-time"
    | DurableAppNeed.Log message -> "log:" + message

let statusText =
    function
    | DurableAppWorkflowStatus.NotFound -> "not-found"
    | DurableAppWorkflowStatus.Running workflow -> "running:" + workflow
    | DurableAppWorkflowStatus.Waiting(workflow, need) -> "waiting:" + workflow + ":" + needText need
    | DurableAppWorkflowStatus.Completed(workflow, payload) -> "completed:" + workflow + ":" + payload
    | DurableAppWorkflowStatus.Failed failure -> "failed:" + string failure

let typedIntStatusText =
    function
    | DurableAppTypedWorkflowStatus.NotFound -> "not-found"
    | DurableAppTypedWorkflowStatus.Running -> "running"
    | DurableAppTypedWorkflowStatus.Waiting need -> "waiting:" + needText need
    | DurableAppTypedWorkflowStatus.Completed value -> "completed:" + string value
    | DurableAppTypedWorkflowStatus.Failed failure -> "failed:" + string failure

let startInstance =
    function
    | DurableAppStartResult.Started instanceId -> instanceId
    | DurableAppStartResult.Rejected failure -> failwith ("start failed: " + string failure)

module D = Eff.Foundation.Durable.App.Durable

let reserve =
    Activity.define "reserve" (fun orderId -> async { return "reserved:" + orderId })

let charge =
    Activity.define "charge" (fun reservation -> async { return "charged:" + reservation })

let addOne =
    Activity.defineWith "add-one" string int string int (fun value -> async { return value + 1 })

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

            return!
                D.anyOf
                    [ D.raceSignal approved (fun approver -> "approved:" + approver)
                      D.raceTimer deadline "timed-out" ]
        })

let typedMath =
    Workflow.defineWith "typed-math" string int string int (fun value ->
        durable {
            let! incremented = D.call addOne value
            return incremented + 10
        })

let app =
    durableApp {
        activity reserve
        activity charge
        activity addOne
        workflow checkout
        workflow approval
        workflow approvalOrTimeout
        workflow typedMath
    }

let deleteInstance basin instanceId =
    async {
        let key = DurableClient.instanceKey instanceId
        do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
        do! basin |> S2.deleteStream (StorageKey.logStreamName key)
    }

let tutorial =
    async {
        let environment = "tutorial"

        let basinName =
            let tutorialBasin = env "EFF_FIREGRID_TUTORIAL_BASIN"

            if isBlank tutorialBasin then
                requireEnv "EFF_FIREGRID_BASIN"
            else
                tutorialBasin

        let s2 = S2Cli.connect ()
        let basin = s2 |> S2.basin basinName

        let client =
            app
            |> DurableApp.client
                { Environment = environment
                  BasinName = None }

        let worker =
            app
            |> DurableApp.worker
                { Environment = environment
                  BasinName = None
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

        let typedMathId = InstanceId.create ("typed-math-" + string (nowMillis ()))
        let! _ = client.startWith typedMathId typedMath 31
        let! _ = worker.runUntilIdle typedMathId

        let! typedMathStatus = client.statusOf typedMath typedMathId
        printfn "typed math status: %s" (typedIntStatusText typedMathStatus)

        do! deleteInstance basin typedMathId
        do! deleteInstance basin timeoutId
        do! deleteInstance basin approvalId
        do! deleteInstance basin checkoutId
    }

Async.StartImmediate tutorial
