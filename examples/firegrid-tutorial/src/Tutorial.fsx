// F#-native Firegrid authoring tutorial.
//
// This script is compiled by `npm run check` as a public API smoke test.

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
#load "../../../src/Firegrid.fs"

open Eff.Firegrid

module Domain =
    let reserve orderId = async { return "reserved:" + orderId }

    let charge reservation =
        async { return "charged:" + reservation }

    let greet city = async { return "Hello, " + city }

let reserveStep = step "reserve" Domain.reserve

let chargeStep = step "charge" Domain.charge

let greetStep = step "greet" Domain.greet

let approved = signal "approved"

let checkout orderId =
    durable {
        let! reservation = call reserveStep orderId
        let! receipt = call chargeStep reservation
        return receipt
    }

let helloSequence (cities: string) =
    durable {
        let! greetings =
            cities.Split(",")
            |> Array.toList
            |> List.map (call greetStep)
            |> Durable.Parallel

        return String.concat " | " greetings
    }

let reserveAndGreet orderId =
    durable {
        let! reservation = call reserveStep orderId
        and! greeting = call greetStep orderId

        return reservation + " / " + greeting
    }

let approval orderId =
    durable {
        let! approver = waitForSignal approved
        return orderId + ":approved-by:" + approver
    }

let checkoutWorkflow = workflow "checkout" checkout

let helloWorkflow = workflow "hello-sequence" helloSequence

let reserveAndGreetWorkflow = workflow "reserve-and-greet" reserveAndGreet

let approvalWorkflow = workflow "approval" approval

let app =
    firegrid {
        step reserveStep
        step chargeStep
        step greetStep
        workflow checkoutWorkflow
        workflow helloWorkflow
        workflow reserveAndGreetWorkflow
        workflow approvalWorkflow
    }

let stepNames = FiregridApp.stepNames app

let workflowNames = FiregridApp.workflowNames app

let localHost = Firegrid.localTestHost app

let localPreview =
    async {
        let! receipt = localHost.run checkoutWorkflow "order-123"
        let! run = localHost.inspect checkoutWorkflow "order-124"
        let! approvalStatus = localHost.tryRun approvalWorkflow "order-124"
        let! _ = localHost.expectCompleted checkoutWorkflow "order-125" "charged:reserved:order-125"
        let! _ = localHost.expectWaiting approvalWorkflow "order-126" "signal:approved"
        let! _ = localHost.expectSteps checkoutWorkflow "order-127" [ Step.name reserveStep; Step.name chargeStep ]

        printfn "local checkout: %s" receipt
        printfn "local checkout steps: %A" (run.Steps |> List.map (fun step -> step.Name))
        printfn "local approval status: %A" approvalStatus
    }

printfn "steps: %A" stepNames
printfn "workflows: %A" workflowNames
