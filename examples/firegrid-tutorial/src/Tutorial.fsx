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

let uppercaseStep =
    cliStep
        "uppercase"
        (CliStepConfig.create "node"
         |> CliStepConfig.withArgs
             [ "-e"
               "let input = ''; process.stdin.setEncoding('utf8'); process.stdin.on('data', chunk => input += chunk); process.stdin.on('end', () => process.stdout.write(input.trim().toUpperCase()));" ]
         |> CliStepConfig.withTimeoutMillis 5000)

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

let uppercase input =
    durable { return! call uppercaseStep input }

let checkoutWorkflow = workflow "checkout" checkout

let helloWorkflow = workflow "hello-sequence" helloSequence

let reserveAndGreetWorkflow = workflow "reserve-and-greet" reserveAndGreet

let approvalWorkflow = workflow "approval" approval

let uppercaseWorkflow = workflow "uppercase" uppercase

let app =
    firegrid {
        step reserveStep
        step chargeStep
        step greetStep
        step uppercaseStep
        workflow checkoutWorkflow
        workflow helloWorkflow
        workflow reserveAndGreetWorkflow
        workflow approvalWorkflow
        workflow uppercaseWorkflow
    }

let stepNames = FiregridApp.stepNames app

let workflowNames = FiregridApp.workflowNames app

let localHost = Firegrid.localTestHost app

let localPreview =
    async {
        let! receipt = localHost.run checkoutWorkflow "order-123"
        let! upper = localHost.run uppercaseWorkflow "hello from cli"
        let! approvalStatus = localHost.tryRun approvalWorkflow "order-124"

        printfn "local checkout: %s" receipt
        printfn "local cli step: %s" upper
        printfn "local approval status: %A" approvalStatus
    }

let serveFromEnvironment =
    Firegrid.serveWith (ServeConfig.environment "dev" "tutorial") app

printfn "steps: %A" stepNames
printfn "workflows: %A" workflowNames
