namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable
open Eff.Firegrid

module FiregridSurfaceProof =
    type FiregridSurfaceResult =
        { WorkflowCompleted: bool
          ParallelCompleted: bool
          ParallelBindCompleted: bool
          LocalHostCompleted: bool
          LocalHostReportedWaiting: bool
          CliStepCompleted: bool
          ServeCompletesWorkflow: bool
          ClientResultReadsCompletion: bool
          SignalCompletesWorkflow: bool
          AppCapturedDescriptors: bool }

    module Domain =
        let reserve orderId = async { return "reserved:" + orderId }

        let charge reservation =
            async { return "charged:" + reservation }

        let hello city = async { return "Hello, " + city }

    let private reserveStep = step "firegrid-reserve" Domain.reserve

    let private chargeStep = step "firegrid-charge" Domain.charge

    let private helloStep = step "firegrid-hello" Domain.hello

    let private cliEchoStep =
        cliStep
            "firegrid-cli-echo"
            (CliStepConfig.create "node"
             |> CliStepConfig.withArgs
                 [ "-e"
                   "let input = ''; process.stdin.setEncoding('utf8'); process.stdin.on('data', chunk => input += chunk); process.stdin.on('end', () => process.stdout.write('cli:' + input.trim().toUpperCase()));" ]
             |> CliStepConfig.withTimeoutMillis 5000)

    let private approvedSignal = signal "firegrid-approved"

    let private checkout orderId =
        durable {
            let! reserved = call reserveStep orderId
            let! charged = call chargeStep reserved
            return charged
        }

    let private helloSequence (cities: string) =
        durable {
            let! greetings =
                cities.Split(",")
                |> Array.toList
                |> List.map (call helloStep)
                |> Durable.Parallel

            return String.concat "|" greetings
        }

    let private reserveAndHello input =
        durable {
            let! reserved = call reserveStep input
            and! greeting = call helloStep input

            return reserved + "|" + greeting
        }

    let private approval orderId =
        durable {
            let! approver = waitForSignal approvedSignal
            return orderId + ":approved-by:" + approver
        }

    let private cliEcho input =
        durable {
            let! output = call cliEchoStep input
            return output
        }

    let private checkoutWorkflow = workflow "firegrid-checkout" checkout

    let private helloWorkflow = workflow "firegrid-hello-sequence" helloSequence

    let private parallelBindWorkflow = workflow "firegrid-parallel-bind" reserveAndHello

    let private approvalWorkflow = workflow "firegrid-approval" approval

    let private cliEchoWorkflow = workflow "firegrid-cli-echo" cliEcho

    let private app =
        firegrid {
            step reserveStep
            step chargeStep
            step helloStep
            step cliEchoStep
            workflow checkoutWorkflow
            workflow helloWorkflow
            workflow parallelBindWorkflow
            workflow approvalWorkflow
            workflow cliEchoWorkflow
        }

    let private deleteInstance basin instanceId =
        async {
            let key = DurableClient.instanceKey instanceId
            do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
            do! basin |> S2.deleteStream (StorageKey.logStreamName key)
        }

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "firegrid.surface"
            "firegrid-surface"
            { ProofOperationOptions.empty with
                Key = Some "firegrid-surface" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx
                let basinName = "firegrid-surface-" + string (int64 (Reports.nowMillis ()))
                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                let storage = basin |> Storage.s2
                let host = Firegrid.testHostWith storage "fg-surface" app
                let localHost = Firegrid.localTestHost app

                let instance suffix =
                    InstanceId.create (basinName + "-" + suffix)

                let checkoutId = instance "checkout-1"
                let parallelId = instance "parallel"
                let parallelBindId = instance "parallel-bind"
                let checkoutClientId = instance "checkout-2"
                let approvalId = instance "approval"
                let serveId = instance "serve"
                let cliId = instance "cli"

                let! checkoutOutput = host.runWith checkoutId checkoutWorkflow "order-1"
                let! parallelOutput = host.runWith parallelId helloWorkflow "Tokyo,Seattle,London"
                let! parallelBindOutput = host.runWith parallelBindId parallelBindWorkflow "order-2"
                let! cliOutput = host.runWith cliId cliEchoWorkflow "hello from process"
                let! localOutput = localHost.run checkoutWorkflow "local-order"
                let! localApprovalStatus = localHost.tryRun approvalWorkflow "local-approval"
                let! checkoutStart = host.client.startWith checkoutClientId checkoutWorkflow "order-3"
                let! approvalStart = host.client.startWith approvalId approvalWorkflow "order-4"

                let waitForResult attempts instanceId =
                    let rec loop remaining =
                        async {
                            let! result = host.client.result checkoutWorkflow instanceId

                            match result, remaining with
                            | Some output, _ -> return Some output
                            | None, 0 -> return None
                            | None, _ ->
                                do! Async.Sleep 100
                                return! loop (remaining - 1)
                        }

                    loop attempts

                let! clientResultReadsCompletion =
                    match checkoutStart with
                    | StartResult.Rejected _ -> async { return false }
                    | StartResult.Started instanceId ->
                        async {
                            do! host.worker.runUntilIdle instanceId
                            let! result = host.client.result checkoutWorkflow instanceId
                            return result = Some "charged:reserved:order-3"
                        }

                let! serveCompletesWorkflow =
                    async {
                        use cts = new System.Threading.CancellationTokenSource()

                        let serveConfig =
                            { ServeConfig.create storage "fg-serve" with
                                MaxRunUntilIdleTicks = Some 10
                                CancellationToken = Some cts.Token }

                        Firegrid.serveWith serveConfig app |> Async.StartImmediate

                        let! start = host.client.startWith serveId checkoutWorkflow "order-5"

                        let! result =
                            match start with
                            | StartResult.Rejected _ -> async { return None }
                            | StartResult.Started instanceId -> waitForResult 20 instanceId

                        cts.Cancel()

                        return result = Some "charged:reserved:order-5"
                    }

                let! signalCompletesWorkflow =
                    match approvalStart with
                    | StartResult.Rejected _ -> async { return false }
                    | StartResult.Started instanceId ->
                        async {
                            do! host.worker.runUntilIdle instanceId
                            let! signalResult = host.client.signal instanceId approvedSignal "alice"
                            do! host.worker.runUntilIdle instanceId
                            let! result = host.client.result approvalWorkflow instanceId
                            return signalResult = Ok() && result = Some "order-4:approved-by:alice"
                        }

                let workflowCompleted = checkoutOutput = "charged:reserved:order-1"
                let parallelCompleted = parallelOutput = "Hello, Tokyo|Hello, Seattle|Hello, London"
                let parallelBindCompleted = parallelBindOutput = "reserved:order-2|Hello, order-2"
                let localHostCompleted = localOutput = "charged:reserved:local-order"
                let cliStepCompleted = cliOutput = "cli:HELLO FROM PROCESS"

                let localHostReportedWaiting =
                    match localApprovalStatus with
                    | WorkflowStatus.Waiting "signal:firegrid-approved" -> true
                    | _ -> false

                let expectedAppSteps: string list =
                    [ "firegrid-reserve"; "firegrid-charge"; "firegrid-hello"; "firegrid-cli-echo" ]

                let expectedAppWorkflows: string list =
                    [ "firegrid-checkout"
                      "firegrid-hello-sequence"
                      "firegrid-parallel-bind"
                      "firegrid-approval"
                      "firegrid-cli-echo" ]

                let appCapturedDescriptors =
                    (FiregridApp.stepNames app = expectedAppSteps)
                    && (FiregridApp.workflowNames app = expectedAppWorkflows)

                let result =
                    { WorkflowCompleted = workflowCompleted
                      ParallelCompleted = parallelCompleted
                      ParallelBindCompleted = parallelBindCompleted
                      LocalHostCompleted = localHostCompleted
                      LocalHostReportedWaiting = localHostReportedWaiting
                      CliStepCompleted = cliStepCompleted
                      ServeCompletesWorkflow = serveCompletesWorkflow
                      ClientResultReadsCompletion = clientResultReadsCompletion
                      SignalCompletesWorkflow = signalCompletesWorkflow
                      AppCapturedDescriptors = appCapturedDescriptors }

                do!
                    ctx.EmitSpan
                        "proof.firegrid_surface.completed"
                        [ "proof.property", "firegrid-surface"
                          "firegrid.workflow", string result.WorkflowCompleted
                          "firegrid.parallel", string result.ParallelCompleted
                          "firegrid.parallel_bind", string result.ParallelBindCompleted
                          "firegrid.local_host", string result.LocalHostCompleted
                          "firegrid.local_waiting", string result.LocalHostReportedWaiting
                          "firegrid.cli_step", string result.CliStepCompleted
                          "firegrid.serve", string result.ServeCompletesWorkflow
                          "firegrid.client_result", string result.ClientResultReadsCompletion
                          "firegrid.signal", string result.SignalCompletesWorkflow
                          "firegrid.descriptors", string result.AppCapturedDescriptors ]

                do! deleteInstance basin checkoutId
                do! deleteInstance basin parallelId
                do! deleteInstance basin parallelBindId
                do! deleteInstance basin checkoutClientId
                do! deleteInstance basin approvalId
                do! deleteInstance basin serveId
                do! deleteInstance basin cliId

                return result
            })

    let surfaceProperty =
        property "firegrid-surface" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "workflow completes through public Firegrid surface" (fun result ->
                      result.WorkflowCompleted)
                  v.Expect.Workload "Durable.Parallel completes first-class step calls" (fun result ->
                      result.ParallelCompleted)
                  v.Expect.Workload "durable and! completes two first-class step calls" (fun result ->
                      result.ParallelBindCompleted)
                  v.Expect.Workload "local Firegrid test host completes public workflow" (fun result ->
                      result.LocalHostCompleted)
                  v.Expect.Workload "local Firegrid test host reports external waits" (fun result ->
                      result.LocalHostReportedWaiting)
                  v.Expect.Workload "cliStep completes through the public workflow surface" (fun result ->
                      result.CliStepCompleted)
                  v.Expect.Workload "Firegrid.serveWith completes public workflow" (fun result ->
                      result.ServeCompletesWorkflow)
                  v.Expect.Workload "public client reads workflow result" (fun result ->
                      result.ClientResultReadsCompletion)
                  v.Expect.Workload "public client signal completes waiting workflow" (fun result ->
                      result.SignalCompletesWorkflow)
                  v.Expect.Workload "firegrid app captures step and workflow descriptors" (fun result ->
                      result.AppCapturedDescriptors)
                  v.Trace.SpanExists
                      "firegrid surface proof span emitted"
                      "proof.firegrid_surface.completed"
                      [ "proof.property", "firegrid-surface" ]
                  v.Trace.Operation
                      "firegrid surface operation was recorded"
                      ({ TraceOperationMatch.named "firegrid.surface" with
                          Status = Some "ok"
                          OutputContains =
                              [ "WorkflowCompleted"
                                "ParallelCompleted"
                                "ParallelBindCompleted"
                                "LocalHostCompleted"
                                "LocalHostReportedWaiting"
                                "CliStepCompleted"
                                "ServeCompletesWorkflow"
                                "ClientResultReadsCompletion"
                                "SignalCompletesWorkflow"
                                "AppCapturedDescriptors" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "firegrid-surface" {
            describedAs
                "The F#-native Firegrid surface lets authors lift ordinary functions into first-class durable steps and compose them in durable workflows without touching registries or runtime internals."

            property surfaceProperty
        }
