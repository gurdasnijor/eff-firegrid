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

    let private checkoutWorkflow = workflow "firegrid-checkout" checkout

    let private helloWorkflow = workflow "firegrid-hello-sequence" helloSequence

    let private parallelBindWorkflow = workflow "firegrid-parallel-bind" reserveAndHello

    let private approvalWorkflow = workflow "firegrid-approval" approval

    let private app =
        firegrid {
            step reserveStep
            step chargeStep
            step helloStep
            workflow checkoutWorkflow
            workflow helloWorkflow
            workflow parallelBindWorkflow
            workflow approvalWorkflow
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

                let! checkoutOutput = host.runWith checkoutId checkoutWorkflow "order-1"
                let! parallelOutput = host.runWith parallelId helloWorkflow "Tokyo,Seattle,London"
                let! parallelBindOutput = host.runWith parallelBindId parallelBindWorkflow "order-2"
                let! localOutput = localHost.run checkoutWorkflow "local-order"
                let! localApprovalStatus = localHost.tryRun approvalWorkflow "local-approval"
                let! checkoutStart = host.client.startWith checkoutClientId checkoutWorkflow "order-3"
                let! approvalStart = host.client.startWith approvalId approvalWorkflow "order-4"

                let! clientResultReadsCompletion =
                    match checkoutStart with
                    | StartResult.Rejected _ -> async { return false }
                    | StartResult.Started instanceId ->
                        async {
                            do! host.worker.runUntilIdle instanceId
                            let! result = host.client.result checkoutWorkflow instanceId
                            return result = Some "charged:reserved:order-3"
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

                let localHostReportedWaiting =
                    match localApprovalStatus with
                    | WorkflowStatus.Waiting "signal:firegrid-approved" -> true
                    | _ -> false

                let expectedAppSteps: string list =
                    [ "firegrid-reserve"; "firegrid-charge"; "firegrid-hello" ]

                let expectedAppWorkflows: string list =
                    [ "firegrid-checkout"
                      "firegrid-hello-sequence"
                      "firegrid-parallel-bind"
                      "firegrid-approval" ]

                let appCapturedDescriptors =
                    (FiregridApp.stepNames app = expectedAppSteps)
                    && (FiregridApp.workflowNames app = expectedAppWorkflows)

                let result =
                    { WorkflowCompleted = workflowCompleted
                      ParallelCompleted = parallelCompleted
                      ParallelBindCompleted = parallelBindCompleted
                      LocalHostCompleted = localHostCompleted
                      LocalHostReportedWaiting = localHostReportedWaiting
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
                          "firegrid.client_result", string result.ClientResultReadsCompletion
                          "firegrid.signal", string result.SignalCompletesWorkflow
                          "firegrid.descriptors", string result.AppCapturedDescriptors ]

                do! deleteInstance basin checkoutId
                do! deleteInstance basin parallelId
                do! deleteInstance basin parallelBindId
                do! deleteInstance basin checkoutClientId
                do! deleteInstance basin approvalId

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
