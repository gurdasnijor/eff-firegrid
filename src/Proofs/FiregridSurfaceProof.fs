namespace Eff.Proofs

open Eff
open Eff.Firegrid

module FiregridSurfaceProof =
    type FiregridSurfaceResult =
        { WorkflowCompleted: bool
          AllCompleted: bool
          BothCompleted: bool
          ClientResultReadsCompletion: bool
          AppCapturedDescriptors: bool }

    module Domain =
        let reserve orderId = async { return "reserved:" + orderId }

        let charge reservation =
            async { return "charged:" + reservation }

        let hello city = async { return "Hello, " + city }

    let private reserveStep = step "firegrid-reserve" Domain.reserve

    let private chargeStep = step "firegrid-charge" Domain.charge

    let private helloStep = step "firegrid-hello" Domain.hello

    let private checkout orderId =
        durable {
            let! reserved = call reserveStep orderId
            let! charged = call chargeStep reserved
            return charged
        }

    let private helloSequence (cities: string) =
        durable {
            let! greetings = cities.Split(",") |> Array.toList |> List.map (call helloStep) |> all

            return String.concat "|" greetings
        }

    let private reserveAndHello input =
        durable {
            let! reserved, greeting = both (call reserveStep input) (call helloStep input)
            return reserved + "|" + greeting
        }

    let private checkoutWorkflow = workflow "firegrid-checkout" checkout

    let private helloWorkflow = workflow "firegrid-hello-sequence" helloSequence

    let private bothWorkflow = workflow "firegrid-both" reserveAndHello

    let private app =
        firegrid {
            step reserveStep
            step chargeStep
            step helloStep
            workflow checkoutWorkflow
            workflow helloWorkflow
            workflow bothWorkflow
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
                let storage = s2.Client |> S2.basin basinName |> Storage.s2
                let host = Firegrid.testHostWith storage "fg-surface" app

                let! checkoutOutput = host.run checkoutWorkflow "order-1"
                let! allOutput = host.run helloWorkflow "Tokyo,Seattle,London"
                let! bothOutput = host.run bothWorkflow "order-2"
                let! checkoutStart = host.client.start checkoutWorkflow "order-3"

                let! clientResultReadsCompletion =
                    match checkoutStart with
                    | StartResult.Rejected _ -> async { return false }
                    | StartResult.Started instanceId ->
                        async {
                            do! host.worker.runUntilIdle instanceId
                            let! result = host.client.result checkoutWorkflow instanceId
                            return result = Some "charged:reserved:order-3"
                        }

                let workflowCompleted = checkoutOutput = "charged:reserved:order-1"
                let allCompleted = allOutput = "Hello, Tokyo|Hello, Seattle|Hello, London"
                let bothCompleted = bothOutput = "reserved:order-2|Hello, order-2"

                let appCapturedDescriptors =
                    FiregridApp.stepNames app = [ "firegrid-reserve"; "firegrid-charge"; "firegrid-hello" ]
                    && FiregridApp.workflowNames app = [ "firegrid-checkout"
                                                         "firegrid-hello-sequence"
                                                         "firegrid-both" ]

                let result =
                    { WorkflowCompleted = workflowCompleted
                      AllCompleted = allCompleted
                      BothCompleted = bothCompleted
                      ClientResultReadsCompletion = clientResultReadsCompletion
                      AppCapturedDescriptors = appCapturedDescriptors }

                do!
                    ctx.EmitSpan
                        "proof.firegrid_surface.completed"
                        [ "proof.property", "firegrid-surface"
                          "firegrid.workflow", string result.WorkflowCompleted
                          "firegrid.all", string result.AllCompleted
                          "firegrid.both", string result.BothCompleted
                          "firegrid.client_result", string result.ClientResultReadsCompletion
                          "firegrid.descriptors", string result.AppCapturedDescriptors ]

                return result
            })

    let surfaceProperty =
        property "firegrid-surface" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "workflow completes through public Firegrid surface" (fun result ->
                      result.WorkflowCompleted)
                  v.Expect.Workload "durable all completes first-class step calls" (fun result -> result.AllCompleted)
                  v.Expect.Workload "durable both completes two first-class step calls" (fun result ->
                      result.BothCompleted)
                  v.Expect.Workload "public client reads workflow result" (fun result ->
                      result.ClientResultReadsCompletion)
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
                                "AllCompleted"
                                "BothCompleted"
                                "ClientResultReadsCompletion"
                                "AppCapturedDescriptors" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "firegrid-surface" {
            describedAs
                "The F#-native Firegrid surface lets authors lift ordinary functions into first-class durable steps and compose them in durable workflows without touching registries or runtime internals."

            property surfaceProperty
        }
