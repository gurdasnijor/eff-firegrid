namespace Eff.Proofs

open Eff
open Eff.Firegrid

module FiregridSurfaceProof =
    type FiregridSurfaceResult =
        { WorkflowCompleted: bool
          AllCompleted: bool
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

    let private checkoutWorkflow = workflow "firegrid-checkout" checkout

    let private helloWorkflow = workflow "firegrid-hello-sequence" helloSequence

    let private app =
        firegrid {
            step reserveStep
            step chargeStep
            step helloStep
            workflow checkoutWorkflow
            workflow helloWorkflow
        }

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "firegrid.surface"
            "firegrid-surface"
            { ProofOperationOptions.empty with
                Key = Some "firegrid-surface" }
            (async {
                let! host = DurableTestHost.start ctx (FiregridApp.toDurableApp app)

                try
                    let! workflowCompleted =
                        DurableTestHost.runUntilCompleted
                            host
                            (Workflow.toDurableWorkflow checkoutWorkflow)
                            "order-1"
                            "charged:reserved:order-1"

                    let! allCompleted =
                        DurableTestHost.runUntilCompleted
                            host
                            (Workflow.toDurableWorkflow helloWorkflow)
                            "Tokyo,Seattle,London"
                            "Hello, Tokyo|Hello, Seattle|Hello, London"

                    let appCapturedDescriptors =
                        FiregridApp.stepNames app = [ "firegrid-reserve"; "firegrid-charge"; "firegrid-hello" ]
                        && FiregridApp.workflowNames app = [ "firegrid-checkout"; "firegrid-hello-sequence" ]

                    let result =
                        { WorkflowCompleted = workflowCompleted
                          AllCompleted = allCompleted
                          AppCapturedDescriptors = appCapturedDescriptors }

                    do!
                        ctx.EmitSpan
                            "proof.firegrid_surface.completed"
                            [ "proof.property", "firegrid-surface"
                              "firegrid.workflow", string result.WorkflowCompleted
                              "firegrid.all", string result.AllCompleted
                              "firegrid.descriptors", string result.AppCapturedDescriptors ]

                    do! host.cleanup ()
                    return result
                with error ->
                    do! host.cleanup ()
                    return raise error
            })

    let surfaceProperty =
        property "firegrid-surface" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "workflow completes through public Firegrid surface" (fun result ->
                      result.WorkflowCompleted)
                  v.Expect.Workload "durable all completes first-class step calls" (fun result -> result.AllCompleted)
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
                          OutputContains = [ "WorkflowCompleted"; "AllCompleted"; "AppCapturedDescriptors" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "firegrid-surface" {
            describedAs
                "The F#-native Firegrid surface lets authors lift ordinary functions into first-class durable steps and compose them in durable workflows without touching registries or runtime internals."

            property surfaceProperty
        }
