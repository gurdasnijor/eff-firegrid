namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable
open Eff.Foundation.Durable.App
open Fable.Core

module DurableEnvironmentBootstrapProof =
    module D = Eff.Foundation.Durable.App.Durable

    [<Emit("process.env[$0] || ''")>]
    let private getEnv (_name: string) : string = jsNative

    [<Emit("process.env[$0] = $1")>]
    let private setEnv (_name: string) (_value: string) : unit = jsNative

    [<Emit("delete process.env[$0]")>]
    let private deleteEnv (_name: string) : unit = jsNative

    type DurableEnvironmentBootstrapResult =
        { EnvironmentClientStartsWorkflow: bool
          EnvironmentWorkerCompletesWorkflow: bool
          EnvironmentStatusReadsCompletion: bool
          MissingBasinFailsBeforeBootstrap: bool }

    let private addOne =
        Activity.defineWith "env-add-one" string int string int (fun value -> async { return value + 1 })

    let private typedMath =
        Workflow.defineWith "env-typed-math" string int string int (fun value ->
            durable {
                let! incremented = D.call addOne value
                return incremented + 10
            })

    let private app =
        durableApp {
            activity addOne
            workflow typedMath
        }

    let private preserve names =
        names |> List.map (fun name -> name, getEnv name)

    let private restore preserved =
        for name, value in preserved do
            if System.String.IsNullOrWhiteSpace value then
                deleteEnv name
            else
                setEnv name value

    let private deleteInstance basin instanceId =
        async {
            let key = DurableClient.instanceKey instanceId
            do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
            do! basin |> S2.deleteStream (StorageKey.logStreamName key)
        }

    let private startedInstance =
        function
        | DurableAppStartResult.Started instanceId -> Some instanceId
        | DurableAppStartResult.Rejected _ -> None

    let private lastStatus =
        function
        | [] -> None
        | statuses -> statuses |> List.rev |> List.tryHead

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.environment_bootstrap"
            "durable-environment-bootstrap"
            { ProofOperationOptions.empty with
                Key = Some "durable-environment-bootstrap" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let endpoint =
                    match s2.Endpoint with
                    | Some endpoint -> endpoint
                    | None -> failwith "environment bootstrap proof requires s2Lite endpoint"

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-env-bootstrap-" + suffix
                let environment = "bootstrap"

                let envKeys =
                    [ "EFF_FIREGRID_BOOTSTRAP_BASIN"
                      "EFF_FIREGRID_BOOTSTRAP_ACCESS_TOKEN"
                      "EFF_FIREGRID_BOOTSTRAP_S2_ACCOUNT_ENDPOINT"
                      "EFF_FIREGRID_BOOTSTRAP_S2_BASIN_ENDPOINT"
                      "EFF_FIREGRID_MISSING_BASIN"
                      "EFF_FIREGRID_BASIN" ]

                let preserved = preserve envKeys

                try
                    let! _ = s2.Client |> S2.createBasin basinName
                    let basin = s2.Client |> S2.basin basinName

                    setEnv "EFF_FIREGRID_BOOTSTRAP_BASIN" basinName
                    setEnv "EFF_FIREGRID_BOOTSTRAP_ACCESS_TOKEN" "durable-env-bootstrap-proof"
                    setEnv "EFF_FIREGRID_BOOTSTRAP_S2_ACCOUNT_ENDPOINT" endpoint
                    setEnv "EFF_FIREGRID_BOOTSTRAP_S2_BASIN_ENDPOINT" endpoint

                    deleteEnv "EFF_FIREGRID_MISSING_BASIN"
                    deleteEnv "EFF_FIREGRID_BASIN"

                    let client =
                        app
                        |> DurableApp.client (
                            { Environment = environment
                              BasinName = None }
                            : DurableAppEnvironmentClientConfig
                        )

                    let worker =
                        app
                        |> DurableApp.worker (
                            { Environment = environment
                              BasinName = None
                              HostId = "env-proof"
                              MaxRunUntilIdleTicks = Some 10 }
                            : DurableAppEnvironmentWorkerConfig
                        )

                    let instance = InstanceId.create ("env-typed-math-" + suffix)
                    let! start = client.startWith instance typedMath 31
                    let! ticks = worker.runUntilIdle instance
                    let! status = client.status instance

                    let missingBasinFailsBeforeBootstrap =
                        try
                            let _ =
                                app
                                |> DurableApp.client (
                                    { Environment = "missing"
                                      BasinName = None }
                                    : DurableAppEnvironmentClientConfig
                                )

                            false
                        with _ ->
                            true

                    do! deleteInstance basin instance

                    let result =
                        { EnvironmentClientStartsWorkflow = startedInstance start = Some instance
                          EnvironmentWorkerCompletesWorkflow =
                            match lastStatus ticks with
                            | Some(DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed("42", _))) -> true
                            | _ -> false
                          EnvironmentStatusReadsCompletion =
                            status = DurableAppWorkflowStatus.Completed("env-typed-math", "42")
                          MissingBasinFailsBeforeBootstrap = missingBasinFailsBeforeBootstrap }

                    do!
                        ctx.EmitSpan
                            "proof.durable_environment_bootstrap.completed"
                            [ "proof.property", "durable-environment-bootstrap"
                              "environment.client", string result.EnvironmentClientStartsWorkflow
                              "environment.worker", string result.EnvironmentWorkerCompletesWorkflow
                              "environment.status", string result.EnvironmentStatusReadsCompletion
                              "environment.missing", string result.MissingBasinFailsBeforeBootstrap ]

                    restore preserved

                    return result
                with error ->
                    restore preserved
                    return raise error
            })

    let environmentBootstrapProperty =
        property "durable-environment-bootstrap" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "environment client starts workflow" (fun result ->
                      result.EnvironmentClientStartsWorkflow)
                  v.Expect.Workload "environment worker completes workflow" (fun result ->
                      result.EnvironmentWorkerCompletesWorkflow)
                  v.Expect.Workload "environment status reads completion" (fun result ->
                      result.EnvironmentStatusReadsCompletion)
                  v.Expect.Workload "missing basin fails before bootstrap" (fun result ->
                      result.MissingBasinFailsBeforeBootstrap)
                  v.Trace.SpanExists
                      "durable environment bootstrap proof span emitted"
                      "proof.durable_environment_bootstrap.completed"
                      [ "proof.property", "durable-environment-bootstrap" ]
                  v.Trace.Operation
                      "durable environment bootstrap operation was recorded"
                      ({ TraceOperationMatch.named "durable.environment_bootstrap" with
                          Status = Some "ok"
                          OutputContains =
                              [ "EnvironmentClientStartsWorkflow"
                                "EnvironmentStatusReadsCompletion"
                                "MissingBasinFailsBeforeBootstrap" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-environment-bootstrap" {
            describedAs
                "DurableApp.client and DurableApp.worker resolve environment-specific S2 bootstrap settings before returning app client and worker handles."

            property environmentBootstrapProperty
        }
