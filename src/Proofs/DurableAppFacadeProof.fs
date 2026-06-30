namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable
open Eff.Foundation.Durable.App

module DurableAppFacadeProof =
    module D = Eff.Foundation.Durable.App.Durable
    module T = Eff.Foundation.Durable.App.DurableTask

    type DurableAppFacadeResult =
        { TypedStartReturnsInstance: bool
          WorkerCompletesActivityWorkflow: bool
          ClientStatusReadsCompletion: bool
          ClientSignalCompletesWorkflow: bool
          RaceWorkflowCompletesFromTypedSignal: bool
          CodecWorkflowCompletes: bool
          CodecSignalCompletesWorkflow: bool }

    let private reserve =
        Activity.define "app-reserve" (fun orderId -> async { return "reserved:" + orderId })

    let private charge =
        Activity.define "app-charge" (fun reservation -> async { return "charged:" + reservation })

    let private approved = Signal.define "app-approved"

    let private scoreSignal = Signal.defineWith "app-score" string int

    let private checkout =
        Workflow.define "app-checkout" (fun orderId ->
            durable {
                let! reserved = D.call reserve orderId
                let! charged = D.call charge reserved
                return charged
            })

    let private approval =
        Workflow.define "app-approval" (fun orderId ->
            durable {
                let! approvedBy = D.waitForSignal approved
                return orderId + ":approved-by:" + approvedBy
            })

    let private approvalOrTimeout =
        Workflow.define "app-approval-or-timeout" (fun deadlineText ->
            durable {
                let deadline = int64 deadlineText
                let! winner = D.any [ T.signal approved; T.timer deadline ]

                match winner with
                | EventWon(_, Signal "app-approved", approver) -> return "approved:" + approver
                | EventWon(_, Timer _, _) -> return "timed-out"
                | ActivityWon _ -> return "unexpected-activity"
                | EventWon(_, Signal name, _) -> return "unexpected-signal:" + name
            })

    let private addOne =
        Activity.defineWith "app-add-one" string int string int (fun value -> async { return value + 1 })

    let private typedMath =
        Workflow.defineWith "app-typed-math" string int string int (fun value ->
            durable {
                let! incremented = D.call addOne value
                return incremented + 10
            })

    let private typedSignal =
        Workflow.define "app-typed-signal" (fun label ->
            durable {
                let! score = D.waitForSignal scoreSignal
                return label + ":" + string (score + 1)
            })

    let private app =
        durableApp {
            activity reserve
            activity charge
            activity addOne
            workflow checkout
            workflow approval
            workflow approvalOrTimeout
            workflow typedMath
            workflow typedSignal
        }

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
            "durable.app_facade"
            "durable-app-facade"
            { ProofOperationOptions.empty with
                Key = Some "durable-app-facade" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-app-facade-" + suffix

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                let storage = DurableStorage.s2 basin

                let client =
                    app |> DurableApp.clientWith ({ Storage = storage }: DurableAppClientConfig)

                let worker =
                    app
                    |> DurableApp.workerWith (
                        { Storage = storage
                          HostId = "app-proof"
                          MaxRunUntilIdleTicks = Some 10 }
                        : DurableAppWorkerConfig
                    )

                let! checkoutStart = client.start checkout "order-1"
                let checkoutInstance = startedInstance checkoutStart

                let typedStartReturnsInstance =
                    match checkoutInstance with
                    | Some instanceId ->
                        InstanceId.value instanceId <> ""
                        && (InstanceId.value instanceId).StartsWith("app-checkout")
                    | None -> false

                let! checkoutTicks =
                    match checkoutInstance with
                    | Some instanceId -> worker.runUntilIdle instanceId
                    | None -> async { return [] }

                let workerCompletesActivityWorkflow =
                    match lastStatus checkoutTicks with
                    | Some(DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed("charged:reserved:order-1",
                                                                                            _))) -> true
                    | _ -> false

                let! checkoutStatus =
                    match checkoutInstance with
                    | Some instanceId -> client.status instanceId
                    | None -> async { return DurableAppWorkflowStatus.NotFound }

                let clientStatusReadsCompletion =
                    match checkoutStatus with
                    | DurableAppWorkflowStatus.Completed("app-checkout", "charged:reserved:order-1") -> true
                    | _ -> false

                let approvalInstance = InstanceId.create ("app-approval-" + suffix)
                let! _ = client.startWith approvalInstance approval "order-2"
                let! _ = worker.runUntilIdle approvalInstance
                let! approvalSignal = client.signal approvalInstance approved "alice"
                let! approvalTicks = worker.runUntilIdle approvalInstance

                let clientSignalCompletesWorkflow =
                    approvalSignal = DurableAppSignalResult.Accepted
                    && match lastStatus approvalTicks with
                       | Some(DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed("order-2:approved-by:alice",
                                                                                               _))) -> true
                       | _ -> false

                let raceInstance = InstanceId.create ("app-race-" + suffix)
                let futureDeadline = int64 (Reports.nowMillis ()) + 60000L
                let! _ = client.startWith raceInstance approvalOrTimeout (string futureDeadline)
                let! _ = worker.runUntilIdle raceInstance
                let! raceSignal = client.signal raceInstance approved "bob"
                let! raceTicks = worker.runUntilIdle raceInstance

                let raceWorkflowCompletesFromTypedSignal =
                    raceSignal = DurableAppSignalResult.Accepted
                    && match lastStatus raceTicks with
                       | Some(DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed("approved:bob", _))) ->
                           true
                       | _ -> false

                let typedMathInstance = InstanceId.create ("app-typed-math-" + suffix)
                let! _ = client.startWith typedMathInstance typedMath 31
                let! typedMathTicks = worker.runUntilIdle typedMathInstance
                let! typedMathStatus = client.status typedMathInstance

                let codecWorkflowCompletes =
                    match lastStatus typedMathTicks, typedMathStatus with
                    | Some(DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed("42", _))),
                      DurableAppWorkflowStatus.Completed("app-typed-math", "42") -> true
                    | _ -> false

                let typedSignalInstance = InstanceId.create ("app-typed-signal-" + suffix)
                let! _ = client.startWith typedSignalInstance typedSignal "score"
                let! _ = worker.runUntilIdle typedSignalInstance
                let! typedSignalAck = client.signal typedSignalInstance scoreSignal 41
                let! typedSignalTicks = worker.runUntilIdle typedSignalInstance

                let codecSignalCompletesWorkflow =
                    typedSignalAck = DurableAppSignalResult.Accepted
                    && match lastStatus typedSignalTicks with
                       | Some(DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed("score:42", _))) -> true
                       | _ -> false

                match checkoutInstance with
                | Some instanceId -> do! deleteInstance basin instanceId
                | None -> ()

                do! deleteInstance basin approvalInstance
                do! deleteInstance basin raceInstance
                do! deleteInstance basin typedMathInstance
                do! deleteInstance basin typedSignalInstance

                let result =
                    { TypedStartReturnsInstance = typedStartReturnsInstance
                      WorkerCompletesActivityWorkflow = workerCompletesActivityWorkflow
                      ClientStatusReadsCompletion = clientStatusReadsCompletion
                      ClientSignalCompletesWorkflow = clientSignalCompletesWorkflow
                      RaceWorkflowCompletesFromTypedSignal = raceWorkflowCompletesFromTypedSignal
                      CodecWorkflowCompletes = codecWorkflowCompletes
                      CodecSignalCompletesWorkflow = codecSignalCompletesWorkflow }

                do!
                    ctx.EmitSpan
                        "proof.durable_app_facade.completed"
                        [ "proof.property", "durable-app-facade"
                          "app.start", string result.TypedStartReturnsInstance
                          "app.activity", string result.WorkerCompletesActivityWorkflow
                          "app.status", string result.ClientStatusReadsCompletion
                          "app.signal", string result.ClientSignalCompletesWorkflow
                          "app.race", string result.RaceWorkflowCompletesFromTypedSignal
                          "app.codec_workflow", string result.CodecWorkflowCompletes
                          "app.codec_signal", string result.CodecSignalCompletesWorkflow ]

                return result
            })

    let facadeProperty =
        property "durable-app-facade" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "typed workflow start returns an instance id" (fun result ->
                      result.TypedStartReturnsInstance)
                  v.Expect.Workload "worker completes typed activity workflow" (fun result ->
                      result.WorkerCompletesActivityWorkflow)
                  v.Expect.Workload "client status reads typed workflow completion" (fun result ->
                      result.ClientStatusReadsCompletion)
                  v.Expect.Workload "typed signal completes waiting workflow" (fun result ->
                      result.ClientSignalCompletesWorkflow)
                  v.Expect.Workload "typed signal can win a race workflow" (fun result ->
                      result.RaceWorkflowCompletesFromTypedSignal)
                  v.Expect.Workload "codec-backed activity workflow completes" (fun result ->
                      result.CodecWorkflowCompletes)
                  v.Expect.Workload "codec-backed signal workflow completes" (fun result ->
                      result.CodecSignalCompletesWorkflow)
                  v.Trace.SpanExists
                      "durable app facade proof span emitted"
                      "proof.durable_app_facade.completed"
                      [ "proof.property", "durable-app-facade" ]
                  v.Trace.Operation
                      "durable app facade operation was recorded"
                      ({ TraceOperationMatch.named "durable.app_facade" with
                          Status = Some "ok"
                          OutputContains =
                              [ "TypedStartReturnsInstance"
                                "WorkerCompletesActivityWorkflow"
                                "RaceWorkflowCompletesFromTypedSignal"
                                "CodecWorkflowCompletes"
                                "CodecSignalCompletesWorkflow" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-app-facade" {
            describedAs
                "The public app facade lets authors define activities, workflows, and signals with handles, then run them through client and worker objects without constructing registries."

            property facadeProperty
        }
