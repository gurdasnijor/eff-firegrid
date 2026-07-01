namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable
open Eff.Foundation.Durable.App

module DurableProductionUsecasesProof =
    module D = Eff.Foundation.Durable.App.Durable

    type DurableProductionUsecasesResult =
        { CheckoutSuccess: bool
          DuplicateSignalFoldedOnce: bool
          SignalWinsTimeoutRace: bool
          TimerWinsTimeoutRace: bool
          FanOutFanInCompletes: bool
          WorkerRestartCompletes: bool }

    let private reserveInventory =
        Activity.define "prod.reserve-inventory" (fun orderId -> async { return "reservation:" + orderId })

    let private chargePayment =
        Activity.define "prod.charge-payment" (fun reservation -> async { return "charge:" + reservation })

    let private sendReceipt =
        Activity.define "prod.send-receipt" (fun chargeId -> async { return "receipt:" + chargeId })

    let private fanItem =
        Activity.define "prod.fan-item" (fun item -> async { return "done:" + item })

    let private approved = Signal.define "prod.approved"

    let private checkout =
        Workflow.define "prod.checkout" (fun orderId ->
            durable {
                let! reservation = D.call reserveInventory orderId
                let! chargeId = D.call chargePayment reservation
                return! D.call sendReceipt chargeId
            })

    let private approvalTwice =
        Workflow.define "prod.approval-twice" (fun requestId ->
            durable {
                let! first = D.waitForSignal approved
                let! second = D.waitForSignal approved
                return requestId + ":" + first + "|" + second
            })

    let private approvalOrTimeout =
        Workflow.defineWith "prod.approval-timeout" string int64 id id (fun deadline ->
            durable {
                return!
                    D.anyOf
                        [ D.raceSignal approved (fun approver -> "approved:" + approver)
                          D.raceTimer deadline "timed-out" ]
            })

    let private fanOutFanIn =
        Workflow.define "prod.fan-out-fan-in" (fun batchId ->
            durable {
                let calls =
                    [ "pack"; "label"; "ship" ]
                    |> List.map (fun item -> Activities.create "prod.fan-item" (batchId + ":" + item))

                let! outputs = Eff.Foundation.Durable.Workflow.all calls
                return outputs |> String.concat "|"
            })

    let private app =
        durableApp {
            activity reserveInventory
            activity chargePayment
            activity sendReceipt
            activity fanItem
            workflow checkout
            workflow approvalTwice
            workflow approvalOrTimeout
            workflow fanOutFanIn
        }

    let private startedInstance =
        function
        | DurableAppStartResult.Started instanceId -> Some instanceId
        | DurableAppStartResult.Rejected _ -> None

    let private deleteInstance basin instanceId =
        async {
            let key = DurableClient.instanceKey instanceId
            do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
            do! basin |> S2.deleteStream (StorageKey.logStreamName key)
        }

    let private typedCompleted expected =
        function
        | DurableAppTypedWorkflowStatus.Completed actual -> actual = expected
        | _ -> false

    let private typedWaiting =
        function
        | DurableAppTypedWorkflowStatus.Waiting _ -> true
        | _ -> false

    let private acceptedSignal =
        function
        | DurableClientSignalStatus.Accepted _ -> true
        | DurableClientSignalStatus.Failed _ -> false

    let private emitScenario ctx name completed =
        ctx.EmitSpan
            ("proof.durable_production." + name)
            [ "proof.property", "durable-production-usecases"
              "scenario", name
              "completed", string completed ]

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.production_usecases"
            "durable-production-usecases"
            { ProofOperationOptions.empty with
                Key = Some "durable-production-usecases" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx
                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-production-" + suffix

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                let storage = DurableStorage.s2 basin

                let client =
                    app |> DurableApp.clientWith ({ Storage = storage }: DurableAppClientConfig)

                let worker =
                    app
                    |> DurableApp.workerWith (
                        { Storage = storage
                          HostId = "prod-a"
                          MaxRunUntilIdleTicks = Some 20 }
                        : DurableAppWorkerConfig
                    )

                let restartedWorker =
                    app
                    |> DurableApp.workerWith (
                        { Storage = storage
                          HostId = "prod-b"
                          MaxRunUntilIdleTicks = Some 20 }
                        : DurableAppWorkerConfig
                    )

                let instances = ResizeArray<InstanceId>()

                let track instanceId =
                    instances.Add instanceId
                    instanceId

                let checkoutId = InstanceId.create ("prod-checkout-" + suffix) |> track
                let! _ = client.startWith checkoutId checkout "order-1"
                let! _ = worker.runUntilIdle checkoutId
                let! checkoutStatus = client.statusOf checkout checkoutId

                let checkoutSuccess =
                    checkoutStatus |> typedCompleted "receipt:charge:reservation:order-1"

                do! emitScenario ctx "checkout" checkoutSuccess

                let duplicateSignalId = InstanceId.create ("prod-approval-dup-" + suffix) |> track
                let! _ = client.startWith duplicateSignalId approvalTwice "request-1"
                let! _ = worker.runUntilIdle duplicateSignalId

                let! duplicateFirst =
                    DurableClient.raiseSignalFrom basin duplicateSignalId "human-approval" 7L "prod.approved" "alice"

                let! duplicateAgain =
                    DurableClient.raiseSignalFrom basin duplicateSignalId "human-approval" 7L "prod.approved" "alice"

                let! _ = worker.runUntilIdle duplicateSignalId
                let! afterDuplicateStatus = client.statusOf approvalTwice duplicateSignalId

                let! secondApproval =
                    DurableClient.raiseSignalFrom basin duplicateSignalId "human-approval" 8L "prod.approved" "bob"

                let! _ = worker.runUntilIdle duplicateSignalId
                let! afterSecondStatus = client.statusOf approvalTwice duplicateSignalId

                let duplicateSignalFoldedOnce =
                    acceptedSignal duplicateFirst
                    && acceptedSignal duplicateAgain
                    && acceptedSignal secondApproval
                    && typedWaiting afterDuplicateStatus
                    && typedCompleted "request-1:alice|bob" afterSecondStatus

                do! emitScenario ctx "duplicate-signal" duplicateSignalFoldedOnce

                let signalRaceId = InstanceId.create ("prod-race-signal-" + suffix) |> track
                let futureDeadline = int64 (Reports.nowMillis ()) + 60000L
                let! _ = client.startWith signalRaceId approvalOrTimeout futureDeadline
                let! _ = worker.runUntilIdle signalRaceId
                let! signalAck = client.signal signalRaceId approved "carol"
                let! _ = worker.runUntilIdle signalRaceId
                let! signalRaceStatus = client.statusOf approvalOrTimeout signalRaceId

                let signalWinsTimeoutRace =
                    signalAck = DurableAppSignalResult.Accepted
                    && typedCompleted "approved:carol" signalRaceStatus

                do! emitScenario ctx "signal-timeout-signal-wins" signalWinsTimeoutRace

                let timerRaceId = InstanceId.create ("prod-race-timer-" + suffix) |> track
                let pastDeadline = int64 (Reports.nowMillis ()) - 1L
                let! _ = client.startWith timerRaceId approvalOrTimeout pastDeadline
                let! _ = worker.runUntilIdle timerRaceId
                let! timerRaceStatus = client.statusOf approvalOrTimeout timerRaceId

                let timerWinsTimeoutRace = timerRaceStatus |> typedCompleted "timed-out"

                do! emitScenario ctx "signal-timeout-timer-wins" timerWinsTimeoutRace

                let fanOutId = InstanceId.create ("prod-fan-out-" + suffix) |> track
                let! _ = client.startWith fanOutId fanOutFanIn "batch-1"
                let! _ = worker.runUntilIdle fanOutId
                let! fanOutStatus = client.statusOf fanOutFanIn fanOutId

                let fanOutFanInCompletes =
                    fanOutStatus
                    |> typedCompleted "done:batch-1:pack|done:batch-1:label|done:batch-1:ship"

                do! emitScenario ctx "fan-out-fan-in" fanOutFanInCompletes

                let restartId = InstanceId.create ("prod-restart-" + suffix) |> track
                let! _ = client.startWith restartId checkout "order-restart"
                let! _ = worker.runOnce restartId
                let! _ = restartedWorker.runUntilIdle restartId
                let! restartStatus = client.statusOf checkout restartId

                let workerRestartCompletes =
                    restartStatus |> typedCompleted "receipt:charge:reservation:order-restart"

                do! emitScenario ctx "worker-restart" workerRestartCompletes

                for instanceId in instances do
                    do! deleteInstance basin instanceId

                let result =
                    { CheckoutSuccess = checkoutSuccess
                      DuplicateSignalFoldedOnce = duplicateSignalFoldedOnce
                      SignalWinsTimeoutRace = signalWinsTimeoutRace
                      TimerWinsTimeoutRace = timerWinsTimeoutRace
                      FanOutFanInCompletes = fanOutFanInCompletes
                      WorkerRestartCompletes = workerRestartCompletes }

                do!
                    ctx.EmitSpan
                        "proof.durable_production.completed"
                        [ "proof.property", "durable-production-usecases"
                          "checkout", string result.CheckoutSuccess
                          "duplicate_signal", string result.DuplicateSignalFoldedOnce
                          "signal_wins_timeout", string result.SignalWinsTimeoutRace
                          "timer_wins_timeout", string result.TimerWinsTimeoutRace
                          "fan_out_fan_in", string result.FanOutFanInCompletes
                          "worker_restart", string result.WorkerRestartCompletes ]

                return result
            })

    let productionUsecasesProperty =
        property "durable-production-usecases" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "checkout completes with durable typed status" (fun result ->
                      result.CheckoutSuccess)
                  v.Expect.Workload "duplicate approval signal folds once" (fun result ->
                      result.DuplicateSignalFoldedOnce)
                  v.Expect.Workload "signal wins approval timeout race" (fun result -> result.SignalWinsTimeoutRace)
                  v.Expect.Workload "timer wins approval timeout race" (fun result -> result.TimerWinsTimeoutRace)
                  v.Expect.Workload "fan-out/fan-in completes all activities" (fun result ->
                      result.FanOutFanInCompletes)
                  v.Expect.Workload "worker restart completes from durable records" (fun result ->
                      result.WorkerRestartCompletes)
                  v.Trace.SpanExists
                      "checkout scenario span emitted"
                      "proof.durable_production.checkout"
                      [ "scenario", "checkout" ]
                  v.Trace.SpanExists
                      "duplicate signal scenario span emitted"
                      "proof.durable_production.duplicate-signal"
                      [ "scenario", "duplicate-signal" ]
                  v.Trace.SpanExists
                      "signal timeout signal-wins span emitted"
                      "proof.durable_production.signal-timeout-signal-wins"
                      [ "scenario", "signal-timeout-signal-wins" ]
                  v.Trace.SpanExists
                      "signal timeout timer-wins span emitted"
                      "proof.durable_production.signal-timeout-timer-wins"
                      [ "scenario", "signal-timeout-timer-wins" ]
                  v.Trace.SpanExists
                      "fan-out/fan-in scenario span emitted"
                      "proof.durable_production.fan-out-fan-in"
                      [ "scenario", "fan-out-fan-in" ]
                  v.Trace.SpanExists
                      "worker restart scenario span emitted"
                      "proof.durable_production.worker-restart"
                      [ "scenario", "worker-restart" ]
                  v.Trace.Operation
                      "durable production operation was recorded"
                      ({ TraceOperationMatch.named "durable.production_usecases" with
                          Status = Some "ok"
                          OutputContains =
                              [ "CheckoutSuccess"
                                "DuplicateSignalFoldedOnce"
                                "SignalWinsTimeoutRace"
                                "TimerWinsTimeoutRace"
                                "FanOutFanInCompletes"
                                "WorkerRestartCompletes" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-production-usecases" {
            describedAs
                "Production-shaped durable app scenarios complete through S2-backed execution using the public app facade where possible."

            property productionUsecasesProperty
        }
