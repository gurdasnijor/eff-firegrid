namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable
open Eff.Foundation.Durable.App

module DurableRaceFacadeProof =
    module D = Eff.Foundation.Durable.App.Durable

    type DurableRaceFacadeResult =
        { SignalRaceCompletes: bool
          TimerRaceCompletes: bool
          WaitingNeedIsRace: bool
          TypedStatusReadsRaceCompletion: bool }

    let private approved = Signal.define "race-facade-approved"

    let private approvalOrTimeout =
        Workflow.define "race-facade-approval-or-timeout" (fun deadlineText ->
            durable {
                let deadline = int64 deadlineText

                return!
                    D.anyOf
                        [ D.raceSignal approved (fun approver -> "approved:" + approver)
                          D.raceTimer deadline "timed-out" ]
            })

    let private app = durableApp { workflow approvalOrTimeout }

    let private deleteInstance basin instanceId =
        async {
            let key = DurableClient.instanceKey instanceId
            do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
            do! basin |> S2.deleteStream (StorageKey.logStreamName key)
        }

    let private completed expected =
        function
        | DurableAppTypedWorkflowStatus.Completed actual -> actual = expected
        | _ -> false

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.race_facade"
            "durable-race-facade"
            { ProofOperationOptions.empty with
                Key = Some "durable-race-facade" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx
                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-race-facade-" + suffix

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                let storage = DurableStorage.s2 basin

                let client =
                    app |> DurableApp.clientWith ({ Storage = storage }: DurableAppClientConfig)

                let worker =
                    app
                    |> DurableApp.workerWith (
                        { Storage = storage
                          HostId = "race-facade"
                          MaxRunUntilIdleTicks = Some 10 }
                        : DurableAppWorkerConfig
                    )

                let signalInstance = InstanceId.create ("race-facade-signal-" + suffix)
                let futureDeadline = int64 (Reports.nowMillis ()) + 60000L
                let! _ = client.startWith signalInstance approvalOrTimeout (string futureDeadline)
                let! _ = worker.runUntilIdle signalInstance
                let! waiting = client.statusOf approvalOrTimeout signalInstance
                let! signalAck = client.signal signalInstance approved "alice"
                let! _ = worker.runUntilIdle signalInstance
                let! signalStatus = client.statusOf approvalOrTimeout signalInstance

                let signalRaceCompletes =
                    signalAck = DurableAppSignalResult.Accepted
                    && completed "approved:alice" signalStatus

                let waitingNeedIsRace =
                    match waiting with
                    | DurableAppTypedWorkflowStatus.Waiting(DurableAppNeed.Race contenders) -> contenders.Length = 2
                    | _ -> false

                let timerInstance = InstanceId.create ("race-facade-timer-" + suffix)
                let pastDeadline = int64 (Reports.nowMillis ()) - 1L
                let! _ = client.startWith timerInstance approvalOrTimeout (string pastDeadline)
                let! _ = worker.runUntilIdle timerInstance
                let! timerStatus = client.statusOf approvalOrTimeout timerInstance

                let timerRaceCompletes = completed "timed-out" timerStatus

                let typedStatusReadsRaceCompletion =
                    signalStatus = DurableAppTypedWorkflowStatus.Completed "approved:alice"
                    && timerStatus = DurableAppTypedWorkflowStatus.Completed "timed-out"

                do! deleteInstance basin signalInstance
                do! deleteInstance basin timerInstance

                let result =
                    { SignalRaceCompletes = signalRaceCompletes
                      TimerRaceCompletes = timerRaceCompletes
                      WaitingNeedIsRace = waitingNeedIsRace
                      TypedStatusReadsRaceCompletion = typedStatusReadsRaceCompletion }

                do!
                    ctx.EmitSpan
                        "proof.durable_race_facade.completed"
                        [ "proof.property", "durable-race-facade"
                          "race_facade.signal", string result.SignalRaceCompletes
                          "race_facade.timer", string result.TimerRaceCompletes
                          "race_facade.waiting", string result.WaitingNeedIsRace
                          "race_facade.typed_status", string result.TypedStatusReadsRaceCompletion ]

                return result
            })

    let raceFacadeProperty =
        property "durable-race-facade" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "raceSignal branch completes without raw EventWon matching" (fun result ->
                      result.SignalRaceCompletes)
                  v.Expect.Workload "raceTimer branch completes without raw EventWon matching" (fun result ->
                      result.TimerRaceCompletes)
                  v.Expect.Workload "race facade still reports a durable race wait" (fun result ->
                      result.WaitingNeedIsRace)
                  v.Expect.Workload "typed status reads race workflow completion" (fun result ->
                      result.TypedStatusReadsRaceCompletion)
                  v.Trace.SpanExists
                      "durable race facade proof span emitted"
                      "proof.durable_race_facade.completed"
                      [ "proof.property", "durable-race-facade" ]
                  v.Trace.Operation
                      "durable race facade operation was recorded"
                      ({ TraceOperationMatch.named "durable.race_facade" with
                          Status = Some "ok"
                          OutputContains =
                              [ "SignalRaceCompletes"
                                "TimerRaceCompletes"
                                "TypedStatusReadsRaceCompletion" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-race-facade" {
            describedAs
                "Durable app workflows can express signal/timer races through typed facade race tasks instead of matching raw RaceResult indexes."

            property raceFacadeProperty
        }
