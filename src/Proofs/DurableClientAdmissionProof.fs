namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable

module DurableClientAdmissionProof =
    type DurableClientAdmissionResult =
        { StartAcceptedBeforeHostRuns: bool
          DuplicateStartFoldsOnce: bool
          HostStartsRegisteredWorkflow: bool
          WorkflowCompletesAfterActivityAdmission: bool
          MissingWorkflowIsTypedFailure: bool
          EmptyInstanceReportsNoStart: bool }

    let private instance suffix name = InstanceId.create (name + "-" + suffix)

    let private checkout = WorkflowName.create "checkout"

    let private reserve = Activities.create "reserve" "order-1"

    let private registerWorkflow () =
        match
            WorkflowRegistry.empty
            |> WorkflowRegistry.register "checkout" (fun orderId ->
                durable {
                    let! reserved = Workflow.call "reserve" orderId
                    return reserved
                })
        with
        | Ok registry -> registry
        | Error error -> failwith (string error)

    let private registerActivities () =
        match
            ActivityRegistry.empty
            |> ActivityRegistry.register "reserve" (fun orderId -> async { return "reserved:" + orderId })
        with
        | Ok registry -> registry
        | Error error -> failwith (string error)

    let private options hostId timestamp =
        { DurableHostTickOptions.create hostId timestamp with
            MaxInboxRecords = 10
            MaxActivityCommands = 10
            MaxTimerCommands = 10 }

    let private forceDecoded entries =
        entries
        |> List.map (fun (seqNum, decoded) ->
            match decoded with
            | Ok entry -> seqNum, entry
            | Error error -> failwith error)

    let private readLog owner =
        async {
            let! raw = S2Substrate.readLogText StepRecordCodec.decode owner
            return forceDecoded raw
        }

    let private workflowStartedCount entries =
        entries
        |> List.filter (function
            | _, Incoming(WorkflowStarted(WorkflowName "checkout", "order-1")) -> true
            | _ -> false)
        |> List.length

    let private hasActivityCommand entries =
        entries
        |> List.exists (function
            | _, Outgoing(Command(CallActivity(OpId 0, called))) when called = reserve -> true
            | _ -> false)

    let private activityCompletedCount entries =
        entries
        |> List.filter (function
            | _, Incoming(HistoryEvent(ActivityCompleted(OpId 0, "reserved:order-1"))) -> true
            | _ -> false)
        |> List.length

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.client_admission"
            "durable-client-admission"
            { ProofOperationOptions.empty with
                Key = Some "durable-client-admission" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-client-admission-" + suffix
                let instanceId = instance suffix "checkout"
                let missingInstanceId = instance suffix "missing"
                let emptyInstanceId = instance suffix "empty"

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                let key = DurableClient.instanceKey instanceId
                let missingKey = DurableClient.instanceKey missingInstanceId
                let emptyKey = DurableClient.instanceKey emptyInstanceId

                let workflows = registerWorkflow ()
                let activities = registerActivities ()

                let! firstStart = DurableClient.startWith basin instanceId checkout "order-1"
                let! retryStart = DurableClient.startWith basin instanceId checkout "order-1"

                let startAcceptedBeforeHostRuns =
                    match firstStart, retryStart with
                    | DurableClientStartStatus.Accepted first, DurableClientStartStatus.Accepted retry ->
                        first.InstanceId = instanceId
                        && first.InboxSeqNum = 0L
                        && retry.InboxSeqNum = 1L
                    | _ -> false

                let pair = S2Substrate.streams basin key
                let! owner = S2Substrate.claimWith (FenceToken "client-admission/owner") pair

                let! firstTick =
                    DurableHost.runWorkflowTick (options "client-admission" 100L) workflows activities owner

                let! afterFirstTick = readLog owner

                let duplicateStartFoldsOnce = workflowStartedCount afterFirstTick = 1

                let hostStartsRegisteredWorkflow =
                    match firstTick with
                    | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Advanced _) ->
                        duplicateStartFoldsOnce && hasActivityCommand afterFirstTick
                    | _ -> false

                let! secondTick =
                    DurableHost.runWorkflowTick (options "client-admission" 101L) workflows activities owner

                let! afterSecondTick = readLog owner

                let workflowCompletesAfterActivityAdmission =
                    match secondTick with
                    | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed("reserved:order-1", _)) ->
                        workflowStartedCount afterSecondTick = 1
                        && activityCompletedCount afterSecondTick = 1
                    | _ -> false

                let! missingStart =
                    DurableClient.startWith basin missingInstanceId (WorkflowName.create "missing") "order-1"

                let! missingOwner =
                    S2Substrate.claimWith (FenceToken "client-admission/missing") (S2Substrate.streams basin missingKey)

                let! missingTick =
                    DurableHost.runWorkflowTick (options "client-admission" 200L) workflows activities missingOwner

                let missingWorkflowIsTypedFailure =
                    match missingStart, missingTick with
                    | DurableClientStartStatus.Accepted _,
                      DurableWorkflowHostStatus.Failed(DurableWorkflowHostFailure.WorkflowNotFound(WorkflowName "missing")) ->
                        true
                    | _ -> false

                do! S2Substrate.ensureStreams basin emptyKey

                let! emptyOwner =
                    S2Substrate.claimWith (FenceToken "client-admission/empty") (S2Substrate.streams basin emptyKey)

                let! emptyTick =
                    DurableHost.runWorkflowTick (options "client-admission" 300L) workflows activities emptyOwner

                let emptyInstanceReportsNoStart =
                    match emptyTick with
                    | DurableWorkflowHostStatus.Failed DurableWorkflowHostFailure.NoStart -> true
                    | _ -> false

                do! basin |> S2.deleteStream (StorageKey.inboxStreamName emptyKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName emptyKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName missingKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName missingKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
                do! basin |> S2.deleteStream (StorageKey.logStreamName key)

                let result =
                    { StartAcceptedBeforeHostRuns = startAcceptedBeforeHostRuns
                      DuplicateStartFoldsOnce = duplicateStartFoldsOnce
                      HostStartsRegisteredWorkflow = hostStartsRegisteredWorkflow
                      WorkflowCompletesAfterActivityAdmission = workflowCompletesAfterActivityAdmission
                      MissingWorkflowIsTypedFailure = missingWorkflowIsTypedFailure
                      EmptyInstanceReportsNoStart = emptyInstanceReportsNoStart }

                do!
                    ctx.EmitSpan
                        "proof.durable_client_admission.completed"
                        [ "proof.property", "durable-client-admission"
                          "client.start", string result.StartAcceptedBeforeHostRuns
                          "client.duplicate", string result.DuplicateStartFoldsOnce
                          "client.workflow", string result.HostStartsRegisteredWorkflow
                          "client.completed", string result.WorkflowCompletesAfterActivityAdmission ]

                return result
            })

    let clientAdmissionProperty =
        property "durable-client-admission" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "start is durably accepted before host runs" (fun result ->
                      result.StartAcceptedBeforeHostRuns)
                  v.Expect.Workload "duplicate StartWith folds into one effective workflow start" (fun result ->
                      result.DuplicateStartFoldsOnce)
                  v.Expect.Workload "host starts the registered workflow from inbox start" (fun result ->
                      result.HostStartsRegisteredWorkflow)
                  v.Expect.Workload "workflow completes after activity completion admission" (fun result ->
                      result.WorkflowCompletesAfterActivityAdmission)
                  v.Expect.Workload "missing workflow is a typed failure" (fun result ->
                      result.MissingWorkflowIsTypedFailure)
                  v.Expect.Workload "empty instance reports no start" (fun result ->
                      result.EmptyInstanceReportsNoStart)
                  v.Trace.SpanExists
                      "durable client admission proof span emitted"
                      "proof.durable_client_admission.completed"
                      [ "proof.property", "durable-client-admission" ]
                  v.Trace.Operation
                      "durable client admission operation was recorded"
                      ({ TraceOperationMatch.named "durable.client_admission" with
                          Status = Some "ok"
                          OutputContains =
                              [ "StartAcceptedBeforeHostRuns"
                                "DuplicateStartFoldsOnce"
                                "WorkflowCompletesAfterActivityAdmission" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-client-admission" {
            describedAs "Durable client StartWith admits a workflow start through inbox and host registry execution."

            property clientAdmissionProperty
        }
