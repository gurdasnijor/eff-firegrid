namespace Eff.Foundation.Durable.App

open Eff.Foundation.Durable

type DurableAppClient internal (runtime: DurableRuntime) =
    static member private StartResult =
        function
        | DurableClientStartStatus.Accepted ack -> DurableAppStartResult.Started ack.InstanceId
        | DurableClientStartStatus.Failed failure ->
            match failure with
            | DurableClientFailure.StartAppendFailed error
            | DurableClientFailure.SignalAppendFailed error ->
                DurableAppStartResult.Rejected(DurableAppStartFailure.AppendFailed error)

    static member private SignalResult =
        function
        | DurableClientSignalStatus.Accepted _ -> DurableAppSignalResult.Accepted
        | DurableClientSignalStatus.Failed failure ->
            match failure with
            | DurableClientFailure.SignalAppendFailed error
            | DurableClientFailure.StartAppendFailed error ->
                DurableAppSignalResult.Rejected(DurableAppSignalFailure.AppendFailed error)

    static member private TaskText =
        function
        | RaceActivity activity -> "activity:" + activity.Name
        | RaceEvent(Timer deadline) -> "timer:" + string deadline
        | RaceEvent(Signal name) -> "signal:" + name

    static member private NeedSummary =
        function
        | NeedsActivity activity -> DurableAppNeed.Activity activity.Name
        | NeedsActivities activities ->
            activities
            |> List.map (fun (_, activity) -> activity.Name)
            |> DurableAppNeed.Activities
        | NeedsEvent(Timer deadline) -> DurableAppNeed.Timer deadline
        | NeedsEvent(Signal name) -> DurableAppNeed.Signal name
        | NeedsRace tasks ->
            tasks
            |> List.map (fun (_, task) -> DurableAppClient.TaskText task)
            |> DurableAppNeed.Race
        | NeedsTimerCancellation timers -> DurableAppNeed.TimerCancellation timers.Length
        | NeedsCurrentTime -> DurableAppNeed.CurrentTime
        | NeedsLog message -> DurableAppNeed.Log message

    static member private StatusFailure =
        function
        | DurableClientStatusFailure.StatusReadFailed error -> DurableAppStatusFailure.ReadFailed error
        | DurableClientStatusFailure.StatusDecodeFailed(seqNum, error) ->
            DurableAppStatusFailure.DecodeFailed(seqNum, error)
        | DurableClientStatusFailure.WorkflowNotFound workflow ->
            DurableAppStatusFailure.WorkflowNotFound(WorkflowName.value workflow)

    static member private WorkflowStatus =
        function
        | DurableClientStatusRead.Succeeded InstanceNotFound -> DurableAppWorkflowStatus.NotFound
        | DurableClientStatusRead.Succeeded(InstanceRunning workflow) ->
            DurableAppWorkflowStatus.Running(WorkflowName.value workflow)
        | DurableClientStatusRead.Succeeded(InstanceWaiting(workflow, _, need)) ->
            DurableAppWorkflowStatus.Waiting(WorkflowName.value workflow, DurableAppClient.NeedSummary need)
        | DurableClientStatusRead.Succeeded(InstanceCompleted(workflow, payload)) ->
            DurableAppWorkflowStatus.Completed(WorkflowName.value workflow, payload)
        | DurableClientStatusRead.Failed failure ->
            DurableAppWorkflowStatus.Failed(DurableAppClient.StatusFailure failure)

    static member private DecodeWorkflowOutput (workflow: Workflow<'input, 'output>) payload =
        try
            Ok(workflow.DecodeOutput payload)
        with error ->
            Error(DurableAppStatusFailure.OutputDecodeFailed(WorkflowName.value workflow.Name, error.Message))

    static member private TypedWorkflowStatus (workflow: Workflow<'input, 'output>) status =
        let expected = WorkflowName.value workflow.Name

        let mismatch actual =
            DurableAppStatusFailure.WorkflowMismatch(expected, actual)

        match status with
        | DurableAppWorkflowStatus.NotFound -> DurableAppTypedWorkflowStatus.NotFound
        | DurableAppWorkflowStatus.Running actual ->
            if actual = expected then
                DurableAppTypedWorkflowStatus.Running
            else
                DurableAppTypedWorkflowStatus.Failed(mismatch actual)
        | DurableAppWorkflowStatus.Waiting(actual, need) ->
            if actual = expected then
                DurableAppTypedWorkflowStatus.Waiting need
            else
                DurableAppTypedWorkflowStatus.Failed(mismatch actual)
        | DurableAppWorkflowStatus.Completed(actual, payload) ->
            if actual = expected then
                match DurableAppClient.DecodeWorkflowOutput workflow payload with
                | Ok output -> DurableAppTypedWorkflowStatus.Completed output
                | Error failure -> DurableAppTypedWorkflowStatus.Failed failure
            else
                DurableAppTypedWorkflowStatus.Failed(mismatch actual)
        | DurableAppWorkflowStatus.Failed failure -> DurableAppTypedWorkflowStatus.Failed failure

    member _.start (workflow: Workflow<'input, 'output>) (input: 'input) =
        async {
            let! result = runtime.Client.Start workflow.Name (workflow.EncodeInput input)
            return DurableAppClient.StartResult result
        }

    member _.startWith (instanceId: InstanceId) (workflow: Workflow<'input, 'output>) (input: 'input) =
        async {
            let! result = runtime.Client.StartWith instanceId workflow.Name (workflow.EncodeInput input)
            return DurableAppClient.StartResult result
        }

    member _.signal (instanceId: InstanceId) (signal: Signal<'payload>) (payload: 'payload) =
        async {
            let! result = runtime.Client.RaiseSignal instanceId signal.Name (signal.Encode payload)
            return DurableAppClient.SignalResult result
        }

    member _.status instanceId =
        async {
            let! result = runtime.Client.GetStatus instanceId
            return DurableAppClient.WorkflowStatus result
        }

    member this.statusOf (workflow: Workflow<'input, 'output>) instanceId =
        async {
            let! status = this.status instanceId
            return DurableAppClient.TypedWorkflowStatus workflow status
        }
