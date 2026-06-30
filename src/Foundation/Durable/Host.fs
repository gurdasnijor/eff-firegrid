namespace Eff.Foundation.Durable

open Eff

[<RequireQualifiedAccess>]
type DurableHostFailure =
    | LogReadFailed of string
    | DecodeFailed of seqNum: int64 * error: string
    | CommitFailed of S2Errors.S2Failure
    | UnexpectedNoCommit
    | Unexpected of string

[<RequireQualifiedAccess>]
type DurableHostStatus<'a> =
    | Completed of 'a
    | Committed of S2.AppendAck
    | Waiting of OpId * Need
    | Deposed of expectedFence: string
    | Failed of DurableHostFailure

type DurableHostTickOptions =
    { HostId: string
      Timestamp: int64
      MaxInboxRecords: int
      MaxActivityCommands: int
      MaxTimerCommands: int }

type DurableHostTickReport<'a> =
    { Key: StorageKey
      Fence: FenceToken
      Inbox: InboxFoldReport option
      Step: DurableHostStatus<'a> option
      Activities: ActivityCommandAdapterReport option
      Timers: TimerCommandAdapterReport option }

[<RequireQualifiedAccess>]
type DurableHostTickFailure =
    | InboxFailed of InboxFoldFailure
    | StepFailed of DurableHostFailure
    | ActivityFailed of ActivityCommandAdapterFailure
    | TimerFailed of TimerCommandAdapterFailure

[<RequireQualifiedAccess>]
type DurableHostTickStatus<'a> =
    | Completed of value: 'a * report: DurableHostTickReport<'a>
    | Waiting of opId: OpId * need: Need * report: DurableHostTickReport<'a>
    | Advanced of DurableHostTickReport<'a>
    | Deposed of expectedFence: string * report: DurableHostTickReport<'a>
    | Failed of DurableHostTickFailure * report: DurableHostTickReport<'a>

[<RequireQualifiedAccess>]
type DurableWorkflowHostFailure =
    | StartFoldFailed of InboxFoldFailure
    | LogReadFailed of string
    | DecodeFailed of seqNum: int64 * error: string
    | NoStart
    | WorkflowNotFound of WorkflowName

[<RequireQualifiedAccess>]
type DurableWorkflowHostStatus =
    | Ticked of DurableHostTickStatus<Payload>
    | Deposed of expectedFence: string
    | Failed of DurableWorkflowHostFailure

[<RequireQualifiedAccess>]
module DurableHostTickOptions =
    let create hostId timestamp =
        { HostId = hostId
          Timestamp = timestamp
          MaxInboxRecords = 100
          MaxActivityCommands = 100
          MaxTimerCommands = 100 }

[<RequireQualifiedAccess>]
module DurableHost =
    let private emptyTickReport (owned: OwnedKey) =
        { Key = owned.Key
          Fence = owned.Fence
          Inbox = None
          Step = None
          Activities = None
          Timers = None }

    let private decodeLog decoded =
        let rec loop records =
            function
            | [] -> Ok(List.rev records)
            | (_, Ok record) :: rest -> loop (record :: records) rest
            | (seqNum, Error error) :: _ -> Error(DurableHostFailure.DecodeFailed(seqNum, error))

        loop [] decoded

    let private readLog decode owned =
        async {
            try
                let! decoded = S2Substrate.readLogText decode owned
                return Ok decoded
            with error ->
                return Error(DurableHostFailure.LogReadFailed error.Message)
        }

    let stepOnce encode decode timestamp owned program =
        async {
            let! log = readLog decode owned

            match log with
            | Error failure -> return DurableHostStatus.Failed failure
            | Ok decoded ->
                try
                    match decodeLog decoded with
                    | Error failure -> return DurableHostStatus.Failed failure
                    | Ok records ->
                        let history = DurableStepper.historyFromRecords records

                        match DurableStepper.plan timestamp history program with
                        | Complete value -> return DurableHostStatus.Completed value
                        | Waiting(opId, need) -> return DurableHostStatus.Waiting(opId, need)
                        | Commit _ as plan ->
                            let! commit = DurableStepper.commit encode owned plan

                            return
                                match commit with
                                | StepCommitted ack -> DurableHostStatus.Committed ack
                                | StepDeposed expected -> DurableHostStatus.Deposed expected
                                | StepCommitFailed failure ->
                                    DurableHostStatus.Failed(DurableHostFailure.CommitFailed failure)
                                | StepNotRequired -> DurableHostStatus.Failed DurableHostFailure.UnexpectedNoCommit
                with error ->
                    return DurableHostStatus.Failed(DurableHostFailure.Unexpected error.Message)
        }

    let runOnce encode decode timestamp owned program =
        stepOnce encode decode timestamp owned program

    let private inboxMadeProgress report =
        match report.Commit with
        | Some _ -> true
        | None -> false

    let private activitiesMadeProgress report =
        not (List.isEmpty report.Completed)
        || report.AlreadyPublished > 0
        || report.Ignored > 0
        || match report.Checkpoint with
           | CommandDispatchCheckpointResult.Checkpointed _ -> true
           | CommandDispatchCheckpointResult.NotRequired
           | CommandDispatchCheckpointResult.Deposed _
           | CommandDispatchCheckpointResult.Failed _ -> false

    let private timersMadeProgress report =
        not (List.isEmpty report.Published)
        || report.AlreadyPublished > 0
        || report.Canceled > 0
        || report.Ignored > 0
        || match report.Checkpoint with
           | CommandDispatchCheckpointResult.Checkpointed _ -> true
           | CommandDispatchCheckpointResult.NotRequired
           | CommandDispatchCheckpointResult.Deposed _
           | CommandDispatchCheckpointResult.Failed _ -> false

    let runOwnedTick options activities (owned: OwnedKey) program =
        async {
            let initial = emptyTickReport owned

            let! inbox =
                InboxFold.runOnce
                    StepRecordCodec.encode
                    StepRecordCodec.decode
                    InboxEnvelopeCodec.decode
                    options.MaxInboxRecords
                    owned

            match inbox with
            | InboxFoldStatus.Deposed expected -> return DurableHostTickStatus.Deposed(expected, initial)
            | InboxFoldStatus.Failed failure ->
                return DurableHostTickStatus.Failed(DurableHostTickFailure.InboxFailed failure, initial)
            | InboxFoldStatus.Folded inboxReport ->
                let reportAfterInbox =
                    { initial with
                        Inbox = Some inboxReport }

                let! step = stepOnce StepRecordCodec.encode StepRecordCodec.decode options.Timestamp owned program

                let reportAfterStep =
                    { reportAfterInbox with
                        Step = Some step }

                match step with
                | DurableHostStatus.Deposed expected -> return DurableHostTickStatus.Deposed(expected, reportAfterStep)
                | DurableHostStatus.Failed failure ->
                    return DurableHostTickStatus.Failed(DurableHostTickFailure.StepFailed failure, reportAfterStep)
                | DurableHostStatus.Completed value -> return DurableHostTickStatus.Completed(value, reportAfterStep)
                | DurableHostStatus.Committed _
                | DurableHostStatus.Waiting _ ->
                    let! activity =
                        ActivityCommandAdapter.runOnce
                            StepRecordCodec.encode
                            StepRecordCodec.decode
                            options.MaxActivityCommands
                            activities
                            owned

                    match activity with
                    | ActivityCommandAdapterStatus.Deposed expected ->
                        return DurableHostTickStatus.Deposed(expected, reportAfterStep)
                    | ActivityCommandAdapterStatus.Failed failure ->
                        return
                            DurableHostTickStatus.Failed(DurableHostTickFailure.ActivityFailed failure, reportAfterStep)
                    | ActivityCommandAdapterStatus.Processed activityReport ->
                        let reportAfterActivities =
                            { reportAfterStep with
                                Activities = Some activityReport }

                        let! timer =
                            TimerCommandAdapter.runOnce
                                StepRecordCodec.decode
                                options.Timestamp
                                options.MaxTimerCommands
                                owned

                        match timer with
                        | TimerCommandAdapterStatus.Deposed expected ->
                            return DurableHostTickStatus.Deposed(expected, reportAfterActivities)
                        | TimerCommandAdapterStatus.Failed failure ->
                            return
                                DurableHostTickStatus.Failed(
                                    DurableHostTickFailure.TimerFailed failure,
                                    reportAfterActivities
                                )
                        | TimerCommandAdapterStatus.Processed timerReport ->
                            let report =
                                { reportAfterActivities with
                                    Timers = Some timerReport }

                            match step with
                            | DurableHostStatus.Waiting(opId, need) when
                                not (inboxMadeProgress inboxReport)
                                && not (activitiesMadeProgress activityReport)
                                && not (timersMadeProgress timerReport)
                                ->
                                return DurableHostTickStatus.Waiting(opId, need, report)
                            | _ -> return DurableHostTickStatus.Advanced report
        }

    let claimAndRunTick options activities pair program =
        async {
            let! owned = S2Substrate.claim options.HostId pair
            return! runOwnedTick options activities owned program
        }

    let private startedWorkflow decode owned =
        async {
            try
                let! decoded = S2Substrate.readLogText decode owned

                let rec loop =
                    function
                    | [] -> Ok None
                    | (seqNum, Error error) :: _ -> Error(DurableWorkflowHostFailure.DecodeFailed(seqNum, error))
                    | (_, Ok(Incoming(WorkflowStarted(name, input)))) :: _ -> Ok(Some(name, input))
                    | _ :: rest -> loop rest

                return loop decoded
            with error ->
                return Error(DurableWorkflowHostFailure.LogReadFailed error.Message)
        }

    let runWorkflowTick options workflows activities owned =
        async {
            let! startFold =
                InboxFold.runOnce
                    StepRecordCodec.encode
                    StepRecordCodec.decode
                    InboxEnvelopeCodec.decode
                    options.MaxInboxRecords
                    owned

            match startFold with
            | InboxFoldStatus.Deposed expected -> return DurableWorkflowHostStatus.Deposed expected
            | InboxFoldStatus.Failed failure ->
                return DurableWorkflowHostStatus.Failed(DurableWorkflowHostFailure.StartFoldFailed failure)
            | InboxFoldStatus.Folded _ ->
                let! started = startedWorkflow StepRecordCodec.decode owned

                match started with
                | Error failure -> return DurableWorkflowHostStatus.Failed failure
                | Ok None -> return DurableWorkflowHostStatus.Failed DurableWorkflowHostFailure.NoStart
                | Ok(Some(workflowName, input)) ->
                    match WorkflowRegistry.require (WorkflowName.value workflowName) workflows with
                    | Error(DurableRegistryError.WorkflowNotFound missing) ->
                        return DurableWorkflowHostStatus.Failed(DurableWorkflowHostFailure.WorkflowNotFound missing)
                    | Error error ->
                        return DurableWorkflowHostStatus.Failed(DurableWorkflowHostFailure.LogReadFailed(string error))
                    | Ok factory ->
                        let! tick = runOwnedTick options activities owned (factory input)
                        return DurableWorkflowHostStatus.Ticked tick
        }

    let claimAndRunWorkflowTick options workflows activities pair =
        async {
            let! owned = S2Substrate.claim options.HostId pair
            return! runWorkflowTick options workflows activities owned
        }
