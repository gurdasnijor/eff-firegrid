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
      MaxActivityCommands: int }

type DurableHostTickReport<'a> =
    { Key: StorageKey
      Fence: FenceToken
      Inbox: InboxFoldReport option
      Step: DurableHostStatus<'a> option
      Activities: ActivityCommandAdapterReport option }

[<RequireQualifiedAccess>]
type DurableHostTickFailure =
    | InboxFailed of InboxFoldFailure
    | StepFailed of DurableHostFailure
    | ActivityFailed of ActivityCommandAdapterFailure

[<RequireQualifiedAccess>]
type DurableHostTickStatus<'a> =
    | Completed of value: 'a * report: DurableHostTickReport<'a>
    | Waiting of opId: OpId * need: Need * report: DurableHostTickReport<'a>
    | Advanced of DurableHostTickReport<'a>
    | Deposed of expectedFence: string * report: DurableHostTickReport<'a>
    | Failed of DurableHostTickFailure * report: DurableHostTickReport<'a>

[<RequireQualifiedAccess>]
module DurableHostTickOptions =
    let create hostId timestamp =
        { HostId = hostId
          Timestamp = timestamp
          MaxInboxRecords = 100
          MaxActivityCommands = 100 }

[<RequireQualifiedAccess>]
module DurableHost =
    let private emptyTickReport (owned: OwnedKey) =
        { Key = owned.Key
          Fence = owned.Fence
          Inbox = None
          Step = None
          Activities = None }

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
                        let report =
                            { reportAfterStep with
                                Activities = Some activityReport }

                        match step with
                        | DurableHostStatus.Waiting(opId, need) when
                            not (inboxMadeProgress inboxReport)
                            && not (activitiesMadeProgress activityReport)
                            ->
                            return DurableHostTickStatus.Waiting(opId, need, report)
                        | _ -> return DurableHostTickStatus.Advanced report
        }

    let claimAndRunTick options activities pair program =
        async {
            let! owned = S2Substrate.claim options.HostId pair
            return! runOwnedTick options activities owned program
        }
