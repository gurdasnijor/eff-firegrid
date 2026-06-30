namespace Eff.Foundation.Durable

type DurableClientStartAck =
    { InstanceId: InstanceId
      InboxSeqNum: int64 }

type DurableClientSignalAck =
    { InstanceId: InstanceId
      InboxSeqNum: int64
      Source: string
      SignalName: string
      SourceSeqNum: int64 }

type DurableInstanceStatus =
    | InstanceNotFound
    | InstanceRunning of WorkflowName
    | InstanceWaiting of WorkflowName * OpId * Need
    | InstanceCompleted of WorkflowName * Payload

[<RequireQualifiedAccess>]
type DurableClientFailure =
    | StartAppendFailed of string
    | SignalAppendFailed of string

[<RequireQualifiedAccess>]
type DurableClientStatusFailure =
    | StatusReadFailed of string
    | StatusDecodeFailed of seqNum: int64 * error: string
    | WorkflowNotFound of WorkflowName

[<RequireQualifiedAccess>]
type DurableClientStartStatus =
    | Accepted of DurableClientStartAck
    | Failed of DurableClientFailure

[<RequireQualifiedAccess>]
type DurableClientSignalStatus =
    | Accepted of DurableClientSignalAck
    | Failed of DurableClientFailure

[<RequireQualifiedAccess>]
type DurableClientStatusRead =
    | Succeeded of DurableInstanceStatus
    | Failed of DurableClientStatusFailure

[<RequireQualifiedAccess>]
module DurableClient =
    let private startSource = "client:start"
    let private signalSource = "client:signal"

    let instanceKey instanceId = StorageKey(InstanceId.value instanceId)

    let startWith basin instanceId workflowName input =
        async {
            try
                let key = instanceKey instanceId

                do! S2Substrate.ensureStreams basin key

                let pair = S2Substrate.streams basin key

                let envelope =
                    { Source = startSource
                      SourceSeqNum = 0L
                      Message = StartWorkflow(workflowName, input) }

                let! ack = S2Substrate.appendInboxText [ "kind", "start" ] (InboxEnvelopeCodec.encode envelope) pair

                return
                    DurableClientStartStatus.Accepted
                        { InstanceId = instanceId
                          InboxSeqNum = ack.Start.SeqNum }
            with error ->
                return DurableClientStartStatus.Failed(DurableClientFailure.StartAppendFailed error.Message)
        }

    let raiseSignalFrom basin instanceId source sourceSeqNum name payload =
        async {
            try
                if System.String.IsNullOrWhiteSpace source then
                    invalidArg (nameof source) "source must be non-empty"

                if sourceSeqNum < 0L then
                    invalidArg (nameof sourceSeqNum) "sourceSeqNum must be non-negative"

                let key = instanceKey instanceId

                do! S2Substrate.ensureStreams basin key

                let pair = S2Substrate.streams basin key

                let envelope =
                    { Source = source
                      SourceSeqNum = sourceSeqNum
                      Message = RaiseSignal(name, payload) }

                let! ack =
                    S2Substrate.appendInboxText
                        [ "kind", "signal"; "name", name ]
                        (InboxEnvelopeCodec.encode envelope)
                        pair

                return
                    DurableClientSignalStatus.Accepted
                        { InstanceId = instanceId
                          InboxSeqNum = ack.Start.SeqNum
                          Source = source
                          SignalName = name
                          SourceSeqNum = sourceSeqNum }
            with error ->
                return DurableClientSignalStatus.Failed(DurableClientFailure.SignalAppendFailed error.Message)
        }

    let raiseSignalWith basin instanceId sourceSeqNum name payload =
        raiseSignalFrom basin instanceId signalSource sourceSeqNum name payload

    let private decodeLog decoded =
        let rec loop records =
            function
            | [] -> Ok(List.rev records)
            | (_, Ok record) :: rest -> loop (record :: records) rest
            | (seqNum, Error error) :: _ -> Error(DurableClientStatusFailure.StatusDecodeFailed(seqNum, error))

        loop [] decoded

    let private startedWorkflow records =
        records
        |> List.tryPick (function
            | Incoming(WorkflowStarted(name, input)) -> Some(name, input)
            | _ -> None)

    let getStatusWith basin workflows instanceId =
        async {
            try
                let key = instanceKey instanceId

                do! S2Substrate.ensureStreams basin key

                let pair = S2Substrate.streams basin key

                let owned =
                    { Key = key
                      Fence = FenceToken "client:status"
                      Log = pair.Log
                      Inbox = pair.Inbox }

                let! decoded = S2Substrate.readLogText StepRecordCodec.decode owned

                match decodeLog decoded with
                | Error failure -> return DurableClientStatusRead.Failed failure
                | Ok records ->
                    match startedWorkflow records with
                    | None -> return DurableClientStatusRead.Succeeded InstanceNotFound
                    | Some(workflowName, input) ->
                        match WorkflowRegistry.require (WorkflowName.value workflowName) workflows with
                        | Error(DurableRegistryError.WorkflowNotFound missing) ->
                            return DurableClientStatusRead.Failed(DurableClientStatusFailure.WorkflowNotFound missing)
                        | Error error ->
                            return
                                DurableClientStatusRead.Failed(
                                    DurableClientStatusFailure.StatusReadFailed(string error)
                                )
                        | Ok factory ->
                            let history = DurableStepper.historyFromRecords records

                            return
                                match DurableStepper.plan 0L history (factory input) with
                                | Complete value ->
                                    DurableClientStatusRead.Succeeded(InstanceCompleted(workflowName, value))
                                | Waiting(opId, need) ->
                                    DurableClientStatusRead.Succeeded(InstanceWaiting(workflowName, opId, need))
                                | Commit _ -> DurableClientStatusRead.Succeeded(InstanceRunning workflowName)
            with error ->
                return DurableClientStatusRead.Failed(DurableClientStatusFailure.StatusReadFailed error.Message)
        }
