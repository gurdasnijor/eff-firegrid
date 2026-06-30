namespace Eff.Foundation.Durable

type DurableClientStartAck =
    { InstanceId: InstanceId
      InboxSeqNum: int64 }

type DurableClientSignalAck =
    { InstanceId: InstanceId
      InboxSeqNum: int64
      SignalName: string
      SourceSeqNum: int64 }

[<RequireQualifiedAccess>]
type DurableClientFailure =
    | StartAppendFailed of string
    | SignalAppendFailed of string

[<RequireQualifiedAccess>]
type DurableClientStartStatus =
    | Accepted of DurableClientStartAck
    | Failed of DurableClientFailure

[<RequireQualifiedAccess>]
type DurableClientSignalStatus =
    | Accepted of DurableClientSignalAck
    | Failed of DurableClientFailure

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

    let raiseSignalWith basin instanceId sourceSeqNum name payload =
        async {
            try
                if sourceSeqNum < 0L then
                    invalidArg (nameof sourceSeqNum) "sourceSeqNum must be non-negative"

                let key = instanceKey instanceId

                do! S2Substrate.ensureStreams basin key

                let pair = S2Substrate.streams basin key

                let envelope =
                    { Source = signalSource
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
                          SignalName = name
                          SourceSeqNum = sourceSeqNum }
            with error ->
                return DurableClientSignalStatus.Failed(DurableClientFailure.SignalAppendFailed error.Message)
        }
