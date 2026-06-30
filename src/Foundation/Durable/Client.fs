namespace Eff.Foundation.Durable

type DurableClientStartAck =
    { InstanceId: InstanceId
      InboxSeqNum: int64 }

[<RequireQualifiedAccess>]
type DurableClientFailure = StartAppendFailed of string

[<RequireQualifiedAccess>]
type DurableClientStartStatus =
    | Accepted of DurableClientStartAck
    | Failed of DurableClientFailure

[<RequireQualifiedAccess>]
module DurableClient =
    let private startSource = "client:start"

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
