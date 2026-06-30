namespace Eff.Foundation.Durable

open Eff
open Fable.Core

type DurableRuntimeOptions =
    { HostId: string
      Timestamp: unit -> int64
      MaxInboxRecords: int
      MaxActivityCommands: int
      MaxTimerCommands: int
      MaxRunUntilIdleTicks: int }

type DurableRuntimeClient =
    { Start: WorkflowName -> Payload -> Async<DurableClientStartStatus>
      StartWith: InstanceId -> WorkflowName -> Payload -> Async<DurableClientStartStatus>
      RaiseSignal: InstanceId -> string -> Payload -> Async<DurableClientSignalStatus>
      RaiseSignalWith: InstanceId -> int64 -> string -> Payload -> Async<DurableClientSignalStatus>
      GetStatus: InstanceId -> Async<DurableClientStatusRead> }

type DurableRuntimeHost =
    { RunOnce: InstanceId -> Async<DurableWorkflowHostStatus>
      RunUntilIdle: InstanceId -> Async<DurableWorkflowHostStatus list>
      RunUntilIdleWith: int -> InstanceId -> Async<DurableWorkflowHostStatus list> }

type DurableRuntime =
    { Client: DurableRuntimeClient
      Host: DurableRuntimeHost
      Workflows: WorkflowRegistry
      Activities: ActivityRegistry }

[<RequireQualifiedAccess>]
module DurableRuntimeOptions =
    [<Emit("Date.now()")>]
    let private nowMillis () : int64 = jsNative

    let create hostId =
        { HostId = hostId
          Timestamp = nowMillis
          MaxInboxRecords = 100
          MaxActivityCommands = 100
          MaxTimerCommands = 100
          MaxRunUntilIdleTicks = 100 }

[<RequireQualifiedAccess>]
module DurableRuntime =
    [<Emit("Date.now().toString(36) + '-' + Math.random().toString(36).slice(2)")>]
    let private entropy () : string = jsNative

    let private instanceIdFor workflowName =
        InstanceId.create (WorkflowName.value workflowName + "-" + entropy ())

    let private tickOptions (options: DurableRuntimeOptions) =
        { HostId = options.HostId
          Timestamp = options.Timestamp()
          MaxInboxRecords = options.MaxInboxRecords
          MaxActivityCommands = options.MaxActivityCommands
          MaxTimerCommands = options.MaxTimerCommands }

    let private shouldStop =
        function
        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Advanced _) -> false
        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed _)
        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Waiting _)
        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Deposed _)
        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Failed _)
        | DurableWorkflowHostStatus.Deposed _
        | DurableWorkflowHostStatus.Failed _ -> true

    let create options basin workflows activities =
        let runtimeId = "runtime:" + entropy ()
        let signalSource = runtimeId + ":signal"
        let mutable nextSignalSourceSeqNum = 0L

        let allocateSignalSourceSeqNum () =
            let seqNum = nextSignalSourceSeqNum
            nextSignalSourceSeqNum <- nextSignalSourceSeqNum + 1L
            seqNum

        let runOnce instanceId =
            async {
                let key = DurableClient.instanceKey instanceId
                do! S2Substrate.ensureStreams basin key
                let pair = S2Substrate.streams basin key
                return! DurableHost.claimAndRunWorkflowTick (tickOptions options) workflows activities pair
            }

        let runUntilIdleWith maxTicks instanceId =
            async {
                if maxTicks < 1 then
                    invalidArg (nameof maxTicks) "maxTicks must be positive"

                let rec loop remaining acc =
                    async {
                        if remaining = 0 then
                            return List.rev acc
                        else
                            let! tick = runOnce instanceId
                            let acc = tick :: acc

                            if shouldStop tick then
                                return List.rev acc
                            else
                                return! loop (remaining - 1) acc
                    }

                return! loop maxTicks []
            }

        { Client =
            { Start =
                fun workflowName input ->
                    let instanceId = instanceIdFor workflowName
                    DurableClient.startWith basin instanceId workflowName input
              StartWith =
                fun instanceId workflowName input -> DurableClient.startWith basin instanceId workflowName input
              RaiseSignal =
                fun instanceId name payload ->
                    DurableClient.raiseSignalFrom
                        basin
                        instanceId
                        signalSource
                        (allocateSignalSourceSeqNum ())
                        name
                        payload
              RaiseSignalWith =
                fun instanceId sourceSeqNum name payload ->
                    DurableClient.raiseSignalFrom basin instanceId signalSource sourceSeqNum name payload
              GetStatus = DurableClient.getStatusWith basin workflows }
          Host =
            { RunOnce = runOnce
              RunUntilIdle = runUntilIdleWith options.MaxRunUntilIdleTicks
              RunUntilIdleWith = runUntilIdleWith }
          Workflows = workflows
          Activities = activities }
