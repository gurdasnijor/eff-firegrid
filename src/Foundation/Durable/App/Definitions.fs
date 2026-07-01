namespace Eff.Foundation.Durable.App

open Eff.Foundation.Durable

type Activity<'input, 'output> =
    internal
        { Name: ActivityName
          EncodeInput: 'input -> Payload
          DecodeInput: Payload -> 'input
          EncodeOutput: 'output -> Payload
          DecodeOutput: Payload -> 'output
          Handler: 'input -> Async<'output> }

type Workflow<'input, 'output> =
    internal
        { Name: WorkflowName
          EncodeInput: 'input -> Payload
          DecodeInput: Payload -> 'input
          EncodeOutput: 'output -> Payload
          DecodeOutput: Payload -> 'output
          Factory: 'input -> Durable<'output> }

type Signal<'payload> =
    internal
        { Name: string
          Encode: 'payload -> Payload
          Decode: Payload -> 'payload }

type DurableRace<'result> =
    internal
        { Task: RaceTask
          Project: RaceResult -> 'result option }

type DurableAppClientConfig = { Storage: DurableStorage }

type DurableAppWorkerConfig =
    { Storage: DurableStorage
      HostId: string
      MaxRunUntilIdleTicks: int option }

type DurableAppEnvironmentClientConfig =
    { Environment: string
      BasinName: string option }

type DurableAppEnvironmentWorkerConfig =
    { Environment: string
      BasinName: string option
      HostId: string
      MaxRunUntilIdleTicks: int option }

[<RequireQualifiedAccess>]
type DurableAppStartFailure = AppendFailed of string

[<RequireQualifiedAccess>]
type DurableAppSignalFailure = AppendFailed of string

[<RequireQualifiedAccess>]
type DurableAppStatusFailure =
    | ReadFailed of string
    | DecodeFailed of seqNum: int64 * error: string
    | WorkflowNotFound of workflow: string
    | WorkflowMismatch of expected: string * actual: string
    | OutputDecodeFailed of workflow: string * error: string

[<RequireQualifiedAccess>]
type DurableAppStartResult =
    | Started of InstanceId
    | Rejected of DurableAppStartFailure

[<RequireQualifiedAccess>]
type DurableAppSignalResult =
    | Accepted
    | Rejected of DurableAppSignalFailure

[<RequireQualifiedAccess>]
type DurableAppNeed =
    | Activity of name: string
    | Activities of names: string list
    | Timer of deadline: int64
    | Signal of name: string
    | Race of contenders: string list
    | TimerCancellation of count: int
    | CurrentTime
    | Log of message: string

[<RequireQualifiedAccess>]
type DurableAppWorkflowStatus =
    | NotFound
    | Running of workflow: string
    | Waiting of workflow: string * need: DurableAppNeed
    | Completed of workflow: string * output: string
    | Failed of DurableAppStatusFailure

[<RequireQualifiedAccess>]
type DurableAppTypedWorkflowStatus<'output> =
    | NotFound
    | Running
    | Waiting of DurableAppNeed
    | Completed of 'output
    | Failed of DurableAppStatusFailure

type DurableAppWorkerInstanceResult =
    { InstanceId: InstanceId
      Ticks: DurableWorkflowHostStatus list
      Active: bool }

type DurableAppWorkerPass =
    { Instances: DurableAppWorkerInstanceResult list
      ActiveInstances: int }

type DurableAppWorker =
    { runOnce: InstanceId -> Async<DurableWorkflowHostStatus>
      runUntilIdle: InstanceId -> Async<DurableWorkflowHostStatus list>
      runUntilIdleWith: int -> InstanceId -> Async<DurableWorkflowHostStatus list>
      discover: unit -> Async<InstanceId list>
      runReady: unit -> Async<DurableAppWorkerPass>
      runReadyWith: int -> Async<DurableAppWorkerPass>
      runForever: System.Threading.CancellationToken -> Async<unit> }

type internal ActivityRegistration =
    { Name: string
      Register: ActivityRegistry -> Result<ActivityRegistry, DurableRegistryError> }

type internal WorkflowRegistration =
    { Name: string
      Register: WorkflowRegistry -> Result<WorkflowRegistry, DurableRegistryError> }
