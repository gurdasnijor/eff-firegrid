namespace Eff.Proofs

open Eff

type TraceStore =
    { TrialId: string
      Root: string
      SpansJsonl: string }

type S2Resource =
    { Client: S2.Client
      Kind: string
      Endpoint: string option
      LocalRoot: string option }

module S2Resource =
    let basin name resource = resource.Client |> S2.basin name

type WorkloadContext =
    { TrialId: string
      Root: string
      Traces: TraceStore
      Seed: int
      S2: S2Resource option
      NextOperationId: unit -> int
      EmitSpan: string -> (string * string) list -> Async<unit> }

module WorkloadContext =
    let requireS2 (ctx: WorkloadContext) =
        match ctx.S2 with
        | Some s2 -> s2
        | None -> failwith "workload requires s2LiveFromEnv or s2Lite but no S2 resource was declared"

    let s2Basin name ctx =
        ctx |> requireS2 |> S2Resource.basin name

type ProofOperationOptions =
    { ClientId: string option
      OperationId: string option
      Key: string option }

module ProofOperationOptions =
    let empty =
        { ClientId = None
          OperationId = None
          Key = None }

type CompletedTrial<'result> =
    { ProofName: string
      PropertyName: string
      TrialId: string
      Result: Result<'result, string>
      Traces: TraceStore }

type Check<'result> =
    { Name: string
      RunCheck: CompletedTrial<'result> -> Async<Result<unit, string>> }

type NegativeControlSpec<'result> =
    { Name: string
      Workload: (WorkloadContext -> Async<'result>) option
      Verifiers: Check<'result> list
      ExpectedFailure: string option }

type ResourceSpec =
    | S2LiveFromEnv
    | S2Lite of root: string
    | ProcessHost of name: string

type PropertySpec<'result> =
    { Name: string
      Resources: ResourceSpec list
      Workload: WorkloadContext -> Async<'result>
      Verifiers: Check<'result> list
      NegativeControls: NegativeControlSpec<'result> list
      RequiresNegativeControl: bool }

type CheckReport =
    { Name: string
      Passed: bool
      Message: string option }

type NegativeControlReport =
    { Name: string
      Passed: bool
      ExpectedFailure: string option
      FailedChecks: string list
      Message: string option }

type PropertyReport =
    { ProofName: string
      PropertyName: string
      TrialId: string
      Passed: bool
      WorkloadFailed: bool
      Checks: CheckReport list
      NegativeControls: NegativeControlReport list
      ReportPath: string }

type RunnerConfig =
    { Root: string
      ProofFilter: string option
      TrialId: string option
      Preserve: bool
      Seed: int }

type RunnableProperty =
    { Name: string
      RunProperty: RunnerConfig -> string -> Async<PropertyReport> }

type ProofSpec =
    { Name: string
      Description: string option
      Properties: RunnableProperty list }

type ProofDraft =
    { Description: string option
      Properties: RunnableProperty list }
