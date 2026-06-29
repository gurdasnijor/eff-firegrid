namespace Eff.Proofs

type TraceStore =
    { TrialId: string
      Root: string
      SpansJsonl: string }

type WorkloadContext =
    { TrialId: string
      Root: string
      Traces: TraceStore
      Seed: int
      EmitSpan: string -> (string * string) list -> Async<unit> }

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
