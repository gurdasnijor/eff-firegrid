namespace Eff.Proofs

module Property =
    type NegativeControlDraft<'result> =
        { Workload: (WorkloadContext -> Async<'result>) option
          Verifiers: Check<'result> list
          ExpectedFailure: string option }

    type PropertyDraft<'result> =
        { Resources: ResourceSpec list
          Workload: (WorkloadContext -> Async<'result>) option
          Verifiers: Check<'result> list
          NegativeControls: NegativeControlSpec<'result> list
          RequiresNegativeControl: bool }

    let private unsupportedResources resources =
        resources
        |> List.choose (function
            | S2Lite _ -> Some "s2Lite is not implemented yet"
            | ProcessHost name -> Some(sprintf "processHost '%s' is not implemented yet" name)
            | S2LiveFromEnv -> None)

    let private checkReport name result =
        match result with
        | Ok() ->
            { Name = name
              Passed = true
              Message = None }
        | Error message ->
            { Name = name
              Passed = false
              Message = Some message }

    let private runWorkload work ctx =
        async {
            try
                let! result = work ctx
                return Ok result
            with e ->
                return Error e.Message
        }

    let private runChecks (trial: CompletedTrial<'result>) (checks: Check<'result> list) =
        async {
            let reports = ResizeArray<CheckReport>()

            for check in checks do
                let! result = check.RunCheck trial
                reports.Add(checkReport check.Name result)

            return List.ofSeq reports
        }

    let private workloadContext (trialId: string) (seed: int) (store: TraceStore) =
        let nextOperation =
            let counter = ref 0

            fun () ->
                counter.Value <- counter.Value + 1
                counter.Value

        { TrialId = trialId
          Root = store.Root
          Traces = store
          Seed = seed
          NextOperationId = nextOperation
          EmitSpan = Reports.emitSpan store }

    let private runNegativeControl
        (proofName: string)
        (propertyName: string)
        (root: string)
        (seed: int)
        (positiveWorkload: WorkloadContext -> Async<'result>)
        (traces: TraceStore)
        (control: NegativeControlSpec<'result>)
        =
        async {
            let trialId = Reports.trialId (propertyName + "-negative")
            let store = Reports.traceStore root trialId

            let ctx = workloadContext trialId seed store

            let work = control.Workload |> Option.defaultValue positiveWorkload
            let! result = runWorkload work ctx

            let completed =
                { ProofName = proofName
                  PropertyName = propertyName
                  TrialId = trialId
                  Result = result
                  Traces = store }

            let! checkReports = runChecks completed control.Verifiers

            let failedChecks =
                checkReports
                |> List.filter (fun report -> not report.Passed)
                |> List.map (fun report -> report.Name)

            let matchedExpectedFailure =
                match control.ExpectedFailure with
                | None -> not (List.isEmpty failedChecks)
                | Some expected -> failedChecks |> List.exists (fun failed -> failed.Contains expected)

            let passed = matchedExpectedFailure

            do!
                Reports.emitSpan
                    traces
                    "verification.negative_control"
                    [ "proof.property", propertyName
                      "negative.name", control.Name
                      "negative.passed", string passed ]

            return
                { Name = control.Name
                  Passed = passed
                  ExpectedFailure = control.ExpectedFailure
                  FailedChecks = failedChecks
                  Message =
                    if passed then
                        None
                    else
                        Some "negative control did not fail for the expected reason" }
        }

    let private runProperty (proofName: string) (config: RunnerConfig) (spec: PropertySpec<'result>) =
        async {
            let trialId = config.TrialId |> Option.defaultValue (Reports.trialId spec.Name)

            let store = Reports.traceStore config.Root trialId
            let reportPath = Reports.join [ store.Root; "report.json" ]

            do!
                Reports.emitSpan
                    store
                    "verification.property.start"
                    [ "proof.name", proofName; "proof.property", spec.Name ]

            let unsupported = unsupportedResources spec.Resources

            let! workloadResult =
                if List.isEmpty unsupported then
                    workloadContext trialId config.Seed store |> runWorkload spec.Workload
                else
                    async { return Error(String.concat "; " unsupported) }

            let completed =
                { ProofName = proofName
                  PropertyName = spec.Name
                  TrialId = trialId
                  Result = workloadResult
                  Traces = store }

            let! checks = runChecks completed spec.Verifiers

            let! negativeControls =
                spec.NegativeControls
                |> List.map (runNegativeControl proofName spec.Name config.Root config.Seed spec.Workload store)
                |> Async.Sequential

            let missingNegativeControl =
                spec.RequiresNegativeControl && Array.isEmpty negativeControls

            let failedChecks = checks |> List.exists (fun check -> not check.Passed)

            let failedNegativeControls =
                negativeControls |> Array.exists (fun control -> not control.Passed)

            let workloadFailed = Result.isError workloadResult

            let passed =
                not workloadFailed
                && not failedChecks
                && not failedNegativeControls
                && not missingNegativeControl

            let checks =
                if missingNegativeControl then
                    { Name = "negative control required"
                      Passed = false
                      Message = Some "property requires a negative control but none were declared" }
                    :: checks
                else
                    checks

            do!
                Reports.emitSpan
                    store
                    "verification.property.finish"
                    [ "proof.name", proofName
                      "proof.property", spec.Name
                      "verification.passed", string passed ]

            let report =
                { ProofName = proofName
                  PropertyName = spec.Name
                  TrialId = trialId
                  Passed = passed
                  WorkloadFailed = workloadFailed
                  Checks = checks
                  NegativeControls = Array.toList negativeControls
                  ReportPath = reportPath }

            Reports.writePropertyReport report
            return report
        }

    type NegativeControlBuilder<'result>(name: string) =
        member _.Yield(_) : NegativeControlDraft<'result> =
            { Workload = None
              Verifiers = []
              ExpectedFailure = None }

        [<CustomOperation("workload")>]
        member _.Workload(state: NegativeControlDraft<'result>, work) = { state with Workload = Some work }

        [<CustomOperation("verify")>]
        member _.Verify(state: NegativeControlDraft<'result>, verifiers) =
            { state with
                Verifiers = state.Verifiers @ verifiers }

        [<CustomOperation("expectFailure")>]
        member _.ExpectFailure(state: NegativeControlDraft<'result>, expected) =
            { state with
                ExpectedFailure = Some expected }

        member _.Run(state: NegativeControlDraft<'result>) : NegativeControlSpec<'result> =
            { Name = name
              Workload = state.Workload
              Verifiers = state.Verifiers
              ExpectedFailure = state.ExpectedFailure }

    let make name resources workload verifiers negativeControls requiresNegativeControl =
        if List.isEmpty verifiers then
            failwithf "property '%s' must declare at least one verifier" name

        let spec =
            { Name = name
              Resources = resources
              Workload = workload
              Verifiers = verifiers
              NegativeControls = negativeControls
              RequiresNegativeControl = requiresNegativeControl }

        { Name = name
          RunProperty = fun config proofName -> runProperty proofName config spec }

    let negativeControl<'result> name = NegativeControlBuilder<'result> name

    type PropertyBuilder(name: string) =
        member _.Yield(_) : PropertyDraft<unit> =
            { Resources = []
              Workload = None
              Verifiers = []
              NegativeControls = []
              RequiresNegativeControl = false }

        [<CustomOperation("resource")>]
        member _.Resource(state: PropertyDraft<'result>, resource) =
            { state with
                Resources = state.Resources @ [ resource ] }

        [<CustomOperation("resources")>]
        member _.Resources(state: PropertyDraft<'result>, resources) =
            { state with
                Resources = state.Resources @ resources }

        [<CustomOperation("s2LiveFromEnv")>]
        member _.S2LiveFromEnv(state: PropertyDraft<'result>) =
            { state with
                Resources = state.Resources @ [ S2LiveFromEnv ] }

        [<CustomOperation("s2Lite")>]
        member _.S2Lite(state: PropertyDraft<'result>, root) =
            { state with
                Resources = state.Resources @ [ S2Lite root ] }

        [<CustomOperation("processHost")>]
        member _.ProcessHost(state: PropertyDraft<'result>, host) =
            { state with
                Resources = state.Resources @ [ ProcessHost host ] }

        [<CustomOperation("workload")>]
        member _.Workload(state: PropertyDraft<'previous>, workload: WorkloadContext -> Async<'result>) =
            { Resources = state.Resources
              Workload = Some workload
              Verifiers = []
              NegativeControls = []
              RequiresNegativeControl = state.RequiresNegativeControl }

        [<CustomOperation("verify")>]
        member _.Verify(state: PropertyDraft<'result>, verifiers: Check<'result> list) =
            { state with
                Verifiers = state.Verifiers @ verifiers }

        [<CustomOperation("verify")>]
        member _.Verify(state: PropertyDraft<'result>, factory: Verifiers<'result> -> Check<'result> list) =
            { state with
                Verifiers = state.Verifiers @ factory (Verification.verifiers ()) }

        [<CustomOperation("negativeControl")>]
        member _.NegativeControl(state: PropertyDraft<'result>, control) =
            { state with
                NegativeControls = state.NegativeControls @ [ control ] }

        [<CustomOperation("requiresNegativeControl")>]
        member _.RequiresNegativeControl(state: PropertyDraft<'result>) =
            { state with
                RequiresNegativeControl = true }

        member _.Run(state: PropertyDraft<'result>) =
            match state.Workload with
            | None -> failwithf "property '%s' must declare a workload" name
            | Some workload ->
                make name state.Resources workload state.Verifiers state.NegativeControls state.RequiresNegativeControl

    let property name = PropertyBuilder name

[<AutoOpen>]
module PropertySyntax =
    let property name = Property.property name

    let propertyWithChecks name workload verifiers =
        Property.make name [] workload verifiers [] false

    let propertyWith name resources workload verifiers =
        Property.make name resources workload verifiers [] false

    let propertyWithControls name resources workload verifiers controls requiresNegativeControl =
        Property.make name resources workload verifiers controls requiresNegativeControl

    let propertyWithVerifiers name workload factory =
        Property.make name [] workload (factory (Verification.verifiers ())) [] false

    let propertyWithResourcesAndVerifiers name resources workload factory =
        Property.make name resources workload (factory (Verification.verifiers ())) [] false

    let propertyWithControlsAndVerifiers name resources workload factory controls requiresNegativeControl =
        Property.make name resources workload (factory (Verification.verifiers ())) controls requiresNegativeControl

    let negativeControl<'result> name = Property.negativeControl<'result> name
