namespace Eff.Foundation.Durable

[<Struct>]
type ActivityName = ActivityName of string

[<Struct>]
type WorkflowName = WorkflowName of string

type Payload = Value

type ActivityHandler = Payload -> Async<Payload>

type WorkflowFactory = Payload -> Durable<Payload>

type ActivityRegistry = private ActivityRegistry of Map<string, ActivityHandler>

type WorkflowRegistry = private WorkflowRegistry of Map<string, WorkflowFactory>

[<RequireQualifiedAccess>]
type DurableRegistryError =
    | DuplicateActivity of ActivityName
    | ActivityNotFound of ActivityName
    | DuplicateWorkflow of WorkflowName
    | WorkflowNotFound of WorkflowName

[<RequireQualifiedAccess>]
module ActivityName =
    let create name = ActivityName name

    let value (ActivityName name) = name

[<RequireQualifiedAccess>]
module WorkflowName =
    let create name = WorkflowName name

    let value (WorkflowName name) = name

[<RequireQualifiedAccess>]
module ActivityRegistry =
    let empty = ActivityRegistry Map.empty

    let register name handler (ActivityRegistry handlers) =
        if Map.containsKey name handlers then
            Error(DurableRegistryError.DuplicateActivity(ActivityName name))
        else
            handlers |> Map.add name handler |> ActivityRegistry |> Ok

    let tryFind name (ActivityRegistry handlers) = Map.tryFind name handlers

    let require name registry =
        match tryFind name registry with
        | Some handler -> Ok handler
        | None -> Error(DurableRegistryError.ActivityNotFound(ActivityName name))

    let names (ActivityRegistry handlers) = handlers |> Map.toList |> List.map fst

    let count (ActivityRegistry handlers) = Map.count handlers

[<RequireQualifiedAccess>]
module WorkflowRegistry =
    let empty = WorkflowRegistry Map.empty

    let register name factory (WorkflowRegistry factories) =
        if Map.containsKey name factories then
            Error(DurableRegistryError.DuplicateWorkflow(WorkflowName name))
        else
            factories |> Map.add name factory |> WorkflowRegistry |> Ok

    let tryFind name (WorkflowRegistry factories) = Map.tryFind name factories

    let require name registry =
        match tryFind name registry with
        | Some factory -> Ok factory
        | None -> Error(DurableRegistryError.WorkflowNotFound(WorkflowName name))

    let names (WorkflowRegistry factories) = factories |> Map.toList |> List.map fst

    let count (WorkflowRegistry factories) = Map.count factories
