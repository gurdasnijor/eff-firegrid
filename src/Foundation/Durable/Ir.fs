namespace Eff.Foundation.Durable

type ValueExpr =
    | Literal of Value
    | ActivityResult of OpId
    | EventResult of OpId
    | CurrentTimeResult of OpId

type IrOperation =
    | CallActivity of OpId * name: string * input: ValueExpr
    | AwaitEvent of OpId * EventKey
    | ReadCurrentTime of OpId
    | WriteLog of OpId * message: string

type DurableIr =
    { Operations: IrOperation list
      Return: ValueExpr }

type DurableIrDraft =
    { NextOpId: OpId
      Operations: IrOperation list }

type DurableWorkflow = { Name: string; Program: DurableIr }

type DurableIrIssue =
    | EmptyWorkflowName
    | DuplicateOperationId of OpId
    | NonContiguousOperationId of expected: OpId * actual: OpId
    | MissingValueSource of OpId
    | FutureValueSource of source: OpId * consumer: OpId

type DurableWorkflowReplay =
    | InvalidWorkflow of DurableIrIssue list
    | ValidWorkflow of Outcome<Value>

[<RequireQualifiedAccess>]
module ValueExpr =
    let literal value = Literal value

    let activityResult opId = ActivityResult opId

    let eventResult opId = EventResult opId

    let currentTimeResult opId = CurrentTimeResult opId

[<RequireQualifiedAccess>]
module DurableIr =
    let empty =
        { NextOpId = OpId.zero
          Operations = [] }

    let create operations returnValue =
        { Operations = operations
          Return = returnValue }

    let finish returnValue draft =
        { Operations = draft.Operations
          Return = returnValue }

    let private allocate draft =
        let opId = draft.NextOpId

        let next = { draft with NextOpId = OpId.next opId }

        opId, next

    let callActivity opId name input = CallActivity(opId, name, input)

    let awaitEvent opId key = AwaitEvent(opId, key)

    let readCurrentTime opId = ReadCurrentTime opId

    let writeLog opId message = WriteLog(opId, message)

    let appendCallActivity name input draft =
        let opId, draft = allocate draft

        { draft with
            Operations = draft.Operations @ [ callActivity opId name input ] },
        ValueExpr.activityResult opId

    let appendAwaitEvent key draft =
        let opId, draft = allocate draft

        { draft with
            Operations = draft.Operations @ [ awaitEvent opId key ] },
        ValueExpr.eventResult opId

    let appendCurrentTime draft =
        let opId, draft = allocate draft

        { draft with
            Operations = draft.Operations @ [ readCurrentTime opId ] },
        ValueExpr.currentTimeResult opId

    let appendLog message draft =
        let opId, draft = allocate draft

        { draft with
            Operations = draft.Operations @ [ writeLog opId message ] }

    let private operationId operation =
        match operation with
        | CallActivity(opId, _, _) -> opId
        | AwaitEvent(opId, _) -> opId
        | ReadCurrentTime opId -> opId
        | WriteLog(opId, _) -> opId

    let private operationOutput operation =
        match operation with
        | CallActivity(opId, _, _) -> Some(ActivityResult opId)
        | AwaitEvent(opId, _) -> Some(EventResult opId)
        | ReadCurrentTime opId -> Some(CurrentTimeResult opId)
        | WriteLog _ -> None

    let private operationInput operation =
        match operation with
        | CallActivity(_, _, input) -> Some input
        | AwaitEvent _
        | ReadCurrentTime _
        | WriteLog _ -> None

    let private expressionOpId expression =
        match expression with
        | Literal _ -> None
        | ActivityResult opId
        | EventResult opId
        | CurrentTimeResult opId -> Some opId

    let private validateExpression allOutputs availableOutputs consumer expression =
        match expression, expressionOpId expression with
        | Literal _, _ -> []
        | _, None -> []
        | _, Some _ when List.contains expression availableOutputs -> []
        | _, Some source when List.contains expression allOutputs -> [ FutureValueSource(source, consumer) ]
        | _, Some source -> [ MissingValueSource source ]

    let validate (program: DurableIr) =
        let allOutputs = program.Operations |> List.choose operationOutput

        let rec loop expected seen availableOutputs issues operations =
            match operations with
            | [] ->
                let returnIssues =
                    validateExpression allOutputs availableOutputs (OpId expected) program.Return

                issues @ returnIssues
            | operation :: rest ->
                let opId = operationId operation
                let expectedOpId = OpId expected

                let sequenceIssues =
                    let continuityIssues =
                        if opId <> expectedOpId then
                            [ NonContiguousOperationId(expectedOpId, opId) ]
                        else
                            []

                    if List.contains opId seen then
                        continuityIssues @ [ DuplicateOperationId opId ]
                    else
                        continuityIssues

                let inputIssues =
                    match operationInput operation with
                    | Some input -> validateExpression allOutputs availableOutputs opId input
                    | None -> []

                let availableOutputs =
                    match operationOutput operation with
                    | Some output -> availableOutputs @ [ output ]
                    | None -> availableOutputs

                loop (expected + 1) (seen @ [ opId ]) availableOutputs (issues @ sequenceIssues @ inputIssues) rest

        loop 0 [] [] [] program.Operations

    let private value history expr =
        match expr with
        | Literal value -> Some value
        | ActivityResult opId -> History.completed opId history
        | EventResult opId ->
            History.toList history
            |> List.tryPick (function
                | TimerFired id when id = opId -> Some ""
                | SignalReceived(id, _, payload) when id = opId -> Some payload
                | _ -> None)
        | CurrentTimeResult opId -> History.currentTime opId history |> Option.map string

    let private replayOperation history operation =
        match operation with
        | CallActivity(opId, name, input) ->
            match History.completed opId history with
            | Some _ -> None
            | None ->
                match value history input with
                | Some resolvedInput -> Some(Blocked(opId, NeedsActivity { Name = name; Input = resolvedInput }))
                | None -> Some(Blocked(opId, NeedsActivity { Name = name; Input = "" }))
        | AwaitEvent(opId, key) ->
            match History.resolved opId key history with
            | Some _ -> None
            | None -> Some(Blocked(opId, NeedsEvent key))
        | ReadCurrentTime opId ->
            match History.currentTime opId history with
            | Some _ -> None
            | None -> Some(Blocked(opId, NeedsCurrentTime))
        | WriteLog(opId, message) ->
            if History.logEmitted opId message history then
                None
            else
                Some(Blocked(opId, NeedsLog message))

    let replay history (program: DurableIr) =
        let rec loop operations =
            match operations with
            | [] ->
                match value history program.Return with
                | Some output -> Done output
                | None -> Done ""
            | operation :: rest ->
                match replayOperation history operation with
                | Some blocked -> blocked
                | None -> loop rest

        loop program.Operations

[<RequireQualifiedAccess>]
module DurableWorkflow =
    let create name (program: DurableIr) : DurableWorkflow = { Name = name; Program = program }

    let validate (workflow: DurableWorkflow) =
        let nameIssues = if workflow.Name = "" then [ EmptyWorkflowName ] else []

        nameIssues @ DurableIr.validate workflow.Program

    let replay history workflow =
        match validate workflow with
        | [] -> DurableIr.replay history workflow.Program |> ValidWorkflow
        | issues -> InvalidWorkflow issues
