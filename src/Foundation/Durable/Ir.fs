namespace Eff.Foundation.Durable

type ValueExpr =
    | Literal of Value
    | ActivityResult of OpId
    | EventResult of OpId
    | CurrentTimeResult of OpId

type ActivityCallExpr =
    { CallOpId: OpId
      CallName: string
      CallInput: ValueExpr }

type IrOperation =
    | IrCallActivity of OpId * name: string * input: ValueExpr
    | IrCallActivities of ActivityCallExpr list
    | IrAwaitEvent of OpId * EventKey
    | IrReadCurrentTime of OpId
    | IrWriteLog of OpId * message: string

type DurableIr =
    { Operations: IrOperation list
      Return: ValueExpr }

type DurableIrDraft =
    { NextOpId: OpId
      Operations: IrOperation list }

type DurableWorkflow = { Name: string; Program: DurableIr }

type DurableIrApp = { Workflows: DurableWorkflow list }

type DurableIrIssue =
    | EmptyWorkflowName
    | DuplicateOperationId of OpId
    | NonContiguousOperationId of expected: OpId * actual: OpId
    | MissingValueSource of OpId
    | FutureValueSource of source: OpId * consumer: OpId

type DurableIrAppIssue =
    | EmptyDurableIrApp
    | DuplicateWorkflowName of string
    | InvalidWorkflowDescriptor of workflowName: string * issues: DurableIrIssue list

type DurableWorkflowReplay =
    | InvalidWorkflow of DurableIrIssue list
    | ValidWorkflow of Outcome<Value>

type DurableIrAppReplay =
    | InvalidDurableIrApp of DurableIrAppIssue list
    | DurableIrWorkflowNotFound of string
    | DurableIrWorkflowReplay of DurableWorkflowReplay

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

    let private allocateMany count draft =
        let rec loop remaining current ids =
            if remaining <= 0 then
                ids, { draft with NextOpId = current }
            else
                loop (remaining - 1) (OpId.next current) (ids @ [ current ])

        loop count draft.NextOpId []

    let callActivity opId name input = IrCallActivity(opId, name, input)

    let callActivities calls = IrCallActivities calls

    let awaitEvent opId key = IrAwaitEvent(opId, key)

    let readCurrentTime opId = IrReadCurrentTime opId

    let writeLog opId message = IrWriteLog(opId, message)

    let appendCallActivity name input draft =
        let opId, draft = allocate draft

        { draft with
            Operations = draft.Operations @ [ callActivity opId name input ] },
        ValueExpr.activityResult opId

    let appendCallActivities calls draft =
        let calls = calls |> List.ofSeq
        let opIds, draft = allocateMany calls.Length draft

        let planned =
            List.zip opIds calls
            |> List.map (fun (opId, (name, input)) ->
                { CallOpId = opId
                  CallName = name
                  CallInput = input })

        { draft with
            Operations = draft.Operations @ [ callActivities planned ] },
        opIds |> List.map ValueExpr.activityResult

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
        | IrCallActivity(opId, _, _) -> opId
        | IrCallActivities calls ->
            calls
            |> List.tryHead
            |> Option.map (fun call -> call.CallOpId)
            |> Option.defaultValue OpId.zero
        | IrAwaitEvent(opId, _) -> opId
        | IrReadCurrentTime opId -> opId
        | IrWriteLog(opId, _) -> opId

    let private operationIds operation =
        match operation with
        | IrCallActivity(opId, _, _) -> [ opId ]
        | IrCallActivities calls -> calls |> List.map (fun call -> call.CallOpId)
        | IrAwaitEvent(opId, _) -> [ opId ]
        | IrReadCurrentTime opId -> [ opId ]
        | IrWriteLog(opId, _) -> [ opId ]

    let private operationOutputs operation =
        match operation with
        | IrCallActivity(opId, _, _) -> [ ActivityResult opId ]
        | IrCallActivities calls -> calls |> List.map (fun call -> ActivityResult call.CallOpId)
        | IrAwaitEvent(opId, _) -> [ EventResult opId ]
        | IrReadCurrentTime opId -> [ CurrentTimeResult opId ]
        | IrWriteLog _ -> []

    let private operationInputs operation =
        match operation with
        | IrCallActivity(_, _, input) -> [ operationId operation, input ]
        | IrCallActivities calls -> calls |> List.map (fun call -> call.CallOpId, call.CallInput)
        | IrAwaitEvent _
        | IrReadCurrentTime _
        | IrWriteLog _ -> []

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
        let allOutputs = program.Operations |> List.collect operationOutputs

        let rec loop expected seen availableOutputs issues operations =
            match operations with
            | [] ->
                let returnIssues =
                    validateExpression allOutputs availableOutputs (OpId expected) program.Return

                issues @ returnIssues
            | operation :: rest ->
                let opIds = operationIds operation

                let sequenceIssues =
                    let rec validateIds offset seenIds issues ids =
                        match ids with
                        | [] -> issues
                        | actual :: rest ->
                            let expected = OpId(expected + offset)

                            let continuityIssues =
                                if actual <> expected then
                                    issues @ [ NonContiguousOperationId(expected, actual) ]
                                else
                                    issues

                            let duplicateIssues =
                                if List.contains actual seen || List.contains actual seenIds then
                                    continuityIssues @ [ DuplicateOperationId actual ]
                                else
                                    continuityIssues

                            validateIds (offset + 1) (seenIds @ [ actual ]) duplicateIssues rest

                    validateIds 0 [] [] opIds

                let inputIssues =
                    operationInputs operation
                    |> List.collect (fun (consumer, input) ->
                        validateExpression allOutputs availableOutputs consumer input)

                let availableOutputs = availableOutputs @ operationOutputs operation

                loop
                    (expected + opIds.Length)
                    (seen @ opIds)
                    availableOutputs
                    (issues @ sequenceIssues @ inputIssues)
                    rest

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
        | IrCallActivity(opId, name, input) ->
            match History.completed opId history with
            | Some _ -> None
            | None ->
                match value history input with
                | Some resolvedInput -> Some(Blocked(opId, NeedsActivity { Name = name; Input = resolvedInput }))
                | None -> Some(Blocked(opId, NeedsActivity { Name = name; Input = "" }))
        | IrCallActivities calls ->
            let missing =
                calls
                |> List.choose (fun call ->
                    match History.completed call.CallOpId history with
                    | Some _ -> None
                    | None ->
                        match value history call.CallInput with
                        | Some resolvedInput ->
                            Some(
                                call.CallOpId,
                                { Name = call.CallName
                                  Input = resolvedInput }
                            )
                        | None -> Some(call.CallOpId, { Name = call.CallName; Input = "" }))

            if List.isEmpty missing then
                None
            else
                Some(Blocked(operationId operation, NeedsActivities missing))
        | IrAwaitEvent(opId, key) ->
            match History.resolved opId key history with
            | Some _ -> None
            | None -> Some(Blocked(opId, NeedsEvent key))
        | IrReadCurrentTime opId ->
            match History.currentTime opId history with
            | Some _ -> None
            | None -> Some(Blocked(opId, NeedsCurrentTime))
        | IrWriteLog(opId, message) ->
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

[<RequireQualifiedAccess>]
module DurableIrApp =
    let empty = { Workflows = [] }

    let create workflows = { Workflows = workflows |> List.ofSeq }

    let bindWorkflow workflow app =
        { app with
            Workflows = app.Workflows @ [ workflow ] }

    let private duplicateNameIssues workflows =
        let rec loop seen issues remaining =
            match remaining with
            | [] -> issues
            | workflow :: rest ->
                if List.contains workflow.Name seen then
                    loop seen (issues @ [ DuplicateWorkflowName workflow.Name ]) rest
                else
                    loop (seen @ [ workflow.Name ]) issues rest

        loop [] [] workflows

    let validate app =
        let appIssues =
            if List.isEmpty app.Workflows then
                [ EmptyDurableIrApp ]
            else
                []

        let duplicateIssues = duplicateNameIssues app.Workflows

        let workflowIssues =
            app.Workflows
            |> List.collect (fun workflow ->
                match DurableWorkflow.validate workflow with
                | [] -> []
                | issues -> [ InvalidWorkflowDescriptor(workflow.Name, issues) ])

        appIssues @ duplicateIssues @ workflowIssues

    let tryFindWorkflow name app =
        app.Workflows |> List.tryFind (fun workflow -> workflow.Name = name)

    let replayWorkflow name history app =
        match validate app with
        | [] ->
            match tryFindWorkflow name app with
            | Some workflow -> DurableWorkflow.replay history workflow |> DurableIrWorkflowReplay
            | None -> DurableIrWorkflowNotFound name
        | issues -> InvalidDurableIrApp issues
