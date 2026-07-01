namespace Eff.Foundation.Durable

type ValueExpr =
    | Literal of Value
    | WorkflowInput
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

type DurableIrStep = { StepName: string }

type DurableIrApp =
    { Steps: DurableIrStep list
      Workflows: DurableWorkflow list }

type DurableIrIssue =
    | EmptyWorkflowName
    | DuplicateOperationId of OpId
    | NonContiguousOperationId of expected: OpId * actual: OpId
    | MissingValueSource of OpId
    | FutureValueSource of source: OpId * consumer: OpId

type DurableIrAppIssue =
    | EmptyDurableIrApp
    | DuplicateStepName of string
    | DuplicateWorkflowName of string
    | MissingStepBinding of workflowName: string * stepName: string
    | InvalidWorkflowDescriptor of workflowName: string * issues: DurableIrIssue list

type DurableWorkflowReplay =
    | InvalidWorkflow of DurableIrIssue list
    | ValidWorkflow of Outcome<Value>

type DurableIrAppReplay =
    | InvalidDurableIrApp of DurableIrAppIssue list
    | DurableIrWorkflowNotFound of string
    | DurableIrWorkflowReplay of DurableWorkflowReplay

type DurableIrCommand =
    | DurableIrCommandCallActivity of OpId * Activity
    | DurableIrCommandScheduleTimer of OpId * deadline: int64
    | DurableIrCommandCancelTimer of OpId
    | DurableIrCommandWriteLog of OpId * message: string

type DurableIrCommitRecord =
    | DurableIrCommitHistory of Event
    | DurableIrCommitCommand of DurableIrCommand

type DurableIrPlan =
    | DurableIrPlanComplete of Value
    | DurableIrPlanCommit of DurableIrCommitRecord list
    | DurableIrPlanWaiting of OpId * Need

type DurableIrAppPlan =
    | InvalidDurableIrAppPlan of DurableIrAppIssue list
    | DurableIrPlanWorkflowNotFound of string
    | DurableIrPlanReady of DurableIrPlan

[<RequireQualifiedAccess>]
module ValueExpr =
    let literal value = Literal value

    let workflowInput = WorkflowInput

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

    let private operationActivityNames operation =
        match operation with
        | IrCallActivity(_, name, _) -> [ name ]
        | IrCallActivities calls -> calls |> List.map (fun call -> call.CallName)
        | IrAwaitEvent _
        | IrReadCurrentTime _
        | IrWriteLog _ -> []

    let private expressionOpId expression =
        match expression with
        | Literal _
        | WorkflowInput -> None
        | ActivityResult opId
        | EventResult opId
        | CurrentTimeResult opId -> Some opId

    let private validateExpression allOutputs availableOutputs consumer expression =
        match expression, expressionOpId expression with
        | Literal _, _ -> []
        | WorkflowInput, _ -> []
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

    let activityNames (program: DurableIr) =
        program.Operations |> List.collect operationActivityNames

    let private value input history expr =
        match expr with
        | Literal value -> Some value
        | WorkflowInput -> Some input
        | ActivityResult opId -> History.completed opId history
        | EventResult opId ->
            History.toList history
            |> List.tryPick (function
                | TimerFired id when id = opId -> Some ""
                | SignalReceived(id, _, payload) when id = opId -> Some payload
                | _ -> None)
        | CurrentTimeResult opId -> History.currentTime opId history |> Option.map string

    let private replayOperation workflowInput history operation =
        match operation with
        | IrCallActivity(opId, name, input) ->
            match History.completed opId history with
            | Some _ -> None
            | None ->
                match value workflowInput history input with
                | Some resolvedInput -> Some(Blocked(opId, NeedsActivity { Name = name; Input = resolvedInput }))
                | None -> Some(Blocked(opId, NeedsActivity { Name = name; Input = "" }))
        | IrCallActivities calls ->
            let missing =
                calls
                |> List.choose (fun call ->
                    match History.completed call.CallOpId history with
                    | Some _ -> None
                    | None ->
                        match value workflowInput history call.CallInput with
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

    let replay workflowInput history (program: DurableIr) =
        let rec loop operations =
            match operations with
            | [] ->
                match value workflowInput history program.Return with
                | Some output -> Done output
                | None -> Done ""
            | operation :: rest ->
                match replayOperation workflowInput history operation with
                | Some blocked -> blocked
                | None -> loop rest

        loop program.Operations

    let private events history = History.toList history

    let private hasActivityCall opId activity history =
        events history
        |> List.exists (function
            | ActivityCalled(id, called) when id = opId && called = activity -> true
            | _ -> false)

    let private hasTimerCreated opId deadline history =
        events history
        |> List.exists (function
            | TimerCreated(id, created) when id = opId && created = deadline -> true
            | _ -> false)

    let private hasCurrentTime opId history =
        History.currentTime opId history |> Option.isSome

    let private hasLog opId message history = History.logEmitted opId message history

    let private hasTimerCanceled opId history = History.timerCanceled opId history

    let private history event = DurableIrCommitHistory event

    let private command command = DurableIrCommitCommand command

    let private activityRecords opId activity =
        [ history (ActivityCalled(opId, activity))
          command (DurableIrCommandCallActivity(opId, activity)) ]

    let private timerRecords opId deadline =
        [ history (TimerCreated(opId, deadline))
          command (DurableIrCommandScheduleTimer(opId, deadline)) ]

    let private cancelTimerRecords opId =
        [ history (TimerCanceled opId); command (DurableIrCommandCancelTimer opId) ]

    let private currentTimeRecords opId timestamp =
        [ history (CurrentTimeRecorded(opId, timestamp)) ]

    let private logRecords opId message =
        [ history (LogEmitted(opId, message))
          command (DurableIrCommandWriteLog(opId, message)) ]

    let private missingForTask history (opId, task) =
        match task with
        | RaceActivity activity ->
            if hasActivityCall opId activity history then
                []
            else
                activityRecords opId activity
        | RaceEvent(Timer deadline) ->
            if hasTimerCreated opId deadline history then
                []
            else
                timerRecords opId deadline
        | RaceEvent(Signal _) -> []

    let private missingForNeed timestamp history opId need =
        match need with
        | NeedsActivity requested ->
            if hasActivityCall opId requested history then
                []
            else
                activityRecords opId requested
        | NeedsActivities pending ->
            pending
            |> List.collect (fun (id, requested) ->
                if hasActivityCall id requested history then
                    []
                else
                    activityRecords id requested)
        | NeedsEvent(Timer deadline) ->
            if hasTimerCreated opId deadline history then
                []
            else
                timerRecords opId deadline
        | NeedsEvent(Signal _) -> []
        | NeedsRace pending -> pending |> List.collect (missingForTask history)
        | NeedsTimerCancellation timers ->
            timers
            |> List.collect (fun timerId ->
                if hasTimerCanceled timerId history then
                    []
                else
                    cancelTimerRecords timerId)
        | NeedsCurrentTime ->
            if hasCurrentTime opId history then
                []
            else
                currentTimeRecords opId timestamp
        | NeedsLog message ->
            if hasLog opId message history then
                []
            else
                logRecords opId message

    let plan timestamp workflowInput history program =
        match replay workflowInput history program with
        | Done value -> DurableIrPlanComplete value
        | Blocked(opId, need) ->
            match missingForNeed timestamp history opId need with
            | [] -> DurableIrPlanWaiting(opId, need)
            | records -> DurableIrPlanCommit records

[<RequireQualifiedAccess>]
module DurableWorkflow =
    let create name (program: DurableIr) : DurableWorkflow = { Name = name; Program = program }

    let validate (workflow: DurableWorkflow) =
        let nameIssues = if workflow.Name = "" then [ EmptyWorkflowName ] else []

        nameIssues @ DurableIr.validate workflow.Program

    let replay workflowInput history workflow =
        match validate workflow with
        | [] -> DurableIr.replay workflowInput history workflow.Program |> ValidWorkflow
        | issues -> InvalidWorkflow issues

[<RequireQualifiedAccess>]
module DurableIrStep =
    let create name = { StepName = name }

[<RequireQualifiedAccess>]
module DurableIrApp =
    let empty = { Steps = []; Workflows = [] }

    let create steps workflows =
        { Steps = steps |> List.ofSeq
          Workflows = workflows |> List.ofSeq }

    let bindStep step app =
        { app with
            Steps = app.Steps @ [ step ] }

    let bindWorkflow workflow app =
        { app with
            Workflows = app.Workflows @ [ workflow ] }

    let private duplicateStepIssues steps =
        let rec loop seen issues remaining =
            match remaining with
            | [] -> issues
            | step :: rest ->
                if List.contains step.StepName seen then
                    loop seen (issues @ [ DuplicateStepName step.StepName ]) rest
                else
                    loop (seen @ [ step.StepName ]) issues rest

        loop [] [] steps

    let private duplicateWorkflowIssues workflows =
        let rec loop seen issues remaining =
            match remaining with
            | [] -> issues
            | workflow :: rest ->
                if List.contains workflow.Name seen then
                    loop seen (issues @ [ DuplicateWorkflowName workflow.Name ]) rest
                else
                    loop (seen @ [ workflow.Name ]) issues rest

        loop [] [] workflows

    let private missingStepIssues app =
        let registered = app.Steps |> List.map (fun step -> step.StepName)

        app.Workflows
        |> List.collect (fun workflow ->
            workflow.Program
            |> DurableIr.activityNames
            |> List.choose (fun stepName ->
                if List.contains stepName registered then
                    None
                else
                    Some(MissingStepBinding(workflow.Name, stepName))))

    let validate app =
        let appIssues =
            if List.isEmpty app.Workflows then
                [ EmptyDurableIrApp ]
            else
                []

        let duplicateIssues =
            duplicateStepIssues app.Steps @ duplicateWorkflowIssues app.Workflows

        let missingSteps = missingStepIssues app

        let workflowIssues =
            app.Workflows
            |> List.collect (fun workflow ->
                match DurableWorkflow.validate workflow with
                | [] -> []
                | issues -> [ InvalidWorkflowDescriptor(workflow.Name, issues) ])

        appIssues @ duplicateIssues @ missingSteps @ workflowIssues

    let tryFindWorkflow name app =
        app.Workflows |> List.tryFind (fun workflow -> workflow.Name = name)

    let replayWorkflow name workflowInput history app =
        match validate app with
        | [] ->
            match tryFindWorkflow name app with
            | Some workflow -> DurableWorkflow.replay workflowInput history workflow |> DurableIrWorkflowReplay
            | None -> DurableIrWorkflowNotFound name
        | issues -> InvalidDurableIrApp issues

    let planWorkflow timestamp name workflowInput history app =
        match validate app with
        | [] ->
            match tryFindWorkflow name app with
            | Some workflow ->
                DurableIr.plan timestamp workflowInput history workflow.Program
                |> DurableIrPlanReady
            | None -> DurableIrPlanWorkflowNotFound name
        | issues -> InvalidDurableIrAppPlan issues

[<RequireQualifiedAccess>]
module DurableIrCommitCodec =
    let private opText (OpId value) = string value

    let private field (value: string) = string value.Length + ":" + value

    let private fields values =
        values |> List.map field |> String.concat ""

    let private prefixed prefix values = prefix + "|" + fields values

    let private eventRecord event =
        match event with
        | ActivityCalled(opId, activity) ->
            prefixed "event.activity-called" [ opText opId; activity.Name; activity.Input ]
        | ActivityCompleted(opId, value) -> prefixed "event.activity-completed" [ opText opId; value ]
        | CurrentTimeRecorded(opId, timestamp) -> prefixed "event.current-time" [ opText opId; string timestamp ]
        | LogEmitted(opId, message) -> prefixed "event.log" [ opText opId; message ]
        | TimerCreated(opId, deadline) -> prefixed "event.timer-created" [ opText opId; string deadline ]
        | TimerFired opId -> prefixed "event.timer-fired" [ opText opId ]
        | TimerCanceled opId -> prefixed "event.timer-canceled" [ opText opId ]
        | SignalReceived(opId, name, payload) -> prefixed "event.signal" [ opText opId; name; payload ]

    let private commandRecord command =
        match command with
        | DurableIrCommandCallActivity(opId, activity) ->
            prefixed "command.activity" [ opText opId; activity.Name; activity.Input ]
        | DurableIrCommandScheduleTimer(opId, deadline) -> prefixed "command.timer" [ opText opId; string deadline ]
        | DurableIrCommandCancelTimer opId -> prefixed "command.cancel-timer" [ opText opId ]
        | DurableIrCommandWriteLog(opId, message) -> prefixed "command.log" [ opText opId; message ]

    let encode record =
        match record with
        | DurableIrCommitHistory event -> "in|" + eventRecord event
        | DurableIrCommitCommand command -> "out|" + commandRecord command
