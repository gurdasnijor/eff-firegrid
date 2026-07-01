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

    let replay history program =
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
