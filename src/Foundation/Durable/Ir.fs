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

[<RequireQualifiedAccess>]
module ValueExpr =
    let literal value = Literal value

    let activityResult opId = ActivityResult opId

    let eventResult opId = EventResult opId

    let currentTimeResult opId = CurrentTimeResult opId

[<RequireQualifiedAccess>]
module DurableIr =
    let create operations returnValue =
        { Operations = operations
          Return = returnValue }

    let callActivity opId name input = CallActivity(opId, name, input)

    let awaitEvent opId key = AwaitEvent(opId, key)

    let readCurrentTime opId = ReadCurrentTime opId

    let writeLog opId message = WriteLog(opId, message)

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
