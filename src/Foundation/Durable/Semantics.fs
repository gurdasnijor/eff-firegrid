namespace Eff.Foundation.Durable

[<Struct>]
type OpId = OpId of int

type Value = string

type Activity = { Name: string; Input: Value }

type EventKey =
    | Timer of deadline: int64
    | Signal of name: string

type Durable<'a> =
    | Return of 'a
    | Perform of Activity * k: (Value -> Durable<'a>)
    | Await of EventKey * k: (Value -> Durable<'a>)

type Event =
    | ActivityCalled of OpId * Activity
    | ActivityCompleted of OpId * Value
    | TimerCreated of OpId * deadline: int64
    | TimerFired of OpId
    | SignalReceived of OpId * name: string * payload: Value

type History = private History of Event list

type Need =
    | NeedsActivity of Activity
    | NeedsEvent of EventKey

type Outcome<'a> =
    | Done of 'a
    | Blocked of OpId * Need

[<RequireQualifiedAccess>]
module OpId =
    let zero = OpId 0

    let next (OpId value) = OpId(value + 1)

[<RequireQualifiedAccess>]
module History =
    let empty = History []

    let ofList events = History events

    let toList (History events) = events

    let append event (History events) = History(events @ [ event ])

    let completed opId (History events) =
        events
        |> List.tryPick (function
            | ActivityCompleted(id, value) when id = opId -> Some value
            | _ -> None)

    let resolved opId key (History events) =
        match key with
        | Timer _ ->
            events
            |> List.tryPick (function
                | TimerFired id when id = opId -> Some ""
                | _ -> None)
        | Signal name ->
            events
            |> List.tryPick (function
                | SignalReceived(id, signalName, payload) when id = opId && signalName = name -> Some payload
                | _ -> None)

[<RequireQualifiedAccess>]
module Durable =
    let result value = Return value

    let bind binder program =
        let rec loop =
            function
            | Return value -> binder value
            | Perform(activity, k) -> Perform(activity, k >> loop)
            | Await(key, k) -> Await(key, k >> loop)

        loop program

    let map mapper program = bind (mapper >> result) program

    let perform activity = Perform(activity, Return)

    let await key = Await(key, Return)

    let replay history program =
        let rec loop opId current =
            match current with
            | Return value -> Done value
            | Perform(activity, k) ->
                match History.completed opId history with
                | Some value -> loop (OpId.next opId) (k value)
                | None -> Blocked(opId, NeedsActivity activity)
            | Await(key, k) ->
                match History.resolved opId key history with
                | Some value -> loop (OpId.next opId) (k value)
                | None -> Blocked(opId, NeedsEvent key)

        loop OpId.zero program
