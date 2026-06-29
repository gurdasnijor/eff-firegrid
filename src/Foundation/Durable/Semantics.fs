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
    | PerformAll of Activity list * k: (Value list -> Durable<'a>)
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
    | NeedsActivities of (OpId * Activity) list
    | NeedsEvent of EventKey

type Outcome<'a> =
    | Done of 'a
    | Blocked of OpId * Need

[<RequireQualifiedAccess>]
module OpId =
    let zero = OpId 0

    let add offset (OpId value) = OpId(value + offset)

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
            | PerformAll(activities, k) -> PerformAll(activities, k >> loop)
            | Await(key, k) -> Await(key, k >> loop)

        loop program

    let map mapper program = bind (mapper >> result) program

    let perform activity = Perform(activity, Return)

    let performAll activities = PerformAll(activities, Return)

    let await key = Await(key, Return)

    let replay history program =
        let rec loop opId current =
            match current with
            | Return value -> Done value
            | Perform(activity, k) ->
                match History.completed opId history with
                | Some value -> loop (OpId.next opId) (k value)
                | None -> Blocked(opId, NeedsActivity activity)
            | PerformAll(activities, k) ->
                let pending =
                    activities |> List.mapi (fun index activity -> OpId.add index opId, activity)

                let completed =
                    pending
                    |> List.map (fun (id, activity) -> id, activity, History.completed id history)

                let missing =
                    completed
                    |> List.choose (fun (id, activity, value) ->
                        match value with
                        | Some _ -> None
                        | None -> Some(id, activity))

                if List.isEmpty missing then
                    let values = completed |> List.choose (fun (_, _, value) -> value)
                    loop (OpId.add activities.Length opId) (k values)
                else
                    Blocked(opId, NeedsActivities missing)
            | Await(key, k) ->
                match History.resolved opId key history with
                | Some value -> loop (OpId.next opId) (k value)
                | None -> Blocked(opId, NeedsEvent key)

        loop OpId.zero program
