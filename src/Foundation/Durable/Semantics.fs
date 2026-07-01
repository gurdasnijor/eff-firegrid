namespace Eff.Foundation.Durable

[<Struct>]
type OpId = OpId of int

type Value = string

type Activity = { Name: string; Input: Value }

type EventKey =
    | Timer of int64
    | Signal of string

type RaceTask =
    | RaceActivity of Activity
    | RaceEvent of EventKey

type RaceResult =
    | ActivityWon of index: int * value: Value
    | EventWon of index: int * key: EventKey * value: Value

type Durable<'a> =
    | Return of 'a
    | Perform of Activity * k: (Value -> Durable<'a>)
    | PerformAll of Activity list * k: (Value list -> Durable<'a>)
    | Await of EventKey * k: (Value -> Durable<'a>)
    | WhenAny of RaceTask list * k: (RaceResult -> Durable<'a>)
    | CurrentTime of k: (int64 -> Durable<'a>)
    | Log of message: string * k: (unit -> Durable<'a>)

type Event =
    | ActivityCalled of OpId * Activity
    | ActivityCompleted of OpId * Value
    | CurrentTimeRecorded of OpId * timestamp: int64
    | LogEmitted of OpId * message: string
    | TimerCreated of OpId * deadline: int64
    | TimerFired of OpId
    | TimerCanceled of OpId
    | SignalReceived of OpId * name: string * payload: Value

type History = private History of Event list

type Need =
    | NeedsActivity of Activity
    | NeedsActivities of (OpId * Activity) list
    | NeedsEvent of EventKey
    | NeedsRace of (OpId * RaceTask) list
    | NeedsTimerCancellation of OpId list
    | NeedsCurrentTime
    | NeedsLog of message: string

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

    let currentTime opId (History events) =
        events
        |> List.tryPick (function
            | CurrentTimeRecorded(id, timestamp) when id = opId -> Some timestamp
            | _ -> None)

    let logEmitted opId message (History events) =
        events
        |> List.exists (function
            | LogEmitted(id, logged) when id = opId && logged = message -> true
            | _ -> false)

    let resolved opId key (History events) =
#if FABLE_RUST
        match key with
        | EventKey.Timer _ ->
            events
            |> List.tryPick (function
                | TimerFired id when id = opId -> Some ""
                | _ -> None)
        | _ -> None
#else
        match key with
        | EventKey.Timer _ ->
            events
            |> List.tryPick (function
                | TimerFired id when id = opId -> Some ""
                | _ -> None)
        | EventKey.Signal name ->
            events
            |> List.tryPick (function
                | SignalReceived(id, signalName, payload) when id = opId && signalName = name -> Some payload
                | _ -> None)
#endif

    let timerCanceled opId (History events) =
        events
        |> List.exists (function
            | TimerCanceled id when id = opId -> true
            | _ -> false)

    let raceWinner (tasks: (int * OpId * RaceTask) list) (History events) =
#if FABLE_RUST
        let tryMatch event =
            tasks
            |> List.tryPick (fun (index, opId, task) ->
                match task, event with
                | RaceActivity _, ActivityCompleted(id, value) when id = opId -> Some(ActivityWon(index, value))
                | RaceEvent(EventKey.Timer deadline), TimerFired id when id = opId ->
                    Some(EventWon(index, EventKey.Timer deadline, ""))
                | _ -> None)

        events |> List.tryPick tryMatch
#else
        let tryMatch event =
            tasks
            |> List.tryPick (fun (index, opId, task) ->
                match task, event with
                | RaceActivity _, ActivityCompleted(id, value) when id = opId -> Some(ActivityWon(index, value))
                | RaceEvent(EventKey.Timer deadline), TimerFired id when id = opId ->
                    Some(EventWon(index, EventKey.Timer deadline, ""))
                | RaceEvent(EventKey.Signal name), SignalReceived(id, signalName, payload) when
                    id = opId && signalName = name
                    ->
                    Some(EventWon(index, EventKey.Signal name, payload))
                | _ -> None)

        events |> List.tryPick tryMatch
#endif

[<RequireQualifiedAccess>]
module Durable =
    let result value = Return value

#if FABLE_RUST
    let bind _binder _program =
        failwith "Durable.bind is not yet supported by the Fable Rust target"
#else
    let bind binder program =
        let rec loop =
            function
            | Return value -> binder value
            | Perform(activity, k) -> Perform(activity, k >> loop)
            | PerformAll(activities, k) -> PerformAll(activities, k >> loop)
            | Await(key, k) -> Await(key, k >> loop)
            | WhenAny(tasks, k) -> WhenAny(tasks, k >> loop)
            | CurrentTime k -> CurrentTime(k >> loop)
            | Log(message, k) -> Log(message, fun () -> loop (k ()))

        loop program
#endif

    let map mapper program = bind (mapper >> result) program

    let perform activity = Perform(activity, Return)

    let performAll activities = PerformAll(activities, Return)

    let await key = Await(key, Return)

    let whenAny tasks = WhenAny(tasks, Return)

    let currentTime = CurrentTime Return

    let log message = Log(message, fun () -> Return())

#if FABLE_RUST
    let replay history program =
        match program with
        | Return value -> Done value
        | Perform(activity, _) ->
            match History.completed OpId.zero history with
            | Some value -> Done value
            | None -> Blocked(OpId.zero, NeedsActivity activity)
        | PerformAll(activities, _) ->
            let pending =
                activities
                |> List.mapi (fun index activity -> OpId.add index OpId.zero, activity)

            Blocked(OpId.zero, NeedsActivities pending)
        | Await(key, _) -> Blocked(OpId.zero, NeedsEvent key)
        | WhenAny(tasks, _) ->
            let pending = tasks |> List.mapi (fun index task -> OpId.add index OpId.zero, task)
            Blocked(OpId.zero, NeedsRace pending)
        | CurrentTime _ -> Blocked(OpId.zero, NeedsCurrentTime)
        | Log(message, _) -> Blocked(OpId.zero, NeedsLog message)
#else
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
            | WhenAny(tasks, k) ->
                let pending =
                    tasks |> List.mapi (fun index task -> index, OpId.add index opId, task)

                match History.raceWinner pending history with
                | None ->
                    pending
                    |> List.map (fun (_, id, task) -> id, task)
                    |> NeedsRace
                    |> fun need -> Blocked(opId, need)
                | Some winner ->
                    let winnerIndex =
                        match winner with
                        | ActivityWon(index, _) -> index
                        | EventWon(index, _, _) -> index

                    let uncanceledTimers =
                        pending
                        |> List.choose (fun (index, id, task) ->
                            match task with
                            | RaceEvent(EventKey.Timer _) when
                                index <> winnerIndex && not (History.timerCanceled id history)
                                ->
                                Some id
                            | _ -> None)

                    if List.isEmpty uncanceledTimers then
                        loop (OpId.add tasks.Length opId) (k winner)
                    else
                        Blocked(opId, NeedsTimerCancellation uncanceledTimers)
            | CurrentTime k ->
                match History.currentTime opId history with
                | Some timestamp -> loop (OpId.next opId) (k timestamp)
                | None -> Blocked(opId, NeedsCurrentTime)
            | Log(message, k) ->
                if History.logEmitted opId message history then
                    loop (OpId.next opId) (k ())
                else
                    Blocked(opId, NeedsLog message)

        loop OpId.zero program
#endif
