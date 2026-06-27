namespace Eff.Foundation

open Fable.Core
open Eff

/// The ergonomic durable-actor surface. Two authoring modes share one substrate
/// (`SubjectHistory`: one S2 stream = one actor's authoritative log):
///
///   Durable.actor    — a STATEFUL ACTOR: persisted state rebuilt by folding a
///                      command log through a pure reducer. You serialize commands,
///                      never state.
///   Durable.workflow — a DURABLE WORKFLOW: an imperative multi-step body that
///                      survives crashes and durable sleeps via deterministic replay.
///
/// Everything below the public functions — journals, codecs, folds, the replay
/// engine, addressing — is private. Application code never sees it.
module Durable =

    [<Emit("Date.now()")>]
    let private nowMs () : float = jsNative

    // ----- addressing: actor kind + instance id  ->  one S2 subject stream -----
    type Id = Id of string

    let private subjectOf (kind: string) (Id instance) =
        SubjectHistory.SubjectId(kind + ":" + instance)

    /// Create the backing stream if it does not exist yet (best-effort).
    let private ensure (basin: S2.Basin) (SubjectHistory.SubjectId name) =
        async {
            try
                do! basin |> S2.createStream name
            with _ ->
                () // already exists, or a racing creator made it — fine
        }

    /// Build a command codec from a total encode/decode pair.
    let codec (encode: 'cmd -> string) (decode: string -> 'cmd) : SubjectHistory.Codec<'cmd> =
        { Encode = encode
          Decode = fun s -> Ok(decode s) }

    // =====================================================================
    //  Stateful actor
    //  State is a fold of the command log; you write a reducer, send commands,
    //  read state. State is never serialized — only commands are.
    // =====================================================================
    type Actor<'cmd, 'state> =
        internal
            { Kind: string
              Initial: 'state
              Reduce: 'state -> 'cmd -> 'state
              Codec: SubjectHistory.Codec<'cmd> }

    /// Define an actor kind: its initial state, its reducer, and a command codec.
    let actor (kind: string) (initial: 'state) (reduce: 'state -> 'cmd -> 'state) codec : Actor<'cmd, 'state> =
        { Kind = kind
          Initial = initial
          Reduce = reduce
          Codec = codec }

    /// Send a command to an actor instance (durably appended to its log).
    let send (basin: S2.Basin) (a: Actor<'cmd, 'state>) (instance: Id) (cmd: 'cmd) =
        async {
            let subject = subjectOf a.Kind instance
            do! ensure basin subject
            let! _ = SubjectHistory.append basin a.Codec subject [ cmd ]
            return ()
        }

    /// Read an actor instance's current state, rebuilt by folding its log.
    let state (basin: S2.Basin) (a: Actor<'cmd, 'state>) (instance: Id) =
        async {
            let subject = subjectOf a.Kind instance
            do! ensure basin subject
            let! tail = SubjectHistory.tail basin subject

            let! st, _ =
                SubjectHistory.foldTo
                    basin
                    a.Codec
                    subject
                    (SubjectHistory.Seq 0L)
                    tail
                    a.Initial
                    (fun s record -> a.Reduce s record.Body)

            return st
        }

    /// Decommission an actor instance (delete its stream).
    let remove (basin: S2.Basin) (a: Actor<'cmd, 'state>) (instance: Id) =
        let (SubjectHistory.SubjectId name) = subjectOf a.Kind instance
        basin |> S2.deleteStream name

    // =====================================================================
    //  Durable workflow
    //  An imperative body driven by deterministic replay: completed steps are
    //  memoized in the log, durable sleeps suspend and resume on a deadline.
    // =====================================================================

    /// The context handed to a workflow body. `Step` memoizes a side effect;
    /// `Sleep` is a durable wait that survives a crash.
    type Ctx =
        { Step: string * (unit -> Async<string>) -> Async<string>
          Sleep: float -> Async<unit> }

    type Workflow =
        internal
            { Kind: string
              Body: string -> Ctx -> Async<string> }

    /// Define a workflow kind from an imperative body `input -> ctx -> result`.
    let workflow (kind: string) (body: string -> Ctx -> Async<string>) : Workflow = { Kind = kind; Body = body }

    // ---- private replay engine (SuspendTurn ends a turn at a not-yet-due Sleep) ----
    exception SuspendTurn

    type private J =
        | Started
        | StepDone of ord: int * result: string
        | TimerSet of ord: int * deadlineMs: float
        | TimerFired of ord: int
        | Finished of result: string

    let private jencode j =
        match j with
        | Started -> "started"
        | StepDone(ord, result) -> sprintf "step|%d|%s" ord result
        | TimerSet(ord, deadlineMs) -> sprintf "timer|%d|%d" ord (int64 deadlineMs)
        | TimerFired ord -> sprintf "fired|%d" ord
        | Finished result -> "done|" + result

    let private jdecode (s: string) =
        match s.Split('|') |> Array.toList with
        | [ "started" ] -> Ok Started
        | [ "step"; ord; result ] -> Ok(StepDone(int ord, result))
        | [ "timer"; ord; deadline ] -> Ok(TimerSet(int ord, float deadline))
        | [ "fired"; ord ] -> Ok(TimerFired(int ord))
        | [ "done"; result ] -> Ok(Finished result)
        | _ -> Error("bad journal record: " + s)

    let private jcodec: SubjectHistory.Codec<J> = { Encode = jencode; Decode = jdecode }

    type private WfView =
        { Started: bool
          Steps: Map<int, string>
          Timers: Map<int, float>
          Fired: Set<int>
          Done: string option }

    let private emptyView =
        { Started = false
          Steps = Map.empty
          Timers = Map.empty
          Fired = Set.empty
          Done = None }

    let private applyView v (record: SubjectHistory.StoredRecord<J>) =
        match record.Body with
        | Started -> { v with Started = true }
        | StepDone(ord, result) -> { v with Steps = Map.add ord result v.Steps }
        | TimerSet(ord, deadline) ->
            { v with
                Timers = Map.add ord deadline v.Timers }
        | TimerFired ord -> { v with Fired = Set.add ord v.Fired }
        | Finished result -> { v with Done = Some result }

    let private loadView basin subject =
        async {
            let! tail = SubjectHistory.tail basin subject
            let! v, _ = SubjectHistory.foldTo basin jcodec subject (SubjectHistory.Seq 0L) tail emptyView applyView
            return v
        }

    let private emit basin subject (records: J list) =
        async {
            let! _ = SubjectHistory.append basin jcodec subject records
            return ()
        }

    /// Run a workflow instance to completion, durably. If the host died mid-run,
    /// calling `run` again resumes from the log without re-executing finished steps.
    let run (basin: S2.Basin) (wf: Workflow) (instance: Id) (input: string) : Async<string> =
        async {
            let subject = subjectOf wf.Kind instance
            do! ensure basin subject

            // one replay turn: rebuild the view, run the body top-to-bottom, journal
            // each fresh step, and suspend (raise) on a sleep that is not yet due.
            let runTurn () =
                async {
                    let! view = loadView basin subject

                    if view.Done.IsSome then
                        return Choice1Of2 view.Done.Value
                    else
                        do! (if not view.Started then emit basin subject [ Started ] else async { return () })

                        let stepOrd = ref 0
                        let timerOrd = ref 0

                        let ctx =
                            { Step =
                                fun (_label, thunk) ->
                                    async {
                                        let ord = stepOrd.Value
                                        stepOrd.Value <- ord + 1

                                        match Map.tryFind ord view.Steps with
                                        | Some recorded -> return recorded // memoized: thunk NOT re-run
                                        | None ->
                                            let! result = thunk ()
                                            do! emit basin subject [ StepDone(ord, result) ]
                                            return result
                                    }
                              Sleep =
                                fun ms ->
                                    async {
                                        let ord = timerOrd.Value
                                        timerOrd.Value <- ord + 1

                                        if Set.contains ord view.Fired then
                                            return ()
                                        else
                                            match Map.tryFind ord view.Timers with
                                            | Some _ -> return raise SuspendTurn // scheduled; the run loop waits + fires
                                            | None ->
                                                do! emit basin subject [ TimerSet(ord, nowMs () + ms) ]
                                                return raise SuspendTurn
                                    } }

                        try
                            let! result = wf.Body input ctx
                            do! emit basin subject [ Finished result ]
                            return Choice1Of2 result
                        with
                        | SuspendTurn -> return Choice2Of2()
                }

            let mutable result = None
            let mutable go = true

            while go do
                let! outcome = runTurn ()

                match outcome with
                | Choice1Of2 r ->
                    result <- Some r
                    go <- false
                | Choice2Of2() ->
                    // scheduler: wait until each pending timer's deadline, then fire it
                    let! v = loadView basin subject

                    for timer in v.Timers do
                        if not (Set.contains timer.Key v.Fired) then
                            let waitMs = timer.Value - nowMs ()

                            if waitMs > 0.0 then
                                do! Async.Sleep(int waitMs)

                            do! emit basin subject [ TimerFired timer.Key ]

            return result |> Option.defaultValue ""
        }
