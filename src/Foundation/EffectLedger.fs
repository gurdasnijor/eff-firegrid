namespace Eff.Foundation

open Eff

/// The minimal durable-execution kernel: journaled steps over one
/// `SubjectHistory`. This is the property that turns an append-only log into
/// durable execution — Restate's "every system is a log": on a retry, check the
/// journal for the step's result and restore it instead of re-running.
///
/// A step writes its INTENT (`EffectRequested`) before running its effect and
/// its RESULT (`EffectCompleted`) after, then is skipped on replay. Recovery is
/// just a re-fold of the log:
///   - a COMPLETED step is never re-run; its recorded result is returned;
///   - an INTERRUPTED step (intent journaled, result not) re-runs on recovery,
///     so effects must be idempotent (at-least-once on that crash boundary).
///
/// The step id is positional — the count of intents folded so far — so a
/// recovering host regenerates the identical id sequence and asks the same
/// "have I already done step N?" question. No ownership/fencing here: single
/// writership is a separate concern (see `FencingProof`); this kernel rides on
/// `SubjectHistory`'s position-CAS for admission.
module EffectLedger =

    type Entry =
        | EffectRequested of id: int * label: string
        | EffectCompleted of id: int * result: string

    module Entry =
        let encode entry =
            match entry with
            | EffectRequested(id, label) -> sprintf "req|%d|%s" id label
            | EffectCompleted(id, result) -> sprintf "done|%d|%s" id result

        // Cut off the leading "tag|" once; the remainder is verbatim so a label
        // or result may itself contain '|' without corrupting the record.
        let private cut (s: string) =
            match s.IndexOf('|') with
            | -1 -> s, ""
            | i -> s.Substring(0, i), s.Substring(i + 1)

        let decode (body: string) =
            let tag, rest = cut body

            match tag with
            | "req" ->
                let id, label = cut rest
                Ok(EffectRequested(int id, label))
            | "done" ->
                let id, result = cut rest
                Ok(EffectCompleted(int id, result))
            | _ -> Error("bad effect-ledger entry: " + body)

        let codec: SubjectHistory.Codec<Entry> = { Encode = encode; Decode = decode }

    /// The deterministic fold of the journal: which step ids were requested, and
    /// the recorded result of each completed step.
    type Ledger =
        { Requested: Set<int>
          Completed: Map<int, string> }

    module Ledger =
        let empty =
            { Requested = Set.empty
              Completed = Map.empty }

        let apply ledger (record: SubjectHistory.StoredRecord<Entry>) =
            match record.Body with
            | EffectRequested(id, _) ->
                { ledger with
                    Requested = Set.add id ledger.Requested }
            | EffectCompleted(id, result) ->
                { ledger with
                    Completed = Map.add id result ledger.Completed }

        /// The next deterministic step id = the number of intents folded so far.
        let nextId ledger = Set.count ledger.Requested

    /// Recover the ledger by folding the whole subject log to its tail.
    let load (basin: S2.Basin) subject =
        async {
            let! tail = SubjectHistory.tail basin subject

            let! ledger, _ =
                SubjectHistory.foldTo basin Entry.codec subject (SubjectHistory.Seq 0L) tail Ledger.empty Ledger.apply

            return ledger
        }

    /// Run one durable step at `id`. If already completed, return the logged
    /// result without re-running the effect. Otherwise journal the intent (once,
    /// idempotent when resuming a pending request), run the effect, journal the
    /// result, and return it with the advanced ledger.
    let step (basin: S2.Basin) subject (ledger: Ledger) (id: int) (label: string) (effect: unit -> Async<string>) =
        async {
            match Map.tryFind id ledger.Completed with
            | Some recorded -> return recorded, ledger
            | None ->
                let! ledger1 =
                    if Set.contains id ledger.Requested then
                        async { return ledger }
                    else
                        async {
                            let! _ = SubjectHistory.append basin Entry.codec subject [ EffectRequested(id, label) ]

                            return
                                { ledger with
                                    Requested = Set.add id ledger.Requested }
                        }

                let! result = effect ()
                let! _ = SubjectHistory.append basin Entry.codec subject [ EffectCompleted(id, result) ]

                return
                    result,
                    { ledger1 with
                        Completed = Map.add id result ledger1.Completed }
        }
