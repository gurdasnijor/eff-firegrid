namespace Eff.Foundation

open Eff

type ReadConsistency =
    | Eventual
    | Strong

type ViewState<'state> =
    { State: 'state
      AppliedTail: SubjectHistory.Version }

type private PendingRead<'state> =
    { Required: SubjectHistory.Version
      Reply: ViewState<'state> -> unit
      Error: exn -> unit }

type StateView<'record, 'state> =
    private
        { Basin: S2.Basin
          Subject: SubjectHistory.SubjectId
          Codec: SubjectHistory.Codec<'record>
          mutable Cursor: SubjectHistory.Cursor<'record>
          Apply: 'state -> SubjectHistory.StoredRecord<'record> -> 'state
          mutable State: 'state
          mutable AppliedTail: SubjectHistory.Version
          mutable PendingReads: PendingRead<'state> list
          mutable StopRequested: bool
          mutable Stopped: bool
          mutable StopWaiters: (unit -> unit) list
          mutable PumpError: exn option }

module StateView =
    let private versionNumber = SubjectHistory.versionNumber

    let private snapshot view =
        { State = view.State
          AppliedTail = view.AppliedTail }

    let private failPending error view =
        let pending = view.PendingReads
        view.PendingReads <- []
        pending |> List.iter (fun read -> read.Error error)

    let private drainPending view =
        let applied = versionNumber view.AppliedTail

        let ready, waiting =
            view.PendingReads
            |> List.partition (fun read -> versionNumber read.Required <= applied)

        view.PendingReads <- waiting

        if not (List.isEmpty ready) then
            let current = snapshot view
            ready |> List.iter (fun read -> read.Reply current)

    let private completeStopped error view =
        async {
            if not view.Stopped then
                match error with
                | Some pumpError -> view.PumpError <- Some pumpError
                | None -> ()

                view.Stopped <- true

                match error with
                | Some pumpError -> failPending pumpError view
                | None -> failPending (exn "StateView stopped") view

                try
                    do! SubjectHistory.closeCursor view.Cursor
                with _ ->
                    ()

                let waiters = view.StopWaiters
                view.StopWaiters <- []
                waiters |> List.iter (fun reply -> reply ())
        }

    let private reopenCursor view =
        async {
            let! cursor =
                SubjectHistory.openCursorWithWait
                    (Some 1)
                    view.Basin
                    view.Codec
                    view.Subject
                    (SubjectHistory.Seq(versionNumber view.AppliedTail))

            view.Cursor <- cursor
        }

    let private pump view =
        let rec loop () =
            async {
                if view.StopRequested then
                    do! completeStopped None view
                else
                    try
                        let! next = SubjectHistory.tryNext view.Cursor

                        match next with
                        | Error message ->
                            if not view.Stopped then
                                do! completeStopped (Some(exn message)) view
                        | Ok None ->
                            if view.StopRequested then
                                do! completeStopped None view
                            else
                                do! reopenCursor view
                                return! loop ()
                        | Ok(Some record) ->
                            let expected = versionNumber view.AppliedTail
                            let actual = SubjectHistory.seqNumber record.Seq

                            if actual <> expected then
                                let error =
                                    exn (sprintf "StateView expected seq %d but cursor returned %d" expected actual)

                                do! completeStopped (Some error) view
                            else
                                view.State <- view.Apply view.State record
                                view.AppliedTail <- SubjectHistory.Version(actual + 1L)
                                drainPending view
                                return! loop ()
                    with error ->
                        if view.StopRequested then
                            do! completeStopped None view
                        else
                            do! completeStopped (Some error) view
            }

        loop ()

    let start basin codec subject recoverFrom initial apply =
        async {
            let! cursor = SubjectHistory.openCursorWithWait (Some 1) basin codec subject recoverFrom

            let view =
                { Basin = basin
                  Subject = subject
                  Codec = codec
                  Cursor = cursor
                  Apply = apply
                  State = initial
                  AppliedTail = SubjectHistory.Version(SubjectHistory.seqNumber recoverFrom)
                  PendingReads = []
                  StopRequested = false
                  Stopped = false
                  StopWaiters = []
                  PumpError = None }

            pump view |> Async.StartImmediate
            return view
        }

    let private ensureReadable view =
        async {
            match view.PumpError with
            | Some error -> return raise error
            | None ->
                if view.Stopped then
                    return raise (exn "StateView is stopped")
                else
                    return ()
        }

    let private waitUntilApplied required view =
        async {
            do! ensureReadable view

            if versionNumber view.AppliedTail >= versionNumber required then
                return snapshot view
            else
                return!
                    Async.FromContinuations(fun (reply, error, _cancel) ->
                        view.PendingReads <-
                            view.PendingReads
                            @ [ { Required = required
                                  Reply = reply
                                  Error = error } ])
        }

    let read consistency view =
        async {
            match consistency with
            | Eventual ->
                do! ensureReadable view
                return snapshot view
            | Strong ->
                do! ensureReadable view
                let! required = SubjectHistory.tail view.Basin view.Subject
                return! waitUntilApplied required view
        }

    let stop view =
        async {
            if not view.Stopped then
                view.StopRequested <- true

                return!
                    Async.FromContinuations(fun (reply, _error, _cancel) ->
                        if view.Stopped then
                            reply ()
                        else
                            view.StopWaiters <- view.StopWaiters @ [ reply ])
        }
