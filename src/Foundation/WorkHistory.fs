namespace Eff.Foundation

/// A tiny durable-work history used to validate the generic SubjectHistory
/// primitive against a concrete domain record and fold.
module WorkHistory =
    type Event =
        | Started of input: string
        | StepRequested of opId: string
        | StepCompleted of opId: string * value: string
        | TimerRequested of opId: string * wakeAt: string

    module Event =
        let encode event =
            match event with
            | Started input -> "started|" + input
            | StepRequested opId -> "step-requested|" + opId
            | StepCompleted(opId, value) -> sprintf "step-completed|%s|%s" opId value
            | TimerRequested(opId, wakeAt) -> sprintf "timer-requested|%s|%s" opId wakeAt

        let decode (body: string) =
            let parts = body.Split('|') |> Array.toList

            match parts with
            | [ "started"; input ] -> Ok(Started input)
            | [ "step-requested"; opId ] -> Ok(StepRequested opId)
            | [ "step-completed"; opId; value ] -> Ok(StepCompleted(opId, value))
            | [ "timer-requested"; opId; wakeAt ] -> Ok(TimerRequested(opId, wakeAt))
            | _ -> Error(sprintf "unknown work-history event body: %s" body)

        let codec: SubjectHistory.Codec<Event> = { Encode = encode; Decode = decode }

    type Status =
        | Empty
        | Running
        | Waiting

    type Snapshot =
        { Status: Status
          Completed: Map<string, string>
          PendingTimers: Map<string, string> }

    module Snapshot =
        let empty =
            { Status = Empty
              Completed = Map.empty
              PendingTimers = Map.empty }

        let apply snapshot (record: SubjectHistory.StoredRecord<Event>) =
            match record.Body with
            | Started _ -> { snapshot with Status = Running }
            | StepRequested _ -> snapshot
            | StepCompleted(opId, value) ->
                { snapshot with
                    Completed = snapshot.Completed |> Map.add opId value }
            | TimerRequested(opId, wakeAt) ->
                { snapshot with
                    Status = Waiting
                    PendingTimers = snapshot.PendingTimers |> Map.add opId wakeAt }

    let appendExpected basin subject expected events =
        SubjectHistory.appendExpected basin Event.codec subject expected events

    let append basin subject events =
        SubjectHistory.append basin Event.codec subject events

    let foldTo basin subject from until =
        SubjectHistory.foldTo basin Event.codec subject from until Snapshot.empty Snapshot.apply
