namespace Eff.Foundation.Durable.App

open Eff.Foundation.Durable

[<RequireQualifiedAccess>]
module Activity =
    let defineWith name encodeInput decodeInput encodeOutput decodeOutput handler : Activity<'input, 'output> =
        { Name = ActivityName.create name
          EncodeInput = encodeInput
          DecodeInput = decodeInput
          EncodeOutput = encodeOutput
          DecodeOutput = decodeOutput
          Handler = handler }

    let define name handler : Activity<string, string> = defineWith name id id id id handler

[<RequireQualifiedAccess>]
module Workflow =
    let defineWith name encodeInput decodeInput encodeOutput decodeOutput factory : Workflow<'input, 'output> =
        { Name = WorkflowName.create name
          EncodeInput = encodeInput
          DecodeInput = decodeInput
          EncodeOutput = encodeOutput
          DecodeOutput = decodeOutput
          Factory = factory }

    let define name factory : Workflow<string, string> = defineWith name id id id id factory

[<RequireQualifiedAccess>]
module Signal =
    let defineWith name encode decode : Signal<'payload> =
        { Name = name
          Encode = encode
          Decode = decode }

    let define name : Signal<string> = defineWith name id id

[<RequireQualifiedAccess>]
module DurableTask =
    let signal (signal: Signal<'payload>) =
        Eff.Foundation.Durable.DurableTask.signal signal.Name

    let timer = Eff.Foundation.Durable.DurableTask.timer

[<RequireQualifiedAccess>]
module Durable =
    let call (activity: Activity<'input, 'output>) (input: 'input) =
        Eff.Foundation.Durable.Workflow.call (ActivityName.value activity.Name) (activity.EncodeInput input)
        |> Eff.Foundation.Durable.Durable.map activity.DecodeOutput

    let waitForSignal (signal: Signal<'payload>) =
        Eff.Foundation.Durable.Workflow.waitForSignal signal.Name
        |> Eff.Foundation.Durable.Durable.map signal.Decode

    let sleepUntil = Eff.Foundation.Durable.Workflow.sleepUntil

    let any tasks =
        Eff.Foundation.Durable.Workflow.any tasks

    let raceSignal (signal: Signal<'payload>) project : DurableRace<'result> =
        { Task = DurableTask.signal signal
          Project =
            function
            | EventWon(_, Signal name, payload) when name = signal.Name -> Some(project (signal.Decode payload))
            | _ -> None }

    let raceTimer deadline result : DurableRace<'result> =
        { Task = DurableTask.timer deadline
          Project =
            function
            | EventWon(_, Timer actual, _) when actual = deadline -> Some result
            | _ -> None }

    let anyOf races =
        let races = races |> List.ofSeq

        races
        |> List.map (fun race -> race.Task)
        |> Eff.Foundation.Durable.Workflow.any
        |> Eff.Foundation.Durable.Durable.map (fun winner ->
            races
            |> List.tryPick (fun race -> race.Project winner)
            |> Option.defaultWith (fun () ->
                failwith ("durable race winner did not match a facade race task: " + string winner)))

    let currentTime = Eff.Foundation.Durable.Workflow.currentTime

    let log = Eff.Foundation.Durable.Workflow.log
