namespace Eff.Foundation.Durable

[<RequireQualifiedAccess>]
module Activities =
    let create name input : Activity = { Name = name; Input = input }

[<RequireQualifiedAccess>]
module DurableTask =
    let activity name input =
        Activities.create name input |> RaceActivity

    let timer deadline = Timer deadline |> RaceEvent

    let signal name = Signal name |> RaceEvent

[<RequireQualifiedAccess>]
module Workflow =
    let call name input =
        Activities.create name input |> Durable.perform

    let all activities =
        activities |> List.ofSeq |> Durable.performAll

    let waitForSignal name = Signal name |> Durable.await

    let sleepUntil deadline =
        Timer deadline |> Durable.await |> Durable.map ignore

    let any tasks = tasks |> List.ofSeq |> Durable.whenAny

    let currentTime = Durable.currentTime

    let log message = Durable.log message

type DurableBuilder() =
    member _.Return value = Durable.result value

    member _.ReturnFrom program = program

    member _.Bind(program, binder) = Durable.bind binder program

    member _.Delay(generator: unit -> Durable<'a>) = generator ()

    member _.Zero() = Durable.result ()

    member _.Combine(first: Durable<unit>, second: Durable<'a>) = Durable.bind (fun () -> second) first

[<AutoOpen>]
module DurableSyntax =
    let durable = DurableBuilder()
