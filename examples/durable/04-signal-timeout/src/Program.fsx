// Durable signal timeout flow.

#r "nuget: Fable.Core, 5.1.0"

#load "../../../../src/S2/Interop.fs"
#load "../../../../src/S2/Errors.fs"
#load "../../../../src/S2/Client.fs"
#load "../../../../src/S2/Cli.fs"
#load "../../../../src/Foundation/Durable/Semantics.fs"
#load "../../../../src/Foundation/Durable/Api.fs"
#load "../../../../src/Foundation/Durable/Registry.fs"
#load "../../../../src/Foundation/Durable/S2Substrate.fs"
#load "../../../../src/Foundation/Durable/Stepper.fs"
#load "../../../../src/Foundation/Durable/StepRecordCodec.fs"
#load "../../../../src/Foundation/Durable/Client.fs"
#load "../../../../src/Foundation/Durable/CommandDispatch.fs"
#load "../../../../src/Foundation/Durable/ActivityAdapter.fs"
#load "../../../../src/Foundation/Durable/InboxFold.fs"
#load "../../../../src/Foundation/Durable/TimerAdapter.fs"
#load "../../../../src/Foundation/Durable/Host.fs"
#load "../../../../src/Foundation/Durable/Runtime.fs"
#load "../../../../src/Foundation/Durable/App/Storage.fs"
#load "../../../../src/Foundation/Durable/App/Definitions.fs"
#load "../../../../src/Foundation/Durable/App/DurableFacade.fs"
#load "../../../../src/Foundation/Durable/App/Environment.fs"
#load "../../../../src/Foundation/Durable/App/Client.fs"
#load "../../../../src/Foundation/Durable/App/Worker.fs"
#load "../../../../src/Foundation/Durable/App/Builder.fs"

open Eff.Foundation.Durable
open Eff.Foundation.Durable.App
open Fable.Core

module D = Eff.Foundation.Durable.App.Durable

[<Emit("Date.now()")>]
let nowMillis () : int64 = jsNative

let approved = Signal.define "approval.timeout.approved"

let approvalOrTimeout =
    Workflow.defineWith "approval.timeout" string int64 id id (fun deadline ->
        durable {
            return!
                D.anyOf
                    [ D.raceSignal approved (fun approver -> "approved:" + approver)
                      D.raceTimer deadline "timed-out" ]
        })

let app = durableApp { workflow approvalOrTimeout }

let startOrFail =
    function
    | DurableAppStartResult.Started instanceId -> instanceId
    | DurableAppStartResult.Rejected failure -> failwith ("start failed: " + string failure)

let run () =
    async {
        let client =
            app
            |> DurableApp.client
                { Environment = "durable"
                  BasinName = None }

        let worker =
            app
            |> DurableApp.worker
                { Environment = "durable"
                  BasinName = None
                  HostId = "timeout"
                  MaxRunUntilIdleTicks = Some 10 }

        let! signalStart = client.start approvalOrTimeout (nowMillis () + 60000L)
        let signalId = startOrFail signalStart
        let! _ = worker.runUntilIdle signalId
        let! _ = client.signal signalId approved "alice"
        let! _ = worker.runUntilIdle signalId
        let! signalStatus = client.statusOf approvalOrTimeout signalId
        printfn "%A" signalStatus

        let! timerStart = client.start approvalOrTimeout (nowMillis () - 1L)
        let timerId = startOrFail timerStart
        let! _ = worker.runUntilIdle timerId
        let! timerStatus = client.statusOf approvalOrTimeout timerId
        printfn "%A" timerStatus
    }

Async.StartImmediate(run ())
