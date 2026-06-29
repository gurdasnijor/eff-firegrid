namespace Eff.Proofs

open Eff
open Fable.Core
open Fable.Core.JsInterop

module S2Lite =
    type private ChildProcess =
        abstract kill: signal: string -> bool

    type Instance =
        { Resource: S2Resource
          Stop: unit -> Async<unit> }

    [<Import("spawn", "node:child_process")>]
    let private spawn (_command: string) (_args: string array) (_options: obj) : ChildProcess = jsNative

    [<Emit("fetch($0).then(() => true).catch(() => false)")>]
    let private fetchReady (_url: string) : JS.Promise<bool> = jsNative

    [<Emit("new Promise(resolve => setTimeout(resolve, $0))")>]
    let private sleep (_millis: int) : JS.Promise<unit> = jsNative

    [<Emit("20000 + Math.floor(Math.random() * 20000)")>]
    let private randomPort () : int = jsNative

    [<Emit("process.env.S2_BIN || (process.env.HOME ? process.env.HOME + '/.s2/bin/s2' : 's2')")>]
    let private defaultBin () : string = jsNative

    let private waitUntilReady endpoint =
        let rec loop remaining =
            async {
                let! ready = fetchReady endpoint |> Async.AwaitPromise

                if ready then
                    return ()
                elif remaining <= 0 then
                    return failwithf "s2 lite did not become ready at %s" endpoint
                else
                    do! sleep 100 |> Async.AwaitPromise
                    return! loop (remaining - 1)
            }

        loop 100

    let start (store: TraceStore) (requestedRoot: string) =
        async {
            let localRoot =
                if requestedRoot.Trim() = "" then
                    Reports.join [ store.Root; "s2-lite" ]
                else
                    requestedRoot

            Reports.ensureDir localRoot
            let port = randomPort ()
            let endpoint = sprintf "http://127.0.0.1:%d" port

            let proc =
                spawn
                    (defaultBin ())
                    [| "lite"; "--port"; string port; "--local-root"; localRoot |]
                    (createObj [ "stdio" ==> "ignore" ])

            try
                do! waitUntilReady endpoint

                let client =
                    S2.connectWith
                        { S2.ConnectOptions.create "s2-lite-proof-runner" with
                            AccountEndpoint = Some endpoint
                            BasinEndpoint = Some endpoint }

                do!
                    Reports.emitSpan
                        store
                        "verification.s2.lite.started"
                        [ "resource.kind", "s2Lite"
                          "s2.endpoint", endpoint
                          "s2.local_root", localRoot ]

                return
                    { Resource =
                        { Client = client
                          Kind = "s2Lite"
                          Endpoint = Some endpoint
                          LocalRoot = Some localRoot }
                      Stop =
                        fun () ->
                            async {
                                proc.kill "SIGTERM" |> ignore

                                do!
                                    Reports.emitSpan
                                        store
                                        "verification.s2.lite.stopped"
                                        [ "resource.kind", "s2Lite"
                                          "s2.endpoint", endpoint
                                          "s2.local_root", localRoot ]
                            } }
            with error ->
                proc.kill "SIGKILL" |> ignore
                return raise error
        }
