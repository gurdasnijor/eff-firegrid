namespace Eff.Proofs

open Fable.Core

module ProofOperation =
    [<Emit("($0 && $0.name) ? String($0.name) : 'Error'")>]
    let private errorName (_error: exn) : string = jsNative

    let run
        (ctx: WorkloadContext)
        (name: string)
        (input: 'input)
        (options: ProofOperationOptions)
        (work: Async<'output>)
        : Async<'output> =
        async {
            let operationId =
                defaultArg options.OperationId (sprintf "%s-%d" name (ctx.NextOperationId()))

            let clientId = defaultArg options.ClientId "default"
            let key = defaultArg options.Key ""
            let inputJson = Reports.json (box input)

            try
                let! output = work
                let outputJson = Reports.json (box output)

                do!
                    ctx.EmitSpan
                        "verification.operation"
                        [ "firegrid.client.id", clientId
                          "firegrid.operation.id", operationId
                          "firegrid.operation.input.json", inputJson
                          "firegrid.operation.key", key
                          "firegrid.operation.name", name
                          "firegrid.operation.output.json", outputJson
                          "firegrid.operation.status", "ok" ]

                return output
            with error ->
                do!
                    ctx.EmitSpan
                        "verification.operation"
                        [ "firegrid.client.id", clientId
                          "firegrid.operation.id", operationId
                          "firegrid.operation.input.json", inputJson
                          "firegrid.operation.key", key
                          "firegrid.operation.name", name
                          "firegrid.operation.failure.kind", errorName error
                          "firegrid.operation.failure.message", error.Message
                          "firegrid.operation.status", "error" ]

                return raise error
        }
