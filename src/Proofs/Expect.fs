namespace Eff.Proofs

module Expect =
    let workload (name: string) (predicate: 'result -> bool) : Check<'result> =
        { Name = name
          RunCheck =
            fun (trial: CompletedTrial<'result>) ->
                async {
                    match trial.Result with
                    | Error error -> return Error("workload failed: " + error)
                    | Ok result ->
                        if predicate result then
                            return Ok()
                        else
                            return Error "predicate returned false"
                } }

    let workloadResult name expected =
        workload name (fun actual -> actual = expected)

    let workloadResultBy name project expected =
        workload name (fun actual -> project actual = expected)
