namespace Eff.Proofs

module TraceExpect =
    let spanExists (label: string) (spanName: string) (attributes: (string * string) list) : Check<'result> =
        { Name = label
          RunCheck =
            fun (trial: CompletedTrial<'result>) ->
                async {
                    let! found =
                        TraceSql.spanExists spanName attributes
                        |> TraceSql.exists
                            { TrialId = trial.TrialId
                              SpansJsonl = trial.Traces.SpansJsonl }

                    if found then
                        return Ok()
                    else
                        return Error(sprintf "span not found: %s" spanName)
                } }
