namespace Eff.Proofs

open Fable.Core
open Fable.Core.JsInterop

module TraceSql =
    type TraceStore = { TrialId: string; SpansJsonl: string }

    type Query =
        { Sql: string
          Parameters: Map<string, string> }

    type ChdbResult =
        abstract text: unit -> string

    let private queryBindAsync (_sql: string) (_parameters: obj) (_options: obj) : JS.Promise<ChdbResult> =
        import "queryBindAsync" "chdb"

    let private formatOptions = createObj [ "format" ==> "JSONEachRow" ]

    let private bindParameters store query =
        query.Parameters
        |> Map.add "trial_id" store.TrialId
        |> Map.add "spans_jsonl" store.SpansJsonl
        |> Seq.map (fun item -> item.Key ==> item.Value)
        |> createObj

    let raw (store: TraceStore) (query: Query) : Async<string> =
        async {
            let! result =
                queryBindAsync query.Sql (bindParameters store query) formatOptions
                |> Async.AwaitPromise

            return result.text ()
        }

    let rows (store: TraceStore) (query: Query) : Async<string list> =
        async {
            let! text = raw store query

            return text.Split("\n") |> Array.toList |> List.filter (fun row -> row.Trim() <> "")
        }

    let scalarInt (store: TraceStore) (query: Query) : Async<int> =
        async {
            let! text = raw store query
            let line = text.Split("\n") |> Array.tryFind (fun row -> row.Trim() <> "")

            match line with
            | None -> return 0
            | Some row ->
                let parsed: obj = JS.JSON.parse row
                return int (parsed?``count()``)
        }

    let exists store query =
        async {
            let! count = scalarInt store query
            return count > 0
        }

    let private attributeField (key: string) =
        "attributes.`" + key.Replace("`", "``") + "`"

    let spanExists spanName attributes =
        let attributeClauses =
            attributes
            |> List.mapi (fun index (key, _) -> sprintf "%s = {attr_%d:String}" (attributeField key) index)

        let sql =
            [ "SELECT count()"
              "FROM file({spans_jsonl:String}, JSONEachRow)"
              "WHERE trial_id = {trial_id:String}"
              "  AND name = {span_name:String}"
              yield! attributeClauses |> List.map (fun clause -> "  AND " + clause) ]
            |> String.concat "\n"

        let parameters =
            attributes
            |> List.mapi (fun index (_, value) -> "attr_" + string index, value)
            |> Map.ofList
            |> Map.add "span_name" spanName

        { Sql = sql; Parameters = parameters }
