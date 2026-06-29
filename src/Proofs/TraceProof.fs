namespace Eff.Proofs

open System
open System.Text.RegularExpressions

type TraceProof = { Name: string; Sql: string }

type TraceOperationMatch =
    { Operation: string
      Status: string option
      Attributes: (string * string) list
      OutputContains: string list
      Count: int option }

module TraceOperationMatch =
    let named operation =
        { Operation = operation
          Status = None
          Attributes = []
          OutputContains = []
          Count = None }

    let status value matchSpec = { matchSpec with Status = Some value }

    let attribute key value matchSpec =
        { matchSpec with
            Attributes = matchSpec.Attributes @ [ key, value ] }

    let outputContains value matchSpec =
        { matchSpec with
            OutputContains = matchSpec.OutputContains @ [ value ] }

    let exactly count matchSpec = { matchSpec with Count = Some count }

module TraceProof =
    let private trialSpansSql =
        """
(
  SELECT *
  FROM file({spans_jsonl:String}, JSONEachRow)
  WHERE trial_id = {trial_id:String}
)
"""

    let private verificationOperationsSql =
        """
(
  SELECT
    trial_id,
    JSONExtractString(toJSONString(attributes), 'firegrid.client.id') AS client_id,
    JSONExtractString(toJSONString(attributes), 'firegrid.operation.id') AS operation_id,
    JSONExtractString(toJSONString(attributes), 'firegrid.operation.name') AS operation,
    JSONExtractString(toJSONString(attributes), 'firegrid.operation.key') AS operation_key,
    JSONExtractString(toJSONString(attributes), 'firegrid.operation.input.json') AS input_json,
    JSONExtractString(toJSONString(attributes), 'firegrid.operation.output.json') AS output_json,
    JSONExtractString(toJSONString(attributes), 'firegrid.operation.status') AS status,
    JSONExtractString(toJSONString(attributes), 'firegrid.operation.failure.kind') AS failure_kind
  FROM trial_spans
  WHERE name = 'verification.operation'
)
"""

    let private hasTraceMacro (sql: string) =
        Regex.IsMatch(sql, @"\b(trial_spans|verification_operations)\b", RegexOptions.IgnoreCase)

    let private containsForbiddenReader (sql: string) =
        Regex.IsMatch(sql, @"\b(file|url|s3|hdfs|mysql|postgresql|remote|cluster)\s*\(", RegexOptions.IgnoreCase)
        || Regex.IsMatch(sql, @"\bsystem\s*\.", RegexOptions.IgnoreCase)

    let private expandTraceMacros (sql: string) =
        sql
        |> fun value ->
            Regex.Replace(value, @"\bverification_operations\b", verificationOperationsSql, RegexOptions.IgnoreCase)
        |> fun value -> Regex.Replace(value, @"\btrial_spans\b", trialSpansSql, RegexOptions.IgnoreCase)

    let normalizeSql (sql: string) =
        let trimmed = sql.Trim().TrimEnd(';').Trim()

        if
            not (
                trimmed.StartsWith("select", StringComparison.OrdinalIgnoreCase)
                || trimmed.StartsWith("with", StringComparison.OrdinalIgnoreCase)
            )
        then
            failwith "trace proof SQL must be a SELECT or WITH query"

        if trimmed.Contains(";") then
            failwith "trace proof SQL must contain one read-only query"

        if not (hasTraceMacro trimmed) then
            failwith "trace proof SQL must query trial_spans or verification_operations"

        if containsForbiddenReader trimmed then
            failwith "trace proof SQL cannot use external table readers"

        expandTraceMacros trimmed

    let sql name body =
        { Name = name; Sql = normalizeSql body }

    let private sqlString (value: string) =
        "'" + value.Replace("\\", "\\\\").Replace("'", "\\'") + "'"

    let private attributeField (key: string) =
        "JSONExtractString(toJSONString(attributes), " + sqlString key + ")"

    let private attributeEquals key value =
        sprintf "%s = %s" (attributeField key) (sqlString value)

    let operation name matchSpec =
        let conditions =
            [ "name = 'verification.operation'"
              attributeEquals "firegrid.operation.name" matchSpec.Operation
              match matchSpec.Status with
              | Some status -> attributeEquals "firegrid.operation.status" status
              | None -> ""
              yield! matchSpec.Attributes |> List.map (fun (key, value) -> attributeEquals key value)
              yield!
                  matchSpec.OutputContains
                  |> List.map (fun value ->
                      sprintf
                          "position(%s, %s) > 0"
                          (attributeField "firegrid.operation.output.json")
                          (sqlString value)) ]
            |> List.filter (fun condition -> condition <> "")

        let expectedCount = matchSpec.Count |> Option.defaultValue 1

        sql
            name
            (sprintf
                """
SELECT countIf(
  %s
) = %d AS ok
FROM trial_spans
"""
                (String.concat "\n  AND " conditions)
                expectedCount)

    let asCheck (proof: TraceProof) : Check<'result> =
        { Name = proof.Name
          RunCheck =
            fun trial ->
                async {
                    let! result =
                        TraceSql.scalarTruthy
                            { TrialId = trial.TrialId
                              SpansJsonl = trial.Traces.SpansJsonl }
                            { Sql = proof.Sql
                              Parameters = Map.empty }

                    return result |> Result.mapError (fun message -> "trace proof failed: " + message)
                } }
