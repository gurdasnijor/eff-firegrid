namespace Eff.Proofs

open Fable.Core
open Fable.Core.JsInterop

module Reports =
    let private fs: obj = importAll "node:fs"
    let private path: obj = importAll "node:path"

    [<Emit("$0.mkdirSync($1, { recursive: true })")>]
    let private mkdirp (_fs: obj) (_path: string) : unit = jsNative

    [<Emit("$0.writeFileSync($1, $2, 'utf8')")>]
    let private writeFile (_fs: obj) (_path: string) (_content: string) : unit = jsNative

    [<Emit("$0.appendFileSync($1, $2, 'utf8')")>]
    let private appendFile (_fs: obj) (_path: string) (_content: string) : unit = jsNative

    [<Emit("$0.join(...$1)")>]
    let private joinPath (_path: obj) (_parts: string array) : string = jsNative

    [<Emit("$0.dirname($1)")>]
    let private dirname (_path: obj) (_value: string) : string = jsNative

    [<Emit("JSON.stringify($0)")>]
    let private stringify (_value: obj) : string = jsNative

    [<Emit("Date.now()")>]
    let nowMillis () : float = jsNative

    let join parts = joinPath path (parts |> List.toArray)

    let ensureDir dir = mkdirp fs dir

    let write path content = writeFile fs path content

    let append path content = appendFile fs path content

    let json value = stringify value

    let trialId prefix =
        sprintf "%s-%d" prefix (int64 (nowMillis ()))

    let traceStore root trialId =
        let trialRoot = join [ root; trialId ]
        let tracesRoot = join [ trialRoot; "traces" ]
        ensureDir tracesRoot

        { TrialId = trialId
          Root = trialRoot
          SpansJsonl = join [ tracesRoot; "spans.jsonl" ] }

    let emitSpan (store: TraceStore) name attributes =
        async {
            let row =
                createObj
                    [ "trial_id" ==> store.TrialId
                      "service_name" ==> "eff-firegrid-proof-runner"
                      "host_id" ==> null
                      "trace_id" ==> ""
                      "span_id" ==> ""
                      "parent_span_id" ==> null
                      "name" ==> name
                      "kind" ==> "INTERNAL"
                      "status_code" ==> "OK"
                      "status_message" ==> null
                      "start_unix_nanos" ==> string (int64 (nowMillis ()) * 1000000L)
                      "end_unix_nanos" ==> string (int64 (nowMillis ()) * 1000000L)
                      "attributes" ==> createObj [ for (key, value) in attributes -> key ==> value ]
                      "events" ==> [||] ]

            append store.SpansJsonl (json row + "\n")
        }

    let writePropertyReport (report: PropertyReport) =
        let checks =
            report.Checks
            |> List.map (fun check ->
                createObj
                    [ "name" ==> check.Name
                      "passed" ==> check.Passed
                      "message" ==> Option.toObj check.Message ])
            |> List.toArray

        let negativeControls =
            report.NegativeControls
            |> List.map (fun control ->
                createObj
                    [ "name" ==> control.Name
                      "passed" ==> control.Passed
                      "expectedFailure" ==> Option.toObj control.ExpectedFailure
                      "failedChecks" ==> List.toArray control.FailedChecks
                      "message" ==> Option.toObj control.Message ])
            |> List.toArray

        let body =
            createObj
                [ "proof" ==> report.ProofName
                  "property" ==> report.PropertyName
                  "trialId" ==> report.TrialId
                  "status" ==> if report.Passed then "passed" else "failed"
                  "workloadFailed" ==> report.WorkloadFailed
                  "checks" ==> checks
                  "negativeControls" ==> negativeControls
                  "reportPath" ==> report.ReportPath ]

        ensureDir (dirname path report.ReportPath)
        write report.ReportPath (json body + "\n")
