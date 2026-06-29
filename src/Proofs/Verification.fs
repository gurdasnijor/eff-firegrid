namespace Eff.Proofs

type ExpectVerifiers<'result>() =
    member _.Workload (name: string) (predicate: 'result -> bool) : Check<'result> = Expect.workload name predicate

    member _.WorkloadResult (name: string) (expected: 'result) : Check<'result> =
        Expect.workload name (fun actual -> Reports.json (box actual) = Reports.json (box expected))

    member _.WorkloadResultBy<'value> (name: string) (project: 'result -> 'value) (expected: 'value) : Check<'result> =
        Expect.workload name (fun actual -> Reports.json (box (project actual)) = Reports.json (box expected))

type TraceVerifiers<'result>() =
    member _.SpanExists (label: string) (spanName: string) (attributes: (string * string) list) : Check<'result> =
        TraceExpect.spanExists label spanName attributes

    member _.Sql (name: string) (sql: string) : Check<'result> =
        TraceProof.sql name sql |> TraceProof.asCheck

    member _.Operation (name: string) (matchSpec: TraceOperationMatch) : Check<'result> =
        TraceProof.operation name matchSpec |> TraceProof.asCheck

type Verifiers<'result> =
    { Expect: ExpectVerifiers<'result>
      Trace: TraceVerifiers<'result> }

module Verification =
    let verifiers<'result> () : Verifiers<'result> =
        { Expect = ExpectVerifiers<'result>()
          Trace = TraceVerifiers<'result>() }
