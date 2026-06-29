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

type HostVerifiers<'result>() =
    member _.Started(hostName: string) : Check<'result> =
        TraceExpect.spanExists
            (sprintf "host '%s' started" hostName)
            "verification.host.start"
            [ "host.name", hostName ]

    member _.Ready(hostName: string) : Check<'result> =
        TraceExpect.spanExists
            (sprintf "host '%s' became ready" hostName)
            "verification.host.ready"
            [ "host.name", hostName ]

    member _.Stopped(hostName: string) : Check<'result> =
        TraceExpect.spanExists (sprintf "host '%s' stopped" hostName) "verification.host.stop" [ "host.name", hostName ]

type FaultVerifiers<'result>() =
    member _.HostKilled(hostName: string) : Check<'result> =
        TraceExpect.spanExists
            (sprintf "host '%s' was killed" hostName)
            "verification.host.kill"
            [ "host.name", hostName; "verification.signal", "SIGKILL" ]

    member _.HostKillAccepted(hostName: string) : Check<'result> =
        TraceExpect.spanExists
            (sprintf "host '%s' kill signal was accepted" hostName)
            "verification.host.kill"
            [ "host.name", hostName
              "verification.signal", "SIGKILL"
              "verification.accepted", "true" ]

type Verifiers<'result> =
    { Expect: ExpectVerifiers<'result>
      Trace: TraceVerifiers<'result>
      Host: HostVerifiers<'result>
      Fault: FaultVerifiers<'result> }

module Verification =
    let verifiers<'result> () : Verifiers<'result> =
        { Expect = ExpectVerifiers<'result>()
          Trace = TraceVerifiers<'result>()
          Host = HostVerifiers<'result>()
          Fault = FaultVerifiers<'result>() }
