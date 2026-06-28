namespace Eff.Proofs

open Eff

/// The article's epoch mechanism: a single authoritative log admits one writer,
/// and a stale writer that never learned it lost is rejected BY THE STORE. S2
/// provides this as fencing tokens (a fence command record sets the token; a
/// fenced append must carry the current token). This proof exercises the raw S2
/// primitive directly — fencing lives below the typed `SubjectHistory` fold
/// (fence records are command records), so promoting it into the typed surface
/// is a deferred step; here we verify the substrate has the mechanism at all.
///
/// `SubjectHistory`'s position-CAS (`appendExpected`/`matchSeqNum`, proven in
/// `foundation-00-subject-history`) handles admission when writers contend on the
/// same expected tail; fencing handles the harder case: durable demotion of an
/// owner that keeps writing after a takeover, with no notification.
module FencingProof =

    let proof =
        Harness.proof "fencing" (fun ctx ->
            async {
                let s2 = S2Cli.connect ()
                let basin = s2 |> S2.basin Harness.config.Basin
                let streamName = Harness.uniq ctx "fencing"

                printfn "fenced stream: %s/%s" Harness.config.Basin streamName

                do! basin |> S2.createStream streamName

                Harness.onCleanup ctx (sprintf "delete stream %s" streamName) (fun () ->
                    basin |> S2.deleteStream streamName)

                let raw = basin |> S2.stream streamName

                let fenced token records =
                    raw
                    |> S2.tryAppendWith (S2.AppendOptions.none |> S2.AppendOptions.fencingToken token) records

                do!
                    Harness.section
                        ctx
                        "a newer fence durably demotes the prior owner"
                        (async {
                            let tokenA = Harness.uniq ctx "token-A"

                            // owner A installs its fence token, then writes under it
                            let! _ = raw |> S2.append [ S2.Record.fence tokenA ]
                            let! writeA = fenced tokenA [ S2.Record.text "A-1" ]

                            Harness.check
                                ctx
                                "current owner A writes under its token"
                                (match writeA with
                                 | Ok _ -> true
                                 | _ -> false)

                            // takeover: owner B installs a new token, demoting A
                            let tokenB = Harness.uniq ctx "token-B"
                            let! _ = raw |> S2.append [ S2.Record.fence tokenB ]

                            // A never observed the takeover and retries under its OLD token
                            let! staleA = fenced tokenA [ S2.Record.text "A-2" ]

                            Harness.check
                                ctx
                                "demoted owner A is fenced out by the store (FencingTokenMismatch)"
                                (match staleA with
                                 | Error(S2Errors.FencingTokenMismatch _) -> true
                                 | _ -> false)

                            // the current owner's own ack tells it the tail — no checkTail needed
                            let! writeB = fenced tokenB [ S2.Record.text "B-1" ]

                            match writeB with
                            | Ok ack ->
                                let! pos = raw |> S2.checkTail

                                Harness.check
                                    ctx
                                    "owner-local end equals checkTail (the live owner IS the tail)"
                                    (ack.End.SeqNum = pos.SeqNum)
                            | other -> Harness.check ctx (sprintf "owner B write should succeed, got %A" other) false
                        })
            })
