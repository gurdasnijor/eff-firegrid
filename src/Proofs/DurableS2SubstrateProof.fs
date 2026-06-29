namespace Eff.Proofs

open System
open Eff
open Eff.Foundation.Durable

module DurableS2SubstrateProof =
    type DurableS2SubstrateResult =
        { StreamsEnsured: bool
          ClaimWritesFenceInvisibleToReplay: bool
          FencedCommitAccepted: bool
          LogReplayReadsCommittedEntries: bool
          StaleOwnerCommitDeposed: bool
          InboxAppendReadable: bool
          RelayDeliversOutgoingOnly: bool
          RelayAdvancesCursor: bool }

    module private EntryCodec =
        let private field (value: string) = string value.Length + ":" + value

        let private readField (text: string) (index: int) =
            let colon = text.IndexOf(':', index)

            if colon < 0 then
                Error "missing field length separator"
            else
                let lengthText = text.Substring(index, colon - index)

                match Int32.TryParse lengthText with
                | false, _ -> Error("bad field length: " + lengthText)
                | true, length ->
                    let start = colon + 1
                    let finish = start + length

                    if finish > text.Length then
                        Error "field length exceeds record body"
                    else
                        Ok(text.Substring(start, length), finish)

        let encode =
            function
            | Incoming message -> "in|" + field message
            | Outgoing message -> "out|" + field message

        let decode (body: string) =
            if body.StartsWith("in|", StringComparison.Ordinal) then
                readField body 3
                |> Result.bind (fun (message, finish) ->
                    if finish = body.Length then
                        Ok(Incoming message)
                    else
                        Error "trailing incoming data")
            elif body.StartsWith("out|", StringComparison.Ordinal) then
                readField body 4
                |> Result.bind (fun (message, finish) ->
                    if finish = body.Length then
                        Ok(Outgoing message)
                    else
                        Error "trailing outgoing data")
            else
                Error("unknown history entry body: " + body)

    let private forceDecoded entries =
        entries
        |> List.map (fun (seq, decoded) ->
            match decoded with
            | Ok entry -> seq, entry
            | Error error -> failwith error)

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.s2_substrate"
            "durable-s2-substrate"
            { ProofOperationOptions.empty with
                Key = Some "durable-s2-substrate" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-s2-" + suffix
                let sourceKey = StorageKey("source-" + suffix)
                let destinationKey = StorageKey("destination-" + suffix)

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                do! S2Substrate.ensureStreams basin sourceKey
                do! S2Substrate.ensureStreams basin destinationKey

                let sourcePair = S2Substrate.streams basin sourceKey
                let destinationPair = S2Substrate.streams basin destinationKey

                let! streams = basin |> S2.listStreamsWith (StorageKey.value sourceKey)

                let streamsEnsured =
                    streams
                    |> List.map (fun stream -> stream.Name)
                    |> Set.ofList
                    |> Set.isSuperset (
                        Set.ofList [ StorageKey.logStreamName sourceKey; StorageKey.inboxStreamName sourceKey ]
                    )

                let firstFence = FenceToken "host-a/fence-1"
                let secondFence = FenceToken "host-b/fence-2"

                let! firstOwner = S2Substrate.claimWith firstFence sourcePair

                let! afterClaimReplay = S2Substrate.readLogText EntryCodec.decode firstOwner

                let claimWritesFenceInvisibleToReplay = List.isEmpty afterClaimReplay

                let committedEntries =
                    [ Incoming "self-start"; Outgoing "deliver-one"; Outgoing "deliver-two" ]

                let! commit = S2Substrate.commitText EntryCodec.encode committedEntries firstOwner

                let fencedCommitAccepted =
                    match commit with
                    | Committed ack -> ack.Start.SeqNum = 1L && ack.End.SeqNum = 4L
                    | _ -> false

                let! replayedRaw = S2Substrate.readLogText EntryCodec.decode firstOwner

                let replayed = forceDecoded replayedRaw

                let logReplayReadsCommittedEntries =
                    (replayed |> List.map snd) = committedEntries
                    && (replayed |> List.map fst) = [ 1L; 2L; 3L ]

                let! inboxAck = S2Substrate.appendInboxText [ "kind", "signal" ] "approved" sourcePair

                let! inboxRecords = S2Substrate.readInbox 0L 10 firstOwner

                let inboxAppendReadable =
                    inboxAck.Start.SeqNum = 0L
                    && inboxRecords
                       |> List.exists (fun record ->
                           record.SeqNum = 0L
                           && record.Body = "approved"
                           && record.Headers |> List.contains ("kind", "signal"))

                let! relay =
                    S2Substrate.relayTextBatch
                        EntryCodec.decode
                        id
                        (fun _ -> destinationKey)
                        (fun key -> (S2Substrate.streams basin key).Inbox)
                        0L
                        10
                        firstOwner

                let! destinationInbox = destinationPair.Inbox |> S2.read (S2.FromSeqNum 0L) 10

                let relayDeliversOutgoingOnly =
                    (destinationInbox |> List.map (fun record -> record.Body)) = [ "deliver-one"; "deliver-two" ]
                    && destinationInbox
                       |> List.forall (fun record ->
                           record.Headers
                           |> List.exists (fun (key, value) -> key = "src" && value = S2.streamName firstOwner.Log)
                           && record.Headers |> List.exists (fun (key, _) -> key = "seq"))

                let relayAdvancesCursor = relay.NextSeqNum = 4L && relay.Delivered = 2

                let! _secondOwner = S2Substrate.claimWith secondFence sourcePair
                let! staleCommit = S2Substrate.commitText EntryCodec.encode [ Outgoing "stale" ] firstOwner

                let staleOwnerCommitDeposed =
                    match staleCommit with
                    | Deposed expected -> expected = FenceToken.value secondFence
                    | _ -> false

                do! basin |> S2.deleteStream (StorageKey.inboxStreamName destinationKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName destinationKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName sourceKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName sourceKey)

                let result =
                    { StreamsEnsured = streamsEnsured
                      ClaimWritesFenceInvisibleToReplay = claimWritesFenceInvisibleToReplay
                      FencedCommitAccepted = fencedCommitAccepted
                      LogReplayReadsCommittedEntries = logReplayReadsCommittedEntries
                      StaleOwnerCommitDeposed = staleOwnerCommitDeposed
                      InboxAppendReadable = inboxAppendReadable
                      RelayDeliversOutgoingOnly = relayDeliversOutgoingOnly
                      RelayAdvancesCursor = relayAdvancesCursor }

                do!
                    ctx.EmitSpan
                        "proof.durable_s2_substrate.completed"
                        [ "proof.property", "durable-s2-substrate"
                          "substrate.streams", string result.StreamsEnsured
                          "substrate.commit", string result.FencedCommitAccepted
                          "substrate.deposed", string result.StaleOwnerCommitDeposed
                          "substrate.relay", string result.RelayDeliversOutgoingOnly ]

                return result
            })

    let substrateProperty =
        property "durable-s2-substrate" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "two streams are ensured for a durable key" (fun result -> result.StreamsEnsured)
                  v.Expect.Workload "claim writes a fence invisible to log replay" (fun result ->
                      result.ClaimWritesFenceInvisibleToReplay)
                  v.Expect.Workload "fenced commit is accepted for the owner" (fun result ->
                      result.FencedCommitAccepted)
                  v.Expect.Workload "log replay reads committed data entries in order" (fun result ->
                      result.LogReplayReadsCommittedEntries)
                  v.Expect.Workload "stale owner commit is deposed" (fun result -> result.StaleOwnerCommitDeposed)
                  v.Expect.Workload "inbox append is readable" (fun result -> result.InboxAppendReadable)
                  v.Expect.Workload "relay delivers outgoing entries only" (fun result ->
                      result.RelayDeliversOutgoingOnly)
                  v.Expect.Workload "relay advances cursor past scanned log records" (fun result ->
                      result.RelayAdvancesCursor)
                  v.Trace.SpanExists
                      "durable S2 substrate proof span emitted"
                      "proof.durable_s2_substrate.completed"
                      [ "proof.property", "durable-s2-substrate" ]
                  v.Trace.Operation
                      "durable S2 substrate operation was recorded"
                      ({ TraceOperationMatch.named "durable.s2_substrate" with
                          Status = Some "ok"
                          OutputContains =
                              [ "FencedCommitAccepted"
                                "StaleOwnerCommitDeposed"
                                "RelayDeliversOutgoingOnly" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-s2-substrate" {
            describedAs "S2 two-stream substrate claim, fenced commit, inbox, and relay invariants."
            property substrateProperty
        }
