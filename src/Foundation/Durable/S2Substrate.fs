namespace Eff.Foundation.Durable

open Eff
open Fable.Core

[<Struct>]
type StorageKey = StorageKey of string

[<Struct>]
type FenceToken = FenceToken of string

type StreamPair =
    { Key: StorageKey
      Log: S2.Stream
      Inbox: S2.Stream }

type OwnedKey =
    { Key: StorageKey
      Fence: FenceToken
      Log: S2.Stream
      Inbox: S2.Stream }

type HistoryEntry<'message> =
    | Incoming of 'message
    | Outgoing of 'message

type CommitResult =
    | Committed of S2.AppendAck
    | Deposed of expectedFence: string
    | CommitFailed of S2Errors.S2Failure

type RelayResult = { NextSeqNum: int64; Delivered: int }

[<RequireQualifiedAccess>]
module StorageKey =
    let value (StorageKey key) = key

    let logStreamName key = value key + "/log"

    let inboxStreamName key = value key + "/in"

[<RequireQualifiedAccess>]
module FenceToken =
    [<Emit("Date.now().toString(36) + '-' + Math.random().toString(36).slice(2)")>]
    let private entropy () : string = jsNative

    let value (FenceToken token) = token

    let create host = FenceToken(host + "/" + entropy ())

[<RequireQualifiedAccess>]
module S2Substrate =
    let streams (basin: S2.Basin) key : StreamPair =
        { Key = key
          Log = basin |> S2.stream (StorageKey.logStreamName key)
          Inbox = basin |> S2.stream (StorageKey.inboxStreamName key) }

    let ensureStreamsWith config (basin: S2.Basin) key =
        async {
            let! _ = basin |> S2.ensureStreamWith config (StorageKey.logStreamName key)
            let! _ = basin |> S2.ensureStreamWith config (StorageKey.inboxStreamName key)
            return ()
        }

    let ensureStreams basin key =
        ensureStreamsWith S2.StreamConfig.empty basin key

    let claimWith token (pair: StreamPair) : Async<OwnedKey> =
        async {
            let! _ = pair.Log |> S2.append [ S2.Record.fence (FenceToken.value token) ]

            return
                { Key = pair.Key
                  Fence = token
                  Log = pair.Log
                  Inbox = pair.Inbox }
        }

    let claim host pair = claimWith (FenceToken.create host) pair

    let readLogText decode (owned: OwnedKey) =
        async {
            try
                let! records =
                    owned.Log
                    |> S2.readWith
                        { S2.ReadOptions.empty with
                            Start = Some(S2.FromSeqNum 0L)
                            IgnoreCommandRecords = true }

                return records |> List.map (fun record -> record.SeqNum, decode record.Body)
            with error ->
                match S2Errors.classify error with
                | S2Errors.RangeNotSatisfiable _ -> return []
                | _ -> return raise error
        }

    let readInbox from count (owned: OwnedKey) =
        owned.Inbox |> S2.read (S2.FromSeqNum from) count

    let appendInboxText headers body (pair: StreamPair) =
        pair.Inbox |> S2.append [ S2.Record.textWith headers body ]

    let commitRecords records (owned: OwnedKey) =
        async {
            let opts =
                S2.AppendOptions.none
                |> S2.AppendOptions.fencingToken (FenceToken.value owned.Fence)

            let! result = owned.Log |> S2.tryAppendWith opts records

            return
                match result with
                | Ok ack -> Committed ack
                | Error(S2Errors.FencingTokenMismatch expected) -> Deposed expected
                | Error failure -> CommitFailed failure
        }

    let commitText encode entries owned =
        let records = entries |> List.map (encode >> S2.Record.text)
        commitRecords records owned

    let relayTextBatch decode encodeMessage destinationKey inboxOf from count (owned: OwnedKey) =
        async {
            let! records =
                owned.Log
                |> S2.readWith
                    { S2.ReadOptions.empty with
                        Start = Some(S2.FromSeqNum from)
                        Count = Some count
                        IgnoreCommandRecords = true }

            let mutable delivered = 0

            for record in records do
                match decode record.Body with
                | Ok(Outgoing message) ->
                    let dest = inboxOf (destinationKey message)

                    let headers = [ "src", S2.streamName owned.Log; "seq", string record.SeqNum ]

                    let! _ = dest |> S2.append [ S2.Record.textWith headers (encodeMessage message) ]
                    delivered <- delivered + 1
                | Ok(Incoming _) -> ()
                | Error error -> failwith error

            let nextSeqNum =
                match List.rev records with
                | last :: _ -> last.SeqNum + 1L
                | [] -> from

            return
                { NextSeqNum = nextSeqNum
                  Delivered = delivered }
        }
