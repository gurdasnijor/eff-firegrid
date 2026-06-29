namespace Eff.Foundation.Durable

open Eff

type DispatchCommand =
    { SourceSeqNum: int64
      Command: StepCommand }

type DispatchBatch =
    private
        { FromSeqNum: int64
          NextSeqNum: int64
          Scanned: int
          Commands: DispatchCommand list }

[<RequireQualifiedAccess>]
type CommandDispatchFailure =
    | LogReadFailed of string
    | DecodeFailed of seqNum: int64 * error: string
    | CheckpointCommitFailed of S2Errors.S2Failure

[<RequireQualifiedAccess>]
type CommandDispatchCheckpointResult =
    | Checkpointed of S2.AppendAck
    | NotRequired
    | Deposed of expectedFence: string
    | Failed of CommandDispatchFailure

[<RequireQualifiedAccess>]
module DispatchBatch =
    let fromSeqNum batch = batch.FromSeqNum

    let nextSeqNum batch = batch.NextSeqNum

    let scanned batch = batch.Scanned

    let commands batch = batch.Commands

[<RequireQualifiedAccess>]
module DurableCommandDispatch =
    let private dispatchCursor decoded =
        decoded
        |> List.choose (function
            | seqNum, Incoming(CommandDispatchCheckpoint nextSeqNum) -> Some(max nextSeqNum (seqNum + 1L))
            | _ -> None)
        |> List.fold max 0L

    let private decodeLog decoded =
        let rec loop records =
            function
            | [] -> Ok(List.rev records)
            | (seqNum, Ok record) :: rest -> loop ((seqNum, record) :: records) rest
            | (seqNum, Error error) :: _ -> Error(CommandDispatchFailure.DecodeFailed(seqNum, error))

        loop [] decoded

    let selectFromDecoded maxRecords decoded =
        if maxRecords <= 0 then
            invalidArg (nameof maxRecords) "maxRecords must be positive"

        let fromSeqNum = dispatchCursor decoded

        let scanned =
            decoded
            |> List.filter (fun (seqNum, _) -> seqNum >= fromSeqNum)
            |> List.truncate maxRecords

        let commands =
            scanned
            |> List.choose (function
                | seqNum, Outgoing(Command command) ->
                    Some
                        { SourceSeqNum = seqNum
                          Command = command }
                | _ -> None)

        let nextSeqNum =
            match List.rev scanned with
            | (seqNum, _) :: _ -> seqNum + 1L
            | [] -> fromSeqNum

        { FromSeqNum = fromSeqNum
          NextSeqNum = nextSeqNum
          Scanned = List.length scanned
          Commands = commands }

    let trySelect maxRecords decoded =
        decodeLog decoded |> Result.map (selectFromDecoded maxRecords)

    let readPending decode maxRecords owned =
        async {
            try
                let! decoded = S2Substrate.readLogText decode owned
                return trySelect maxRecords decoded
            with error ->
                return Error(CommandDispatchFailure.LogReadFailed error.Message)
        }

    let checkpoint encode owned batch =
        async {
            if batch.NextSeqNum <= batch.FromSeqNum then
                return CommandDispatchCheckpointResult.NotRequired
            else
                let checkpoint = Incoming(CommandDispatchCheckpoint batch.NextSeqNum)
                let! result = S2Substrate.commitText encode [ checkpoint ] owned

                return
                    match result with
                    | Committed ack -> CommandDispatchCheckpointResult.Checkpointed ack
                    | Deposed expected -> CommandDispatchCheckpointResult.Deposed expected
                    | CommitFailed failure ->
                        CommandDispatchCheckpointResult.Failed(CommandDispatchFailure.CheckpointCommitFailed failure)
        }
