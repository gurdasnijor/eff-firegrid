namespace Eff.Foundation.Durable

open Eff

[<RequireQualifiedAccess>]
type DurableHostFailure =
    | LogReadFailed of string
    | DecodeFailed of seqNum: int64 * error: string
    | CommitFailed of S2Errors.S2Failure
    | UnexpectedNoCommit
    | Unexpected of string

[<RequireQualifiedAccess>]
type DurableHostStatus<'a> =
    | Completed of 'a
    | Committed of S2.AppendAck
    | Waiting of OpId * Need
    | Deposed of expectedFence: string
    | Failed of DurableHostFailure

[<RequireQualifiedAccess>]
module DurableHost =
    let private decodeLog decoded =
        let rec loop records =
            function
            | [] -> Ok(List.rev records)
            | (_, Ok record) :: rest -> loop (record :: records) rest
            | (seqNum, Error error) :: _ -> Error(DurableHostFailure.DecodeFailed(seqNum, error))

        loop [] decoded

    let private readLog decode owned =
        async {
            try
                let! decoded = S2Substrate.readLogText decode owned
                return Ok decoded
            with error ->
                return Error(DurableHostFailure.LogReadFailed error.Message)
        }

    let stepOnce encode decode timestamp owned program =
        async {
            let! log = readLog decode owned

            match log with
            | Error failure -> return DurableHostStatus.Failed failure
            | Ok decoded ->
                try
                    match decodeLog decoded with
                    | Error failure -> return DurableHostStatus.Failed failure
                    | Ok records ->
                        let history = DurableStepper.historyFromRecords records

                        match DurableStepper.plan timestamp history program with
                        | Complete value -> return DurableHostStatus.Completed value
                        | Waiting(opId, need) -> return DurableHostStatus.Waiting(opId, need)
                        | Commit _ as plan ->
                            let! commit = DurableStepper.commit encode owned plan

                            return
                                match commit with
                                | StepCommitted ack -> DurableHostStatus.Committed ack
                                | StepDeposed expected -> DurableHostStatus.Deposed expected
                                | StepCommitFailed failure ->
                                    DurableHostStatus.Failed(DurableHostFailure.CommitFailed failure)
                                | StepNotRequired -> DurableHostStatus.Failed DurableHostFailure.UnexpectedNoCommit
                with error ->
                    return DurableHostStatus.Failed(DurableHostFailure.Unexpected error.Message)
        }

    let runOnce encode decode timestamp owned program =
        stepOnce encode decode timestamp owned program
