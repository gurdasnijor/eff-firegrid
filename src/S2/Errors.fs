namespace Eff

open Fable.Core
open Fable.Core.JsInterop

/// Typed S2 error classification.
///
/// The base SDK throws JS error classes for conditional-append and read-range
/// failures. `classify` turns a caught exception into an F# union so callers can
/// branch on it (e.g. recover the expected seq num after a `matchSeqNum` miss).
module S2Errors =

    [<Import("SeqNumMismatchError", "@s2-dev/streamstore")>]
    let private seqNumMismatchError: obj = jsNative

    [<Import("FencingTokenMismatchError", "@s2-dev/streamstore")>]
    let private fencingTokenMismatchError: obj = jsNative

    [<Import("RangeNotSatisfiableError", "@s2-dev/streamstore")>]
    let private rangeNotSatisfiableError: obj = jsNative

    [<Emit("$1 instanceof $0")>]
    let private isInstance (_cls: obj) (_e: obj) : bool = jsNative

    [<Emit("$0 == null")>]
    let private isNil (_x: obj) : bool = jsNative

    [<Emit("(typeof $0 === 'number' && Number.isFinite($0))")>]
    let private isFiniteNum (_x: obj) : bool = jsNative

    // Best-effort seq num from an error's `tail` (the field may be camelCase or wire snake_case).
    let private tailSeqNum (tail: obj) : int64 option =
        if isNil tail then
            None
        else
            let sn = tail?seqNum

            if isFiniteNum sn then
                Some(int64 (unbox<float> sn))
            else
                let sn2 = tail?seq_num

                if isFiniteNum sn2 then
                    Some(int64 (unbox<float> sn2))
                else
                    None

    /// A classified S2 failure.
    type S2Failure =
        /// Conditional append failed: the stream tail seq num did not match.
        /// Carries the actual expected next seq num.
        | SeqNumMismatch of expected: int64
        /// Conditional append failed: the stream fencing token did not match.
        | FencingTokenMismatch of expected: string
        /// Read start position was out of range; carries the current tail seq num if known.
        | RangeNotSatisfiable of tailSeqNum: int64 option
        /// Any other error (network, validation, server, …).
        | Other of message: string

    /// Classify a caught exception into an `S2Failure`.
    let classify (e: exn) : S2Failure =
        let o = box e

        if isInstance seqNumMismatchError o then
            SeqNumMismatch(int64 (unbox<float> (o?expectedSeqNum)))
        elif isInstance fencingTokenMismatchError o then
            FencingTokenMismatch(unbox<string> (o?expectedFencingToken))
        elif isInstance rangeNotSatisfiableError o then
            RangeNotSatisfiable(tailSeqNum (o?tail))
        else
            Other e.Message
