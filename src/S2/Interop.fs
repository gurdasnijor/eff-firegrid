namespace Eff

open System
open Fable.Core
open Fable.Core.JsInterop

/// Thin, faithful bindings over the JS classes/objects exported by
/// `@s2-dev/streamstore`. Everything here is `internal` — consumers use the
/// ergonomic `S2` module (see Client.fs), never these raw shapes.
module internal Interop =

    type [<AllowNullLiteral>] IStreamPosition =
        abstract seqNum: float
        abstract timestamp: DateTime

    type [<AllowNullLiteral>] IAppendAck =
        abstract start: IStreamPosition
        abstract ``end``: IStreamPosition
        abstract tail: IStreamPosition

    type [<AllowNullLiteral>] IReadRecord =
        abstract seqNum: float
        abstract body: string
        abstract headers: string[][]
        abstract timestamp: DateTime

    type [<AllowNullLiteral>] IReadBatch =
        abstract records: IReadRecord[]
        abstract tail: IStreamPosition

    type [<AllowNullLiteral>] ITailResponse =
        abstract tail: IStreamPosition

    type [<AllowNullLiteral>] IStreamInfo =
        abstract name: string
        abstract createdAt: DateTime
        abstract deletedAt: DateTime

    type [<AllowNullLiteral>] IBasinInfo =
        abstract name: string
        abstract createdAt: DateTime
        abstract deletedAt: DateTime

    type [<AllowNullLiteral>] IListStreams =
        abstract streams: IStreamInfo[]
        abstract hasMore: bool

    type [<AllowNullLiteral>] IListBasins =
        abstract basins: IBasinInfo[]
        abstract hasMore: bool

    // Config shapes (SDK camelCase format). Nullable scalars typed as obj.
    type [<AllowNullLiteral>] IRetentionPolicy =
        abstract ageSecs: obj
        abstract infinite: obj

    type [<AllowNullLiteral>] IDeleteOnEmpty =
        abstract minAgeSecs: obj

    type [<AllowNullLiteral>] ITimestamping =
        abstract mode: obj
        abstract uncapped: obj

    type [<AllowNullLiteral>] IStreamConfig =
        abstract deleteOnEmpty: IDeleteOnEmpty
        abstract retentionPolicy: IRetentionPolicy
        abstract storageClass: obj
        abstract timestamping: ITimestamping

    type [<AllowNullLiteral>] IBasinConfig =
        abstract createStreamOnAppend: obj
        abstract createStreamOnRead: obj
        abstract defaultStreamConfig: IStreamConfig
        abstract streamCipher: obj

    type [<AllowNullLiteral>] IEnsureStreamResult =
        abstract result: string
        abstract stream: IStreamInfo

    type [<AllowNullLiteral>] IEnsureBasinResult =
        abstract result: string
        abstract basin: IBasinInfo

    type [<AllowNullLiteral>] IStreamsMgr =
        abstract create: args: obj -> JS.Promise<obj>
        abstract ensure: args: obj -> JS.Promise<IEnsureStreamResult>
        abstract delete: args: obj -> JS.Promise<obj>
        abstract getConfig: args: obj -> JS.Promise<IStreamConfig>
        abstract reconfigure: args: obj -> JS.Promise<IStreamConfig>
        abstract list: args: obj -> JS.Promise<IListStreams>

    type [<AllowNullLiteral>] IBasinsMgr =
        abstract create: args: obj -> JS.Promise<IBasinInfo>
        abstract ensure: args: obj -> JS.Promise<IEnsureBasinResult>
        abstract delete: args: obj -> JS.Promise<obj>
        abstract getConfig: args: obj -> JS.Promise<IBasinConfig>
        abstract reconfigure: args: obj -> JS.Promise<IBasinConfig>
        abstract list: args: obj -> JS.Promise<IListBasins>

    type [<AllowNullLiteral>] IBatchSubmitTicket =
        abstract ack: unit -> JS.Promise<IAppendAck>

    type [<AllowNullLiteral>] IAppendSession =
        abstract submit: input: obj -> JS.Promise<IBatchSubmitTicket>
        abstract close: unit -> JS.Promise<unit>
        abstract lastAckedPosition: unit -> IAppendAck

    type [<AllowNullLiteral>] IReadSession =
        abstract cancel: unit -> JS.Promise<unit>

    type [<AllowNullLiteral>] IStream =
        abstract name: string
        abstract append: input: obj -> JS.Promise<IAppendAck>
        abstract read: input: obj -> JS.Promise<IReadBatch>
        abstract checkTail: unit -> JS.Promise<ITailResponse>
        abstract appendSession: opts: obj -> JS.Promise<IAppendSession>
        abstract readSession: input: obj * options: obj -> JS.Promise<IReadSession>

    type [<AllowNullLiteral>] IBasin =
        abstract name: string
        abstract streams: IStreamsMgr
        abstract stream: name: string -> IStream

    type [<AllowNullLiteral>] IS2 =
        abstract basins: IBasinsMgr
        abstract basin: name: string -> IBasin

    // Imported JS values.
    let s2Ctor: obj = import "S2" "@s2-dev/streamstore"
    let appendRecordNs: obj = import "AppendRecord" "@s2-dev/streamstore"
    let appendInputNs: obj = import "AppendInput" "@s2-dev/streamstore"

    [<Emit("new $0($1)")>]
    let newS2 (ctor: obj) (opts: obj) : IS2 = jsNative

    [<Emit("$0.string($1)")>]
    let recString (ns: obj) (p: obj) : obj = jsNative

    [<Emit("$0.bytes($1)")>]
    let recBytes (ns: obj) (p: obj) : obj = jsNative

    [<Emit("$0.fence($1)")>]
    let recFence (ns: obj) (token: string) : obj = jsNative

    [<Emit("$0.trim($1)")>]
    let recTrim (ns: obj) (seqNum: float) : obj = jsNative

    // AppendInput.create(records, options) — options carries matchSeqNum / fencingToken.
    [<Emit("$0.create($1, $2)")>]
    let inputCreate (ns: obj) (records: obj) (opts: obj) : obj = jsNative

    /// JS `== null` — true for both `null` and `undefined`.
    [<Emit("$0 == null")>]
    let isNil (x: obj) : bool = jsNative

    // Async-iterator protocol, for consuming read sessions.
    [<Emit("$0[Symbol.asyncIterator]()")>]
    let asyncIterator (x: obj) : obj = jsNative

    [<Emit("$0.next()")>]
    let iterNext (it: obj) : JS.Promise<obj> = jsNative

    [<Emit("$0.done")>]
    let iterDone (r: obj) : bool = jsNative

    [<Emit("$0.value")>]
    let iterValue (r: obj) : obj = jsNative

    // Stop an async iterator early: releases the stream lock and cancels it.
    [<Emit("($0.return ? $0.return() : Promise.resolve({}))")>]
    let iterReturn (it: obj) : JS.Promise<obj> = jsNative
