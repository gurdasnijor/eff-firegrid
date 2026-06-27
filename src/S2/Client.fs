namespace Eff

open System
open Fable.Core
open Fable.Core.JsInterop
open Eff.Interop

/// Ergonomic, F#-native S2 API.
///
///   let s2     = S2.connect token
///   let basin  = s2 |> S2.basin "my-basin"
///   do! basin  |> S2.ensureStream "events"
///   let stream = basin |> S2.stream "events"
///   let! ack   = stream |> S2.appendStrings [ "hello"; "world" ]
///   let! recs  = stream |> S2.read (S2.FromSeqNum ack.Start.SeqNum) 100
module S2 =

    // ---- Domain types (clean F# records / DUs) ----

    /// Position of a record in a stream.
    type StreamPosition = { SeqNum: int64; Timestamp: DateTime }

    /// Acknowledged range plus stream tail after an append.
    type AppendAck =
        { Start: StreamPosition
          End: StreamPosition
          Tail: StreamPosition }

    /// A record read back from a stream.
    type ReadRecord =
        { SeqNum: int64
          Body: string
          Headers: (string * string) list
          Timestamp: DateTime }

    type StreamInfo =
        { Name: string
          CreatedAt: DateTime
          DeletedAt: DateTime option }

    type BasinInfo =
        { Name: string
          CreatedAt: DateTime
          DeletedAt: DateTime option }

    /// Where to start a read.
    type ReadFrom =
        | FromSeqNum of int64
        | FromTimestamp of DateTime
        | FromTailOffset of int64

    // ---- Configuration types ----

    type StorageClass =
        | Standard
        | Express

    type TimestampingMode =
        | ClientPrefer
        | ClientRequire
        | Arrival

    type EncryptionAlgorithm =
        | Aegis256
        | Aes256Gcm

    /// Retention policy for automatic trimming.
    type RetentionPolicy =
        | RetainForSecs of int
        | RetainForever

    type Timestamping =
        { Mode: TimestampingMode option
          Uncapped: bool option }

    /// Stream configuration. Build from `StreamConfig.empty` and set fields.
    type StreamConfig =
        { DeleteOnEmptyMinAgeSecs: int option
          RetentionPolicy: RetentionPolicy option
          StorageClass: StorageClass option
          Timestamping: Timestamping option }

    /// Basin configuration. Build from `BasinConfig.empty` and set fields.
    type BasinConfig =
        { CreateStreamOnAppend: bool option
          CreateStreamOnRead: bool option
          DefaultStreamConfig: StreamConfig option
          StreamCipher: EncryptionAlgorithm option }

    /// Outcome of an idempotent `ensure`.
    type ProvisionResult =
        | Created
        | Updated
        | Noop

    // ---- Connect options (Phase 5) ----

    type Compression =
        | NoCompression
        | Gzip
        | Zstd

    type Retry =
        { MaxAttempts: int option
          MinBaseDelayMillis: int option
          MaxBaseDelayMillis: int option }

    /// Options for `connectWith`. Build from `ConnectOptions.create token`.
    type ConnectOptions =
        { AccessToken: string
          AccountEndpoint: string option
          BasinEndpoint: string option
          RequestTimeoutMillis: int option
          ConnectionTimeoutMillis: int option
          Retry: Retry option
          Compression: Compression option }

    // ---- Managers (Phase 3) ----

    type LocationInfo = { Name: string; IsPrivate: bool }

    /// A metric in a metric set response.
    type Metric =
        | Scalar of name: string * value: float
        | Series of name: string * points: (DateTime * float) list
        | Labels of name: string * values: string list

    type MetricSet = Metric list

    type AccountMetricSet =
        | ActiveBasins
        | AccountOps

    type BasinMetricSet =
        | BasinStorage
        | AppendOps
        | ReadOps
        | ReadThroughput
        | AppendThroughput
        | BasinOps

    type TimeseriesInterval =
        | Minute
        | Hour
        | Day

    type TokenInfo = { Id: string; AutoPrefixStreams: bool }

    /// A resource-set matcher for an access-token scope.
    type ResourceMatch =
        | Exact of string
        | Prefix of string

    /// An access-token scope (subset; basins/streams resource sets + op names).
    type TokenScope =
        { Basins: ResourceMatch option
          Streams: ResourceMatch option
          Ops: string list }

    /// A record to append. Use the `Record` module to construct.
    /// NOTE: a single append batch must be one format — `text`/`fence` are
    /// string-format; `bytes`/`trim` are bytes-format. Don't mix the two.
    type Record =
        internal
        | RText of body: string * headers: (string * string) list
        | RBytes of body: byte[] * headers: (byte[] * byte[]) list
        | RFence of token: string
        | RTrim of seqNum: int64

    /// Conditions applied to an append (conditional / single-writer primitives).
    type AppendOptions =
        {
            /// Require the stream tail to equal this seq num, else `SeqNumMismatch`.
            MatchSeqNum: int64 option
            /// Require the stream's fencing token to equal this, else `FencingTokenMismatch`.
            FencingToken: string option
        }

    /// Full read configuration. Use `ReadOptions.empty` and override fields.
    type ReadOptions =
        {
            Start: ReadFrom option
            /// Start from the tail if the requested position is beyond it.
            Clamp: bool
            /// Stop after this many records.
            Count: int option
            /// Stop after this many metered bytes.
            Bytes: int option
            /// Stop at this timestamp (exclusive).
            UntilTimestamp: DateTime option
            /// Wait up to this many seconds for new records before stopping.
            WaitSecs: int option
            /// Filter command records (fence/trim) out of the result.
            IgnoreCommandRecords: bool
        }

    // ---- Opaque client handles (single-field wrappers over JS objects) ----

    type Client = internal { Raw: IS2 }
    type Basin = internal { Raw: IBasin }
    type Stream = internal { Raw: IStream }

    // ---- Record constructors ----

    [<RequireQualifiedAccess>]
    module Record =
        /// A text record.
        let text (body: string) : Record = RText(body, [])
        /// A text record with string headers.
        let textWith (headers: (string * string) list) (body: string) : Record = RText(body, headers)
        /// A binary record.
        let bytes (body: byte[]) : Record = RBytes(body, [])
        /// A binary record with binary headers.
        let bytesWith (headers: (byte[] * byte[]) list) (body: byte[]) : Record = RBytes(body, headers)
        /// A fence command record — sets/requires the stream's fencing token (single-writer).
        let fence (token: string) : Record = RFence token
        /// A trim command record — requests deletion of records before `seqNum`.
        let trim (seqNum: int64) : Record = RTrim seqNum

    [<RequireQualifiedAccess>]
    module AppendOptions =
        /// No conditions.
        let none =
            { MatchSeqNum = None
              FencingToken = None }

        /// Require the stream tail to equal `seqNum`.
        let matchSeqNum (seqNum: int64) (o: AppendOptions) = { o with MatchSeqNum = Some seqNum }
        /// Require the stream fencing token to equal `token`.
        let fencingToken (token: string) (o: AppendOptions) = { o with FencingToken = Some token }

    [<RequireQualifiedAccess>]
    module ReadOptions =
        let empty =
            { Start = None
              Clamp = false
              Count = None
              Bytes = None
              UntilTimestamp = None
              WaitSecs = None
              IgnoreCommandRecords = false }

    [<RequireQualifiedAccess>]
    module StreamConfig =
        let empty =
            { DeleteOnEmptyMinAgeSecs = None
              RetentionPolicy = None
              StorageClass = None
              Timestamping = None }

    [<RequireQualifiedAccess>]
    module BasinConfig =
        let empty =
            { CreateStreamOnAppend = None
              CreateStreamOnRead = None
              DefaultStreamConfig = None
              StreamCipher = None }

    [<RequireQualifiedAccess>]
    module Retry =
        let empty =
            { MaxAttempts = None
              MinBaseDelayMillis = None
              MaxBaseDelayMillis = None }

    [<RequireQualifiedAccess>]
    module ConnectOptions =
        /// Default options for an access token (AWS environment).
        let create (token: string) =
            { AccessToken = token
              AccountEndpoint = None
              BasinEndpoint = None
              RequestTimeoutMillis = None
              ConnectionTimeoutMillis = None
              Retry = None
              Compression = None }

    [<RequireQualifiedAccess>]
    module TokenScope =
        let empty =
            { Basins = None
              Streams = None
              Ops = [] }

    // ---- Internal mapping helpers ----

    let private optDate (d: DateTime) : DateTime option = if isNil (box d) then None else Some d

    let private toPos (p: IStreamPosition) : StreamPosition =
        { SeqNum = int64 p.seqNum
          Timestamp = p.timestamp }

    let private toAck (a: IAppendAck) : AppendAck =
        { Start = toPos a.start
          End = toPos a.``end``
          Tail = toPos a.tail }

    let private toRecord (r: IReadRecord) : ReadRecord =
        { SeqNum = int64 r.seqNum
          Body = r.body
          Headers = [ for h in r.headers -> (h.[0], h.[1]) ]
          Timestamp = r.timestamp }

    let private mkRecord (r: Record) : obj =
        match r with
        | RText(body, headers) ->
            recString
                appendRecordNs
                (box
                    {| body = body
                       headers = List.toArray headers |})
        | RBytes(body, headers) ->
            recBytes
                appendRecordNs
                (box
                    {| body = body
                       headers = List.toArray headers |})
        | RFence token -> recFence appendRecordNs token
        | RTrim seqNum -> recTrim appendRecordNs (float seqNum)

    // `internal` (not `private`) so benchmarks/tests can exercise the hot build path.
    let internal mkInput (records: Record list) (opts: AppendOptions) : obj =
        let recs = records |> List.map mkRecord |> List.toArray

        let optsObj =
            createObj
                [ if opts.MatchSeqNum.IsSome then
                      "matchSeqNum" ==> float opts.MatchSeqNum.Value
                  if opts.FencingToken.IsSome then
                      "fencingToken" ==> opts.FencingToken.Value ]

        inputCreate appendInputNs (box recs) optsObj

    let private fromObj (from: ReadFrom) : obj =
        match from with
        | FromSeqNum n -> box {| seqNum = float n |}
        | FromTimestamp t -> box {| timestamp = t |}
        | FromTailOffset n -> box {| tailOffset = float n |}

    let internal readInput (o: ReadOptions) : obj =
        let startObj =
            createObj
                [ match o.Start with
                  | Some f -> "from" ==> fromObj f
                  | None -> ()
                  if o.Clamp then
                      "clamp" ==> true ]

        let stopObj =
            createObj
                [ if o.Count.IsSome || o.Bytes.IsSome then
                      "limits"
                      ==> createObj
                              [ if o.Count.IsSome then
                                    "count" ==> o.Count.Value
                                if o.Bytes.IsSome then
                                    "bytes" ==> o.Bytes.Value ]
                  if o.UntilTimestamp.IsSome then
                      "untilTimestamp" ==> o.UntilTimestamp.Value
                  if o.WaitSecs.IsSome then
                      "waitSecs" ==> o.WaitSecs.Value ]

        createObj
            [ "start" ==> startObj
              "stop" ==> stopObj
              if o.IgnoreCommandRecords then
                  "ignoreCommandRecords" ==> true ]

    // ---- Config <-> JS mapping (SDK camelCase format; enum values are wire strings) ----

    let private storageClassStr =
        function
        | Standard -> "standard"
        | Express -> "express"

    let private storageClassOf =
        function
        | "standard" -> Some Standard
        | "express" -> Some Express
        | _ -> None

    let private tsModeStr =
        function
        | ClientPrefer -> "client-prefer"
        | ClientRequire -> "client-require"
        | Arrival -> "arrival"

    let private tsModeOf =
        function
        | "client-prefer" -> Some ClientPrefer
        | "client-require" -> Some ClientRequire
        | "arrival" -> Some Arrival
        | _ -> None

    let private encAlgoStr =
        function
        | Aegis256 -> "aegis-256"
        | Aes256Gcm -> "aes-256-gcm"

    let private encAlgoOf =
        function
        | "aegis-256" -> Some Aegis256
        | "aes-256-gcm" -> Some Aes256Gcm
        | _ -> None

    let private streamConfigObj (c: StreamConfig) : obj =
        createObj
            [ match c.DeleteOnEmptyMinAgeSecs with
              | Some n -> "deleteOnEmpty" ==> createObj [ "minAgeSecs" ==> n ]
              | None -> ()
              match c.RetentionPolicy with
              | Some(RetainForSecs s) -> "retentionPolicy" ==> createObj [ "ageSecs" ==> s ]
              | Some RetainForever -> "retentionPolicy" ==> createObj [ "infinite" ==> createObj [] ]
              | None -> ()
              match c.StorageClass with
              | Some sc -> "storageClass" ==> storageClassStr sc
              | None -> ()
              match c.Timestamping with
              | Some ts ->
                  "timestamping"
                  ==> createObj
                          [ match ts.Mode with
                            | Some m -> "mode" ==> tsModeStr m
                            | None -> ()
                            match ts.Uncapped with
                            | Some u -> "uncapped" ==> u
                            | None -> () ]
              | None -> () ]

    let private basinConfigObj (c: BasinConfig) : obj =
        createObj
            [ match c.CreateStreamOnAppend with
              | Some b -> "createStreamOnAppend" ==> b
              | None -> ()
              match c.CreateStreamOnRead with
              | Some b -> "createStreamOnRead" ==> b
              | None -> ()
              match c.DefaultStreamConfig with
              | Some sc -> "defaultStreamConfig" ==> streamConfigObj sc
              | None -> ()
              match c.StreamCipher with
              | Some e -> "streamCipher" ==> encAlgoStr e
              | None -> () ]

    let private parseStreamConfig (c: IStreamConfig) : StreamConfig =
        { DeleteOnEmptyMinAgeSecs =
            if isNil (box c.deleteOnEmpty) || isNil c.deleteOnEmpty.minAgeSecs then
                None
            else
                Some(int (unbox<float> c.deleteOnEmpty.minAgeSecs))
          RetentionPolicy =
            if isNil (box c.retentionPolicy) then
                None
            elif not (isNil c.retentionPolicy.ageSecs) then
                Some(RetainForSecs(int (unbox<float> c.retentionPolicy.ageSecs)))
            elif not (isNil c.retentionPolicy.infinite) then
                Some RetainForever
            else
                None
          StorageClass =
            (if isNil c.storageClass then
                 None
             else
                 storageClassOf (unbox<string> c.storageClass))
          Timestamping =
            if isNil (box c.timestamping) then
                None
            else
                Some
                    { Mode =
                        (if isNil c.timestamping.mode then
                             None
                         else
                             tsModeOf (unbox<string> c.timestamping.mode))
                      Uncapped =
                        (if isNil c.timestamping.uncapped then
                             None
                         else
                             Some(unbox<bool> c.timestamping.uncapped)) } }

    let private parseBasinConfig (c: IBasinConfig) : BasinConfig =
        { CreateStreamOnAppend =
            (if isNil c.createStreamOnAppend then
                 None
             else
                 Some(unbox<bool> c.createStreamOnAppend))
          CreateStreamOnRead =
            (if isNil c.createStreamOnRead then
                 None
             else
                 Some(unbox<bool> c.createStreamOnRead))
          DefaultStreamConfig =
            (if isNil (box c.defaultStreamConfig) then
                 None
             else
                 Some(parseStreamConfig c.defaultStreamConfig))
          StreamCipher =
            (if isNil c.streamCipher then
                 None
             else
                 encAlgoOf (unbox<string> c.streamCipher)) }

    let private provisionOf =
        function
        | "created" -> Created
        | "updated" -> Updated
        | _ -> Noop

    let private compressionStr =
        function
        | NoCompression -> "none"
        | Gzip -> "gzip"
        | Zstd -> "zstd"

    let private accountMetricSetStr =
        function
        | ActiveBasins -> "active-basins"
        | AccountOps -> "account-ops"

    let private basinMetricSetStr =
        function
        | BasinStorage -> "storage"
        | AppendOps -> "append-ops"
        | ReadOps -> "read-ops"
        | ReadThroughput -> "read-throughput"
        | AppendThroughput -> "append-throughput"
        | BasinOps -> "basin-ops"

    let private parseMetric (m: obj) : Metric =
        let series (node: obj) =
            [ for p in unbox<obj[]> (node?values) ->
                  let arr = unbox<obj[]> p
                  (unbox<DateTime> arr.[0], unbox<float> arr.[1]) ]

        if not (isNil (m?scalar)) then
            Scalar(unbox<string> (m?scalar?name), unbox<float> (m?scalar?value))
        elif not (isNil (m?label)) then
            Labels(unbox<string> (m?label?name), List.ofArray (unbox<string[]> (m?label?values)))
        elif not (isNil (m?accumulation)) then
            Series(unbox<string> (m?accumulation?name), series (m?accumulation))
        elif not (isNil (m?gauge)) then
            Series(unbox<string> (m?gauge?name), series (m?gauge))
        else
            Labels("unknown", [])

    let private resourceObj =
        function
        | Exact s -> createObj [ "exact" ==> s ]
        | Prefix p -> createObj [ "prefix" ==> p ]

    let private scopeObj (sc: TokenScope) : obj =
        createObj
            [ match sc.Basins with
              | Some r -> "basins" ==> resourceObj r
              | None -> ()
              match sc.Streams with
              | Some r -> "streams" ==> resourceObj r
              | None -> ()
              if not (List.isEmpty sc.Ops) then
                  "ops" ==> List.toArray sc.Ops ]

    // ---- Connect ----

    /// Create a client against the default AWS environment (`aws.s2.dev`).
    let connect (accessToken: string) : Client =
        { Raw = newS2 s2Ctor (box {| accessToken = accessToken |}) }

    /// Create a client with explicit options (endpoints, retry, compression, timeouts).
    let connectWith (o: ConnectOptions) : Client =
        let opts =
            createObj
                [ "accessToken" ==> o.AccessToken
                  if o.AccountEndpoint.IsSome || o.BasinEndpoint.IsSome then
                      "endpoints"
                      ==> createObj
                              [ match o.AccountEndpoint with
                                | Some a -> "account" ==> a
                                | None -> ()
                                match o.BasinEndpoint with
                                | Some b -> "basin" ==> b
                                | None -> () ]
                  match o.RequestTimeoutMillis with
                  | Some t -> "requestTimeoutMillis" ==> t
                  | None -> ()
                  match o.ConnectionTimeoutMillis with
                  | Some t -> "connectionTimeoutMillis" ==> t
                  | None -> ()
                  match o.Retry with
                  | Some r ->
                      "retry"
                      ==> createObj
                              [ match r.MaxAttempts with
                                | Some n -> "maxAttempts" ==> n
                                | None -> ()
                                match r.MinBaseDelayMillis with
                                | Some n -> "minBaseDelayMillis" ==> n
                                | None -> ()
                                match r.MaxBaseDelayMillis with
                                | Some n -> "maxBaseDelayMillis" ==> n
                                | None -> () ]
                  | None -> ()
                  match o.Compression with
                  | Some c -> "compression" ==> compressionStr c
                  | None -> () ]

        { Raw = newS2 s2Ctor opts }

    // ---- Account / basin management ----

    /// List basins on the account.
    let listBasins (client: Client) : Async<BasinInfo list> =
        async {
            let! r = Async.AwaitPromise(client.Raw.basins.list (createObj []))

            return
                [ for b in r.basins ->
                      { Name = b.name
                        CreatedAt = b.createdAt
                        DeletedAt = optDate b.deletedAt } ]
        }

    /// Create a basin (globally unique name, 8–48 chars).
    let createBasin (name: string) (client: Client) : Async<BasinInfo> =
        async {
            let! b = Async.AwaitPromise(client.Raw.basins.create (box {| basin = name |}))

            return
                { Name = b.name
                  CreatedAt = b.createdAt
                  DeletedAt = optDate b.deletedAt }
        }

    /// Delete a basin.
    let deleteBasin (name: string) (client: Client) : Async<unit> =
        async {
            let! _ = Async.AwaitPromise(client.Raw.basins.delete (box {| basin = name |}))
            return ()
        }

    /// Get a basin-scoped client.
    let basin (name: string) (client: Client) : Basin = { Raw = client.Raw.basin (name) }

    let basinName (b: Basin) : string = b.Raw.name

    /// List basins whose names start with `prefix`.
    let listBasinsWith (prefix: string) (client: Client) : Async<BasinInfo list> =
        async {
            let! r = Async.AwaitPromise(client.Raw.basins.list (box {| prefix = prefix |}))

            return
                [ for b in r.basins ->
                      { Name = b.name
                        CreatedAt = b.createdAt
                        DeletedAt = optDate b.deletedAt } ]
        }

    /// Create a basin with an explicit configuration.
    let createBasinWith (config: BasinConfig) (name: string) (client: Client) : Async<BasinInfo> =
        async {
            let! b =
                Async.AwaitPromise(
                    client.Raw.basins.create (
                        box
                            {| basin = name
                               config = basinConfigObj config |}
                    )
                )

            return
                { Name = b.name
                  CreatedAt = b.createdAt
                  DeletedAt = optDate b.deletedAt }
        }

    /// Get a basin's configuration.
    let getBasinConfig (name: string) (client: Client) : Async<BasinConfig> =
        async {
            let! c = Async.AwaitPromise(client.Raw.basins.getConfig (box {| basin = name |}))
            return parseBasinConfig c
        }

    /// Reconfigure a basin; returns the resulting configuration.
    let reconfigureBasin (config: BasinConfig) (name: string) (client: Client) : Async<BasinConfig> =
        async {
            let args = basinConfigObj config
            args?basin <- name
            let! c = Async.AwaitPromise(client.Raw.basins.reconfigure args)
            return parseBasinConfig c
        }

    // ---- Stream management (within a basin) ----

    /// List streams in the basin.
    let listStreams (b: Basin) : Async<StreamInfo list> =
        async {
            let! r = Async.AwaitPromise(b.Raw.streams.list (createObj []))

            return
                [ for s in r.streams ->
                      { Name = s.name
                        CreatedAt = s.createdAt
                        DeletedAt = optDate s.deletedAt } ]
        }

    /// Create a stream with the basin's default configuration.
    /// Throws if the stream already exists — use `ensureStream` to be idempotent.
    let createStream (name: string) (b: Basin) : Async<unit> =
        async {
            let! _ = Async.AwaitPromise(b.Raw.streams.create (box {| stream = name |}))
            return ()
        }

    /// Idempotently ensure a stream exists (safe to call repeatedly).
    let ensureStream (name: string) (b: Basin) : Async<unit> =
        async {
            let! _ = Async.AwaitPromise(b.Raw.streams.ensure (box {| stream = name |}))
            return ()
        }

    /// Delete a stream.
    let deleteStream (name: string) (b: Basin) : Async<unit> =
        async {
            let! _ = Async.AwaitPromise(b.Raw.streams.delete (box {| stream = name |}))
            return ()
        }

    /// Get a stream-scoped client.
    let stream (name: string) (b: Basin) : Stream = { Raw = b.Raw.stream (name) }

    let streamName (s: Stream) : string = s.Raw.name

    /// List streams whose names start with `prefix`.
    let listStreamsWith (prefix: string) (b: Basin) : Async<StreamInfo list> =
        async {
            let! r = Async.AwaitPromise(b.Raw.streams.list (box {| prefix = prefix |}))

            return
                [ for s in r.streams ->
                      { Name = s.name
                        CreatedAt = s.createdAt
                        DeletedAt = optDate s.deletedAt } ]
        }

    /// Create a stream with an explicit configuration.
    let createStreamWith (config: StreamConfig) (name: string) (b: Basin) : Async<unit> =
        async {
            let! _ =
                Async.AwaitPromise(
                    b.Raw.streams.create (
                        box
                            {| stream = name
                               config = streamConfigObj config |}
                    )
                )

            return ()
        }

    /// Idempotently ensure a stream with a configuration; returns the provisioning result.
    let ensureStreamWith (config: StreamConfig) (name: string) (b: Basin) : Async<ProvisionResult> =
        async {
            let! r =
                Async.AwaitPromise(
                    b.Raw.streams.ensure (
                        box
                            {| stream = name
                               config = streamConfigObj config |}
                    )
                )

            return provisionOf r.result
        }

    /// Get a stream's configuration.
    let getStreamConfig (name: string) (b: Basin) : Async<StreamConfig> =
        async {
            let! c = Async.AwaitPromise(b.Raw.streams.getConfig (box {| stream = name |}))
            return parseStreamConfig c
        }

    /// Reconfigure a stream; returns the resulting configuration.
    let reconfigureStream (config: StreamConfig) (name: string) (b: Basin) : Async<StreamConfig> =
        async {
            let args = streamConfigObj config
            args?stream <- name
            let! c = Async.AwaitPromise(b.Raw.streams.reconfigure args)
            return parseStreamConfig c
        }

    // ---- Append ----

    /// Append records under the given conditions (`matchSeqNum` / `fencingToken`).
    let appendWith (opts: AppendOptions) (records: Record list) (s: Stream) : Async<AppendAck> =
        async {
            let! ack = Async.AwaitPromise(s.Raw.append (mkInput records opts))
            return toAck ack
        }

    /// Append records unconditionally.
    let append (records: Record list) (s: Stream) : Async<AppendAck> =
        s |> appendWith AppendOptions.none records

    /// Append plain string records.
    let appendStrings (bodies: string list) (s: Stream) : Async<AppendAck> =
        s |> append [ for b in bodies -> Record.text b ]

    /// Conditional append: only succeed if the stream tail equals `seqNum`.
    let appendIfSeqNum (seqNum: int64) (records: Record list) (s: Stream) : Async<AppendAck> =
        s |> appendWith (AppendOptions.none |> AppendOptions.matchSeqNum seqNum) records

    /// Fenced append: only succeed if the stream's fencing token equals `token`.
    let appendIfFenced (token: string) (records: Record list) (s: Stream) : Async<AppendAck> =
        s |> appendWith (AppendOptions.none |> AppendOptions.fencingToken token) records

    /// Like `appendWith`, but maps failures into a typed `S2Failure` instead of throwing.
    let tryAppendWith
        (opts: AppendOptions)
        (records: Record list)
        (s: Stream)
        : Async<Result<AppendAck, S2Errors.S2Failure>> =
        async {
            try
                let! ack = s |> appendWith opts records
                return Ok ack
            with e ->
                return Error(S2Errors.classify e)
        }

    // ---- Read ----

    /// Read with full control over start/stop/limits.
    let readWith (opts: ReadOptions) (s: Stream) : Async<ReadRecord list> =
        async {
            let! batch = Async.AwaitPromise(s.Raw.read (readInput opts))
            return [ for r in batch.records -> toRecord r ]
        }

    /// Read up to `count` records starting at `from`.
    let read (from: ReadFrom) (count: int) (s: Stream) : Async<ReadRecord list> =
        s
        |> readWith
            { ReadOptions.empty with
                Start = Some from
                Count = Some count }

    /// Get the current tail (next sequence number / timestamp) of a stream.
    let checkTail (s: Stream) : Async<StreamPosition> =
        async {
            let! r = Async.AwaitPromise(s.Raw.checkTail ())
            return toPos r.tail
        }

    /// Read the last `n` records of a stream.
    /// Robust on short/empty streams — clamps the start to the beginning, so it
    /// never raises `RangeNotSatisfiable` (returns `[]` for an empty stream).
    let readLast (n: int) (s: Stream) : Async<ReadRecord list> =
        async {
            let! tail = s |> checkTail
            let start = max 0L (tail.SeqNum - int64 n)
            return! s |> read (FromSeqNum start) n
        }

    // ---- Append sessions (pipelined, backpressured appends) ----

    /// A pipelined append session: submit batches (with backpressure), then await
    /// durability via the returned ticket. Use for high-throughput sequential appends.
    type AppendSession = internal { Raw: IAppendSession }

    /// Handle to a submitted batch; `ack` resolves once it is durable.
    type Ticket = internal { Raw: IBatchSubmitTicket }

    /// Open an append session on a stream.
    let appendSession (s: Stream) : Async<AppendSession> =
        async {
            let! raw = Async.AwaitPromise(s.Raw.appendSession (createObj []))
            return { AppendSession.Raw = raw }
        }

    /// Submit a batch; returns once enqueued (applies backpressure). `ack` the ticket for durability.
    let submit (records: Record list) (sess: AppendSession) : Async<Ticket> =
        async {
            let! t = Async.AwaitPromise(sess.Raw.submit (mkInput records AppendOptions.none))
            return { Ticket.Raw = t }
        }

    /// Await durability of a submitted batch.
    let ack (t: Ticket) : Async<AppendAck> =
        async {
            let! a = Async.AwaitPromise(t.Raw.ack ())
            return toAck a
        }

    /// Submit a batch and wait for it to become durable.
    let submitAck (records: Record list) (sess: AppendSession) : Async<AppendAck> =
        async {
            let! t = sess |> submit records
            return! ack t
        }

    /// The last acknowledged position, if any.
    let lastAcked (sess: AppendSession) : AppendAck option =
        let p = sess.Raw.lastAckedPosition ()
        if isNil (box p) then None else Some(toAck p)

    /// Flush and close an append session.
    let closeAppendSession (sess: AppendSession) : Async<unit> =
        async { do! Async.AwaitPromise(sess.Raw.close ()) }

    // ---- Read sessions (streaming reads) ----

    /// A streaming read session. Consume with `iter`.
    type ReadSession = internal { Raw: IReadSession }

    /// A pull-based read cursor over a streaming read session.
    type ReadCursor =
        internal
            { Raw: IReadSession
              Iterator: obj }

    /// Open a streaming read session over the given range.
    let readSession (opts: ReadOptions) (s: Stream) : Async<ReadSession> =
        async {
            let! raw = Async.AwaitPromise(s.Raw.readSession (readInput opts, createObj []))
            return { ReadSession.Raw = raw }
        }

    /// Open a pull-based read cursor over the given range.
    let readCursor (opts: ReadOptions) (s: Stream) : Async<ReadCursor> =
        async {
            let! raw = Async.AwaitPromise(s.Raw.readSession (readInput opts, createObj []))

            return
                { Raw = raw
                  Iterator = asyncIterator (box raw) }
        }

    /// Pull the next record from a read cursor, leaving the session open.
    let tryNext (cursor: ReadCursor) : Async<ReadRecord option> =
        async {
            let! res = Async.AwaitPromise(iterNext cursor.Iterator)

            if iterDone res then
                return None
            else
                return Some(toRecord (unbox<IReadRecord> (iterValue res)))
        }

    /// Cancel/close a read cursor.
    let closeReadCursor (cursor: ReadCursor) : Async<unit> =
        async {
            let! _ = Async.AwaitPromise(iterReturn cursor.Iterator)
            do! Async.AwaitPromise(cursor.Raw.cancel ())
        }

    /// Consume a read session, invoking `handler` for each record until it ends.
    let iter (handler: ReadRecord -> Async<unit>) (sess: ReadSession) : Async<unit> =
        async {
            let it = asyncIterator (box sess.Raw)
            let mutable go = true

            while go do
                let! res = Async.AwaitPromise(iterNext it)

                if iterDone res then
                    go <- false
                else
                    do! handler (toRecord (unbox<IReadRecord> (iterValue res)))
        }

    /// Cancel/close a read session.
    let closeReadSession (sess: ReadSession) : Async<unit> =
        async { do! Async.AwaitPromise(sess.Raw.cancel ()) }

    /// Read up to `n` records from a session, then stop. Use for a bounded read
    /// over a streaming session (which otherwise tails indefinitely). Stopping
    /// early releases the reader lock and cancels the session.
    let take (n: int) (sess: ReadSession) : Async<ReadRecord list> =
        async {
            let it = asyncIterator (box sess.Raw)
            let acc = ResizeArray<ReadRecord>()
            let mutable go = true

            while go && acc.Count < n do
                let! res = Async.AwaitPromise(iterNext it)

                if iterDone res then
                    go <- false
                else
                    acc.Add(toRecord (unbox<IReadRecord> (iterValue res)))

            let! _ = Async.AwaitPromise(iterReturn it)
            return List.ofSeq acc
        }

    // ---- Internal bridges for the patterns layer (src/S2/Patterns.fs) ----

    /// Open the raw underlying append session (for higher-level wrappers).
    let internal openRawAppendSession (s: Stream) : Async<IAppendSession> =
        Async.AwaitPromise(s.Raw.appendSession (createObj []))

    /// Open a raw bytes-format read session (for higher-level wrappers).
    let internal openRawBytesReadSession (opts: ReadOptions) (s: Stream) : Async<IReadSession> =
        Async.AwaitPromise(s.Raw.readSession (readInput opts, createObj [ "as" ==> "bytes" ]))

    // ---- Locations ----

    /// List available locations.
    let listLocations (client: Client) : Async<LocationInfo list> =
        async {
            let! r = Async.AwaitPromise(client.Raw.locations.list ())

            return
                [ for l in r ->
                      { Name = l.name
                        IsPrivate = l.isPrivate } ]
        }

    /// Get the default location used when creating basins without an explicit one.
    let getDefaultLocation (client: Client) : Async<LocationInfo> =
        async {
            let! l = Async.AwaitPromise(client.Raw.locations.getDefault ())

            return
                { Name = l.name
                  IsPrivate = l.isPrivate }
        }

    /// Set the default location.
    let setDefaultLocation (location: string) (client: Client) : Async<LocationInfo> =
        async {
            let! l = Async.AwaitPromise(client.Raw.locations.setDefault (box {| location = location |}))

            return
                { Name = l.name
                  IsPrivate = l.isPrivate }
        }

    // ---- Metrics ----

    /// Account-level metrics over a `[start, end)` window.
    let accountMetrics
        (set: AccountMetricSet)
        (startTime: DateTime)
        (endTime: DateTime)
        (client: Client)
        : Async<MetricSet> =
        async {
            let! r =
                Async.AwaitPromise(
                    client.Raw.metrics.account (
                        box
                            {| set = accountMetricSetStr set
                               start = startTime
                               ``end`` = endTime |}
                    )
                )

            return [ for m in r.values -> parseMetric m ]
        }

    /// Basin-level metrics over a `[start, end)` window.
    let basinMetrics
        (set: BasinMetricSet)
        (name: string)
        (startTime: DateTime)
        (endTime: DateTime)
        (client: Client)
        : Async<MetricSet> =
        async {
            let! r =
                Async.AwaitPromise(
                    client.Raw.metrics.basin (
                        box
                            {| basin = name
                               set = basinMetricSetStr set
                               start = startTime
                               ``end`` = endTime |}
                    )
                )

            return [ for m in r.values -> parseMetric m ]
        }

    /// Stream-level storage metrics over a `[start, end)` window.
    let streamMetrics
        (basinName: string)
        (streamName: string)
        (startTime: DateTime)
        (endTime: DateTime)
        (client: Client)
        : Async<MetricSet> =
        async {
            let! r =
                Async.AwaitPromise(
                    client.Raw.metrics.stream (
                        box
                            {| basin = basinName
                               stream = streamName
                               set = "storage"
                               start = startTime
                               ``end`` = endTime |}
                    )
                )

            return [ for m in r.values -> parseMetric m ]
        }

    // ---- Access tokens ----

    /// List access tokens on the account.
    let listTokens (client: Client) : Async<TokenInfo list> =
        async {
            let! r = Async.AwaitPromise(client.Raw.accessTokens.list (createObj []))

            return
                [ for t in r.accessTokens ->
                      { Id = t.id
                        AutoPrefixStreams =
                          (if isNil t.autoPrefixStreams then
                               false
                           else
                               unbox<bool> t.autoPrefixStreams) } ]
        }

    /// Issue a new access token; returns the secret token string.
    let issueToken (id: string) (scope: TokenScope) (client: Client) : Async<string> =
        async {
            let! r = Async.AwaitPromise(client.Raw.accessTokens.issue (box {| id = id; scope = scopeObj scope |}))
            return r.accessToken
        }

    /// Revoke an access token by id.
    let revokeToken (id: string) (client: Client) : Async<unit> =
        async {
            let! _ = Async.AwaitPromise(client.Raw.accessTokens.revoke (box {| id = id |}))
            return ()
        }
