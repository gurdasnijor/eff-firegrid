namespace Eff.Telemetry

open Fable.Core
open Fable.Core.JsInterop

module Otel =
    type Config =
        { ServiceName: string
          Endpoint: string option
          ResourceAttributes: Trace.Attribute list }

    type NodeSdk =
        abstract start: unit -> JS.Promise<unit>
        abstract shutdown: unit -> JS.Promise<unit>

    let private nodeSdkCtor: obj = import "NodeSDK" "@opentelemetry/sdk-node"

    let private otlpTraceExporterCtor: obj =
        import "OTLPTraceExporter" "@opentelemetry/exporter-trace-otlp-http"

    let mutable private currentSdk: NodeSdk option = None

    [<Emit("new $0($1)")>]
    let private construct<'a> (_ctor: obj) (_options: obj) : 'a = jsNative

    [<Emit("process.env[$0]")>]
    let private env (_name: string) : string = jsNative

    [<Emit("process.env[$0] = $1")>]
    let private setEnv (_name: string) (_value: string) : unit = jsNative

    [<Emit("delete process.env[$0]")>]
    let private deleteEnv (_name: string) : unit = jsNative

    [<Emit("($0 == null || String($0).trim() === '')")>]
    let private isBlank (_value: string) : bool = jsNative

    let private mergeResourceAttributes attributes =
        attributes
        |> List.map (fun (key, value) -> key + "=" + value)
        |> String.concat ","

    let private configureEnvironment config =
        setEnv "OTEL_SERVICE_NAME" config.ServiceName

        match config.Endpoint with
        | Some endpoint when not (isBlank endpoint) -> setEnv "OTEL_EXPORTER_OTLP_ENDPOINT" endpoint
        | _ -> ()

        match mergeResourceAttributes config.ResourceAttributes with
        | "" -> ()
        | attrs -> setEnv "OTEL_RESOURCE_ATTRIBUTES" attrs

    let private exporterOptions endpoint =
        match endpoint with
        | Some value when not (isBlank value) -> createObj [ "url" ==> value ]
        | _ -> createObj []

    let start (config: Config) : Async<unit> =
        async {
            match currentSdk with
            | Some _ -> return ()
            | None ->
                configureEnvironment config

                let exporter =
                    construct<obj> otlpTraceExporterCtor (exporterOptions config.Endpoint)

                let sdk =
                    construct<NodeSdk> nodeSdkCtor (createObj [ "traceExporter" ==> exporter ])

                do! sdk.start () |> Async.AwaitPromise
                currentSdk <- Some sdk
        }

    let startFromEnv (serviceName: string) (resourceAttributes: Trace.Attribute list) : Async<unit> =
        async {
            let endpoint = env "OTEL_EXPORTER_OTLP_ENDPOINT"

            do!
                start
                    { ServiceName = serviceName
                      Endpoint = if isBlank endpoint then None else Some endpoint
                      ResourceAttributes = resourceAttributes }
        }

    let shutdown () : Async<unit> =
        async {
            match currentSdk with
            | None -> return ()
            | Some sdk ->
                do! sdk.shutdown () |> Async.AwaitPromise
                currentSdk <- None
        }

    let disableForProcess () =
        deleteEnv "OTEL_EXPORTER_OTLP_ENDPOINT"
        deleteEnv "OTEL_RESOURCE_ATTRIBUTES"
        deleteEnv "OTEL_SERVICE_NAME"
