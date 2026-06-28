namespace Eff.Telemetry

open Fable.Core
open Fable.Core.JsInterop

module Trace =
    type Attribute = string * string

    type Span = obj

    let private traceApi: obj = import "trace" "@opentelemetry/api"
    let private spanStatusCode: obj = import "SpanStatusCode" "@opentelemetry/api"

    [<Emit("$0.getTracer($1)")>]
    let private getTracer (_traceApi: obj) (_name: string) : obj = jsNative

    [<Emit("$0.getActiveSpan()")>]
    let private activeSpan (_traceApi: obj) : Span = jsNative

    [<Emit("($0 == null)")>]
    let private isNil (_value: obj) : bool = jsNative

    [<Emit("for (const [key, value] of $1) { $0.setAttribute(key, value); }")>]
    let private setAttributes (_span: Span) (_attributes: Attribute array) : unit = jsNative

    [<Emit("$0.addEvent($1, Object.fromEntries($2))")>]
    let private addEvent (_span: Span) (_name: string) (_attributes: Attribute array) : unit = jsNative

    [<Emit("""$0.startActiveSpan($1, function(span) {
  for (const [key, value] of $2) {
    span.setAttribute(key, value);
  }

  return $3().then(
    function(value) {
      span.setStatus({ code: $4.OK });
      span.end();
      return value;
    },
    function(error) {
      span.recordException(error);
      span.setStatus({
        code: $4.ERROR,
        message: (error && error.message) ? String(error.message) : String(error)
      });
      span.end();
      throw error;
    }
  );
})""")>]
    let private startActiveSpan
        (_tracer: obj)
        (_name: string)
        (_attributes: Attribute array)
        (_work: unit -> JS.Promise<'a>)
        (_statusCode: obj)
        : JS.Promise<'a> =
        jsNative

    let withSpan (name: string) (attributes: Attribute list) (work: Async<'a>) : Async<'a> =
        async {
            let tracer = getTracer traceApi "eff-firegrid"

            let promise =
                startActiveSpan
                    tracer
                    name
                    (attributes |> List.toArray)
                    (fun () -> Async.StartAsPromise work)
                    spanStatusCode

            return! Async.AwaitPromise promise
        }

    let annotate (attributes: Attribute list) : Async<unit> =
        async {
            let span = activeSpan traceApi

            if not (isNil span) then
                setAttributes span (attributes |> List.toArray)
        }

    let event (name: string) (attributes: Attribute list) : Async<unit> =
        async {
            let span = activeSpan traceApi

            if not (isNil span) then
                addEvent span name (attributes |> List.toArray)
        }

    let setAttribute (key: string) (value: string) : Async<unit> = annotate [ key, value ]

    let currentSpan () : Async<Span option> =
        async {
            let span = activeSpan traceApi
            return if isNil span then None else Some span
        }
