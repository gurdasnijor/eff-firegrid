namespace Eff.Foundation.Durable.App

open Eff
open Fable.Core

[<RequireQualifiedAccess>]
module DurableAppEnvironment =
    [<Emit("process.env[$0] || ''")>]
    let private env (_name: string) : string = jsNative

    let private isBlank value = System.String.IsNullOrWhiteSpace value

    let private normalize (environment: string) =
        environment.Trim().ToUpperInvariant().Replace("-", "_")

    let private envOption name =
        let value = env name

        if isBlank value then None else Some value

    let private configured environment suffix =
        let environmentKey = "EFF_FIREGRID_" + normalize environment + "_" + suffix
        let fallbackKey = "EFF_FIREGRID_" + suffix

        match envOption environmentKey with
        | Some value -> Some value
        | None -> envOption fallbackKey

    let private configuredBasin environment basinName =
        match basinName with
        | Some name when not (isBlank name) -> name
        | _ ->
            match configured environment "BASIN" with
            | Some basin -> basin
            | None ->
                invalidArg
                    (nameof basinName)
                    ("BasinName is required, or set EFF_FIREGRID_"
                     + normalize environment
                     + "_BASIN / EFF_FIREGRID_BASIN.")

    let private connectClient environment =
        let accessToken = configured environment "ACCESS_TOKEN"
        let accountEndpoint = configured environment "S2_ACCOUNT_ENDPOINT"
        let basinEndpoint = configured environment "S2_BASIN_ENDPOINT"

        match accessToken, accountEndpoint, basinEndpoint with
        | None, None, None -> S2Cli.connect ()
        | _ ->
            S2.connectWith
                { S2.ConnectOptions.create (accessToken |> Option.defaultValue "durable-app-environment") with
                    AccountEndpoint = accountEndpoint
                    BasinEndpoint = basinEndpoint }

    let storage environment basinName =
        if isBlank environment then
            invalidArg (nameof environment) "Environment must be non-empty."

        let basinName = configuredBasin environment basinName
        let s2 = connectClient environment
        s2 |> S2.basin basinName |> DurableStorage.s2
