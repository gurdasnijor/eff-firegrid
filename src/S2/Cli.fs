namespace Eff

open Fable.Core
open Fable.Core.JsInterop

/// Node-only helpers that read the local `s2` CLI configuration, so the
/// playground needs no environment variables. The access token stays in
/// ~/.config/s2/config.toml — it's never copied into source or build output.
module S2Cli =

    [<Import("readFileSync", "node:fs")>]
    let private readFileSync (path: string) (encoding: string) : string = jsNative

    [<Import("homedir", "node:os")>]
    let private homedir () : string = jsNative

    // Respect XDG_CONFIG_HOME if set, else ~/.config (matches the s2 CLI).
    [<Emit("(process.env.XDG_CONFIG_HOME || ($0 + '/.config'))")>]
    let private configDir (home: string) : string = jsNative

    /// Path to the s2 CLI config file.
    let configPath () : string =
        configDir (homedir ()) + "/s2/config.toml"

    let private parseToken (toml: string) : string option =
        toml.Split('\n')
        |> Array.tryPick (fun raw ->
            let line = raw.Trim()

            if line.StartsWith "access_token" then
                match line.Split '"' with
                | [| _; token; _ |] -> Some token
                | _ -> None
            else
                None)

    /// Read the access token from the s2 CLI config.
    let accessToken () : string =
        let path = configPath ()

        match parseToken (readFileSync path "utf8") with
        | Some token -> token
        | None -> failwithf "no access_token found in %s" path

    /// Connect to S2 using the access token stored by the s2 CLI.
    let connect () : S2.Client = S2.connect (accessToken ())
