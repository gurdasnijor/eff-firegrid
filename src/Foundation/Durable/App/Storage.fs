namespace Eff.Foundation.Durable.App

open Eff

type DurableStorage = internal DurableStorage of S2.Basin

[<RequireQualifiedAccess>]
module DurableStorage =
    let s2 basin = DurableStorage basin
