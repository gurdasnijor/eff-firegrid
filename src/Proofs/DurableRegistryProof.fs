namespace Eff.Proofs

open Eff.Foundation.Durable

module DurableRegistryProof =
    type DurableRegistryResult =
        { ActivityRegistrationFindsHandler: bool
          ActivityDuplicateRejected: bool
          ActivityMissingReturnsTypedError: bool
          WorkflowRegistrationFindsFactory: bool
          WorkflowDuplicateRejected: bool
          WorkflowMissingReturnsTypedError: bool
          WorkflowLookupStableAcrossTicks: bool }

    let private reserve input = async { return "reserved:" + input }

    let private replacement input = async { return "replacement:" + input }

    let private checkout input =
        durable {
            let! reserved = Workflow.call "reserve" input
            return reserved
        }

    let private alternate input = durable { return "alternate:" + input }

    let private checkActivityRegistrationFindsHandler () =
        async {
            match ActivityRegistry.empty |> ActivityRegistry.register "reserve" reserve with
            | Ok registry ->
                match ActivityRegistry.require "reserve" registry with
                | Ok handler ->
                    let! result = handler "order-1"
                    return result = "reserved:order-1"
                | Error _ -> return false
            | Error _ -> return false
        }

    let private checkActivityDuplicateRejected () =
        async {
            match ActivityRegistry.empty |> ActivityRegistry.register "reserve" reserve with
            | Error _ -> return false
            | Ok first ->
                match first |> ActivityRegistry.register "reserve" replacement with
                | Ok _ -> return false
                | Error(DurableRegistryError.DuplicateActivity(ActivityName "reserve")) ->
                    match ActivityRegistry.require "reserve" first with
                    | Ok handler ->
                        let! result = handler "order-1"
                        return result = "reserved:order-1"
                    | Error _ -> return false
                | Error _ -> return false
        }

    let private checkActivityMissingReturnsTypedError () =
        match ActivityRegistry.empty |> ActivityRegistry.require "missing" with
        | Error(DurableRegistryError.ActivityNotFound(ActivityName "missing")) -> true
        | _ -> false

    let private checkWorkflowRegistrationFindsFactory () =
        match WorkflowRegistry.empty |> WorkflowRegistry.register "checkout" checkout with
        | Ok registry ->
            match WorkflowRegistry.require "checkout" registry with
            | Ok factory ->
                match Durable.replay History.empty (factory "order-1") with
                | Blocked(OpId 0, NeedsActivity activity) -> activity = Activities.create "reserve" "order-1"
                | _ -> false
            | Error _ -> false
        | Error _ -> false

    let private checkWorkflowDuplicateRejected () =
        match WorkflowRegistry.empty |> WorkflowRegistry.register "checkout" checkout with
        | Error _ -> false
        | Ok first ->
            match first |> WorkflowRegistry.register "checkout" alternate with
            | Ok _ -> false
            | Error(DurableRegistryError.DuplicateWorkflow(WorkflowName "checkout")) ->
                match WorkflowRegistry.require "checkout" first with
                | Ok factory ->
                    match Durable.replay History.empty (factory "order-1") with
                    | Blocked(OpId 0, NeedsActivity activity) -> activity = Activities.create "reserve" "order-1"
                    | _ -> false
                | Error _ -> false
            | Error _ -> false

    let private checkWorkflowMissingReturnsTypedError () =
        match WorkflowRegistry.empty |> WorkflowRegistry.require "missing" with
        | Error(DurableRegistryError.WorkflowNotFound(WorkflowName "missing")) -> true
        | _ -> false

    let private checkWorkflowLookupStableAcrossTicks () =
        match WorkflowRegistry.empty |> WorkflowRegistry.register "checkout" checkout with
        | Error _ -> false
        | Ok registry ->
            let tick input =
                match WorkflowRegistry.require "checkout" registry with
                | Error _ -> None
                | Ok factory ->
                    match Durable.replay History.empty (factory input) with
                    | Blocked(_, NeedsActivity activity) -> Some activity
                    | _ -> None

            tick "order-1" = Some(Activities.create "reserve" "order-1")
            && tick "order-2" = Some(Activities.create "reserve" "order-2")
            && WorkflowRegistry.count registry = 1

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.registry"
            "durable-registry"
            { ProofOperationOptions.empty with
                Key = Some "durable-registry" }
            (async {
                let! activityRegistrationFindsHandler = checkActivityRegistrationFindsHandler ()
                let! activityDuplicateRejected = checkActivityDuplicateRejected ()

                let result =
                    { ActivityRegistrationFindsHandler = activityRegistrationFindsHandler
                      ActivityDuplicateRejected = activityDuplicateRejected
                      ActivityMissingReturnsTypedError = checkActivityMissingReturnsTypedError ()
                      WorkflowRegistrationFindsFactory = checkWorkflowRegistrationFindsFactory ()
                      WorkflowDuplicateRejected = checkWorkflowDuplicateRejected ()
                      WorkflowMissingReturnsTypedError = checkWorkflowMissingReturnsTypedError ()
                      WorkflowLookupStableAcrossTicks = checkWorkflowLookupStableAcrossTicks () }

                do!
                    ctx.EmitSpan
                        "proof.durable_registry.completed"
                        [ "proof.property", "durable-registry"
                          "registry.activity", string result.ActivityRegistrationFindsHandler
                          "registry.activity_duplicate", string result.ActivityDuplicateRejected
                          "registry.workflow", string result.WorkflowRegistrationFindsFactory
                          "registry.workflow_stable", string result.WorkflowLookupStableAcrossTicks ]

                return result
            })

    let registryProperty =
        property "durable-registry" {
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "activity registration finds handler" (fun result ->
                      result.ActivityRegistrationFindsHandler)
                  v.Expect.Workload "duplicate activity registration is rejected" (fun result ->
                      result.ActivityDuplicateRejected)
                  v.Expect.Workload "missing activity returns typed error" (fun result ->
                      result.ActivityMissingReturnsTypedError)
                  v.Expect.Workload "workflow registration finds factory" (fun result ->
                      result.WorkflowRegistrationFindsFactory)
                  v.Expect.Workload "duplicate workflow registration is rejected" (fun result ->
                      result.WorkflowDuplicateRejected)
                  v.Expect.Workload "missing workflow returns typed error" (fun result ->
                      result.WorkflowMissingReturnsTypedError)
                  v.Expect.Workload "workflow lookup is stable across host ticks" (fun result ->
                      result.WorkflowLookupStableAcrossTicks)
                  v.Trace.SpanExists
                      "durable registry proof span emitted"
                      "proof.durable_registry.completed"
                      [ "proof.property", "durable-registry" ]
                  v.Trace.Operation
                      "durable registry operation was recorded"
                      ({ TraceOperationMatch.named "durable.registry" with
                          Status = Some "ok"
                          OutputContains =
                              [ "ActivityRegistrationFindsHandler"
                                "WorkflowRegistrationFindsFactory"
                                "WorkflowLookupStableAcrossTicks" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-registry" {
            describedAs "Durable activity and workflow registries are explicit, immutable, and fail typed."
            property registryProperty
        }
