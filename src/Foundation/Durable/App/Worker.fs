namespace Eff.Foundation.Durable.App

open Eff
open Eff.Foundation.Durable

module internal DurableAppWorkerSupport =
    let discoverInstances basin =
        async {
            let! streams = basin |> S2.listStreamsWith ""

            return
                streams
                |> List.choose (fun stream ->
                    if stream.DeletedAt.IsNone && stream.Name.EndsWith("/in") then
                        stream.Name.Substring(0, stream.Name.Length - 3) |> InstanceId.create |> Some
                    else
                        None)
                |> List.distinct
        }

    let private checkpointed =
        function
        | CommandDispatchCheckpointResult.Checkpointed _ -> true
        | CommandDispatchCheckpointResult.NotRequired
        | CommandDispatchCheckpointResult.Deposed _
        | CommandDispatchCheckpointResult.Failed _ -> false

    let private activityReportActive (report: ActivityCommandAdapterReport) =
        not (List.isEmpty report.Completed)
        || report.AlreadyPublished > 0
        || report.Ignored > 0
        || checkpointed report.Checkpoint

    let private timerReportActive (report: TimerCommandAdapterReport) =
        not (List.isEmpty report.Published)
        || report.AlreadyPublished > 0
        || report.Canceled > 0
        || report.Ignored > 0
        || checkpointed report.Checkpoint

    let private tickReportActive (report: DurableHostTickReport<Payload>) =
        match report.Inbox with
        | Some inbox when inbox.Commit.IsSome -> true
        | _ ->
            match report.Step with
            | Some(DurableHostStatus.Committed _) -> true
            | _ ->
                match report.Signals with
                | Some signal when signal.Delivered.IsSome || signal.AlreadyDelivered > 0 -> true
                | _ ->
                    match report.Activities with
                    | Some activity when activityReportActive activity -> true
                    | _ ->
                        match report.Timers with
                        | Some timer when timerReportActive timer -> true
                        | _ -> false

    let private tickActive =
        function
        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Advanced report) -> tickReportActive report
        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed(_, report))
        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Waiting(_, _, report))
        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Deposed(_, report))
        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Failed(_, report)) -> tickReportActive report
        | DurableWorkflowHostStatus.Deposed _
        | DurableWorkflowHostStatus.Failed _ -> false

    let ticksActive ticks = ticks |> List.exists tickActive
