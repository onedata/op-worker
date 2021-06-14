%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements callbacks for handling automation workflow
%%% execution process.
%%% @end
%%%--------------------------------------------------------------------
-module(atm_workflow_execution_handler).
-author("Bartosz Walkowicz").

-behaviour(workflow_handler).

-include_lib("ctool/include/logging.hrl").

% workflow_handler callbacks
-export([
    prepare/2,
    get_lane_spec/3,
    process_item/6,
    process_result/4,
    handle_task_execution_ended/3,
    handle_lane_execution_ended/3
]).


%%%===================================================================
%%% workflow_handler callbacks
%%%===================================================================


-spec prepare(atm_workflow_execution:id(), atm_workflow_execution_env:record()) ->
    ok | error.
prepare(AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv) ->
    try
        ok = atm_workflow_execution_api:prepare(AtmWorkflowExecutionId)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("FAILED TO PREPARE WORKFLOW ~p DUE TO: ~p", [
            AtmWorkflowExecutionId, Reason
        ]),
        error
    end.


-spec get_lane_spec(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    non_neg_integer()
) ->
    {ok, workflow_engine:lane_spec()} | error.
get_lane_spec(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmLaneIndex) ->
    try
        {ok, atm_workflow_execution_api:get_lane_execution_spec(
            AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmLaneIndex
        )}
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO GET LANE ~p SPEC ~p DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmLaneIndex, Reason
        ]),
        error
    end.


-spec process_item(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    atm_api:item(),
    binary(),
    binary()
) ->
    ok | error.
process_item(
    AtmWorkflowExecutionId,
    AtmWorkflowExecutionEnv,
    AtmTaskExecutionId,
    Item,
    ReportResultUrl,
    HeartbeatUrl
) ->
    try
        ok = atm_task_execution_api:run(
            AtmWorkflowExecutionEnv, AtmTaskExecutionId, Item,
            ReportResultUrl, HeartbeatUrl
        )
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO RUN TASK ~p DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmTaskExecutionId, Reason
        ]),
        catch atm_task_execution_api:handle_results(
            AtmWorkflowExecutionEnv, AtmTaskExecutionId, error
        ),
        error
    end.


-spec process_result(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    {error, term()} | json_utils:json_map()
) ->
    ok | error.
process_result(_AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId, {error, _}) ->
    catch atm_task_execution_api:handle_results(AtmWorkflowExecutionEnv, AtmTaskExecutionId, error),
    error;

process_result(_AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId, Results) ->
    try
        atm_task_execution_api:handle_results(AtmWorkflowExecutionEnv, AtmTaskExecutionId, Results)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("FAILED TO PROCESS RESULT FOR TASK EXECUTION ~p DUE TO: ~p", [
            AtmTaskExecutionId, Reason
        ]),
        catch atm_task_execution_api:handle_results(AtmWorkflowExecutionEnv, AtmTaskExecutionId, error)
    end,
    ok.


-spec handle_task_execution_ended(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id()
) ->
    ok.
handle_task_execution_ended(_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId) ->
    try
        ok = atm_task_execution_api:mark_ended(AtmTaskExecutionId)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("FAILED TO MARK TASK EXECUTION ~p AS ENDED DUE TO: ~p", [
            AtmTaskExecutionId, Reason
        ])
    end.


-spec handle_lane_execution_ended(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    non_neg_integer()
) ->
    ok.
handle_lane_execution_ended(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmLaneExecutionIndex) ->
    try
        ok = atm_workflow_execution_api:report_lane_execution_ended(
            AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmLaneExecutionIndex
        )
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("FAILED TO MARK LANE EXECUTION ~p AS ENDED DUE TO: ~p", [
            AtmLaneExecutionIndex, Reason
        ])
    end.
