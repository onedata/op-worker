%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME
%%% @end
%%%--------------------------------------------------------------------
-module(atm_workflow_execution_handler).
-author("Bartosz Walkowicz").

% TODO substitute bahaviour module
%%-behaviour(workflow_handler).

-include_lib("ctool/include/logging.hrl").

% workflow_handler callbacks
-export([
    prepare/2,
    get_lane_spec/3,
    process_item/6,
    process_result/4,
    handle_task_ended/3
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
    FinishedCallback,
    HeartbeatCallback
) ->
    try
        ok = atm_task_execution_api:run(
            AtmWorkflowExecutionEnv, AtmTaskExecutionId, Item,
            FinishedCallback, HeartbeatCallback
        )
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO RUN TASK ~p DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmTaskExecutionId, Reason
        ]),
        error
    end.


process_result(_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, _AtmTaskExecutionId, _Result) ->
    % TODO VFS-7691 implement result mappers
    ok.


-spec handle_task_ended(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id()
) ->
    ok.
handle_task_ended(_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId) ->
    try
        ok = atm_task_execution_api:mark_ended(AtmTaskExecutionId)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("FAILED TO MARK TASK EXECUTION ~p AS ENDED DUE TO: ~p", [
            AtmTaskExecutionId, Reason
        ])
    end.
