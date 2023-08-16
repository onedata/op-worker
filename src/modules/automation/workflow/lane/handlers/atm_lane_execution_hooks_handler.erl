%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles the lane execution hooks.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution_hooks_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([exec_current_lane_run_pre_execution_hooks/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Performs any operation that needs to be done right before current lane run
%% execution (all previous runs must have been stopped and current lane run is
%% ready to execute).
%% @end
%%--------------------------------------------------------------------
-spec exec_current_lane_run_pre_execution_hooks(
    atm_workflow_execution:record() | atm_workflow_execution:doc()
) ->
    ok.
exec_current_lane_run_pre_execution_hooks(AtmWorkflowExecution = #atm_workflow_execution{
    current_run_num = CurrentRunNum
}) ->
    case atm_lane_execution:get_run({current, current}, AtmWorkflowExecution) of
        {ok, #atm_lane_execution_run{status = ?ENQUEUED_STATUS, run_num = CurrentRunNum} = Run} ->
            atm_parallel_box_execution:set_tasks_run_num(
                CurrentRunNum, Run#atm_lane_execution_run.parallel_boxes
            ),
            atm_store_api:freeze(Run#atm_lane_execution_run.iterated_store_id);
        _ ->
            % execution must have stopped or lane run is still not ready
            % (either not fully prepared or previous run is still ongoing)
            ok
    end;

exec_current_lane_run_pre_execution_hooks(#document{value = AtmWorkflowExecution}) ->
    exec_current_lane_run_pre_execution_hooks(AtmWorkflowExecution).
