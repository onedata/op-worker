%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Behaviour defining callback to be called workflow engine
%%% during workflow execution.
%%% @end
%%%--------------------------------------------------------------------
-module(workflow_handler).
-author("Michal Wrzeszcz").

-type handler() :: module().
-type task_processing_result() :: term().
-type callback_execution_result() :: ok | error.
% TODO VFS-7551 move following types to callback server:
-type finished_callback_id() :: binary().
-type heartbeat_callback_id() :: binary().

-export_type([handler/0, task_processing_result/0,
    finished_callback_id/0, heartbeat_callback_id/0, callback_execution_result/0]).

%%%===================================================================
%%% Callbacks descriptions
%%%===================================================================


-callback prepare(
    workflow_engine:execution_id(),
    workflow_engine:execution_context()
) ->
    callback_execution_result().


%%--------------------------------------------------------------------
%% @doc
%% Callback to get lane spec.
%% Warning: this callback can be called multiple times for single lane.
%% @end
%%--------------------------------------------------------------------
-callback get_lane_spec(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_execution_state:index()
) ->
    {ok, workflow_engine:lane_spec()} | error.


-callback process_item(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:task_id(),
    iterator:item(),
    finished_callback_id(),
    heartbeat_callback_id()
) ->
    callback_execution_result().


-callback process_result(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:task_id(),
    task_processing_result()
) ->
    callback_execution_result().


-callback handle_task_execution_ended(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:task_id()
) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Callback reporting that all tasks have been executed for all items.
%% Warning: this callback can be called multiple times for single lane
%% even after next line processing start. However, it is guaranteed that
%% it will be called at least once before next line processing start.
%% @end
%%--------------------------------------------------------------------
-callback handle_lane_execution_ended(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_execution_state:index()
) ->
    ok.