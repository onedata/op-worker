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

-include("workflow_engine.hrl").
-include_lib("ctool/include/errors.hrl").

-type handler() :: module().
-type async_processing_basic_result() :: term().
-type async_processing_result() :: async_processing_basic_result() | ?ERROR_MALFORMED_DATA | ?ERROR_TIMEOUT.
-type handler_execution_result() :: ok | error.
-type prepare_result() :: {ok, workflow_engine:lane_spec(), workflow_engine:execution_context()} | error.
% engine does not distinguish reason of execution finish - finish_execution is returned
% if processed lane is last lane as well as on error
-type lane_ended_callback_result() :: {continue, NextLaneId :: workflow_engine:lane_id()} | finish_execution.
% TODO VFS-7787 move following types to callback server:
-type finished_callback_id() :: binary().
-type heartbeat_callback_id() :: binary().

-export_type([handler/0, async_processing_result/0, handler_execution_result/0, prepare_result/0,
    finished_callback_id/0, heartbeat_callback_id/0]).

%%%===================================================================
%%% Callbacks descriptions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Callback that prepares workflow lane execution.
%% It will be called exactly once for each lane.
%% @end
%%--------------------------------------------------------------------
-callback prepare_lane(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:lane_id()
) ->
    prepare_result().


%%--------------------------------------------------------------------
%% @doc
%% Callback to get lane spec when execution is restarted from snapshot.
%% @end
%%--------------------------------------------------------------------
-callback restart_lane(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:lane_id()
) ->
    prepare_result().


%%--------------------------------------------------------------------
%% @doc
%% Callback that executes job. It is called once for each job
%% (pair task/item). It can be called in parallel for jobs connected
%% to different items and jobs connected to the same item if tasks
%% are connected to the same parallel job. If any job fails, the
%% callback is not called for next parallel boxes for item connected
%% to the job.
%% @end
%%--------------------------------------------------------------------
-callback process_item(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:task_id(),
    iterator:item(),
    finished_callback_id(),
    heartbeat_callback_id()
) ->
    handler_execution_result().


%%--------------------------------------------------------------------
%% @doc
%% Callback processing result provided by finished_callback
%% (it is executed only for asynchronous jobs).
%% @end
%%--------------------------------------------------------------------
-callback process_result(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:task_id(),
    iterator:item(),
    async_processing_result()
) ->
    handler_execution_result().


%%--------------------------------------------------------------------
%% @doc
%% Callback reporting that task has been executed for all items.
%% This callback is executes once for each task. It is guarantees that
%% callback is called before call of handle_lane_execution_ended
%% callback for task's lane.
%% Warning: there is no guarantee that callbacks for tasks are called
%% exactly the same order as the tasks were finished.
%% @end
%%--------------------------------------------------------------------
-callback handle_task_execution_ended(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:task_id()
) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Callback reporting that all tasks in given lane have been executed
%% for all items.
%% Warning: this callback can be called multiple times for single lane
%% even after next lane processing start. However, it is guaranteed that
%% it will be called at least once before next lane processing start.
%% @end
%%--------------------------------------------------------------------
-callback handle_lane_execution_ended(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_execution_state:index()
) ->
    lane_ended_callback_result().


%%--------------------------------------------------------------------
%% @doc
%% Callback reporting that all tasks in given workflow have been
%% executed for all items. It will be called exactly once.
%% @end
%%--------------------------------------------------------------------
-callback handle_workflow_execution_ended(
    workflow_engine:execution_id(),
    workflow_engine:execution_context()
) ->
    ok.