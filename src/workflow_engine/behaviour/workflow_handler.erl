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
% TODO VFS-7787 move following types to callback server:
-type finished_callback_id() :: binary().
-type heartbeat_callback_id() :: binary().

-export_type([handler/0, task_processing_result/0,
    finished_callback_id/0, heartbeat_callback_id/0, callback_execution_result/0]).

%%%===================================================================
%%% Callbacks descriptions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Callback that prepares workflow execution. It will be called once
%% before get_lane_spec is called for the first line.
%% @end
%%--------------------------------------------------------------------
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
    callback_execution_result().


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
    task_processing_result()
) ->
    callback_execution_result().


%%--------------------------------------------------------------------
%% @doc
%% Callback reporting that task has been executed for all items.
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