%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Behaviour defining callbacks to be called by workflow engine
%%% during workflow execution. Workflow execution starts when
%%% workflow_engine:execute_workflow/2 function is called.
%%% Name of module implementing callback is provided to workflow engine as an
%%% argument to execute_workflow/2 function. The execution can be cancelled
%%% either by calling workflow_engine:cancel_execution/1 function (external cancel)
%%% or by workflow engine as a result or error (internal cancel when number of
%%% callbacks' errors is greater than failure_count_to_cancel lane parameter).
%%% Workflow execution ends when handle_lane_execution_ended callback returns
%%% ?END_EXECUTION or as a result of external or internal cancellation.
%%%
%%% Execution of workflow consists of execution of lanes. Each lane consists
%%% of processing tasks on items provided by iterator. Tasks can be processed
%%% synchronously of asynchronously depending on task_spec. The order of execution of
%%% tasks on single item is determined by parallel boxes. Tasks from one parallel
%%% box can be executed on single item in parallel while tasks from next parallel box
%%% wait for finish of execution of all tasks from previous parallel box to be
%%% executed on this item. Each lane has to be
%%% prepared by calling prepare_lane callback before any task is executed.
%%% Lane can be prepared right before execution of task or be prepared in advance
%%% during execution of previous lane. Lane to be executed and prepared in
%%% advance are returned by handle_lane_execution_ended callback for previous lane.
%%% Thus, lane prepared in advance does not have to be always executed after
%%% preparation as handle_lane_execution_ended for lane currently being executed can
%%% ignore prepared in advance lane and return other lane to be executed next.
%%% handle_lane_execution_ended callback can also return currently executed lane id
%%% to retry lane. In such a case lane is prepared for second time.
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
-type prepare_lane_result() :: {ok, workflow_engine:lane_spec()} | error.
-type lane_ended_callback_result() :: ?CONTINUE(workflow_engine:lane_id(), workflow_engine:lane_id()) |
    ?END_EXECUTION. % engine does not distinguish reason of execution finish - ?END_EXECUTION is returned
                       % if processed lane is last lane as well as on error
% TODO VFS-7787 move following types to callback server:
-type finished_callback_id() :: binary().
-type heartbeat_callback_id() :: binary().

-export_type([handler/0, async_processing_result/0, handler_execution_result/0, prepare_lane_result/0,
    lane_ended_callback_result/0, finished_callback_id/0, heartbeat_callback_id/0]).

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
    prepare_lane_result().


%%--------------------------------------------------------------------
%% @doc
%% Callback to get lane spec when execution is restarted from snapshot.
%% TODO - VFS-8495 - integrate with atm workflow resume and decide
%% if resume should result in clean start
%% @end
%%--------------------------------------------------------------------
-callback restart_lane(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:lane_id()
) ->
    prepare_lane_result().


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
-callback run_task_for_item(
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
-callback process_task_result_for_item(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:task_id(),
    iterator:item(),
    async_processing_result()
) ->
    handler_execution_result().


%%--------------------------------------------------------------------
%% @doc
%% Callback reporting that at least one task for item has failed.
%% NOTE: if any task for an item fails, this callback is executed exactly
%% once after all tasks from parallel box are finished for the item.
%% @end
%%--------------------------------------------------------------------
-callback report_item_error(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    iterator:item()
) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Callback processing streamed task data that was generated during execution of task for item but is not
%% a result of this processing. It is called for every chunk of streamed task data produced for task.
%% @end
%%--------------------------------------------------------------------
-callback process_streamed_task_data(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:task_id(),
    workflow_engine:streamed_task_data()
) ->
    handler_execution_result().


%%--------------------------------------------------------------------
%% @doc
%% Callback called after all jobs for task were executed and their
%% outputs processed. It is executed only when task has data stream.
%% @end
%%--------------------------------------------------------------------
-callback handle_task_results_processed_for_all_items(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:task_id()
) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Callback reporting that all jobs for task were executed, their
%% outputs processed and all streamed task data was processed (in case
%% the task had data stream). This callback is executed once for each task.
%% It is guaranteed that callback is called before call of
%% handle_lane_execution_ended callback for task's lane.
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
%% for all items. It will be called exactly once for lane.
%% @end
%%--------------------------------------------------------------------
-callback handle_lane_execution_ended(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:lane_id()
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