%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for workflow_execution_state processing information
%%% about jobs tasks' stream data being processed by workflow_engine. It
%%% also processes information about data scheduled for processing.
%%%
%%% NOTE: Task data stream is closed when 
%%% workflow_engine:close_task_data_stream/3 has been called. Task 
%%% data stream is processed when no data connected with it is waiting
%%% or ongoing.
%%%
%%% NOTE: #workflow_tasks_data{} record is cleaned when no data connected
%%% to any stream is waiting or ongoing (is_cleaned is test API used to
%%% check structure status, not single stream).
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_tasks_data).
-author("Michal Wrzeszcz").


-include_lib("ctool/include/errors.hrl").


%% API
-export([init/0, register/3, prepare_next/1, mark_done/3,
    mark_task_data_stream_closed/2, is_task_data_stream_closed_and_processed/2, claim_execution_of_callbacks_on_cancel/1]).
%% Test API
-export([is_cleaned/1]).


% Internal record that describe information about all data currently processed or waiting to be processed.
-record(workflow_tasks_data, {
    waiting = #{} :: #{workflow_engine:task_id() => [workflow_cached_task_data:id()]},
    ongoing = #{} :: #{workflow_engine:task_id() => [workflow_cached_task_data:id()]},

    closed_task_data_streams = [] :: [workflow_engine:task_id()],
    task_execution_order = [] :: [workflow_engine:task_id()],
    callbacks_on_cancel_status = not_executed :: executed | not_executed
}).


-opaque tasks_data() :: #workflow_tasks_data{}.
-export_type([tasks_data/0]).


%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> tasks_data().
init() ->
    #workflow_tasks_data{}.


-spec register(workflow_engine:task_id(), workflow_cached_task_data:id(), tasks_data()) -> tasks_data().
register(TaskId, CachedTaskDataId, #workflow_tasks_data{waiting = Waiting, task_execution_order = Order} = TasksData) ->
    case maps:get(TaskId, Waiting, undefined) of
        undefined ->
            TasksData#workflow_tasks_data{
                task_execution_order = Order ++ [TaskId],
                waiting = Waiting#{TaskId => [CachedTaskDataId]}
            };
        CachedTaskDataIds ->
            TasksData#workflow_tasks_data{waiting = Waiting#{TaskId => CachedTaskDataIds ++ [CachedTaskDataId]}}
    end.


-spec prepare_next(tasks_data()) ->
    {ok, workflow_engine:task_id(), workflow_cached_task_data:id(), tasks_data()} | ?ERROR_NOT_FOUND.
prepare_next(#workflow_tasks_data{task_execution_order = []}) ->
    ?ERROR_NOT_FOUND;
prepare_next(#workflow_tasks_data{waiting = Waiting, task_execution_order = [NextTaskId | Order]} = TasksData) ->
    case maps:get(NextTaskId, Waiting) of
        [CachedTaskDataId] ->
            {ok, NextTaskId, CachedTaskDataId, mark_task_ongoing(NextTaskId, CachedTaskDataId, TasksData#workflow_tasks_data{
                task_execution_order = Order,
                waiting = maps:remove(NextTaskId, Waiting)
            })};
        [CachedTaskDataId | TaskDataTail] ->
            {ok, NextTaskId, CachedTaskDataId, mark_task_ongoing(NextTaskId, CachedTaskDataId, TasksData#workflow_tasks_data{
                task_execution_order = Order ++ [NextTaskId],
                waiting = Waiting#{NextTaskId => TaskDataTail}
            })}
    end.


-spec mark_done(workflow_engine:task_id(), workflow_cached_task_data:id(), tasks_data()) -> tasks_data().
mark_done(TaskId, CachedTaskDataId, #workflow_tasks_data{ongoing = Ongoing} = TasksData) ->
    case maps:get(TaskId, Ongoing) of
        [CachedTaskDataId] -> TasksData#workflow_tasks_data{ongoing = maps:remove(TaskId, Ongoing)};
        CachedTaskDataIds -> TasksData#workflow_tasks_data{ongoing = Ongoing#{TaskId => CachedTaskDataIds -- [CachedTaskDataId]}}
    end.


-spec mark_task_data_stream_closed(workflow_engine:task_id(), tasks_data()) -> tasks_data().
mark_task_data_stream_closed(TaskId, #workflow_tasks_data{closed_task_data_streams = ClosedStreams} = TasksData) ->
    TasksData#workflow_tasks_data{closed_task_data_streams = [TaskId | ClosedStreams]}.


-spec is_task_data_stream_closed_and_processed(workflow_engine:task_id(), tasks_data()) -> boolean().
is_task_data_stream_closed_and_processed(TaskId, #workflow_tasks_data{
    waiting = Waiting,
    ongoing = Ongoing,
    closed_task_data_streams = ClosedStreams
}) ->
    case lists:member(TaskId, ClosedStreams) of
        true ->
            not (maps:is_key(TaskId, Waiting) orelse maps:is_key(TaskId, Ongoing));
        false ->
            false
    end.


-spec claim_execution_of_callbacks_on_cancel(tasks_data()) -> {ok, tasks_data()} | already_claimed.
claim_execution_of_callbacks_on_cancel(#workflow_tasks_data{callbacks_on_cancel_status = not_executed} = Data) ->
    {ok, Data#workflow_tasks_data{callbacks_on_cancel_status = executed}};
claim_execution_of_callbacks_on_cancel(#workflow_tasks_data{}) ->
    already_claimed.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec mark_task_ongoing(workflow_engine:task_id(), workflow_cached_task_data:id(), tasks_data()) -> tasks_data().
mark_task_ongoing(TaskId, CachedTaskDataId, #workflow_tasks_data{ongoing = Ongoing} = TasksData) ->
    NewOngoing = maps:update_with(TaskId, fun(OngoingIds) -> [CachedTaskDataId | OngoingIds] end, [CachedTaskDataId], Ongoing),
    TasksData#workflow_tasks_data{ongoing = NewOngoing}.


%%%===================================================================
%%% Test API
%%%===================================================================

-spec is_cleaned(tasks_data()) -> boolean().
is_cleaned(#workflow_tasks_data{
    waiting = Waiting,
    ongoing = Ongoing,
    task_execution_order = Order
}) ->
    maps_utils:is_empty(Waiting) andalso maps_utils:is_empty(Ongoing) andalso lists_utils:is_empty(Order).