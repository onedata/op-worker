%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_tasks_data).
-author("Michal Wrzeszcz").


-include_lib("ctool/include/errors.hrl").


%% API
-export([init/0, register/3, prepare_next/1, mark_done/3,
    mark_task_data_stream_closed/2, is_task_data_stream_closed/2, is_task_data_stream_finished/2,
    verify_callbacks_on_cancel/1]).


% Internal record that describe information about all data currently processed or waiting to be processed.
-record(workflow_tasks_data, {
    waiting = #{} :: task_to_data_map(),
    ongoing = #{} :: task_to_data_map(),

    closed_task_data_streams = [] :: [workflow_engine:task_id()],
    tasks_execution_order = [] :: [workflow_engine:task_id()],
    callbacks_on_cancel_status = not_executed :: executed | not_executed
}).


-opaque tasks_data() :: #workflow_tasks_data{}.
-type task_to_data_map() :: #{workflow_engine:task_id() => [workflow_cached_task_data:id()]}.
-export_type([tasks_data/0]).


%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> tasks_data().
init() ->
    #workflow_tasks_data{}.


-spec register(workflow_engine:task_id(), workflow_cached_task_data:id(), tasks_data()) -> tasks_data().
register(TaskId, TaskDataId, #workflow_tasks_data{waiting = Waiting, tasks_execution_order = Order} = TasksData) ->
    case maps:get(TaskId, Waiting, undefined) of
        undefined ->
            TasksData#workflow_tasks_data{
                tasks_execution_order = Order ++ [TaskId],
                waiting = Waiting#{TaskId => [TaskDataId]}
            };
        TaskDataIds ->
            TasksData#workflow_tasks_data{waiting = Waiting#{TaskId => TaskDataIds ++ [TaskDataId]}}
    end.


-spec prepare_next(tasks_data()) ->
    {ok, workflow_engine:task_id(), workflow_cached_task_data:id(), tasks_data()} | ?ERROR_NOT_FOUND.
prepare_next(#workflow_tasks_data{tasks_execution_order = []}) ->
    ?ERROR_NOT_FOUND;
prepare_next(#workflow_tasks_data{waiting = Waiting, tasks_execution_order = [NextTaskId | Order]} = TasksData) ->
    case maps:get(NextTaskId, Waiting) of
        [TaskDataId] ->
            {ok, NextTaskId, TaskDataId, mark_task_ongoing(NextTaskId, TaskDataId, TasksData#workflow_tasks_data{
                tasks_execution_order = Order,
                waiting = maps:remove(NextTaskId, Waiting)
            })};
        [TaskDataId | TaskDataTail] ->
            {ok, NextTaskId, TaskDataId, mark_task_ongoing(NextTaskId, TaskDataId, TasksData#workflow_tasks_data{
                tasks_execution_order = Order ++ [NextTaskId],
                waiting = Waiting#{NextTaskId => TaskDataTail}
            })}
    end.


-spec mark_done(workflow_engine:task_id(), workflow_cached_task_data:id(), tasks_data()) -> tasks_data().
mark_done(TaskId, TaskDataId, #workflow_tasks_data{ongoing = Ongoing} = TasksData) ->
    case maps:get(TaskId, Ongoing) of
        [TaskDataId] -> TasksData#workflow_tasks_data{ongoing = maps:remove(TaskId, Ongoing)};
        TaskDataIds -> TasksData#workflow_tasks_data{ongoing = Ongoing#{TaskId => TaskDataIds -- [TaskDataId]}}
    end.


-spec mark_task_data_stream_closed(workflow_engine:task_id(), tasks_data()) -> tasks_data().
mark_task_data_stream_closed(TaskId, #workflow_tasks_data{closed_task_data_streams = ClosedStreams} = TasksData) ->
    TasksData#workflow_tasks_data{closed_task_data_streams = [TaskId | ClosedStreams]}.


% TODO - czy ta funkcja jest potrzebna (jak traktujemy cancel?
-spec is_task_data_stream_closed(workflow_engine:task_id(), tasks_data()) -> boolean().
is_task_data_stream_closed(TaskId, #workflow_tasks_data{closed_task_data_streams = ClosedStreams}) ->
    lists:member(TaskId, ClosedStreams).


-spec is_task_data_stream_finished(workflow_engine:task_id(), tasks_data()) -> boolean().
is_task_data_stream_finished(TaskId, #workflow_tasks_data{
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


-spec verify_callbacks_on_cancel(tasks_data()) -> {execute, tasks_data()} | do_nothing.
verify_callbacks_on_cancel(#workflow_tasks_data{callbacks_on_cancel_status = not_executed} = Data) ->
    {execute, Data#workflow_tasks_data{callbacks_on_cancel_status = executed}};
verify_callbacks_on_cancel(#workflow_tasks_data{}) ->
    do_nothing.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec mark_task_ongoing(workflow_engine:task_id(), workflow_cached_task_data:id(), tasks_data()) -> tasks_data().
mark_task_ongoing(TaskId, TaskDataId, #workflow_tasks_data{ongoing = Ongoing} = TasksData) ->
    NewOngoing = maps:update_with(TaskId, fun(OngoingIds) -> [TaskDataId | OngoingIds] end, [TaskDataId], Ongoing),
    TasksData#workflow_tasks_data{ongoing = NewOngoing}.
