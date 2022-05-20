%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for workflow_execution_state processing information
%%% about streamed task data being processed by workflow_engine. It
%%% also processes information about streamed task data scheduled for
%%% processing.
%%%
%%% NOTE: Task data stream is finalized when it received all chunks of data
%%% (mark_all_task_data_received/2 has been called) and no data connected
%%% with it is waiting or ongoing.
%%%
%%% NOTE: #registry{} record is empty when no data connected
%%% to any stream is waiting or ongoing (is_empty is test API used to
%%% check structure status, not single stream).
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_tasks_data_registry).
-author("Michal Wrzeszcz").


-include_lib("ctool/include/errors.hrl").


%% API
-export([empty/0, put/3, take_for_processing/1, mark_processed/3,
    mark_all_task_data_received/2, is_stream_finalized/2,
    claim_execution_of_cancellation_procedures/1]).
%% Test API
-export([is_empty/1]).


% Internal record that describe information about all data currently processed or waiting to be processed.
-record(registry, {
    waiting = #{} :: #{workflow_engine:task_id() => [workflow_cached_task_data:id()]},
    ongoing = #{} :: #{workflow_engine:task_id() => [workflow_cached_task_data:id()]},

    streams_with_all_data_received = [] :: [workflow_engine:task_id()],
    task_execution_order = [] :: [workflow_engine:task_id()],
    cancellation_procedures_claimed = false :: boolean()
}).


-opaque registry() :: #registry{}.
-export_type([registry/0]).


%%%===================================================================
%%% API
%%%===================================================================

-spec empty() -> registry().
empty() ->
    #registry{}.


-spec put(workflow_engine:task_id(), workflow_cached_task_data:id(), registry()) -> registry().
put(TaskId, CachedTaskDataId, #registry{waiting = Waiting, task_execution_order = Order} = Registry) ->
    case maps:get(TaskId, Waiting, undefined) of
        undefined ->
            Registry#registry{
                task_execution_order = Order ++ [TaskId],
                waiting = Waiting#{TaskId => [CachedTaskDataId]}
            };
        CachedTaskDataIds ->
            Registry#registry{waiting = Waiting#{TaskId => CachedTaskDataIds ++ [CachedTaskDataId]}}
    end.


-spec take_for_processing(registry()) ->
    {ok, workflow_engine:task_id(), workflow_cached_task_data:id(), registry()} | ?ERROR_NOT_FOUND.
take_for_processing(#registry{task_execution_order = []}) ->
    ?ERROR_NOT_FOUND;
take_for_processing(#registry{waiting = Waiting, task_execution_order = [NextTaskId | Order]} = Registry) ->
    case maps:get(NextTaskId, Waiting) of
        [CachedTaskDataId] ->
            {ok, NextTaskId, CachedTaskDataId, mark_task_ongoing(NextTaskId, CachedTaskDataId, Registry#registry{
                task_execution_order = Order,
                waiting = maps:remove(NextTaskId, Waiting)
            })};
        [CachedTaskDataId | TaskDataTail] ->
            {ok, NextTaskId, CachedTaskDataId, mark_task_ongoing(NextTaskId, CachedTaskDataId, Registry#registry{
                task_execution_order = Order ++ [NextTaskId],
                waiting = Waiting#{NextTaskId => TaskDataTail}
            })}
    end.


-spec mark_processed(workflow_engine:task_id(), workflow_cached_task_data:id(), registry()) -> registry().
mark_processed(TaskId, CachedTaskDataId, #registry{ongoing = Ongoing} = Registry) ->
    case maps:get(TaskId, Ongoing) of
        [CachedTaskDataId] -> Registry#registry{ongoing = maps:remove(TaskId, Ongoing)};
        CachedTaskDataIds -> Registry#registry{ongoing = Ongoing#{TaskId => CachedTaskDataIds -- [CachedTaskDataId]}}
    end.


-spec mark_all_task_data_received(workflow_engine:task_id(), registry()) -> registry().
mark_all_task_data_received(TaskId, #registry{streams_with_all_data_received = Streams} = Registry) ->
    Registry#registry{streams_with_all_data_received = [TaskId | Streams]}.


-spec is_stream_finalized(workflow_engine:task_id(), registry()) -> boolean().
is_stream_finalized(TaskId, #registry{
    waiting = Waiting,
    ongoing = Ongoing,
    streams_with_all_data_received = StreamWithAllDataReceived
}) ->
    case lists:member(TaskId, StreamWithAllDataReceived) of
        true ->
            not (maps:is_key(TaskId, Waiting) orelse maps:is_key(TaskId, Ongoing));
        false ->
            false
    end.


-spec claim_execution_of_cancellation_procedures(registry()) -> {ok, registry()} | already_claimed.
claim_execution_of_cancellation_procedures(#registry{cancellation_procedures_claimed = false} = Data) ->
    {ok, Data#registry{cancellation_procedures_claimed = true}};
claim_execution_of_cancellation_procedures(#registry{}) ->
    already_claimed.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec mark_task_ongoing(workflow_engine:task_id(), workflow_cached_task_data:id(), registry()) -> registry().
mark_task_ongoing(TaskId, CachedTaskDataId, #registry{ongoing = Ongoing} = Registry) ->
    NewOngoing = maps:update_with(TaskId, fun(OngoingIds) -> [CachedTaskDataId | OngoingIds] end, [CachedTaskDataId], Ongoing),
    Registry#registry{ongoing = NewOngoing}.


%%%===================================================================
%%% Test API
%%%===================================================================

-spec is_empty(registry()) -> boolean().
is_empty(#registry{
    waiting = Waiting,
    ongoing = Ongoing,
    task_execution_order = Order
}) ->
    maps_utils:is_empty(Waiting) andalso maps_utils:is_empty(Ongoing) andalso lists_utils:is_empty(Order).