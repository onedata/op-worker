%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Implementation of workflow_handler behaviour to be used in tests.
%%% TODO VFS-7784 - move to test directory when problem with mocks is solved
%%% @end
%%%--------------------------------------------------------------------
-module(workflow_test_handler).
-author("Michal Wrzeszcz").

-behaviour(workflow_handler).

-include("workflow_engine.hrl").
-include("http/gui_paths.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

% Callbacks
-export([prepare_lane/3, restart_lane/3, process_item/6, process_result/5, report_item_error/3,
    trigger_task_data_stream_termination/3, process_task_data/4,
    handle_task_execution_ended/3, handle_lane_execution_ended/3, handle_workflow_execution_ended/2]).
% API
-export([is_last_lane/1, get_ignored_lane_id/0, get_ignored_lane_predecessor_id/0, pack_task_id/3, decode_task_id/1]).

-define(NEXT_LANE_ID(LaneId), integer_to_binary(binary_to_integer(LaneId) + 1)).
-define(LAST_LANE_ID, <<"5">>).
-define(PENULTIMATE_LANE_ID, <<"4">>).
% Ignored lane is one that is expected to be ignored as a result of lane change by
% handle_lane_execution_ended callback. It has no parallel boxes and iterator so execution of this lane
% results in exception. It is only possible to execute this lane preparation (also in advance).
-define(IGNORED_LANE_ID, 1234).
-define(IGNORED_LANE_PREDECESSOR_ID, <<"2">>).

%% @formatter:off
-type test_execution_context() :: #{
    task_type => sync | async,
    async_call_pools => [workflow_async_call_pool:id()] | undefined,
    lane_to_retry => workflow_engine:lane_id(),
    prepare_in_advance => boolean(),
    % while prepare_in_advance => true ensures that all lanes are prepared in advance, usage of following
    % options allows setting custom setting of single lane preparation in advance
    prepare_ignored_lane_in_advance => boolean(), % when ?IGNORED_LANE_PREDECESSOR_ID finishes,
                                                  % set ?IGNORED_LANE_ID to be prepared in advance
    prepare_in_advance_out_of_order => {LaneId :: workflow_engine:lane_id(),
        LaneIdOutOfOrder :: workflow_engine:lane_id()}, % when LaneId finishes, set LaneIdOutOfOrder
                                                       % to be prepared in advance
    fail_iteration => ItemNum :: non_neg_integer()
}.

-export_type([test_execution_context/0]).
%% @formatter:on

%%%===================================================================
%%% Callbacks
%%%===================================================================

-spec prepare_lane(
    workflow_engine:execution_id(),
    test_execution_context(),
    workflow_engine:lane_id()
) ->
    workflow_handler:prepare_lane_result().
prepare_lane(_ExecutionId, ExecutionContext, ?IGNORED_LANE_ID = LaneId) ->
    {ok, #{
        parallel_boxes => [],
        iterator => undefined,
        execution_context => ExecutionContext#{
            lane_index => LaneId,
            lane_id => LaneId
        }
    }};
prepare_lane(_ExecutionId, #{task_type := Type, async_call_pools := Pools} = ExecutionContext, LaneId) ->
    LaneIndex = binary_to_integer(LaneId),
    TaskStreams = maps:get(LaneIndex, maps:get(task_streams, ExecutionContext, #{}), #{}),
    Boxes = lists:map(fun(BoxIndex) ->
        lists:foldl(fun(TaskIndex, TaskAcc) ->
            TaskAcc#{pack_task_id(LaneIndex, BoxIndex, TaskIndex) => #{
                type => Type,
                async_call_pools => Pools,
                keepalive_timeout => 5,
                has_task_data_stream => maps:is_key({BoxIndex, TaskIndex}, TaskStreams)
            }}
        end, #{}, lists:seq(1, BoxIndex))
    end, lists:seq(1, LaneIndex)),

    ItemCount = maps:get(item_count, ExecutionContext, 200),
    Iterator = case maps:get(fail_iteration, ExecutionContext, undefined) of
        undefined -> workflow_test_iterator:get_first(ItemCount);
        ItemNumToFail -> workflow_test_iterator:get_first(ItemCount, ItemNumToFail)
    end,

    LaneOptions = maps:get(lane_options, ExecutionContext, #{}),
    {ok, LaneOptions#{
        parallel_boxes => Boxes,
        iterator => Iterator,
        execution_context => ExecutionContext#{
            lane_index => LaneIndex,
            lane_id => LaneId
        }
    }}.


-spec restart_lane(
    workflow_engine:execution_id(),
    test_execution_context(),
    workflow_engine:lane_id()
) ->
    workflow_handler:prepare_lane_result().
restart_lane(ExecutionId, ExecutionContext, LaneId) ->
    prepare_lane(ExecutionId, ExecutionContext, LaneId).


-spec process_item(
    workflow_engine:execution_id(),
    test_execution_context(),
    workflow_engine:task_id(),
    iterator:item(),
    workflow_handler:finished_callback_id(),
    workflow_handler:heartbeat_callback_id()
) ->
    workflow_handler:handler_execution_result().
process_item(_ExecutionId, #{task_type := async}, _TaskId, Item, FinishCallback, _) ->
    spawn(fun() ->
        timer:sleep(100), % TODO VFS-7784 - test with different sleep times
        Result = #{<<"result">> => <<"ok">>, <<"item">> => Item},
        case binary_to_integer(Item) =< 10 of
            true ->
                % Use http_client only for part of items as it is much slower than direct `handle_callback` call
                http_client:put(FinishCallback, #{}, json_utils:encode(Result));
            false ->
                workflow_engine_callback_handler:handle_callback(FinishCallback, Result)
        end
    end),
    ok;
process_item(_ExecutionId, _Context, _TaskId, _Item, _FinishCallback, _) ->
    ok.


-spec process_result(
    workflow_engine:execution_id(),
    test_execution_context(),
    workflow_engine:task_id(),
    iterator:item(),
    workflow_handler:async_processing_result()
) ->
    workflow_handler:handler_execution_result().
process_result(_, _, _, _, {error, _}) ->
    error;
process_result(_, _, _, _, #{<<"result">> := Result}) ->
    binary_to_atom(Result, utf8).


-spec report_item_error(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    iterator:item()
) ->
    ok.
report_item_error(_, _, _) ->
    ok.


-spec trigger_task_data_stream_termination(
    workflow_engine:execution_id(),
    test_execution_context(),
    workflow_engine:task_id()
) ->
    ok.
trigger_task_data_stream_termination(_, _, _) ->
    ok.


-spec process_task_data(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:task_id(),
    workflow_engine:task_stream_data()
) ->
    workflow_handler:handler_execution_result().
process_task_data(_, _, _, error) ->
    error;
process_task_data(_, _, _, _) ->
    ok.

-spec handle_task_execution_ended(
    workflow_engine:execution_id(),
    test_execution_context(),
    workflow_engine:task_id()
) ->
    ok.
handle_task_execution_ended(_, _, _) ->
    ok.


-spec handle_lane_execution_ended(
    workflow_engine:execution_id(),
    test_execution_context(),
    workflow_engine:lane_id()
) ->
    workflow_handler:lane_ended_callback_result().
handle_lane_execution_ended(ExecutionId, #{
    lane_to_retry := ?IGNORED_LANE_PREDECESSOR_ID,
    prepare_ignored_lane_in_advance := true
} = ExecutionContext, ?IGNORED_LANE_PREDECESSOR_ID) ->
    case handle_lane_execution_ended(ExecutionId,
        maps:remove(prepare_ignored_lane_in_advance, ExecutionContext), ?IGNORED_LANE_PREDECESSOR_ID) of
        ?CONTINUE(?IGNORED_LANE_PREDECESSOR_ID, _) -> ?CONTINUE(?IGNORED_LANE_PREDECESSOR_ID, ?IGNORED_LANE_ID);
        Other -> Other
    end;
handle_lane_execution_ended(_ExecutionId, #{prepare_ignored_lane_in_advance := true}, ?IGNORED_LANE_PREDECESSOR_ID) ->
    ?CONTINUE(?NEXT_LANE_ID(?IGNORED_LANE_PREDECESSOR_ID), ?IGNORED_LANE_ID);
handle_lane_execution_ended(_ExecutionId, #{prepare_in_advance_out_of_order := {LaneId, LaneIdOutOfOrder}}, LaneId) ->
    ?CONTINUE(?NEXT_LANE_ID(LaneId), LaneIdOutOfOrder);
handle_lane_execution_ended(ExecutionId, #{lane_to_retry := LaneId} = ExecutionContext, LaneId) ->
    case node_cache:get({lane_retried, ExecutionId, LaneId}, undefined) of
        true ->
            handle_lane_execution_ended(ExecutionId, maps:remove(lane_to_retry, ExecutionContext), LaneId);
        _ ->
            node_cache:put({lane_retried, ExecutionId, LaneId}, true),
            ?CONTINUE(LaneId, ?NEXT_LANE_ID(LaneId))
    end;
handle_lane_execution_ended(_ExecutionId, #{prepare_in_advance := true} = ExecutionContext, LaneId) ->
    case {maybe_finish_execution(ExecutionContext, LaneId), LaneId} of
        {?END_EXECUTION, _} ->
            ?END_EXECUTION;
        {continue, ?PENULTIMATE_LANE_ID} ->
            ?CONTINUE(?LAST_LANE_ID, undefined);
        _ ->
            NextLaneId = ?NEXT_LANE_ID(LaneId),
            ?CONTINUE(NextLaneId, ?NEXT_LANE_ID(NextLaneId))
    end;
handle_lane_execution_ended(_ExecutionId, ExecutionContext, LaneId) ->
    case maybe_finish_execution(ExecutionContext, LaneId) of
        ?END_EXECUTION ->
            ?END_EXECUTION;
        _ ->
            ?CONTINUE(?NEXT_LANE_ID(LaneId), undefined)
    end.


-spec handle_workflow_execution_ended(
    workflow_engine:execution_id(),
    test_execution_context()
) ->
    ok.
handle_workflow_execution_ended(_, _) ->
    ok.


%%%===================================================================
%%% API
%%%===================================================================

-spec is_last_lane(workflow_engine:lane_id()) -> boolean().
is_last_lane(LaneId) ->
    LaneId =:= ?LAST_LANE_ID.

-spec get_ignored_lane_id() -> workflow_engine:lane_id().
get_ignored_lane_id() ->
    ?IGNORED_LANE_ID.

-spec get_ignored_lane_predecessor_id() -> workflow_engine:lane_id().
get_ignored_lane_predecessor_id() ->
    ?IGNORED_LANE_PREDECESSOR_ID.

pack_task_id(LaneIndex, BoxIndex, TaskIndex) ->
    <<(integer_to_binary(LaneIndex))/binary, "_",
        (integer_to_binary(BoxIndex))/binary, "_", (integer_to_binary(TaskIndex))/binary>>.

decode_task_id(TaskId) ->
    [LaneIndexBin, BoxIndexBin, TaskIndexBin] = binary:split(TaskId, <<"_">>, [global]),
    {binary_to_integer(LaneIndexBin), binary_to_integer(BoxIndexBin), binary_to_integer(TaskIndexBin)}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

maybe_finish_execution(ExecutionContext, LaneId) ->
    case {LaneId, ExecutionContext} of
        {?LAST_LANE_ID, _} -> ?END_EXECUTION;
        {_, #{finish_on_lane := LaneId}} -> ?END_EXECUTION;
        _ -> continue
    end.