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
-export([prepare_lane/3, restart_lane/3, process_item/6, process_result/5,
    handle_task_execution_ended/3, handle_lane_execution_ended/3, handle_workflow_execution_ended/2]).
% API
-export([get_last_lane_id/0, get_ignored_lane_id/0, get_ignored_lane_predecessor_id/0]).

-define(NEXT_LANE_ID(LaneId), integer_to_binary(binary_to_integer(LaneId) + 1)).
-define(LAST_LANE_ID, <<"5">>).
-define(PENULTIMATE_LANE_ID, <<"4">>).
-define(IGNORED_LANE_ID, 1234).
-define(IGNORED_LANE_PREDECESSOR_ID, <<"2">>).

%%%===================================================================
%%% Callbacks
%%%===================================================================

-spec prepare_lane(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:lane_id()
) ->
    workflow_handler:prepare_result().
prepare_lane(_ExecutionId, ExecutionContext, ?IGNORED_LANE_ID = LaneId) ->
    % Lane prepared in advance that will not be executed
    {ok, #{
        parallel_boxes => [],
        iterator => undefined,
        execution_context => ExecutionContext#{
            lane_index => LaneId,
            lane_id => LaneId
        }
    }};
prepare_lane(_ExecutionId, #{type := Type, async_call_pools := Pools} = ExecutionContext, LaneId) ->
    LaneIndex = binary_to_integer(LaneId),
    Boxes = lists:map(fun(BoxIndex) ->
        lists:foldl(fun(TaskIndex, TaskAcc) ->
            TaskAcc#{<<(integer_to_binary(LaneIndex))/binary, "_",
                (integer_to_binary(BoxIndex))/binary, "_", (integer_to_binary(TaskIndex))/binary>> =>
            #{type => Type, async_call_pools => Pools, keepalive_timeout => 5}}
        end, #{}, lists:seq(1, BoxIndex))
    end, lists:seq(1, LaneIndex)),

    LaneOptions = maps:get(lane_options, ExecutionContext, #{}),
    {ok, LaneOptions#{
        parallel_boxes => Boxes,
        iterator => workflow_test_iterator:get_first(maps:get(items_count, ExecutionContext, 200)),
        execution_context => ExecutionContext#{
            lane_index => LaneIndex,
            lane_id => LaneId
        }
    }}.


-spec restart_lane(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:lane_id()
) ->
    workflow_handler:prepare_result().
restart_lane(ExecutionId, ExecutionContext, LaneId) ->
    prepare_lane(ExecutionId, ExecutionContext, LaneId).


-spec process_item(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:task_id(),
    iterator:item(),
    workflow_handler:finished_callback_id(),
    workflow_handler:heartbeat_callback_id()
) ->
    workflow_handler:handler_execution_result().
process_item(_ExecutionId, #{type := async}, _TaskId, Item, FinishCallback, _) ->
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
    workflow_engine:execution_context(),
    workflow_engine:task_id(),
    iterator:item(),
    workflow_handler:async_processing_result()
) ->
    workflow_handler:handler_execution_result().
process_result(_, _, _, _, {error, _}) ->
    error;
process_result(_, _, _, _, #{<<"result">> := Result}) ->
    binary_to_atom(Result, utf8).


-spec handle_task_execution_ended(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:task_id()
) ->
    ok.
handle_task_execution_ended(_, _, _) ->
    ok.


-spec handle_lane_execution_ended(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:lane_id()
) ->
    workflow_handler:lane_ended_callback_result().
handle_lane_execution_ended(_ExecutionId, #{prepare_ignored_lane_in_advance := true}, ?IGNORED_LANE_PREDECESSOR_ID) ->
    ?CONTINUE(?NEXT_LANE_ID(?IGNORED_LANE_PREDECESSOR_ID), ?IGNORED_LANE_ID);
handle_lane_execution_ended(_ExecutionId, #{prepare_in_advance_out_of_order := {LaneId, LaneIdOutOfOrder}}, LaneId) ->
    ?CONTINUE(?NEXT_LANE_ID(LaneId), LaneIdOutOfOrder);
handle_lane_execution_ended(ExecutionId, #{repeat_lane := LaneId} = ExecutionContext, LaneId) ->
    case node_cache:get({lane_repeated, ExecutionId, LaneId}, undefined) of
        true ->
            handle_lane_execution_ended(ExecutionId, maps:remove(repeat_lane, ExecutionContext), LaneId);
        _ ->
            node_cache:put({lane_repeated, ExecutionId, LaneId}, true),
            ?CONTINUE(LaneId, ?NEXT_LANE_ID(LaneId))
    end;
handle_lane_execution_ended(ExecutionId, #{repeat_lane_and_change_next := {LaneId, NextLaneId}} = ExecutionContext, LaneId) ->
    case handle_lane_execution_ended(ExecutionId,
        (maps:remove(repeat_lane_and_change_next, ExecutionContext))#{repeat_lane => LaneId}, LaneId) of
        ?CONTINUE(LaneId, _) -> ?CONTINUE(LaneId, NextLaneId);
        Other -> Other
    end;
handle_lane_execution_ended(_ExecutionId, #{prepare_in_advance := true} = ExecutionContext, LaneId) ->
    case {maybe_finish_execution(ExecutionContext, LaneId), LaneId} of
        {?FINISH_EXECUTION, _} ->
            ?FINISH_EXECUTION;
        {continue, ?PENULTIMATE_LANE_ID} ->
            ?CONTINUE(?LAST_LANE_ID, undefined);
        _ ->
            NextLaneId = ?NEXT_LANE_ID(LaneId),
            ?CONTINUE(NextLaneId, ?NEXT_LANE_ID(NextLaneId))
    end;
handle_lane_execution_ended(_ExecutionId, ExecutionContext, LaneId) ->
    case maybe_finish_execution(ExecutionContext, LaneId) of
        ?FINISH_EXECUTION ->
            ?FINISH_EXECUTION;
        _ ->
            ?CONTINUE(?NEXT_LANE_ID(LaneId), undefined)
    end.


-spec handle_workflow_execution_ended(
    workflow_engine:execution_id(),
    workflow_engine:execution_context()
) ->
    ok.
handle_workflow_execution_ended(_, _) ->
    ok.


%%%===================================================================
%%% API
%%%===================================================================

-spec get_last_lane_id() -> workflow_engine:lane_id().
get_last_lane_id() ->
    ?LAST_LANE_ID.

-spec get_ignored_lane_id() -> workflow_engine:lane_id().
get_ignored_lane_id() ->
    ?IGNORED_LANE_ID.

-spec get_ignored_lane_predecessor_id() -> workflow_engine:lane_id().
get_ignored_lane_predecessor_id() ->
    ?IGNORED_LANE_PREDECESSOR_ID.

%%%===================================================================
%%% Internal functions
%%%===================================================================

maybe_finish_execution(ExecutionContext, LaneId) ->
    case {LaneId, ExecutionContext} of
        {?LAST_LANE_ID, _} -> ?FINISH_EXECUTION;
        {_, #{finish_on_lane := LaneId}} -> ?FINISH_EXECUTION;
        _ -> continue
    end.