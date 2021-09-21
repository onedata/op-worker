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

-export([prepare_lane/3, restart_lane/3, process_item/6, process_result/5,
    handle_task_execution_ended/3, handle_lane_execution_ended/3, handle_workflow_execution_ended/2]).

%%%===================================================================
%%% API
%%%===================================================================


-spec prepare_lane(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:lane_id()
) ->
    workflow_handler:prepare_result().
prepare_lane(ExecutionId, #{type := Type, async_call_pools := Pools} = ExecutionContext, LaneId) ->
    LaneIndex = binary_to_integer(LaneId),
    Boxes = lists:map(fun(BoxIndex) ->
        lists:foldl(fun(TaskIndex, TaskAcc) ->
            TaskAcc#{<<ExecutionId/binary, "_task", (integer_to_binary(LaneIndex))/binary, "_",
                (integer_to_binary(BoxIndex))/binary, "_", (integer_to_binary(TaskIndex))/binary>> =>
            #{type => Type, async_call_pools => Pools, keepalive_timeout => 10}}
        end, #{}, lists:seq(1, BoxIndex))
    end, lists:seq(1, LaneIndex)),

    {ok, #{
        parallel_boxes => Boxes,
        iterator => workflow_test_iterator:get_first(),
        execution_context => ExecutionContext#{lane_index => LaneIndex}
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
process_item(_ExecutionId, _Context, <<"async", _/binary>> = _TaskId, Item, FinishCallback, _) ->
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
handle_lane_execution_ended(_ExecutionId, _ExecutionContext, LaneId) ->
    case LaneId of
        <<"5">> ->
            ?FINISH_EXECUTION;
        _ ->
            LaneIndex = binary_to_integer(LaneId),
            ?CONTINUE(integer_to_binary(LaneIndex + 1), undefined)
    end.


-spec handle_workflow_execution_ended(
    workflow_engine:execution_id(),
    workflow_engine:execution_context()
) ->
    ok.
handle_workflow_execution_ended(_, _) ->
    ok.