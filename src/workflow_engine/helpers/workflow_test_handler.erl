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

-include("http/gui_paths.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([prepare/2, get_lane_spec/3, process_item/6, process_result/4,
    handle_task_execution_ended/3, handle_lane_execution_ended/3, handle_workflow_execution_ended/2]).

%%%===================================================================
%%% API
%%%===================================================================


-spec prepare(
    workflow_engine:execution_id(),
    workflow_engine:execution_context()
) ->
    ok.
prepare(_, _) ->
    ok.

-spec get_lane_spec(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_execution_state:index()
) ->
    {ok, workflow_engine:lane_spec()}.
get_lane_spec(ExecutionId, #{type := Type, async_call_pools := Pools} =_ExecutionContext, LaneIndex) ->
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
        is_last => LaneIndex =:= 5
    }}.


-spec process_item(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:task_id(),
    iterator:item(),
    workflow_handler:finished_callback_id(),
    workflow_handler:heartbeat_callback_id()
) ->
    workflow_handler:callback_execution_result().
process_item(_ExecutionId, _Context, <<"async", _/binary>> = _TaskId, Item, FinishCallback, _) ->
    spawn(fun() ->
        timer:sleep(100), % TODO VFS-7784 - test with different sleep times
        case binary_to_integer(Item) =< 10 of
            true ->
                % Use http_client only for part of items as it is much slower than direct `handle_callback` call
                http_client:put(FinishCallback, #{}, json_utils:encode(#{<<"result">> => <<"ok">>}));
            false ->
                workflow_engine_callback_handler:handle_callback(FinishCallback, #{<<"result">> => <<"ok">>})
        end
    end),
    ok;
process_item(_ExecutionId, _Context, _TaskId, _Item, _FinishCallback, _) ->
    ok.


-spec process_result(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:task_id(),
    workflow_handler:task_processing_result()
) ->
    workflow_handler:callback_execution_result().
process_result(_, _, _, {error, _}) ->
    error;
process_result(_, _, _, #{<<"result">> := Result}) ->
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
    workflow_execution_state:index()
) ->
    ok.
handle_lane_execution_ended(_, _, _) ->
    ok.

-spec handle_workflow_execution_ended(
    workflow_engine:execution_id(),
    workflow_engine:execution_context()
) ->
    ok.
handle_workflow_execution_ended(_, _) ->
    ok.