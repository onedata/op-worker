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
-export([prepare_lane/3, resume_lane/3, run_task_for_item/5, process_task_result_for_item/5, report_item_error/3,
    handle_task_results_processed_for_all_items/3, process_streamed_task_data/4,
    handle_task_execution_stopped/3, handle_lane_execution_stopped/3, handle_workflow_execution_stopped/2, handle_exception/5]).
% API
-export([is_last_lane/1, get_ignored_lane_id/0, get_ignored_lane_predecessor_id/0, pack_task_id/3, decode_task_id/1]).

-define(NEXT_LANE_ID(LaneId), integer_to_binary(binary_to_integer(LaneId) + 1)).
-define(LAST_LANE_ID, <<"5">>).
-define(PENULTIMATE_LANE_ID, <<"4">>).
% Ignored lane is one that is expected to be ignored as a result of lane change by
% handle_lane_execution_stopped callback. It has no parallel boxes and iterator so execution of this lane
% results in exception. It is only possible to execute this lane preparation (also in advance).
-define(IGNORED_LANE_ID, 1234).
-define(IGNORED_LANE_PREDECESSOR_ID, <<"2">>).

%% @formatter:off
-type test_execution_context() :: #{
    lane_index => workflow_execution_state:index(),
    lane_id => workflow_engine:lane_id(),
    task_type => sync | async,
    lane_to_retry => workflow_engine:lane_id(),
    prepare_in_advance => boolean(),
    % while prepare_in_advance => true ensures that all lanes are prepared in advance, usage of following
    % options allows setting custom setting of single lane preparation in advance
    prepare_ignored_lane_in_advance => boolean(), % when ?IGNORED_LANE_PREDECESSOR_ID finishes,
                                                  % set ?IGNORED_LANE_ID to be prepared in advance
    prepare_in_advance_out_of_order => {LaneId :: workflow_engine:lane_id(),
        LaneIdOutOfOrder :: workflow_engine:lane_id()}, % when LaneId finishes, set LaneIdOutOfOrder
                                                        % to be prepared in advance
    save_progress => boolean()
}.

-type generator_options() :: #{
    async_call_pools => [workflow_async_call_pool:id()] | undefined,
    fail_iteration => ItemNum :: non_neg_integer(),
    task_streams => #{LaneIndex :: workflow_execution_state:index() => #{
        {ParallelBoxIndex :: workflow_execution_state:index(), TaskIndex :: workflow_execution_state:index()} => [
            % Data is streamed for following items
            workflow_test_iterator:item() | {workflow_test_iterator:item(), NumberOfChunks :: non_neg_integer()} |
            % Data is streamed during handle_task_results_processed_for_all_items callback execution
            handle_task_results_processed_for_all_items |
            {handle_task_results_processed_for_all_items, NumberOfChunks :: non_neg_integer()}
        ]
    }},
    item_count => non_neg_integer(),

    % Options to be used constructing context (see test_execution_context() for description)
    lane_index => workflow_execution_state:index(),
    lane_id => workflow_engine:lane_id(),
    task_type => sync | async,
    lane_to_retry => workflow_engine:lane_id(),
    prepare_in_advance => boolean(),
    prepare_ignored_lane_in_advance => boolean(),
    prepare_in_advance_out_of_order => {LaneId :: workflow_engine:lane_id(),
        LaneIdOutOfOrder :: workflow_engine:lane_id()}
}.

-export_type([test_execution_context/0, generator_options/0]).
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
                data_stream_enabled => maps:is_key({BoxIndex, TaskIndex}, TaskStreams)
            }}
        end, #{}, lists:seq(1, BoxIndex))
    end, lists:seq(1, LaneIndex)),

    ItemCount = maps:get(item_count, ExecutionContext, 200),
    Iterator = case maps:get(fail_iteration, ExecutionContext, undefined) of
        undefined -> workflow_test_iterator:initialize(ItemCount);
        ItemNumToFail -> workflow_test_iterator:initialize(ItemCount, ItemNumToFail)
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


-spec resume_lane(
    workflow_engine:execution_id(),
    test_execution_context(),
    workflow_engine:lane_id()
) ->
    workflow_handler:prepare_lane_result().
resume_lane(ExecutionId, ExecutionContext, LaneId) ->
    prepare_lane(ExecutionId, ExecutionContext, LaneId).


-spec run_task_for_item(
    workflow_engine:execution_id(),
    test_execution_context(),
    workflow_engine:task_id(),
    workflow_jobs:encoded_job_identifier(),
    iterator:item()
) ->
    ok.
run_task_for_item(ExecutionId, #{task_type := async}, _TaskId, EncodedJobIdentifier, Item) ->
    spawn(fun() ->
        timer:sleep(100), % TODO VFS-7784 - test with different sleep times
        Result = #{<<"result">> => <<"ok">>, <<"item">> => Item},
        workflow_engine:report_async_task_result(ExecutionId, EncodedJobIdentifier, Result)
    end),
    ok;
run_task_for_item(_ExecutionId, _Context, _TaskId, _EncodedJobIdentifier, _Item) ->
    ok.


-spec process_task_result_for_item(
    workflow_engine:execution_id(),
    test_execution_context(),
    workflow_engine:task_id(),
    iterator:item(),
    workflow_handler:async_processing_result()
) ->
    workflow_handler:handler_execution_result().
process_task_result_for_item(_, _, _, _, {error, _}) ->
    error;
process_task_result_for_item(_, _, _, _, #{<<"result">> := Result}) ->
    binary_to_atom(Result, utf8).


-spec report_item_error(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    iterator:item()
) ->
    ok.
report_item_error(_, _, _) ->
    ok.


-spec process_streamed_task_data(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    workflow_engine:task_id(),
    workflow_engine:streamed_task_data()
) ->
    workflow_handler:handler_execution_result().
process_streamed_task_data(_, _, _, error) ->
    error;
process_streamed_task_data(_, _, _, _) ->
    ok.


-spec handle_task_results_processed_for_all_items(
    workflow_engine:execution_id(),
    test_execution_context(),
    workflow_engine:task_id()
) ->
    ok.
handle_task_results_processed_for_all_items(_, _, _) ->
    ok.


-spec handle_task_execution_stopped(
    workflow_engine:execution_id(),
    test_execution_context(),
    workflow_engine:task_id()
) ->
    ok.
handle_task_execution_stopped(_, _, _) ->
    ok.


-spec handle_lane_execution_stopped(
    workflow_engine:execution_id(),
    test_execution_context(),
    workflow_engine:lane_id()
) ->
    workflow_handler:lane_stopped_callback_result().
handle_lane_execution_stopped(ExecutionId, #{
    lane_to_retry := ?IGNORED_LANE_PREDECESSOR_ID,
    prepare_ignored_lane_in_advance := true
} = ExecutionContext, ?IGNORED_LANE_PREDECESSOR_ID) ->
    case handle_lane_execution_stopped(ExecutionId,
        maps:remove(prepare_ignored_lane_in_advance, ExecutionContext), ?IGNORED_LANE_PREDECESSOR_ID) of
        ?CONTINUE(?IGNORED_LANE_PREDECESSOR_ID, _) -> ?CONTINUE(?IGNORED_LANE_PREDECESSOR_ID, ?IGNORED_LANE_ID);
        Other -> Other
    end;
handle_lane_execution_stopped(_ExecutionId, #{prepare_ignored_lane_in_advance := true}, ?IGNORED_LANE_PREDECESSOR_ID) ->
    ?CONTINUE(?NEXT_LANE_ID(?IGNORED_LANE_PREDECESSOR_ID), ?IGNORED_LANE_ID);
handle_lane_execution_stopped(_ExecutionId, #{prepare_in_advance_out_of_order := {LaneId, LaneIdOutOfOrder}}, LaneId) ->
    ?CONTINUE(?NEXT_LANE_ID(LaneId), LaneIdOutOfOrder);
handle_lane_execution_stopped(ExecutionId, #{lane_to_retry := LaneId} = ExecutionContext, LaneId) ->
    case node_cache:get({lane_retried, ExecutionId, LaneId}, undefined) of
        true ->
            handle_lane_execution_stopped(ExecutionId, maps:remove(lane_to_retry, ExecutionContext), LaneId);
        _ ->
            node_cache:put({lane_retried, ExecutionId, LaneId}, true),
            ?CONTINUE(LaneId, ?NEXT_LANE_ID(LaneId))
    end;
handle_lane_execution_stopped(_ExecutionId, #{prepare_in_advance := true} = ExecutionContext, LaneId) ->
    case {maybe_finish_execution(ExecutionContext, LaneId), LaneId} of
        {?END_EXECUTION, _} ->
            ?END_EXECUTION;
        {continue, ?PENULTIMATE_LANE_ID} ->
            ?CONTINUE(?LAST_LANE_ID, undefined);
        _ ->
            NextLaneId = ?NEXT_LANE_ID(LaneId),
            ?CONTINUE(NextLaneId, ?NEXT_LANE_ID(NextLaneId))
    end;
handle_lane_execution_stopped(_ExecutionId, ExecutionContext, LaneId) ->
    case maybe_finish_execution(ExecutionContext, LaneId) of
        ?END_EXECUTION ->
            ?END_EXECUTION;
        _ ->
            ?CONTINUE(?NEXT_LANE_ID(LaneId), undefined)
    end.


-spec handle_workflow_execution_stopped(
    workflow_engine:execution_id(),
    test_execution_context()
) ->
    workflow_handler:progress_data_persistence().
handle_workflow_execution_stopped(_, #{save_progress := true}) ->
    save_progress;
handle_workflow_execution_stopped(_, _) ->
    clean_progress.


- spec handle_exception(
    workflow_engine:execution_id(),
    workflow_engine:execution_context(),
    throw | error | exit,
    term(),
    list()
) ->
    save_progress.
handle_exception(_, _, _, _, _) ->
    save_progress.


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