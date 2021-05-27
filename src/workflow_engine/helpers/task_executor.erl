%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Just a mock module for test.
%%% @end
%%%-------------------------------------------------------------------
-module(task_executor).
-author("Michal Wrzeszcz").

-include("workflow_engine.hrl").

%% API
-export([get_calls_counter_id/2,
    process_item/4, process_async_result/5, check_ongoing_item_processing/2,
    get_task_description/1]).

-type task_id() :: binary().
-type async_ref() :: reference() | error_mock.
-type result() :: ok | error | {async, task_id(), async_ref(), time:seconds()}.

-export_type([task_id/0, result/0, async_ref/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_calls_counter_id(workflow_engine:execution_id(), task_id()) -> workflow_async_call_pool:id() | undefined.
get_calls_counter_id(<<"NOT_LIMITED_ENGINE">>, _TaskId) ->
    undefined;
get_calls_counter_id(_ExecutionId, _TaskId) ->
    ?DEFAULT_ASYNC_CALL_POOL_ID.

% TODO VFS-7551 return also item to be used by next parallel box (bulk item without elements that returned errors)
-spec process_item(workflow_engine:id(), workflow_engine:execution_id(), task_id(), workflow_store:item()) -> result().
process_item(EngineId, ExecutionId, <<"async", _/binary>> = TaskId, Item) ->
    Ref = make_ref(),
    spawn(fun() ->
        timer:sleep(50), % TODO VFS-7551 - test with different sleep times
        ?MODULE:process_async_result(ExecutionId, EngineId, TaskId, Item, Ref)
    end),
    {async, TaskId, Ref, 30};
process_item(_EngineId, _ExecutionId, _TaskId, _Item) ->
    ok.

-spec process_async_result(workflow_engine:id(), workflow_engine:execution_id(), task_id(),
    workflow_store:item(), async_ref()) -> ok.
process_async_result(ExecutionId, EngineId, TaskId, _Item, Ref) ->
    workflow_engine:report_execution_finish(ExecutionId, EngineId, TaskId, Ref, ok).

-spec check_ongoing_item_processing(task_id(), async_ref()) -> ok | error.
check_ongoing_item_processing(_TaskId, error_mock) ->
    error;
check_ongoing_item_processing(_TaskId, _AsyncRef) ->
    ok. % NOTE - if task processing ended, ok should be return until processing finish is not reported

-spec get_task_description(task_id()) -> binary() | atom() | iolist().
get_task_description(TaskId) ->
    TaskId.