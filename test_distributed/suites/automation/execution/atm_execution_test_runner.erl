%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(atm_execution_test_runner).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/automation/automation.hrl").

%% API
-export([init/1, teardown/1, run/7]).


-type reply_to() :: {pid(), reference()}.


-define(TEST_PROC_PID_KEY(__ATM_WORKFLOW_EXECUTION_ID),
    {atm_test_runner_process, __ATM_WORKFLOW_EXECUTION_ID}
).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(oct_background:entity_selector() | [oct_background:entity_selector()]) ->
    ok.
init(ProviderSelectors) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),
    mock_workflow_execution_factory(Workers),
    mock_workflow_execution_handler(Workers).


-spec teardown(oct_background:entity_selector() | [oct_background:entity_selector()]) ->
    ok.
teardown(ProviderSelectors) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),
    unmock_workflow_execution_factory(Workers),
    unmock_workflow_execution_handler(Workers).


run(
    ProviderPlaceholder,
    UserPlaceholder,
    SpacePlaceholder,
    AtmWorkflowSchemaId,
    AtmWorkflowSchemaRevisionNum,
    StoreInitialValues,
    CallbackUrl
) ->
    SessionId = oct_background:get_user_session_id(UserPlaceholder, ProviderPlaceholder),
    SpaceId = oct_background:get_space_id(SpacePlaceholder),

    Res = opt_atm:schedule_workflow_execution(
        krakow, SessionId, SpaceId, AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum,
        StoreInitialValues#{test_process => self()}, CallbackUrl
    ),
    ct:pal("~n~n~p~n~n", [Res]),

    heh().


heh() ->
    receive {ReplyTo, Msg} ->
        ct:pal("~n~n~p~n~n", [Msg]),
        reply_to_execution_process(ReplyTo, ok),
        heh()
    after timer:seconds(3) ->
        ok
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_nodes([oct_background:entity_selector()]) -> [node()].
get_nodes(ProviderSelectors) ->
    lists:flatmap(fun(ProviderSelector) ->
        oct_background:get_provider_nodes(ProviderSelector)
    end, ProviderSelectors).


%% @private
-spec mock_workflow_execution_factory([node()]) -> ok.
mock_workflow_execution_factory(Workers) ->
    test_utils:mock_new(Workers, atm_workflow_execution_factory, [passthrough, no_history]),

    test_utils:mock_expect(Workers, atm_workflow_execution_factory, create, fun(
        UserCtx,
        SpaceId,
        AtmWorkflowSchemaId,
        AtmWorkflowSchemaRevisionNum,
        StoreInitialValues,
        CallbackUrl
    ) ->
        Result = {#document{key = AtmWorkflowExecutionId}, _} = meck:passthrough([
            UserCtx, SpaceId, AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum,
            StoreInitialValues, CallbackUrl
        ]),
        case maps:get(test_process, StoreInitialValues, undefined) of
            undefined -> ok;
            TestProcPid -> node_cache:put(?TEST_PROC_PID_KEY(AtmWorkflowExecutionId), TestProcPid)
        end,
        Result
    end).


%% @private
-spec unmock_workflow_execution_factory([node()]) -> ok.
unmock_workflow_execution_factory(Workers) ->
    test_utils:mock_unload(Workers, atm_workflow_execution_factory).


%% @private
-spec mock_workflow_execution_handler([node()]) -> ok.
mock_workflow_execution_handler(Workers) ->
    test_utils:mock_new(Workers, atm_workflow_execution_handler, [passthrough, no_history]),

    mock_workflow_execution_handler_callback_function(Workers, prepare_lane, 3),
    mock_workflow_execution_handler_callback_function(Workers, process_item, 6),
    mock_workflow_execution_handler_callback_function(Workers, process_result, 5),
    mock_workflow_execution_handler_callback_function(Workers, report_item_error, 3),
    mock_workflow_execution_handler_callback_function(Workers, handle_task_execution_ended, 3),
    mock_workflow_execution_handler_callback_function(Workers, handle_lane_execution_ended, 3),
    mock_workflow_execution_handler_callback_function(Workers, handle_workflow_execution_ended, 2).


%% @private
-spec unmock_workflow_execution_handler([node()]) -> ok.
unmock_workflow_execution_handler(Workers) ->
    test_utils:mock_unload(Workers, atm_workflow_execution_handler).


%% @private
-spec mock_workflow_execution_handler_callback_function([node()], atom(), 1..6) -> ok.
mock_workflow_execution_handler_callback_function(Workers, FunName, FunArity) ->
    MockFun = build_workflow_execution_handler_callback_function_mock(FunArity, FunName),
    test_utils:mock_expect(Workers, atm_workflow_execution_handler, FunName, MockFun).


%% @private
-spec build_workflow_execution_handler_callback_function_mock(1..6, atom()) ->
    function().
build_workflow_execution_handler_callback_function_mock(1, Label) ->
    fun(Arg1) ->
        Args = [Arg1],
        exec_origin_fun_with_hooks(hd(Args), Label, Args)
    end;

build_workflow_execution_handler_callback_function_mock(2, Label) ->
    fun(Arg1, Arg2) ->
        Args = [Arg1, Arg2],
        exec_origin_fun_with_hooks(hd(Args), Label, Args)
    end;

build_workflow_execution_handler_callback_function_mock(3, Label) ->
    fun(Arg1, Arg2, Arg3) ->
        Args = [Arg1, Arg2, Arg3],
        exec_origin_fun_with_hooks(hd(Args), Label, Args)
    end;

build_workflow_execution_handler_callback_function_mock(4, Label) ->
    fun(Arg1, Arg2, Arg3, Arg4) ->
        Args = [Arg1, Arg2, Arg3, Arg4],
        exec_origin_fun_with_hooks(hd(Args), Label, Args)
    end;

build_workflow_execution_handler_callback_function_mock(5, Label) ->
    fun(Arg1, Arg2, Arg3, Arg4, Arg5) ->
        Args = [Arg1, Arg2, Arg3, Arg4, Arg5],
        exec_origin_fun_with_hooks(hd(Args), Label, Args)
    end;

build_workflow_execution_handler_callback_function_mock(6, Label) ->
    fun(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6) ->
        Args = [Arg1, Arg2, Arg3, Arg4, Arg5, Arg6],
        exec_origin_fun_with_hooks(hd(Args), Label, Args)
    end.


%% @private
-spec exec_origin_fun_with_hooks(atm_workflow_execution:id(), atom(), [term()]) -> term().
exec_origin_fun_with_hooks(AtmWorkflowExecutionId, Label, Args) ->
    case node_cache:get(?TEST_PROC_PID_KEY(AtmWorkflowExecutionId), undefined) of
        undefined ->
            meck:passthrough(Args);
        TestProcPid ->
            {ok, proceed} = call_test_process(TestProcPid, {pre, Label, Args}),
            Result = meck:passthrough(Args),
            {ok, proceed} = call_test_process(TestProcPid, {post, Label, Args}),
            Result
    end.


%% @private
-spec call_test_process(pid(), term()) -> {ok, term()} | no_return().
call_test_process(TestProcPid, Msg) ->
    MRef = erlang:monitor(process, TestProcPid),
    TestProcPid ! {{self(), MRef}, Msg},
    receive
        {MRef, Reply} ->
            erlang:demonitor(MRef, [flush]),
            {ok, Reply};
        {'DOWN', MRef, _, _, Reason} ->
            exit(Reason)
    end.


%% @private
-spec reply_to_execution_process(reply_to(), term()) -> ok.
reply_to_execution_process({ExecutionProcPid, MRef}, Reply) ->
    ExecutionProcPid ! {MRef, Reply}.
