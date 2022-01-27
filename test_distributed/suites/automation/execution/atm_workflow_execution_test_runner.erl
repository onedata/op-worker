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
-module(atm_workflow_execution_test_runner).
-author("Bartosz Walkowicz").

-include("atm_workflow_exeuction_test_runner.hrl").
-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([init/1, teardown/1]).
-export([run/1]).


-type hook_msg() :: {pre | post, Function :: atom(), CallArgs :: [term()]}.
-type hook_call_ctx() :: #atm_hook_call_ctx{}.
-type hook() :: fun((hook_call_ctx()) -> {
    proceed | {return_error, errors:error()},
    no_change | atm_test_workflow_execution_model:model()
}).

-type lane_run_test_spec() :: #atm_lane_run_execution_test_spec{}.
-type incarnation_test_spec() :: #atm_workflow_execution_incarnation_test_spec{}.
-type test_spec() :: #atm_workflow_execution_test_spec{}.

-export_type([hook_call_ctx/0, hook/0]).
-export_type([lane_run_test_spec/0, incarnation_test_spec/0, test_spec/0]).

-record(state, {
    test_spec :: test_spec(),
    workflow_execution_id :: atm_workflow_execution:id(),
    current_lane_index :: atm_lane_execution:index(),
    current_run_num :: atm_lane_execution:run_num(),
    workflow_execution_model,
    ongoing_incarnations :: [incarnation_test_spec()]
}).
-type state() :: #state{}.

-type reply_to() :: {pid(), reference()}.


-define(TEST_PROC_PID_KEY(__ATM_WORKFLOW_EXECUTION_ID),
    {atm_test_runner_process, __ATM_WORKFLOW_EXECUTION_ID}
).
-define(ATM_WORKFLOW_EXECUTION_ID_MSG(__ATM_WORKFLOW_EXECUTION_ID),
    {atm_workflow_execution_id, __ATM_WORKFLOW_EXECUTION_ID}
).

%% TODO -replace with default implementations for each hook
-define(TMP_HOOK(__MSG), fun(_) ->
    ct:pal("~n~n~p~n~n", [Msg]),
    {proceed, no_change}
end).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(oct_background:entity_selector() | [oct_background:entity_selector()]) ->
    ok.
init(ProviderSelectors) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),
    mock_workflow_execution_factory(Workers),
    mock_workflow_execution_handler(Workers),
    mock_lane_execution_factory(Workers).


-spec teardown(oct_background:entity_selector() | [oct_background:entity_selector()]) ->
    ok.
teardown(ProviderSelectors) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),
    unmock_workflow_execution_factory(Workers),
    unmock_workflow_execution_handler(Workers),
    unmock_lane_execution_factory(Workers).


-spec run(test_spec()) -> ok | no_return().
run(TestSpec = #atm_workflow_execution_test_spec{
    provider = ProviderSelector,
    user = UserSelector,
    space = SpaceSelector,
    workflow_schema_alias = AtmWorkflowSchemaAlias,
    workflow_schema_revision_num = AtmWorkflowSchemaRevisionNum,
    store_initial_values = StoreInitialValues,
    callback_url = CallbackUrl,
    incarnations = Incarnations
}) ->
    SessionId = oct_background:get_user_session_id(UserSelector, ProviderSelector),
    SpaceId = oct_background:get_space_id(SpaceSelector),

    AtmWorkflowSchemaId = atm_test_inventory:get_workflow_schema_id(AtmWorkflowSchemaAlias),

    {ok, {AtmWorkflowExecutionId, _}} = ?assertMatch({ok, _}, opt_atm:schedule_workflow_execution(
        ProviderSelector, SessionId, SpaceId, AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum,
        StoreInitialValues#{test_process => self()}, CallbackUrl
    )),

    {ok, AtmLaneSchemasJson} = json_utils:query(
        atm_test_inventory:get_workflow_schema_json(AtmWorkflowSchemaAlias),
        ?JSON_PATH(<<"revisionRegistry.1.lanes">>)
    ),
    AtmLaneSchemas = jsonable_record:list_from_json(AtmLaneSchemasJson, atm_lane_schema),

    AtmWorkflowExecutionModel = atm_test_workflow_execution_model:build(
        SpaceId, global_clock:timestamp_seconds(), AtmLaneSchemas
    ),
    atm_test_workflow_execution_model:assert_match_with_backend(
        ProviderSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionModel
    ),

    monitor_workflow_execution(#state{
        test_spec = TestSpec,
        workflow_execution_id = AtmWorkflowExecutionId,
        current_lane_index = 1,
        current_run_num = 1,
        workflow_execution_model = AtmWorkflowExecutionModel,
        ongoing_incarnations = Incarnations
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec monitor_workflow_execution(state()) -> ok | no_return().
monitor_workflow_execution(State) ->
    receive {ReplyTo, HookMsg} ->
        Hook = get_hook(HookMsg, State),
        {HookResponse, NewModel} = Hook(build_hook_call_ctx(HookMsg, State)),

        NewState1 = ensure_proper_current_lane_run(HookMsg, State),
        NewState2 = ensure_actual_workflow_execution_model(NewModel, NewState1),

        reply_to_execution_process(ReplyTo, HookResponse),

        case NewState2#state.ongoing_incarnations of
            [] ->
                ok;
            [_ | _] ->
                monitor_workflow_execution(NewState2)
        end
    after timer:seconds(30) ->
        %% TODO fail??
        ok
    end.


%% @private
-spec get_hook(hook_msg(), state()) -> hook().
get_hook(Msg = {pre, prepare_lane, [_, _, {AtmLaneIndex, _}]}, State) ->
    case get_lane_run_test_spec(AtmLaneIndex, State) of
        #atm_lane_run_execution_test_spec{pre_prepare_lane_hook = default} ->
            ?TMP_HOOK(Msg);
        #atm_lane_run_execution_test_spec{pre_prepare_lane_hook = Fun} ->
            Fun
    end;

get_hook(Msg = {pre, create_run, [{AtmLaneIndex, _}, _, _]}, State) ->
    case get_lane_run_test_spec(AtmLaneIndex, State) of
        #atm_lane_run_execution_test_spec{pre_create_run_hook = default} ->
            ?TMP_HOOK(Msg);
        #atm_lane_run_execution_test_spec{pre_create_run_hook = Fun} ->
            Fun
    end;

get_hook(Msg = {post, create_run, [{AtmLaneIndex, _}, _, _]}, State) ->
    case get_lane_run_test_spec(AtmLaneIndex, State) of
        #atm_lane_run_execution_test_spec{post_create_run_hook = default} ->
            ?TMP_HOOK(Msg);
        #atm_lane_run_execution_test_spec{post_create_run_hook = Fun} ->
            Fun
    end;

get_hook(Msg = {post, prepare_lane, [_, _, {AtmLaneIndex, _}]}, State) ->
    case get_lane_run_test_spec(AtmLaneIndex, State) of
        #atm_lane_run_execution_test_spec{post_prepare_lane_hook = default} ->
            ?TMP_HOOK(Msg);
        #atm_lane_run_execution_test_spec{post_prepare_lane_hook = Fun} ->
            Fun
    end;

get_hook(Msg = {pre, process_item, _}, State) ->
    case get_current_lane_run_test_spec(State) of
        #atm_lane_run_execution_test_spec{pre_process_item_hook = default} ->
            ?TMP_HOOK(Msg);
        #atm_lane_run_execution_test_spec{pre_process_item_hook = Fun} ->
            Fun
    end;

get_hook(Msg = {post, process_item, _}, State) ->
    case get_current_lane_run_test_spec(State) of
        #atm_lane_run_execution_test_spec{post_process_item_hook = default} ->
            ?TMP_HOOK(Msg);
        #atm_lane_run_execution_test_spec{post_process_item_hook = Fun} ->
            Fun
    end;

get_hook(Msg = {pre, process_result, _}, State) ->
    case get_current_lane_run_test_spec(State) of
        #atm_lane_run_execution_test_spec{pre_process_result_hook = default} ->
            ?TMP_HOOK(Msg);
        #atm_lane_run_execution_test_spec{pre_process_result_hook = Fun} ->
            Fun
    end;

get_hook(Msg = {post, process_result, _}, State) ->
    case get_current_lane_run_test_spec(State) of
        #atm_lane_run_execution_test_spec{post_process_result_hook = default} ->
            ?TMP_HOOK(Msg);
        #atm_lane_run_execution_test_spec{post_process_result_hook = Fun} ->
            Fun
    end;

get_hook(Msg = {pre, report_item_error, _}, State) ->
    case get_current_lane_run_test_spec(State) of
        #atm_lane_run_execution_test_spec{pre_report_item_error_hook = default} ->
            ?TMP_HOOK(Msg);
        #atm_lane_run_execution_test_spec{pre_report_item_error_hook = Fun} ->
            Fun
    end;

get_hook(Msg = {post, report_item_error, _}, State) ->
    case get_current_lane_run_test_spec(State) of
        #atm_lane_run_execution_test_spec{post_report_item_error_hook = default} ->
            ?TMP_HOOK(Msg);
        #atm_lane_run_execution_test_spec{post_report_item_error_hook = Fun} ->
            Fun
    end;

get_hook(Msg = {pre, handle_task_execution_ended, _}, State) ->
    case get_current_lane_run_test_spec(State) of
        #atm_lane_run_execution_test_spec{pre_handle_task_execution_ended_hook = default} ->
            ?TMP_HOOK(Msg);
        #atm_lane_run_execution_test_spec{pre_handle_task_execution_ended_hook = Fun} ->
            Fun
    end;

get_hook(Msg = {post, handle_task_execution_ended, _}, State) ->
    case get_current_lane_run_test_spec(State) of
        #atm_lane_run_execution_test_spec{post_handle_task_execution_ended_hook = default} ->
            ?TMP_HOOK(Msg);
        #atm_lane_run_execution_test_spec{post_handle_task_execution_ended_hook = Fun} ->
            Fun
    end;

get_hook(Msg = {pre, handle_lane_execution_ended, _}, State) ->
    case get_current_lane_run_test_spec(State) of
        #atm_lane_run_execution_test_spec{pre_handle_lane_execution_ended_hook = default} ->
            ?TMP_HOOK(Msg);
        #atm_lane_run_execution_test_spec{pre_handle_lane_execution_ended_hook = Hook} ->
            Hook
    end;

get_hook(Msg = {post, handle_lane_execution_ended, _}, State) ->
    case get_current_lane_run_test_spec(State) of
        #atm_lane_run_execution_test_spec{post_handle_lane_execution_ended_hook = default} ->
            ?TMP_HOOK(Msg);
        #atm_lane_run_execution_test_spec{post_handle_lane_execution_ended_hook = Hook} ->
            Hook
    end;

get_hook(Msg = {pre, handle_workflow_execution_ended, _}, #state{ongoing_incarnations = [
    #atm_workflow_execution_incarnation_test_spec{
        pre_handle_workflow_execution_ended_hook = default
    } | _
]}) ->
    ?TMP_HOOK(Msg);

get_hook({pre, handle_workflow_execution_ended, _}, #state{ongoing_incarnations = [
    #atm_workflow_execution_incarnation_test_spec{
        pre_handle_workflow_execution_ended_hook = Hook
    } | _
]}) ->
    Hook;

get_hook(Msg = {post, handle_workflow_execution_ended, _}, #state{ongoing_incarnations = [
    #atm_workflow_execution_incarnation_test_spec{
        post_handle_workflow_execution_ended_hook = default
    } | _
]}) ->
    ?TMP_HOOK(Msg);

get_hook({post, handle_workflow_execution_ended, _}, #state{ongoing_incarnations = [
    #atm_workflow_execution_incarnation_test_spec{
        post_handle_workflow_execution_ended_hook = Hook
    } | _
]}) ->
    Hook.


%% @private
-spec get_current_lane_run_test_spec(state()) -> lane_run_test_spec().
get_current_lane_run_test_spec(#state{ongoing_incarnations = [
    #atm_workflow_execution_incarnation_test_spec{lane_runs = [
        OngoingLaneRun | _
    ]} | _
]}) ->
    OngoingLaneRun.


%% @private
-spec get_lane_run_test_spec(atm_lane_execution:index(), state()) -> lane_run_test_spec().
get_lane_run_test_spec(TargetAtmLaneIndex, #state{ongoing_incarnations = [
    #atm_workflow_execution_incarnation_test_spec{lane_runs = LaneRunTestSpecs} | _
]}) ->
    hd(lists:dropwhile(fun(#atm_lane_run_execution_test_spec{selector = {AtmLaneIndex, _}}) ->
        AtmLaneIndex < TargetAtmLaneIndex
    end, LaneRunTestSpecs)).


%% @private
-spec build_hook_call_ctx(hook_msg(), state()) -> hook_call_ctx().
build_hook_call_ctx({_, _, CallArgs}, #state{
    workflow_execution_id = AtmWorkflowExecutionId,
    current_lane_index = CurrentAtmLaneIndex,
    current_run_num = CurrentRunNum
}) ->
    #atm_hook_call_ctx{
        workflow_execution_id = AtmWorkflowExecutionId,
        current_lane_index = CurrentAtmLaneIndex,
        current_run_num = CurrentRunNum,
        call_args = CallArgs
    }.


%% @private
-spec ensure_proper_current_lane_run(hook_msg(), state()) -> state().
ensure_proper_current_lane_run(
    {post, handle_workflow_execution_ended, _Args},
    State = #state{ongoing_incarnations = [_]}  %% last incarnation ended
) ->
    State#state{ongoing_incarnations = []};

ensure_proper_current_lane_run(
    {post, handle_workflow_execution_ended, _Args},
    State = #state{ongoing_incarnations = [_ | LeftoverIncarnations]}
) ->
    #atm_workflow_execution_incarnation_test_spec{
        lane_runs = [#atm_lane_run_execution_test_spec{
            selector = {AtmLaneIndex, AtmRunNum}
        } | _]
    } = hd(LeftoverIncarnations),

    State#state{
        current_lane_index = AtmLaneIndex,
        current_run_num = AtmRunNum,
        ongoing_incarnations = LeftoverIncarnations
    };

ensure_proper_current_lane_run(
    {post, handle_lane_execution_ended, _Args},
    State = #state{ongoing_incarnations = [OngoingIncarnation | LeftoverIncarnations]}
) ->
    case OngoingIncarnation#atm_workflow_execution_incarnation_test_spec.lane_runs of
        [_] ->
            NewOngoingIncarnation = OngoingIncarnation#atm_workflow_execution_incarnation_test_spec{
                lane_runs = []
            },
            State#state{ongoing_incarnations = [NewOngoingIncarnation | LeftoverIncarnations]};
        [_ | LeftoverLaneRuns] ->
            #atm_lane_run_execution_test_spec{selector = {AtmLaneIndex, AtmRunNum}} = hd(
                LeftoverLaneRuns
            ),
            NewOngoingIncarnation = OngoingIncarnation#atm_workflow_execution_incarnation_test_spec{
                lane_runs = LeftoverLaneRuns
            },
            State#state{
                current_lane_index = AtmLaneIndex,
                current_run_num = AtmRunNum,
                ongoing_incarnations = [NewOngoingIncarnation | LeftoverIncarnations]
            }
    end;

ensure_proper_current_lane_run(_, State) ->
    State.


%% @private
-spec ensure_actual_workflow_execution_model(
    no_change | atm_test_workflow_execution_model:model(),
    state()
) ->
    state().
ensure_actual_workflow_execution_model(no_change, State) ->
    State;

ensure_actual_workflow_execution_model(NewAtmWorkflowExecutionModel, State = #state{
    test_spec = #atm_workflow_execution_test_spec{
        provider = ProviderSelector
    },
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    atm_test_workflow_execution_model:assert_match_with_backend(
        ProviderSelector, AtmWorkflowExecutionId, NewAtmWorkflowExecutionModel
    ),
    State#state{workflow_execution_model = NewAtmWorkflowExecutionModel}.


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
-spec mock_lane_execution_factory([node()]) -> ok.
mock_lane_execution_factory(Workers) ->
    test_utils:mock_new(Workers, atm_lane_execution_factory, [passthrough, no_history]),

    test_utils:mock_expect(Workers, atm_lane_execution_factory, create_run, fun(
        AtmLaneRunSelector,
        AtmWorkflowExecutionDoc,
        AtmWorkflowExecutionCtx
    ) ->
        exec_origin_fun_with_hooks(
            AtmWorkflowExecutionDoc#document.key,
            create_run,
            [AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx]
        )
    end).


%% @private
-spec unmock_lane_execution_factory([node()]) -> ok.
unmock_lane_execution_factory(Workers) ->
    test_utils:mock_unload(Workers, atm_lane_execution_factory).


%% @private
-spec exec_origin_fun_with_hooks(atm_workflow_execution:id(), atom(), [term()]) -> term().
exec_origin_fun_with_hooks(AtmWorkflowExecutionId, Label, Args) ->
    case node_cache:get(?TEST_PROC_PID_KEY(AtmWorkflowExecutionId), undefined) of
        undefined ->
            meck:passthrough(Args);
        TestProcPid ->
            case call_test_process(TestProcPid, {pre, Label, Args}) of
                proceed ->
                    Result = meck:passthrough(Args),

                    case call_test_process(TestProcPid, {post, Label, Args}) of
                        proceed ->
                            Result;
                        {return_error, PostError} ->
                            PostError
                    end;
                {return_error, PreError} ->
                    PreError
            end
    end.


%% @private
-spec call_test_process(pid(), term()) -> term() | no_return().
call_test_process(TestProcPid, Msg) ->
    MRef = erlang:monitor(process, TestProcPid),
    TestProcPid ! {{self(), MRef}, Msg},
    receive
        {MRef, Reply} ->
            erlang:demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, _, _, Reason} ->
            exit(Reason)
    end.


%% @private
-spec reply_to_execution_process(reply_to(), term()) -> ok.
reply_to_execution_process({ExecutionProcPid, MRef}, Reply) ->
    ExecutionProcPid ! {MRef, Reply}.
