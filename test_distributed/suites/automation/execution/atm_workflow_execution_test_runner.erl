%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Runs automation workflow execution test scenarios in following manner:
%%% 1. appropriate workflow execution modules and functions are mocked so that
%%%    it will be possible to check/assert workflow execution state machine
%%%    properties before/after each step transition.
%%% 2. workflow execution is started.
%%% 3. before/after each step transition:
%%%    a) hook (procedure defined by tester which can be used to e.g. change
%%%       mocks behaviour, simulate sth, etc.) is called if defined.
%%%    b) test view diff is applied and, in case of changes, new test view is
%%%       checked with model stored in Op. Test fails if they differ.
%%% 4. test successfully ends after all steps have executed and no mismatch
%%%    between test view and model in Op was found.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_test_runner).
-author("Bartosz Walkowicz").

-include("atm_workflow_exeuction_test_runner.hrl").
-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([init/1, teardown/1]).
-export([run/1]).


-type mock_call_ctx() :: #atm_mock_call_ctx{}.
-type hook() :: fun((mock_call_ctx()) -> ok).
-type test_view_diff() :: fun((mock_call_ctx()) -> false | {true, atm_workflow_execution_test_view:view()}).

-type step_mock_spec() :: #atm_step_mock_spec{}.
-type lane_run_test_spec() :: #atm_lane_run_execution_test_spec{}.
-type incarnation_test_spec() :: #atm_workflow_execution_incarnation_test_spec{}.
-type test_spec() :: #atm_workflow_execution_test_spec{}.

-export_type([
    mock_call_ctx/0, hook/0, test_view_diff/0,
    step_mock_spec/0, lane_run_test_spec/0, incarnation_test_spec/0, test_spec/0
]).

-record(mock_call_report, {
    timing :: before_step | after_step,
    step :: atom(),
    args :: [term()]
}).
-type mock_call_report() :: #mock_call_report{}.
-type reply_to() :: {pid(), reference()}.

-record(state, {
    test_spec :: test_spec(),
    workflow_execution_id :: atm_workflow_execution:id(),
    current_lane_index :: atm_lane_execution:index(),
    current_run_num :: atm_lane_execution:run_num(),
    workflow_execution_test_view :: atm_workflow_execution_test_view:view(),
    ongoing_incarnations :: [incarnation_test_spec()]
}).
-type state() :: #state{}.


-define(TEST_HUNG_TIMEOUT, timer:seconds(30)).

-define(TEST_PROC_PID_KEY(__ATM_WORKFLOW_EXECUTION_ID),
    {atm_test_runner_process, __ATM_WORKFLOW_EXECUTION_ID}
).
-define(ATM_WORKFLOW_EXECUTION_ID_MSG(__ATM_WORKFLOW_EXECUTION_ID),
    {atm_workflow_execution_id, __ATM_WORKFLOW_EXECUTION_ID}
).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(oct_background:entity_selector() | [oct_background:entity_selector()]) ->
    ok.
init(ProviderSelectors) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),

    mock_workflow_execution_factory(Workers),
    mock_workflow_execution_handler_steps(Workers),
    mock_lane_execution_factory_steps(Workers).


-spec teardown(oct_background:entity_selector() | [oct_background:entity_selector()]) ->
    ok.
teardown(ProviderSelectors) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),

    unmock_lane_execution_factory_steps(Workers),
    unmock_workflow_execution_handler_steps(Workers),
    unmock_workflow_execution_factory(Workers).


-spec run(test_spec()) -> ok | no_return().
run(TestSpec = #atm_workflow_execution_test_spec{
    provider = ProviderSelector,
    user = UserSelector,
    space = SpaceSelector,
    workflow_schema_id = AtmWorkflowSchemaId,
    workflow_schema_revision_num = AtmWorkflowSchemaRevisionNum,
    store_initial_values = StoreInitialValues,
    callback_url = CallbackUrl,
    incarnations = Incarnations
}) ->
    SessionId = oct_background:get_user_session_id(UserSelector, ProviderSelector),
    SpaceId = oct_background:get_space_id(SpaceSelector),

    {ok, {AtmWorkflowExecutionId, _}} = ?assertMatch({ok, _}, opt_atm:schedule_workflow_execution(
        ProviderSelector, SessionId, SpaceId, AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum,
        StoreInitialValues#{test_process => self()}, CallbackUrl
    )),

    AtmLaneSchemas = atm_workflow_schema_test_utils:query(
        atm_test_inventory:get_workflow_schema(AtmWorkflowSchemaId),
        [revision_registry, registry, 1, lanes]
    ),
    AtmWorkflowExecutionTestView = atm_workflow_execution_test_view:build(
        SpaceId, global_clock:timestamp_seconds(), AtmLaneSchemas
    ),
    atm_workflow_execution_test_view:assert_match_with_backend(
        ProviderSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionTestView
    ),

    monitor_workflow_execution(#state{
        test_spec = TestSpec,
        workflow_execution_id = AtmWorkflowExecutionId,
        current_lane_index = 1,
        current_run_num = 1,
        workflow_execution_test_view = AtmWorkflowExecutionTestView,
        ongoing_incarnations = Incarnations
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec monitor_workflow_execution(state()) -> ok | no_return().
monitor_workflow_execution(State) ->
    receive {ReplyTo, StepMockCallReport = #mock_call_report{timing = Timing}} ->
        StepMockSpec = get_step_mock_spec(StepMockCallReport, State),
        StepMockCallCtx = build_mock_call_ctx(StepMockCallReport, State),

        Hook = get_hook(StepMockCallReport, StepMockSpec),
        call_if_defined(Hook, StepMockCallCtx),

        TestViewDiff = get_test_view_diff(StepMockCallReport, StepMockSpec),
        NewState1 = case TestViewDiff(StepMockCallCtx) of
            {true, NewTestViewDiff} ->
                assert_actual_workflow_execution_test_view(NewTestViewDiff, State);
            false ->
                State
        end,

        reply_to_execution_process(ReplyTo, case {Timing, StepMockSpec#atm_step_mock_spec.mock_result} of
            {before_step, false} -> passthrough;
            {before_step, {true, MockedResult}} -> {return, MockedResult};
            {after_step, _} -> ok
        end),

        NewState2 = shift_monitored_lane_run_if_current_one_ended(StepMockCallReport, NewState1),
        case NewState2#state.ongoing_incarnations of
            [] ->
                ok;
            [_ | _] ->
                monitor_workflow_execution(NewState2)
        end
    after ?TEST_HUNG_TIMEOUT ->
        ct:pal("ERROR: Atm workflow execution hung"),
        ?assertEqual(success, failure)
    end.


%% @private
-spec get_step_mock_spec(mock_call_report(), state()) -> step_mock_spec().
get_step_mock_spec(#mock_call_report{step = handle_workflow_execution_ended}, #state{
    ongoing_incarnations = [#atm_workflow_execution_incarnation_test_spec{
        handle_workflow_execution_ended = Spec
    } | _
]}) ->
    Spec;

get_step_mock_spec(#mock_call_report{step = prepare_lane, args = [_, _, {AtmLaneIndex, _}]}, State) ->
    #atm_lane_run_execution_test_spec{prepare_lane = Spec} = get_lane_run_test_spec(
        AtmLaneIndex, State
    ),
    Spec;

get_step_mock_spec(#mock_call_report{step = create_run, args = [{AtmLaneIndex, _}, _, _]}, State) ->
    #atm_lane_run_execution_test_spec{create_run = Spec} = get_lane_run_test_spec(
        AtmLaneIndex, State
    ),
    Spec;

get_step_mock_spec(#mock_call_report{step = Step}, State) ->
    element(
        1 + lists_utils:index_of(Step, record_info(fields, atm_lane_run_execution_test_spec)),
        get_current_lane_run_test_spec(State)
    ).


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
-spec build_mock_call_ctx(mock_call_report(), state()) -> mock_call_ctx().
build_mock_call_ctx(#mock_call_report{args = CallArgs}, #state{
    workflow_execution_id = AtmWorkflowExecutionId,
    current_lane_index = CurrentAtmLaneIndex,
    current_run_num = CurrentRunNum,
    workflow_execution_test_view = AtmWorkflowExecutionTestView
}) ->
    #atm_mock_call_ctx{
        workflow_execution_id = AtmWorkflowExecutionId,
        workflow_execution_test_view = AtmWorkflowExecutionTestView,
        current_lane_index = CurrentAtmLaneIndex,
        current_run_num = CurrentRunNum,
        call_args = CallArgs
    }.


%% @private
-spec get_hook(mock_call_report(), step_mock_spec()) ->
    undefined | hook().
get_hook(#mock_call_report{timing = before_step}, #atm_step_mock_spec{before_step_hook = Hook}) ->
    Hook;
get_hook(#mock_call_report{timing = after_step}, #atm_step_mock_spec{after_step_hook = Hook}) ->
    Hook.


%% @private
-spec call_if_defined(undefined | fun((term()) -> ok), term()) -> ok.
call_if_defined(undefined, _Input) -> ok;
call_if_defined(Fun, Input) -> Fun(Input).


%% @private
-spec get_test_view_diff(mock_call_report(), step_mock_spec()) -> test_view_diff().
get_test_view_diff(
    #mock_call_report{timing = before_step},
    #atm_step_mock_spec{before_step_test_view_diff = default}
) ->
    fun(_) -> false end;

get_test_view_diff(
    #mock_call_report{timing = before_step},
    #atm_step_mock_spec{before_step_test_view_diff = Diff}
) ->
    Diff;

get_test_view_diff(
    #mock_call_report{timing = after_step},
    #atm_step_mock_spec{after_step_test_view_diff = default}
) ->
    fun(_) -> false end;

get_test_view_diff(
    #mock_call_report{timing = after_step},
    #atm_step_mock_spec{after_step_test_view_diff = Diff}
) ->
    Diff.


%% @private
-spec assert_actual_workflow_execution_test_view(atm_workflow_execution_test_view:view(), state()) ->
    state().
assert_actual_workflow_execution_test_view(NewAtmWorkflowExecutionTestView, State = #state{
    test_spec = #atm_workflow_execution_test_spec{
        provider = ProviderSelector
    },
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    atm_workflow_execution_test_view:assert_match_with_backend(
        ProviderSelector, AtmWorkflowExecutionId, NewAtmWorkflowExecutionTestView
    ),
    State#state{workflow_execution_test_view = NewAtmWorkflowExecutionTestView}.


%% @private
-spec shift_monitored_lane_run_if_current_one_ended(mock_call_report(), state()) ->
    state().
shift_monitored_lane_run_if_current_one_ended(
    #mock_call_report{timing = after_step, step = handle_workflow_execution_ended},
    State = #state{ongoing_incarnations = [_]}  %% last incarnation ended
) ->
    State#state{ongoing_incarnations = []};

shift_monitored_lane_run_if_current_one_ended(
    #mock_call_report{timing = after_step, step = handle_workflow_execution_ended},
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

shift_monitored_lane_run_if_current_one_ended(
    #mock_call_report{timing = after_step, step = handle_lane_execution_ended},
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

shift_monitored_lane_run_if_current_one_ended(_, State) ->
    State.


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
-spec mock_workflow_execution_handler_steps([node()]) -> ok.
mock_workflow_execution_handler_steps(Workers) ->
    test_utils:mock_new(Workers, atm_workflow_execution_handler, [passthrough, no_history]),

    mock_workflow_execution_handler_step(Workers, prepare_lane, 3),
    mock_workflow_execution_handler_step(Workers, process_item, 6),
    mock_workflow_execution_handler_step(Workers, process_result, 5),
    mock_workflow_execution_handler_step(Workers, report_item_error, 3),
    mock_workflow_execution_handler_step(Workers, handle_task_execution_ended, 3),
    mock_workflow_execution_handler_step(Workers, handle_lane_execution_ended, 3),
    mock_workflow_execution_handler_step(Workers, handle_workflow_execution_ended, 2).


%% @private
-spec unmock_workflow_execution_handler_steps([node()]) -> ok.
unmock_workflow_execution_handler_steps(Workers) ->
    test_utils:mock_unload(Workers, atm_workflow_execution_handler).


%% @private
-spec mock_workflow_execution_handler_step([node()], atom(), 1..6) -> ok.
mock_workflow_execution_handler_step(Workers, FunName, FunArity) ->
    MockFun = build_workflow_execution_handler_step_function_mock(FunArity, FunName),
    test_utils:mock_expect(Workers, atm_workflow_execution_handler, FunName, MockFun).


%% @private
-spec build_workflow_execution_handler_step_function_mock(1..6, atom()) ->
    function().
build_workflow_execution_handler_step_function_mock(1, Label) ->
    fun(Arg1) ->
        Args = [Arg1],
        exec_mock(hd(Args), Label, Args)
    end;

build_workflow_execution_handler_step_function_mock(2, Label) ->
    fun(Arg1, Arg2) ->
        Args = [Arg1, Arg2],
        exec_mock(hd(Args), Label, Args)
    end;

build_workflow_execution_handler_step_function_mock(3, Label) ->
    fun(Arg1, Arg2, Arg3) ->
        Args = [Arg1, Arg2, Arg3],
        exec_mock(hd(Args), Label, Args)
    end;

build_workflow_execution_handler_step_function_mock(4, Label) ->
    fun(Arg1, Arg2, Arg3, Arg4) ->
        Args = [Arg1, Arg2, Arg3, Arg4],
        exec_mock(hd(Args), Label, Args)
    end;

build_workflow_execution_handler_step_function_mock(5, Label) ->
    fun(Arg1, Arg2, Arg3, Arg4, Arg5) ->
        Args = [Arg1, Arg2, Arg3, Arg4, Arg5],
        exec_mock(hd(Args), Label, Args)
    end;

build_workflow_execution_handler_step_function_mock(6, Label) ->
    fun(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6) ->
        Args = [Arg1, Arg2, Arg3, Arg4, Arg5, Arg6],
        exec_mock(hd(Args), Label, Args)
    end.


%% @private
-spec mock_lane_execution_factory_steps([node()]) -> ok.
mock_lane_execution_factory_steps(Workers) ->
    test_utils:mock_new(Workers, atm_lane_execution_factory, [passthrough, no_history]),

    test_utils:mock_expect(Workers, atm_lane_execution_factory, create_run, fun(
        AtmLaneRunSelector,
        AtmWorkflowExecutionDoc,
        AtmWorkflowExecutionCtx
    ) ->
        exec_mock(
            AtmWorkflowExecutionDoc#document.key,
            create_run,
            [AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx]
        )
    end).


%% @private
-spec unmock_lane_execution_factory_steps([node()]) -> ok.
unmock_lane_execution_factory_steps(Workers) ->
    test_utils:mock_unload(Workers, atm_lane_execution_factory).


%% @private
-spec exec_mock(atm_workflow_execution:id(), atom(), [term()]) -> term().
exec_mock(AtmWorkflowExecutionId, Step, Args) ->
    case node_cache:get(?TEST_PROC_PID_KEY(AtmWorkflowExecutionId), undefined) of
        undefined ->
            meck:passthrough(Args);
        TestProcPid ->
            MockCallReport = #mock_call_report{timing = before_step, step = Step, args = Args},

            case call_test_process(TestProcPid, MockCallReport) of
                passthrough ->
                    Result = meck:passthrough(Args),
                    ok = call_test_process(TestProcPid, MockCallReport#mock_call_report{timing = after_step}),
                    Result;
                {return, MockedResult} ->
                    MockedResult
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
-spec reply_to_execution_process
    % when replying to 'before_step' report
    (reply_to(), passthrough | {return, term()}) -> ok;
    % when replying to 'after_step' report
    (reply_to(), ok) -> ok.
reply_to_execution_process({ExecutionProcPid, MRef}, Reply) ->
    ExecutionProcPid ! {MRef, Reply}.
