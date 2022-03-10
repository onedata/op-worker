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
%%%    b) exp state diff is applied and, in case of changes, new workflow execution
%%%       exp state is checked with data stored in op. Test fails if they differ.
%%% 4. test successfully ends after all steps have executed and no mismatch
%%%    between workflow execution exp state and data stored in Op was found.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_test_runner).
-author("Bartosz Walkowicz").

-include("atm_workflow_exeuction_test_runner.hrl").
-include("modules/automation/atm_execution.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([init/1, teardown/1]).
-export([run/1]).


-type mock_call_ctx() :: #atm_mock_call_ctx{}.
-type hook() :: fun((mock_call_ctx()) -> ok).
-type exp_state_diff() :: fun((mock_call_ctx()) ->
    false | {true, atm_workflow_execution_exp_state_builder:exp_state()}
).

-type step_mock_spec() :: #atm_step_mock_spec{}.
-type lane_run_test_spec() :: #atm_lane_run_execution_test_spec{}.
-type incarnation_test_spec() :: #atm_workflow_execution_incarnation_test_spec{}.
-type test_spec() :: #atm_workflow_execution_test_spec{}.

-export_type([
    mock_call_ctx/0, hook/0, exp_state_diff/0,
    step_mock_spec/0, lane_run_test_spec/0, incarnation_test_spec/0, test_spec/0
]).

-record(mock_call_report, {
    timing :: before_step | after_step,
    step :: atom(),
    args :: [term()]
}).
-type mock_call_report() :: #mock_call_report{}.
-type reply_to() :: {pid(), reference()}.

-record(test_ctx, {
    test_spec :: test_spec(),
    workflow_execution_id :: atm_workflow_execution:id(),
    workflow_execution_exp_state :: atm_workflow_execution_exp_state_builder:exp_state(),
    current_lane_index :: atm_lane_execution:index(),
    current_run_num :: atm_lane_execution:run_num(),
    ongoing_incarnations :: [incarnation_test_spec()]
}).
-type test_ctx() :: #test_ctx{}.


-define(TEST_HUNG_TIMEOUT, timer:seconds(30)).

-define(TEST_PROC_PID_KEY(__ATM_WORKFLOW_EXECUTION_ID),
    {atm_test_runner_process, __ATM_WORKFLOW_EXECUTION_ID}
).
-define(ATM_WORKFLOW_EXECUTION_ID_MSG(__ATM_WORKFLOW_EXECUTION_ID),
    {atm_workflow_execution_id, __ATM_WORKFLOW_EXECUTION_ID}
).


-define(NOW(), global_clock:timestamp_seconds()).


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
    workflow_schema_dump_or_draft = AtmWorkflowSchemaDumpOrDraft,
    workflow_schema_revision_num = AtmWorkflowSchemaRevisionNum,
    store_initial_contents = StoreInitialContents,
    callback_url = CallbackUrl,
    incarnations = Incarnations
}) ->
    SessionId = oct_background:get_user_session_id(UserSelector, ProviderSelector),
    SpaceId = oct_background:get_space_id(SpaceSelector),

    AtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(AtmWorkflowSchemaDumpOrDraft),
    AtmWorkflowSchemaRevision = atm_test_inventory:get_workflow_schema_revision(
        AtmWorkflowSchemaRevisionNum, AtmWorkflowSchemaId
    ),

    TestProcPid = self(),
    {AtmWorkflowExecutionId, _} = ?rpc(ProviderSelector, mi_atm:schedule_workflow_execution(
        SessionId, SpaceId, AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum,
        StoreInitialContents#{test_process => TestProcPid}, CallbackUrl
    )),

    ExpState = atm_workflow_execution_exp_state_builder:init(
        SpaceId, ?NOW(), AtmWorkflowSchemaRevision#atm_workflow_schema_revision.lanes
    ),
    true = atm_workflow_execution_exp_state_builder:assert_matches_with_backend(
        ProviderSelector, AtmWorkflowExecutionId, ExpState
    ),

    monitor_workflow_execution(#test_ctx{
        test_spec = TestSpec,
        workflow_execution_id = AtmWorkflowExecutionId,
        current_lane_index = 1,
        current_run_num = 1,
        workflow_execution_exp_state = ExpState,
        ongoing_incarnations = Incarnations
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec monitor_workflow_execution(test_ctx()) -> ok | no_return().
monitor_workflow_execution(TestCtx) ->
    receive {ReplyTo, StepMockCallReport = #mock_call_report{timing = Timing}} ->
        StepMockSpec = get_step_mock_spec(StepMockCallReport, TestCtx),
        StepMockCallCtx = build_mock_call_ctx(StepMockCallReport, TestCtx),

        Hook = get_hook(StepMockCallReport, StepMockSpec),
        call_if_defined(Hook, StepMockCallCtx),

        ExpStateDiff = get_exp_state_diff(StepMockCallReport, StepMockSpec),
        NewTestCtx1 = case ExpStateDiff(StepMockCallCtx) of
            {true, NewExpState} ->
                NewTestCtx0 = TestCtx#test_ctx{workflow_execution_exp_state = NewExpState},
                assert_exp_workflow_execution_state(NewTestCtx0),
                NewTestCtx0;
            false ->
                TestCtx
        end,

        reply_to_execution_process(ReplyTo, case {Timing, StepMockSpec#atm_step_mock_spec.mock_result} of
            {before_step, false} -> passthrough;
            {before_step, {true, MockedResult}} -> {return, MockedResult};
            {after_step, _} -> ok
        end),

        NewTestCtx2 = shift_monitored_lane_run_if_current_one_ended(StepMockCallReport, NewTestCtx1),
        case NewTestCtx2#test_ctx.ongoing_incarnations of
            [] ->
                ok;
            [_ | _] ->
                monitor_workflow_execution(NewTestCtx2)
        end
    after ?TEST_HUNG_TIMEOUT ->
        ct:pal("ERROR: Atm workflow execution hung"),
        ?assertEqual(success, failure)
    end.


%% @private
-spec get_step_mock_spec(mock_call_report(), test_ctx()) -> step_mock_spec().
get_step_mock_spec(#mock_call_report{step = handle_workflow_execution_ended}, #test_ctx{
    ongoing_incarnations = [#atm_workflow_execution_incarnation_test_spec{
        handle_workflow_execution_ended = Spec
    } | _
]}) ->
    Spec;

get_step_mock_spec(#mock_call_report{step = prepare_lane, args = [_, _, {AtmLaneIndex, _}]}, NewTestCtx) ->
    #atm_lane_run_execution_test_spec{prepare_lane = Spec} = get_lane_run_test_spec(
        AtmLaneIndex, NewTestCtx
    ),
    Spec;

get_step_mock_spec(#mock_call_report{step = create_run, args = [{AtmLaneIndex, _}, _, _]}, NewTestCtx) ->
    #atm_lane_run_execution_test_spec{create_run = Spec} = get_lane_run_test_spec(
        AtmLaneIndex, NewTestCtx
    ),
    Spec;

get_step_mock_spec(#mock_call_report{step = Step}, NewTestCtx) ->
    element(
        1 + lists_utils:index_of(Step, record_info(fields, atm_lane_run_execution_test_spec)),
        get_current_lane_run_test_spec(NewTestCtx)
    ).


%% @private
-spec get_current_lane_run_test_spec(test_ctx()) -> lane_run_test_spec().
get_current_lane_run_test_spec(#test_ctx{ongoing_incarnations = [
    #atm_workflow_execution_incarnation_test_spec{lane_runs = [
        OngoingLaneRun | _
    ]} | _
]}) ->
    OngoingLaneRun.


%% @private
-spec get_lane_run_test_spec(atm_lane_execution:index(), test_ctx()) -> lane_run_test_spec().
get_lane_run_test_spec(TargetAtmLaneIndex, #test_ctx{ongoing_incarnations = [
    #atm_workflow_execution_incarnation_test_spec{lane_runs = LaneRunTestSpecs} | _
]}) ->
    % run must be searched because run num is global and as such lanes will not
    % have all runs (there will be gaps)
    hd(lists:dropwhile(fun(#atm_lane_run_execution_test_spec{selector = {AtmLaneIndex, _}}) ->
        AtmLaneIndex < TargetAtmLaneIndex
    end, LaneRunTestSpecs)).


%% @private
-spec build_mock_call_ctx(mock_call_report(), test_ctx()) -> mock_call_ctx().
build_mock_call_ctx(#mock_call_report{args = CallArgs}, #test_ctx{
    workflow_execution_id = AtmWorkflowExecutionId,
    current_lane_index = CurrentAtmLaneIndex,
    current_run_num = CurrentRunNum,
    workflow_execution_exp_state = ExpState
}) ->
    #atm_mock_call_ctx{
        workflow_execution_id = AtmWorkflowExecutionId,
        workflow_execution_exp_state = ExpState,
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
-spec get_exp_state_diff(mock_call_report(), step_mock_spec()) ->
    exp_state_diff().
get_exp_state_diff(
    #mock_call_report{timing = before_step},
    #atm_step_mock_spec{before_step_exp_state_diff = default}
) ->
    fun(_) -> false end;

get_exp_state_diff(
    #mock_call_report{timing = before_step},
    #atm_step_mock_spec{before_step_exp_state_diff = Diff}
) ->
    Diff;

get_exp_state_diff(
    #mock_call_report{timing = after_step},
    #atm_step_mock_spec{after_step_exp_state_diff = default}
) ->
    fun(_) -> false end;

get_exp_state_diff(
    #mock_call_report{timing = after_step},
    #atm_step_mock_spec{after_step_exp_state_diff = Diff}
) ->
    Diff.


%% @private
-spec assert_exp_workflow_execution_state(test_ctx()) -> test_ctx().
assert_exp_workflow_execution_state(#test_ctx{
    test_spec = #atm_workflow_execution_test_spec{
        provider = ProviderSelector
    },
    workflow_execution_id = AtmWorkflowExecutionId,
    workflow_execution_exp_state = ExpState
}) ->
    case atm_workflow_execution_exp_state_builder:assert_matches_with_backend(
        ProviderSelector, AtmWorkflowExecutionId, ExpState
    ) of
        true -> ok;
        false -> ?assertEqual(success, failure)
    end.


%% @private
-spec shift_monitored_lane_run_if_current_one_ended(mock_call_report(), test_ctx()) ->
    test_ctx().
shift_monitored_lane_run_if_current_one_ended(
    #mock_call_report{timing = after_step, step = handle_workflow_execution_ended},
    TestCtx = #test_ctx{ongoing_incarnations = [_]}  %% last incarnation ended
) ->
    TestCtx#test_ctx{ongoing_incarnations = []};

shift_monitored_lane_run_if_current_one_ended(
    #mock_call_report{timing = after_step, step = handle_workflow_execution_ended},
    TestCtx = #test_ctx{ongoing_incarnations = [_ | LeftoverIncarnations]}
) ->
    #atm_workflow_execution_incarnation_test_spec{
        lane_runs = [#atm_lane_run_execution_test_spec{
            selector = {AtmLaneIndex, AtmRunNum}
        } | _]
    } = hd(LeftoverIncarnations),

    TestCtx#test_ctx{
        current_lane_index = AtmLaneIndex,
        current_run_num = AtmRunNum,
        ongoing_incarnations = LeftoverIncarnations
    };

shift_monitored_lane_run_if_current_one_ended(
    #mock_call_report{timing = after_step, step = handle_lane_execution_ended},
    TestCtx = #test_ctx{ongoing_incarnations = [OngoingIncarnation | LeftoverIncarnations]}
) ->
    case OngoingIncarnation#atm_workflow_execution_incarnation_test_spec.lane_runs of
        [_] ->
            NewOngoingIncarnation = OngoingIncarnation#atm_workflow_execution_incarnation_test_spec{
                lane_runs = []
            },
            TestCtx#test_ctx{ongoing_incarnations = [NewOngoingIncarnation | LeftoverIncarnations]};
        [_ | LeftoverLaneRuns] ->
            #atm_lane_run_execution_test_spec{selector = {AtmLaneIndex, AtmRunNum}} = hd(
                LeftoverLaneRuns
            ),
            NewOngoingIncarnation = OngoingIncarnation#atm_workflow_execution_incarnation_test_spec{
                lane_runs = LeftoverLaneRuns
            },
            TestCtx#test_ctx{
                current_lane_index = AtmLaneIndex,
                current_run_num = AtmRunNum,
                ongoing_incarnations = [NewOngoingIncarnation | LeftoverIncarnations]
            }
    end;

shift_monitored_lane_run_if_current_one_ended(_, TestCtx) ->
    TestCtx.


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
