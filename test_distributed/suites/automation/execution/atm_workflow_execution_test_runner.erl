%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Runner of atm workflow execution test cases.
%%%
%%% The 'init' function must be called first before any test can be executed.
%%% This will mock appropriate op modules and functions so that processes
%%% handling atm workflow execution call (and block waiting for reply allowing
%%% it to continue) testcase process before and after each execution step
%%% (see spec() type). This makes it possible to check not only final execution
%%% state but also intermediate ones.
%%%
%%% Tests are run in following manner:
%%% 1. start atm workflow execution.
%%% 2. await report call from any execution process. If none is received for
%%%    prolonged period of time go to 6.
%%% 3. if received report handling does not need to be blocked until other
%%%    step phase is executed (no 'defer_after' specified in step_mock_spec())
%%%    or such step phase was already executed go to 4. Otherwise, defer it
%%%    and go to 2.
%%% 4. begin handling of step by:
%%%    a) calling hook (procedure defined by tester which can be used to e.g.
%%%       change mocks behaviour, simulate sth, etc.) if defined.
%%%    b) applying exp state diff and storing eventual changes to expectations
%%%       (they are not checked immediately).
%%% 5. save step handling as pending and go to 2.
%%% 6. when all execution processes progressed to next step phase and are
%%%    blocked awaiting response:
%%%    a) assert accumulated changes to workflow execution state expectations
%%%       matches data stored in backend.
%%%    b) end execution of pending steps (this allows execution processes to
%%%       continue).
%%%    c) execute deferred step phases (in the manner described in 4.) if step
%%%       phases they were waiting for ended.
%%%    If all step phases has been executed and no mismatch between workflow
%%%    execution exp state and data stored in op was found, then finish the test.
%%%    Otherwise go to 2.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_test_runner).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test_runner.hrl").
-include("atm_workflow_execution_test.hrl").
-include("modules/automation/atm_execution.hrl").
-include("onenv_test_utils.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([init/1, teardown/1]).
-export([run/1]).


-type step_name() ::
    prepare_lane |
    create_run |
    resume_lane |
    run_task_for_item |
    process_task_result_for_item |
    process_streamed_task_data |
    handle_task_results_processed_for_all_items |
    report_item_error |
    handle_task_execution_stopped |
    handle_lane_execution_stopped |
    handle_workflow_execution_stopped |
    handle_exception |
    handle_workflow_abruptly_stopped.

-type step_phase_timing() :: before_step | after_step.

-type mock_call_ctx() :: #atm_mock_call_ctx{}.
-type hook() :: fun((mock_call_ctx()) -> ok).
-type exp_state_diff_fun() :: fun((mock_call_ctx()) ->
    false | {true, atm_workflow_execution_exp_state_builder:exp_state()}
).
-type exp_state_diff() ::
    exp_state_diff_fun() |
    [atm_workflow_execution_exp_state_builder:expectation()].

-type step_phase_selector() :: {
    step_name(),
    step_phase_timing(),
    atm_lane_execution:lane_run_selector() | atm_workflow_execution:incarnation()
}.
-record(step_phase, {
    selector :: step_phase_selector(),
    mock_call_report :: mock_call_report(),
    mock_spec :: step_mock_spec(),
    reply_to :: atm_workflow_execution_test_mocks:reply_to()
}).
-type step_phase() :: #step_phase{}.

-type result_override() :: {return, term()} | {error | throw, errors:error()}.

-type mock_strategy() ::
    % original step will be run unperturbed
    passthrough |
    % original step will be run after sleeping for specified period of time
    {passthrough_with_delay, time:millis()} |
    % original step will be run but it's result will be replaced with specified one
    {passthrough_with_result_override, result_override()} |
    % original step will not be run and specified result will be returned immediately
    {yield, result_override()}.

-type mock_strategy_spec() ::
    mock_strategy() |
    fun((mock_call_report()) -> mock_strategy()).

-type step_mock_spec() :: #atm_step_mock_spec{}.

-type lane_run_test_spec() :: #atm_lane_run_execution_test_spec{}.
-type incarnation_test_spec() :: #atm_workflow_execution_incarnation_test_spec{}.
-type test_spec() :: #atm_workflow_execution_test_spec{}.

-export_type([
    mock_call_ctx/0, hook/0, exp_state_diff_fun/0, exp_state_diff/0,
    result_override/0, mock_strategy_spec/0, step_phase_selector/0, step_mock_spec/0,
    lane_run_test_spec/0, incarnation_test_spec/0, test_spec/0
]).

-type mock_call_report() :: #mock_call_report{}.

-record(test_ctx, {
    test_spec :: test_spec(),

    session_id :: session:id(),
    workflow_execution_id :: atm_workflow_execution:id(),

    lane_count :: non_neg_integer(),
    current_lane_index :: atm_lane_execution:index(),
    current_run_num :: atm_lane_execution:run_num(),
    ongoing_incarnations :: [incarnation_test_spec()],

    workflow_execution_exp_state :: atm_workflow_execution_exp_state_builder:exp_state(),
    workflow_execution_exp_state_changed :: boolean(),

    prev_incarnations_executed_step_phases = [] :: [{non_neg_integer(), [step_phase_selector()]}],
    executed_step_phases = [] :: [step_phase_selector()],
    pending_step_phases = [] :: [step_phase()],
    deferred_step_phases = #{} :: #{step_phase_selector() => [step_phase()]},

    test_hung_probes_left :: non_neg_integer()
}).
-type test_ctx() :: #test_ctx{}.

-define(AWAIT_OTHER_PARALLEL_PIPELINES_NEXT_STEP_INTERVAL, 150).
-define(TEST_HUNG_MAX_PROBES_NUM, 20).

-define(ASSERT_RETRIES, 45).

-define(NO_DIFF, fun(_) -> false end).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(oct_background:entity_selector() | [oct_background:entity_selector()]) ->
    ok.
init(ProviderSelectors) ->
    atm_workflow_execution_test_mocks:init(ProviderSelectors).


-spec teardown(oct_background:entity_selector() | [oct_background:entity_selector()]) ->
    ok.
teardown(ProviderSelectors) ->
    atm_workflow_execution_test_mocks:teardown(ProviderSelectors).


-spec run(test_spec()) -> ok | no_return().
run(TestSpec = #atm_workflow_execution_test_spec{
    provider = ProviderSelector,
    user = UserSelector,
    space = SpaceSelector,
    workflow_schema_dump_or_draft = AtmWorkflowSchemaDumpOrDraft,
    workflow_schema_revision_num = AtmWorkflowSchemaRevisionNum,
    store_initial_content_overlay = AtmStoreInitialContentOverlay,
    callback_url = CallbackUrl,
    incarnations = Incarnations
}) ->
    SessionId = oct_background:get_user_session_id(UserSelector, ProviderSelector),
    SpaceId = oct_background:get_space_id(SpaceSelector),

    AtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(AtmWorkflowSchemaDumpOrDraft),
    {AtmWorkflowExecutionId, _} = atm_workflow_execution_test_mocks:schedule_workflow_execution_as_test_process(
        ProviderSelector, SessionId, SpaceId, AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum,
        AtmStoreInitialContentOverlay, CallbackUrl
    ),

    #atm_workflow_schema_revision{lanes = AtmLaneSchemas} = atm_test_inventory:get_workflow_schema_revision(
        AtmWorkflowSchemaRevisionNum, AtmWorkflowSchemaId
    ),
    ExpState = atm_workflow_execution_exp_state_builder:init(
        ProviderSelector, SpaceId, AtmWorkflowExecutionId, AtmLaneSchemas
    ),
    true = atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpState, 0),

    monitor_workflow_execution(#test_ctx{
        test_spec = TestSpec,
        session_id = SessionId,
        workflow_execution_id = AtmWorkflowExecutionId,
        lane_count = length(AtmLaneSchemas),
        current_lane_index = 1,
        current_run_num = 1,
        ongoing_incarnations = Incarnations,
        workflow_execution_exp_state = ExpState,
        workflow_execution_exp_state_changed = false,
        test_hung_probes_left = ?TEST_HUNG_MAX_PROBES_NUM
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec monitor_workflow_execution(test_ctx()) -> ok | no_return().
monitor_workflow_execution(TestCtx0) ->
    receive {ReplyTo, StepMockCallReport} ->
        TestCtx1 = TestCtx0#test_ctx{test_hung_probes_left = ?TEST_HUNG_MAX_PROBES_NUM},
        {StepPhaseSelector, StepMockSpec} = get_step_mock_spec(StepMockCallReport, TestCtx0),

        StepPhase = #step_phase{
            selector = StepPhaseSelector,
            mock_call_report = StepMockCallReport,
            mock_spec = StepMockSpec,
            reply_to = ReplyTo
        },

        case should_defer_step_execution(StepMockSpec, TestCtx1) of
            true ->
                monitor_workflow_execution(TestCtx1#test_ctx{deferred_step_phases = maps:update_with(
                    StepMockSpec#atm_step_mock_spec.defer_after,
                    fun(RestDeferredStepPhases) -> [StepPhase | RestDeferredStepPhases] end,
                    [StepPhase],
                    TestCtx1#test_ctx.deferred_step_phases
                )});
            false ->
                monitor_workflow_execution(begin_step_phase_execution(StepPhase, TestCtx1))
        end
    after ?AWAIT_OTHER_PARALLEL_PIPELINES_NEXT_STEP_INTERVAL ->
        case TestCtx0#test_ctx.test_hung_probes_left of
            0 ->
                ct:pal("Automation workflow execution test hung"),
                fail_test(TestCtx0);
            Num ->
                TestCtx1 = TestCtx0#test_ctx{test_hung_probes_left = Num - 1},
                TestCtx2 = address_pending_expectations(TestCtx1),
                TestCtx3 = end_pending_step_phase_executions(TestCtx2),

                case has_workflow_stopped(TestCtx3) of
                    true ->
                        ok;
                    false ->
                        TestCtx4 = begin_deferred_step_phase_executions_if_possible(TestCtx3),
                        monitor_workflow_execution(TestCtx4)
                end
        end
    end.


%% @private
-spec address_pending_expectations(test_ctx()) -> test_ctx().
address_pending_expectations(TestCtx = #test_ctx{workflow_execution_exp_state_changed = true}) ->
    assert_exp_workflow_execution_state(TestCtx),
    TestCtx#test_ctx{workflow_execution_exp_state_changed = false};

address_pending_expectations(TestCtx = #test_ctx{workflow_execution_exp_state_changed = false}) ->
    TestCtx.


%% @private
-spec should_defer_step_execution(step_mock_spec(), test_ctx()) -> boolean().
should_defer_step_execution(#atm_step_mock_spec{defer_after = undefined}, _TestCtx) ->
    false;
should_defer_step_execution(#atm_step_mock_spec{defer_after = StepSelector}, #test_ctx{
    executed_step_phases = ExecutedSteps
}) ->
    not lists:member(StepSelector, ExecutedSteps).


%% @private
-spec begin_step_phase_execution(step_phase(), test_ctx()) -> test_ctx().
begin_step_phase_execution(
    StepPhase = #step_phase{
        mock_call_report = StepMockCallReport,
        mock_spec = StepMockSpec
    },
    TestCtx0 = #test_ctx{pending_step_phases = PendingStepPhases}
) ->
    StepMockCallCtx = build_mock_call_ctx(StepMockCallReport, TestCtx0),

    call_hook_if_defined(get_hook(StepMockCallReport, StepMockSpec), StepMockCallCtx, TestCtx0),

    ExpStateDiffFun = case get_exp_state_diff(StepMockCallReport, StepMockSpec) of
        Fun when is_function(Fun, 1) ->
            Fun;
        Expectations when is_list(Expectations) ->
            fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                {true, atm_workflow_execution_exp_state_builder:expect(ExpState, Expectations)}
            end
    end,
    TestCtx1 = case ExpStateDiffFun(StepMockCallCtx) of
        {true, NewExpState} ->
            TestCtx0#test_ctx{
                workflow_execution_exp_state = NewExpState,
                workflow_execution_exp_state_changed = true
            };
        false ->
            TestCtx0
    end,

    TestCtx1#test_ctx{pending_step_phases = [StepPhase | PendingStepPhases]}.


%% @private
-spec end_pending_step_phase_executions(test_ctx()) -> test_ctx().
end_pending_step_phase_executions(TestCtx = #test_ctx{pending_step_phases = PendingStepPhases}) ->
    lists:foldl(fun(
        #step_phase{
            selector = StepPhaseSelector,
            mock_call_report = StepMockCallReport = #mock_call_report{timing = Timing},
            mock_spec = #atm_step_mock_spec{strategy = MockStrategy},
            reply_to = ReplyTo
        },
        TestCtxAcc0 = #test_ctx{executed_step_phases = ExecutedStepPhases}
    ) ->
        StepMockCallCtx = build_mock_call_ctx(StepMockCallReport, TestCtxAcc0),

        atm_workflow_execution_test_mocks:reply_to_execution_process(ReplyTo, case Timing of
            before_step ->
                case is_function(MockStrategy) of
                    true -> MockStrategy(StepMockCallCtx);
                    false -> MockStrategy
                end;
            after_step ->
                ok
        end),

        shift_monitored_lane_run_if_current_one_stopped(
            StepMockCallReport,
            TestCtxAcc0#test_ctx{executed_step_phases = [StepPhaseSelector | ExecutedStepPhases]}
        )
    end, TestCtx#test_ctx{pending_step_phases = []}, PendingStepPhases).


%% @private
-spec begin_deferred_step_phase_executions_if_possible(test_ctx()) -> test_ctx().
begin_deferred_step_phase_executions_if_possible(TestCtx = #test_ctx{
    executed_step_phases = ExecutedStepPhases,
    deferred_step_phases = DeferredStepPhases
}) ->
    maps:fold(fun(AwaitedStepPhaseSelector, StepPhases, TestCtxAcc) ->
        case lists:member(AwaitedStepPhaseSelector, ExecutedStepPhases) of
            true ->
                lists:foldl(fun(StepPhase, InnerTestCtxAcc) ->
                    begin_step_phase_execution(StepPhase, InnerTestCtxAcc)
                end, TestCtxAcc, StepPhases);
            false ->
                LeftoverDeferredStepPhases = TestCtxAcc#test_ctx.deferred_step_phases,
                TestCtxAcc#test_ctx{deferred_step_phases = LeftoverDeferredStepPhases#{
                    AwaitedStepPhaseSelector => StepPhases
                }}
        end
    end, TestCtx#test_ctx{deferred_step_phases = #{}}, DeferredStepPhases).


%% @private
-spec has_workflow_stopped(test_ctx()) -> boolean().
has_workflow_stopped(#test_ctx{
    ongoing_incarnations = [],
    pending_step_phases = [],
    deferred_step_phases = DeferredStepPhases
}) ->
    maps_utils:is_empty(DeferredStepPhases);

has_workflow_stopped(#test_ctx{}) ->
    false.


%% @private
-spec get_step_mock_spec(mock_call_report(), test_ctx()) ->
    {step_phase_selector(), step_mock_spec()}.
get_step_mock_spec(
    #mock_call_report{step = handle_workflow_execution_stopped, timing = Timing},
    #test_ctx{ongoing_incarnations = [#atm_workflow_execution_incarnation_test_spec{
        incarnation_num = IncarnationNum,
        handle_workflow_execution_stopped = Spec
    } | _]}
) ->
    {{handle_workflow_execution_stopped, Timing, IncarnationNum}, Spec};

get_step_mock_spec(
    #mock_call_report{step = handle_exception, timing = Timing},
    #test_ctx{ongoing_incarnations = [#atm_workflow_execution_incarnation_test_spec{
        incarnation_num = IncarnationNum,
        handle_exception = Spec
    } | _]}
) ->
    {{handle_exception, Timing, IncarnationNum}, Spec};

get_step_mock_spec(
    #mock_call_report{step = handle_workflow_abruptly_stopped, timing = Timing},
    #test_ctx{ongoing_incarnations = [#atm_workflow_execution_incarnation_test_spec{
        incarnation_num = IncarnationNum,
        handle_workflow_abruptly_stopped = Spec
    } | _]}
) ->
    {{handle_workflow_abruptly_stopped, Timing, IncarnationNum}, Spec};

get_step_mock_spec(
    #mock_call_report{step = prepare_lane, timing = Timing, args = [_, _, {AtmLaneIndex, _}]},
    NewTestCtx
) ->
    #atm_lane_run_execution_test_spec{
        selector = Selector,
        prepare_lane = Spec
    } = get_lane_run_test_spec(AtmLaneIndex, NewTestCtx),

    {{prepare_lane, Timing, Selector}, Spec};

get_step_mock_spec(
    #mock_call_report{step = create_run, timing = Timing, args = [{AtmLaneIndex, _}, _, _]},
    NewTestCtx
) ->
    #atm_lane_run_execution_test_spec{
        selector = Selector,
        create_run = Spec
    } = get_lane_run_test_spec(AtmLaneIndex, NewTestCtx),

    {{create_run, Timing, Selector}, Spec};

get_step_mock_spec(
    #mock_call_report{step = resume_lane, timing = Timing, args = [{AtmLaneIndex, _}, _, _]},
    NewTestCtx
) ->
    #atm_lane_run_execution_test_spec{
        selector = Selector,
        create_run = Spec
    } = get_lane_run_test_spec(AtmLaneIndex, NewTestCtx),

    {{resume_lane, Timing, Selector}, Spec};

get_step_mock_spec(
    #mock_call_report{
        step = handle_lane_execution_stopped,
        timing = Timing,
        args = [{AtmLaneIndex, _}, _, _]
    },
    NewTestCtx
) ->
    #atm_lane_run_execution_test_spec{
        selector = Selector,
        handle_lane_execution_stopped = Spec
    } = get_lane_run_test_spec(AtmLaneIndex, NewTestCtx),

    {{handle_lane_execution_stopped, Timing, Selector}, Spec};

get_step_mock_spec(#mock_call_report{step = Step, timing = Timing}, NewTestCtx) ->
    AtmLaneRunTestSpec = get_current_lane_run_test_spec(NewTestCtx),
    StepMockSpec = element(
        1 + lists_utils:index_of(Step, record_info(fields, atm_lane_run_execution_test_spec)),
        AtmLaneRunTestSpec
    ),
    {{Step, Timing, AtmLaneRunTestSpec#atm_lane_run_execution_test_spec.selector}, StepMockSpec}.


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
build_mock_call_ctx(#mock_call_report{args = CallArgs, result = Result}, #test_ctx{
    test_spec = #atm_workflow_execution_test_spec{
        provider = ProviderSelector,
        space = SpaceSelector
    },
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId,
    lane_count = AtmLaneCount,
    current_lane_index = CurrentAtmLaneIndex,
    current_run_num = CurrentRunNum,
    workflow_execution_exp_state = ExpState
}) ->
    #atm_mock_call_ctx{
        provider = ProviderSelector,
        space = SpaceSelector,
        session_id = SessionId,
        workflow_execution_id = AtmWorkflowExecutionId,
        workflow_execution_exp_state = ExpState,
        lane_count = AtmLaneCount,
        current_lane_index = CurrentAtmLaneIndex,
        current_run_num = CurrentRunNum,
        call_args = CallArgs,
        call_result = Result
    }.


%% @private
-spec get_hook(mock_call_report(), step_mock_spec()) ->
    undefined | hook().
get_hook(#mock_call_report{timing = before_step}, #atm_step_mock_spec{before_step_hook = Hook}) ->
    Hook;
get_hook(#mock_call_report{timing = after_step}, #atm_step_mock_spec{after_step_hook = Hook}) ->
    Hook.


%% @private
-spec call_hook_if_defined(undefined | hook(), term(), test_ctx()) -> ok.
call_hook_if_defined(undefined, _Input, _TestCtx) ->
    ok;
call_hook_if_defined(HookFun, Input, TestCtx) ->
    try
        HookFun(Input),
        ok
    catch Type:Error:Stacktrace ->
        ct:pal("Unexpected exception when calling test hook: ~s", [
            iolist_to_binary(lager:pr_stacktrace(Stacktrace, {Type, Error}))
        ]),
        fail_test(TestCtx)
    end.


%% @private
-spec get_exp_state_diff(mock_call_report(), step_mock_spec()) ->
    exp_state_diff().
get_exp_state_diff(
    #mock_call_report{step = prepare_lane, timing = before_step},
    #atm_step_mock_spec{before_step_exp_state_diff = default}
) ->
    ?NO_DIFF;

get_exp_state_diff(
    #mock_call_report{step = create_run, timing = before_step},
    #atm_step_mock_spec{before_step_exp_state_diff = default}
) ->
    fun(#atm_mock_call_ctx{
        workflow_execution_exp_state = ExpState0,
        call_args = [AtmLaneRunSelector, _AtmWorkflowExecutionDoc, _AtmWorkflowExecutionCtx]
    }) ->
        {true, atm_workflow_execution_exp_state_builder:expect_lane_run_started_preparing(
            AtmLaneRunSelector, ExpState0
        )}
    end;

get_exp_state_diff(
    #mock_call_report{step = create_run, timing = after_step},
    #atm_step_mock_spec{after_step_exp_state_diff = default}
) ->
    fun(#atm_mock_call_ctx{
        workflow_execution_exp_state = ExpState0,
        call_args = [AtmLaneRunSelector, _AtmWorkflowExecutionDoc, _AtmWorkflowExecutionCtx]
    }) ->
        {true, atm_workflow_execution_exp_state_builder:expect_lane_run_created(
            AtmLaneRunSelector, ExpState0
        )}
    end;

get_exp_state_diff(
    #mock_call_report{step = prepare_lane, timing = after_step},
    #atm_step_mock_spec{after_step_exp_state_diff = default}
) ->
    fun(#atm_mock_call_ctx{
        workflow_execution_exp_state = ExpState0,
        call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmLaneRunSelector]
    }) ->
        {true, atm_workflow_execution_exp_state_builder:expect_lane_run_enqueued(
            AtmLaneRunSelector, ExpState0
        )}
    end;

get_exp_state_diff(
    #mock_call_report{step = run_task_for_item, timing = before_step},
    #atm_step_mock_spec{before_step_exp_state_diff = default}
) ->
    ?NO_DIFF;

get_exp_state_diff(
    #mock_call_report{step = run_task_for_item, timing = after_step},
    #atm_step_mock_spec{after_step_exp_state_diff = default}
) ->
    fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0, call_args = [
        _AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId,
        _AtmJobBatchId, ItemBatch
    ]}) ->
        ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_transitioned_to_active_status_if_was_in_pending_status(
            AtmTaskExecutionId, ExpState0
        ),
        ExpState2 = atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_active_status_if_was_in_pending_status(
            AtmTaskExecutionId, ExpState1
        ),
        ExpState3 = atm_workflow_execution_exp_state_builder:expect_task_lane_run_transitioned_to_active_status_if_was_in_enqueued_status(
            AtmTaskExecutionId, ExpState2
        ),
        ExpState4 = case atm_workflow_execution_exp_state_builder:get_task_status(AtmTaskExecutionId, ExpState3) of
            <<"active">> ->
                atm_workflow_execution_exp_state_builder:expect_task_items_in_processing_increased(
                    AtmTaskExecutionId, length(ItemBatch), ExpState3
                );
            _ ->
                ExpState3
        end,
        {true, ExpState4}
    end;

get_exp_state_diff(
    #mock_call_report{step = process_task_result_for_item, timing = before_step},
    #atm_step_mock_spec{before_step_exp_state_diff = default}
) ->
    ?NO_DIFF;

get_exp_state_diff(
    #mock_call_report{step = process_task_result_for_item, timing = after_step},
    #atm_step_mock_spec{after_step_exp_state_diff = default}
) ->
    fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0, call_args = [
        _AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId,
        ItemsBatch, _LambdaOutput
    ]}) ->
        {true, atm_workflow_execution_exp_state_builder:expect_task_items_moved_from_processing_to_processed(
            AtmTaskExecutionId, length(ItemsBatch), ExpState0
        )}
    end;

get_exp_state_diff(
    #mock_call_report{step = handle_task_execution_stopped, timing = before_step},
    #atm_step_mock_spec{before_step_exp_state_diff = default}
) ->
    ?NO_DIFF;

get_exp_state_diff(
    #mock_call_report{step = handle_task_execution_stopped, timing = after_step},
    #atm_step_mock_spec{after_step_exp_state_diff = default}
) ->
    fun(#atm_mock_call_ctx{
        workflow_execution_exp_state = ExpState0,
        call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
    }) ->
        ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_finished(
            AtmTaskExecutionId, ExpState0
        ),
        InferStatusFun = fun
            (<<"active">>, [<<"finished">>]) -> <<"finished">>;
            (CurrentStatus, _) -> CurrentStatus
        end,
        ExpState2 = atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
            AtmTaskExecutionId, InferStatusFun, ExpState1
        ),
        {true, ExpState2}
    end;

get_exp_state_diff(
    #mock_call_report{step = handle_lane_execution_stopped, timing = before_step},
    #atm_step_mock_spec{before_step_exp_state_diff = default}
) ->
    ?NO_DIFF;

get_exp_state_diff(
    #mock_call_report{step = handle_lane_execution_stopped, timing = after_step},
    #atm_step_mock_spec{after_step_exp_state_diff = default}
) ->
    fun(#atm_mock_call_ctx{
        workflow_execution_exp_state = ExpState0,
        call_args = [AtmLaneRunSelector, _AtmWorkflowExecutionId, _AtmWorkflowExecutionCtx],
        lane_count = AtmLaneCount,
        current_lane_index = CurrentAtmLaneIndex,
        current_run_num = CurrentAtmRunNum
    }) ->
        ExpState1 = case CurrentAtmLaneIndex < AtmLaneCount of
            true ->
                atm_workflow_execution_exp_state_builder:expect_lane_run_num_set(
                    {CurrentAtmLaneIndex + 1, CurrentAtmRunNum}, CurrentAtmRunNum, ExpState0
                );
            false ->
                atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(
                    ExpState0
                )
        end,
        {true, atm_workflow_execution_exp_state_builder:expect_lane_run_finished(
            AtmLaneRunSelector, ExpState1
        )}
    end;

get_exp_state_diff(
    #mock_call_report{step = handle_workflow_execution_stopped, timing = before_step},
    #atm_step_mock_spec{before_step_exp_state_diff = default}
) ->
    ?NO_DIFF;

get_exp_state_diff(
    #mock_call_report{step = handle_workflow_execution_stopped, timing = after_step},
    #atm_step_mock_spec{after_step_exp_state_diff = default}
) ->
    fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_finished(ExpState0)}
    end;

get_exp_state_diff(
    #mock_call_report{step = Step, timing = before_step},
    #atm_step_mock_spec{after_step_exp_state_diff = default}
) when
    Step =:= handle_exception;
    Step =:= handle_workflow_abruptly_stopped
->
    % Changes made by exception handling depends when it happens and as such no
    % reasonable default implementation can be provided
    ?NO_DIFF;

get_exp_state_diff(
    #mock_call_report{timing = before_step},
    #atm_step_mock_spec{before_step_exp_state_diff = default}
) ->
    fun(_) -> false end;

get_exp_state_diff(
    #mock_call_report{timing = before_step},
    #atm_step_mock_spec{before_step_exp_state_diff = no_diff}
) ->
    ?NO_DIFF;

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
    #atm_step_mock_spec{after_step_exp_state_diff = no_diff}
) ->
    ?NO_DIFF;

get_exp_state_diff(
    #mock_call_report{timing = after_step},
    #atm_step_mock_spec{after_step_exp_state_diff = Diff}
) ->
    Diff.


%% @private
-spec assert_exp_workflow_execution_state(test_ctx()) -> test_ctx().
assert_exp_workflow_execution_state(TestCtx = #test_ctx{workflow_execution_exp_state = ExpState}) ->
    case atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpState, ?ASSERT_RETRIES) of
        true ->
            ok;
        false ->
            ct:pal("Automation workflow execution test failed due to unmet expectations"),
            fail_test(TestCtx)
    end.


%% @private
fail_test(TestCtx) ->
    PendingStepPhaseSelectors = lists:map(
        fun(#step_phase{selector = StepPhaseSelector}) -> StepPhaseSelector end,
        TestCtx#test_ctx.pending_step_phases
    ),
    ct:pal("PENDING STEPS:~n~n~20p~n~n~nEXECUTED STEPS:~n~n~20p~n~n~nPREVIOUS INCARNATIONS:~n~n~20p", [
        PendingStepPhaseSelectors,
        TestCtx#test_ctx.executed_step_phases,
        TestCtx#test_ctx.prev_incarnations_executed_step_phases
    ]),
    ?assertEqual(success, failure).


%% @private
-spec shift_monitored_lane_run_if_current_one_stopped(mock_call_report(), test_ctx()) ->
    test_ctx().
shift_monitored_lane_run_if_current_one_stopped(
    StepMockCallReport = #mock_call_report{timing = after_step, step = Step},
    TestCtx = #test_ctx{ongoing_incarnations = [EndedIncarnation]}  %% last incarnation stopped
) when
    Step =:= handle_workflow_abruptly_stopped;
    Step =:= handle_workflow_execution_stopped
->
    call_hook_if_defined(
        EndedIncarnation#atm_workflow_execution_incarnation_test_spec.after_hook,
        build_mock_call_ctx(StepMockCallReport, TestCtx),
        TestCtx
    ),
    TestCtx#test_ctx{ongoing_incarnations = []};

shift_monitored_lane_run_if_current_one_stopped(
    StepMockCallReport = #mock_call_report{timing = after_step, step = Step},
    TestCtx = #test_ctx{
        workflow_execution_exp_state = ExpState0,
        ongoing_incarnations = [EndedIncarnation | LeftoverIncarnations],
        prev_incarnations_executed_step_phases = PrevIncarnationsExecutedStepPhases,
        executed_step_phases = ExecutedStepPhases
    }
) when
    Step =:= handle_workflow_abruptly_stopped;
    Step =:= handle_workflow_execution_stopped
->
    #atm_workflow_execution_incarnation_test_spec{
        incarnation_num = IncarnationNum,
        after_hook = AfterHook
    } = EndedIncarnation,

    call_hook_if_defined(AfterHook, build_mock_call_ctx(StepMockCallReport, TestCtx), TestCtx),

    #atm_workflow_execution_incarnation_test_spec{
        lane_runs = [#atm_lane_run_execution_test_spec{
            selector = {AtmLaneIndex, AtmRunNum}
        } | _]
    } = hd(LeftoverIncarnations),

    TestCtx#test_ctx{
        current_lane_index = AtmLaneIndex,
        current_run_num = AtmRunNum,
        workflow_execution_exp_state = atm_workflow_execution_exp_state_builder:set_current_lane_run(
            AtmLaneIndex, AtmRunNum, ExpState0
        ),
        ongoing_incarnations = LeftoverIncarnations,
        prev_incarnations_executed_step_phases = [
            {IncarnationNum, ExecutedStepPhases}
            | PrevIncarnationsExecutedStepPhases
        ],
        executed_step_phases = []
    };

shift_monitored_lane_run_if_current_one_stopped(
    StepMockCallReport = #mock_call_report{
        timing = after_step,
        step = Step,
        args = [{AtmLaneIndex, _}, _, _]
    },
    TestCtx = #test_ctx{
        current_lane_index = CurrentAtmLaneIndex,
        current_run_num = CurrentAtmRunNum
    }
) when
    % Lane execution stopped callback may ba called during lane preparation in case of failure
    % - in such case it will be prepare_lane that will finish executing last
    Step =:= prepare_lane;
    Step =:= resume_lane;
    Step =:= handle_lane_execution_stopped
->
    AtmLaneRunTestSpec = get_lane_run_test_spec(AtmLaneIndex, TestCtx),
    AtmLaneRunSelector = AtmLaneRunTestSpec#atm_lane_run_execution_test_spec.selector,
    IsLastLaneRunStepPhase = is_end_phase_of_last_step_of_lane_run(
        StepMockCallReport, AtmLaneRunSelector, TestCtx
    ),
    CurrentAtmLaneRunSelector = {CurrentAtmLaneIndex, CurrentAtmRunNum},

    case {IsLastLaneRunStepPhase, AtmLaneRunSelector} of
        {true, CurrentAtmLaneRunSelector} ->
            shift_monitored_lane_run_after_current_one_stopped(TestCtx);
        {true, _} ->
            filter_out_stopped_lane_run_preparing_in_advance(AtmLaneRunSelector, TestCtx);
        {false, _} ->
            TestCtx
    end;

shift_monitored_lane_run_if_current_one_stopped(_, TestCtx) ->
    TestCtx.


%% @private
-spec is_end_phase_of_last_step_of_lane_run(
    mock_call_report(),
    atm_lane_execution:lane_run_selector(),
    test_ctx()
) ->
    boolean().
is_end_phase_of_last_step_of_lane_run(
    #mock_call_report{timing = after_step, step = handle_lane_execution_stopped},
    AtmLaneRunSelector,
    #test_ctx{executed_step_phases = ExecutedStepPhases}
) ->
    lists:any(
        fun(StepSelector) -> lists:member(StepSelector, ExecutedStepPhases) end,
        [{prepare_lane, after_step, AtmLaneRunSelector}, {resume_lane, after_step, AtmLaneRunSelector}]
    );

is_end_phase_of_last_step_of_lane_run(
    #mock_call_report{timing = after_step},
    AtmLaneRunSelector,
    #test_ctx{executed_step_phases = ExecutedStepPhases}
) ->
    lists:member({handle_lane_execution_stopped, after_step, AtmLaneRunSelector}, ExecutedStepPhases).


%% @private
-spec shift_monitored_lane_run_after_current_one_stopped(test_ctx()) -> test_ctx().
shift_monitored_lane_run_after_current_one_stopped(TestCtx = #test_ctx{
    workflow_execution_exp_state = ExpState0,
    ongoing_incarnations = [OngoingIncarnation | LeftoverIncarnations]
}) ->
    case OngoingIncarnation#atm_workflow_execution_incarnation_test_spec.lane_runs of
        [_] ->
            NewOngoingIncarnation = OngoingIncarnation#atm_workflow_execution_incarnation_test_spec{
                lane_runs = []
            },
            TestCtx#test_ctx{
                ongoing_incarnations = [NewOngoingIncarnation | LeftoverIncarnations]
            };
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
                workflow_execution_exp_state = atm_workflow_execution_exp_state_builder:set_current_lane_run(
                    AtmLaneIndex, AtmRunNum, ExpState0
                ),
                ongoing_incarnations = [NewOngoingIncarnation | LeftoverIncarnations]
            }
    end.


%% @private
-spec filter_out_stopped_lane_run_preparing_in_advance(
    atm_lane_execution:lane_run_selector(),
    test_ctx()
) ->
    test_ctx().
filter_out_stopped_lane_run_preparing_in_advance(AtmLaneRunSelector, TestCtx = #test_ctx{
    ongoing_incarnations = [OngoingIncarnation | LeftoverIncarnations]
}) ->
    NewOngoingIncarnation = OngoingIncarnation#atm_workflow_execution_incarnation_test_spec{
        lane_runs = lists:filter(fun(#atm_lane_run_execution_test_spec{selector = Selector}) ->
            Selector =/= AtmLaneRunSelector
        end, OngoingIncarnation#atm_workflow_execution_incarnation_test_spec.lane_runs)
    },
    TestCtx#test_ctx{ongoing_incarnations = [NewOngoingIncarnation | LeftoverIncarnations]}.
