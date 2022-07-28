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
-include("modules/automation/atm_execution.hrl").
-include("onenv_test_utils.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([init/1, teardown/1]).
-export([run/1]).
-export([cancel_workflow_execution/1]).
-export([browse_store/2, browse_store/3]).

-type step_name() ::
    prepare_lane |
    create_run |
    run_task_for_item |
    process_task_result_for_item |
    process_streamed_task_data |
    report_item_error |
    handle_task_execution_ended |
    handle_lane_execution_ended |
    handle_workflow_execution_ended.

-type step_phase_timing() :: before_step | after_step.

-type mock_call_ctx() :: #atm_mock_call_ctx{}.
-type hook() :: fun((mock_call_ctx()) -> ok).
-type exp_state_diff() :: fun((mock_call_ctx()) ->
    false | {true, atm_workflow_execution_exp_state_builder:exp_state()}
).

-type step_phase_selector() :: {
    step_name(),
    step_phase_timing(),
    atm_lane_execution:lane_run_selector() | atm_workflow_execution:incarnation()
}.
-record(step_phase, {
    selector :: step_phase_selector(),
    mock_call_report :: mock_call_report(),
    mock_spec :: step_mock_spec(),
    reply_to :: pid()
}).
-type step_phase() :: #step_phase{}.

-type result_override() :: {return, term()} | {throw, errors:error()}.

-type mock_strategy() ::
    % original step will be run unperturbed
    passthrough |
    % original step will be run but it's result will be replaced with specified one
    {passthrough_with_result_override, result_override()} |
    % original step will not be run and specified result will be returned immediately
    {yield, result_override()}.

-type step_mock_spec() :: #atm_step_mock_spec{}.

-type lane_run_test_spec() :: #atm_lane_run_execution_test_spec{}.
-type incarnation_test_spec() :: #atm_workflow_execution_incarnation_test_spec{}.
-type test_spec() :: #atm_workflow_execution_test_spec{}.

-export_type([
    mock_call_ctx/0, hook/0, exp_state_diff/0,
    result_override/0, mock_strategy/0, step_phase_selector/0, step_mock_spec/0,
    lane_run_test_spec/0, incarnation_test_spec/0, test_spec/0
]).

% message used for synchronous calls from the execution process to the test process
% to enable applying hooks or modifying expectations before and after execution of
% a specific step (execution process waits for an ACK before it proceeds).
-record(mock_call_report, {
    step :: step_name(),
    timing :: step_phase_timing(),
    args :: [term()]
}).
-type mock_call_report() :: #mock_call_report{}.
-type reply_to() :: {pid(), reference()}.

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

    executed_step_phases = [] :: [step_phase_selector()],
    pending_step_phases = [] :: [step_phase()],
    deferred_step_phases = #{} :: #{step_phase_selector() => [step_phase()]},

    test_hung_probes_left :: non_neg_integer()
}).
-type test_ctx() :: #test_ctx{}.

-define(AWAIT_OTHER_PARALLEL_PIPELINES_NEXT_STEP_INTERVAL, 100).
-define(TEST_HUNG_MAX_PROBES_NUM, 20).

-define(ASSERT_RETRIES, 30).

-define(TEST_PROC_PID_KEY(__ATM_WORKFLOW_EXECUTION_ID),
    {atm_test_runner_process, __ATM_WORKFLOW_EXECUTION_ID}
).
-define(ATM_WORKFLOW_EXECUTION_ID_MSG(__ATM_WORKFLOW_EXECUTION_ID),
    {atm_workflow_execution_id, __ATM_WORKFLOW_EXECUTION_ID}
).


-define(NO_DIFF, fun(_) -> false end).

-define(NOW(), global_clock:timestamp_seconds()).

-define(INFINITE_LOG_BASED_STORES_LISTING_OPTS, #{
    start_from => undefined,
    offset => 0,
    limit => 1000000000
}).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(oct_background:entity_selector() | [oct_background:entity_selector()]) ->
    ok.
init(ProviderSelectors) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),

    mock_workflow_execution_factory(Workers),
    mock_workflow_execution_handler_steps(Workers),
    mock_lane_execution_handler_steps(Workers),
    mock_lane_execution_factory_steps(Workers).


-spec teardown(oct_background:entity_selector() | [oct_background:entity_selector()]) ->
    ok.
teardown(ProviderSelectors) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),

    unmock_lane_execution_factory_steps(Workers),
    unmock_lane_execution_handler_steps(Workers),
    unmock_workflow_execution_handler_steps(Workers),
    unmock_workflow_execution_factory(Workers).


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
    AtmWorkflowSchemaRevision = atm_test_inventory:get_workflow_schema_revision(
        AtmWorkflowSchemaRevisionNum, AtmWorkflowSchemaId
    ),

    TestProcPid = self(),
    {AtmWorkflowExecutionId, _} = ?rpc(ProviderSelector, mi_atm:schedule_workflow_execution(
        SessionId, SpaceId, AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum,
        AtmStoreInitialContentOverlay#{test_process => TestProcPid}, CallbackUrl
    )),

    AtmLaneSchemas = AtmWorkflowSchemaRevision#atm_workflow_schema_revision.lanes,
    ExpState = atm_workflow_execution_exp_state_builder:init(
        ProviderSelector, SpaceId, AtmWorkflowExecutionId, ?NOW(), AtmLaneSchemas
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


-spec cancel_workflow_execution(atm_workflow_execution_test_runner:mock_call_ctx()) ->
    ok.
cancel_workflow_execution(#atm_mock_call_ctx{
    provider = ProviderSelector,
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, mi_atm:cancel_workflow_execution(SessionId, AtmWorkflowExecutionId)).


-spec browse_store(automation:id(), mock_call_ctx()) -> json_utils:json_term().
browse_store(AtmStoreSchemaId, AtmMockCallCtx) ->
    browse_store(AtmStoreSchemaId, undefined, AtmMockCallCtx).


-spec browse_store(
    exception_store | automation:id(),
    undefined | atm_lane_execution:lane_run_selector() | atm_task_execution:id(),
    mock_call_ctx()
) ->
    json_utils:json_term().
browse_store(AtmStoreSchemaId, AtmTaskExecutionIdOrLaneRunSelector, AtmMockCallCtx = #atm_mock_call_ctx{
    provider = ProviderSelector,
    space = SpaceSelector,
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    SpaceId = oct_background:get_space_id(SpaceSelector),
    AtmStoreId = get_store_id(AtmStoreSchemaId, AtmTaskExecutionIdOrLaneRunSelector, AtmMockCallCtx),
    ?rpc(ProviderSelector, browse_store(SessionId, SpaceId, AtmWorkflowExecutionId, AtmStoreId)).


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
                ct:pal("Automation workflow execution test hung after steps: ~p", [
                    TestCtx0#test_ctx.executed_step_phases
                ]),
                ?assertEqual(success, failure);
            Num ->
                TestCtx1 = TestCtx0#test_ctx{test_hung_probes_left = Num - 1},
                TestCtx2 = address_pending_expectations(TestCtx1),
                TestCtx3 = end_pending_step_phase_executions(TestCtx2),

                case has_workflow_ended(TestCtx3) of
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

    call_if_defined(get_hook(StepMockCallReport, StepMockSpec), StepMockCallCtx),

    ExpStateDiff = get_exp_state_diff(StepMockCallReport, StepMockSpec),
    TestCtx1 = case ExpStateDiff(StepMockCallCtx) of
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
        reply_to_execution_process(ReplyTo, case Timing of
            before_step -> MockStrategy;
            after_step -> ok
        end),

        shift_monitored_lane_run_if_current_one_ended(
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
-spec has_workflow_ended(test_ctx()) -> boolean().
has_workflow_ended(#test_ctx{
    ongoing_incarnations = [],
    pending_step_phases = [],
    deferred_step_phases = DeferredStepPhases
}) ->
    maps_utils:is_empty(DeferredStepPhases);

has_workflow_ended(#test_ctx{}) ->
    false.


%% @private
-spec get_step_mock_spec(mock_call_report(), test_ctx()) ->
    {step_phase_selector(), step_mock_spec()}.
get_step_mock_spec(
    #mock_call_report{step = handle_workflow_execution_ended, timing = Timing},
    #test_ctx{ongoing_incarnations = [#atm_workflow_execution_incarnation_test_spec{
        incarnation_num = IncarnationNum,
        handle_workflow_execution_ended = Spec
    } | _]}
) ->
    {{handle_workflow_execution_ended, Timing, IncarnationNum}, Spec};

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
    #mock_call_report{
        step = handle_lane_execution_ended,
        timing = Timing,
        args = [{AtmLaneIndex, _}, _, _]
    },
    NewTestCtx
) ->
    #atm_lane_run_execution_test_spec{
        selector = Selector,
        handle_lane_execution_ended = Spec
    } = get_lane_run_test_spec(AtmLaneIndex, NewTestCtx),

    {{handle_lane_execution_ended, Timing, Selector}, Spec};

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
build_mock_call_ctx(#mock_call_report{args = CallArgs}, #test_ctx{
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
        ItemsBatch, _ReportResultUrl, _HeartbeatUrl
    ]}) ->
        ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_items_in_processing_increased(
            AtmTaskExecutionId, length(ItemsBatch), ExpState0
        ),
        ExpState2 = atm_workflow_execution_exp_state_builder:expect_task_transitioned_to_active_status_if_was_in_pending_status(
            AtmTaskExecutionId, ExpState1
        ),
        ExpState3 = atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_active_status_if_was_in_pending_status(
            AtmTaskExecutionId, ExpState2
        ),
        ExpState4 = atm_workflow_execution_exp_state_builder:expect_task_lane_run_transitioned_to_active_status_if_was_in_enqueued_status(
            AtmTaskExecutionId, ExpState3
        ),
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
    #mock_call_report{step = handle_task_execution_ended, timing = before_step},
    #atm_step_mock_spec{before_step_exp_state_diff = default}
) ->
    ?NO_DIFF;

get_exp_state_diff(
    #mock_call_report{step = handle_task_execution_ended, timing = after_step},
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
    #mock_call_report{step = handle_lane_execution_ended, timing = before_step},
    #atm_step_mock_spec{before_step_exp_state_diff = default}
) ->
    ?NO_DIFF;

get_exp_state_diff(
    #mock_call_report{step = handle_lane_execution_ended, timing = after_step},
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
                ExpState0
        end,
        {true, atm_workflow_execution_exp_state_builder:expect_lane_run_finished(
            AtmLaneRunSelector, ExpState1
        )}
    end;

get_exp_state_diff(
    #mock_call_report{step = handle_workflow_execution_ended, timing = before_step},
    #atm_step_mock_spec{before_step_exp_state_diff = default}
) ->
    ?NO_DIFF;

get_exp_state_diff(
    #mock_call_report{step = handle_workflow_execution_ended, timing = after_step},
    #atm_step_mock_spec{after_step_exp_state_diff = default}
) ->
    fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_finished(ExpState0)}
    end;

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
assert_exp_workflow_execution_state(#test_ctx{
    workflow_execution_exp_state = ExpState,
    executed_step_phases = ExecutedStepPhases
}) ->
    case atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpState, ?ASSERT_RETRIES) of
        true ->
            ok;
        false ->
            ct:pal("Automation workflow execution test failed after steps: ~p", [
                ExecutedStepPhases
            ]),
            ?assertEqual(success, failure)
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
    % Lane execution ended callback may ba called during lane preparation in case of failure
    % - in such case it will be prepare_lane that will finish executing last
    Step =:= prepare_lane;
    Step =:= handle_lane_execution_ended
->
    AtmLaneRunTestSpec = get_lane_run_test_spec(AtmLaneIndex, TestCtx),
    AtmLaneRunSelector = AtmLaneRunTestSpec#atm_lane_run_execution_test_spec.selector,
    IsLastLaneRunStepPhase = is_end_phase_of_last_step_of_lane_run(
        StepMockCallReport, AtmLaneRunSelector, TestCtx
    ),
    CurrentAtmLaneRunSelector = {CurrentAtmLaneIndex, CurrentAtmRunNum},

    case {IsLastLaneRunStepPhase, AtmLaneRunSelector} of
        {true, CurrentAtmLaneRunSelector} ->
            shift_monitored_lane_run_after_current_one_ended(TestCtx);
        {true, _} ->
            filter_out_ended_lane_run_preparing_in_advance(AtmLaneRunSelector, TestCtx);
        {false, _} ->
            TestCtx
    end;

shift_monitored_lane_run_if_current_one_ended(_, TestCtx) ->
    TestCtx.


%% @private
-spec is_end_phase_of_last_step_of_lane_run(
    mock_call_report(),
    atm_lane_execution:lane_run_selector(),
    test_ctx()
) ->
    boolean().
is_end_phase_of_last_step_of_lane_run(
    #mock_call_report{timing = after_step, step = Step},
    AtmLaneRunSelector,
    #test_ctx{executed_step_phases = ExecutedStepPhases}
) ->
    SecondToLastStepPhaseInLaneRunInCaseThisIsTheLastOne = {
        hd([prepare_lane, handle_lane_execution_ended] -- [Step]),
        after_step,
        AtmLaneRunSelector
    },
    lists:member(
        SecondToLastStepPhaseInLaneRunInCaseThisIsTheLastOne,
        ExecutedStepPhases
    ).


%% @private
-spec shift_monitored_lane_run_after_current_one_ended(test_ctx()) -> test_ctx().
shift_monitored_lane_run_after_current_one_ended(TestCtx = #test_ctx{
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
                ongoing_incarnations = [NewOngoingIncarnation | LeftoverIncarnations]
            }
    end.


%% @private
-spec filter_out_ended_lane_run_preparing_in_advance(
    atm_lane_execution:lane_run_selector(),
    test_ctx()
) ->
    test_ctx().
filter_out_ended_lane_run_preparing_in_advance(AtmLaneRunSelector, TestCtx = #test_ctx{
    ongoing_incarnations = [OngoingIncarnation | LeftoverIncarnations]
}) ->
    NewOngoingIncarnation = OngoingIncarnation#atm_workflow_execution_incarnation_test_spec{
        lane_runs = lists:filter(fun(#atm_lane_run_execution_test_spec{selector = Selector}) ->
            Selector =/= AtmLaneRunSelector
        end, OngoingIncarnation#atm_workflow_execution_incarnation_test_spec.lane_runs)
    },
    TestCtx#test_ctx{ongoing_incarnations = [NewOngoingIncarnation | LeftoverIncarnations]}.



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

    test_utils:mock_expect(Workers, atm_workflow_execution_handler, handle_task_results_processed_for_all_items, fun(
        AtmWorkflowExecutionId,
        _AtmWorkflowExecutionEnv,
        AtmTaskExecutionId
    ) ->
        workflow_engine:report_task_data_streaming_concluded(AtmWorkflowExecutionId, AtmTaskExecutionId, success)
    end),

    mock_workflow_execution_handler_step(Workers, prepare_lane, 3),
    mock_workflow_execution_handler_step(Workers, run_task_for_item, 6),
    mock_workflow_execution_handler_step(Workers, process_task_result_for_item, 5),
    mock_workflow_execution_handler_step(Workers, process_streamed_task_data, 4),
    mock_workflow_execution_handler_step(Workers, report_item_error, 3),
    mock_workflow_execution_handler_step(Workers, handle_task_execution_ended, 3),
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
-spec mock_lane_execution_handler_steps([node()]) -> ok.
mock_lane_execution_handler_steps(Workers) ->
    test_utils:mock_new(Workers, atm_lane_execution_handler, [passthrough, no_history]),

    test_utils:mock_expect(Workers, atm_lane_execution_handler, handle_ended, fun(
        AtmLaneRunSelector,
        AtmWorkflowExecutionId,
        AtmWorkflowExecutionCtx
    ) ->
        exec_mock(
            AtmWorkflowExecutionId,
            handle_lane_execution_ended,
            [AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx]
        )
    end).


%% @private
-spec unmock_lane_execution_handler_steps([node()]) -> ok.
unmock_lane_execution_handler_steps(Workers) ->
    test_utils:mock_unload(Workers, atm_lane_execution_handler).


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
            MockExecution = call_test_process(TestProcPid, MockCallReport),

            case MockExecution of
                passthrough ->
                    Result = meck:passthrough(Args),
                    ok = call_test_process(TestProcPid, MockCallReport#mock_call_report{
                        timing = after_step
                    }),
                    Result;
                {passthrough_with_result_override, ResultOverride} ->
                    meck:passthrough(Args),
                    ok = call_test_process(TestProcPid, MockCallReport#mock_call_report{
                        timing = after_step
                    }),
                    apply_result_override(ResultOverride);
                {yield, ResultOverride} ->
                    apply_result_override(ResultOverride)
            end
    end.


%% @private
-spec apply_result_override
    ({return, Result}) -> Result when Result :: term();
    ({throw, errors:error()}) -> no_return().
apply_result_override({return, Result}) -> Result;
apply_result_override({throw, Error}) -> throw(Error).


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
    (reply_to(), mock_strategy()) -> ok;
    % when replying to 'after_step' report
    (reply_to(), ok) -> ok.
reply_to_execution_process({ExecutionProcPid, MRef}, Reply) ->
    ExecutionProcPid ! {MRef, Reply}.


%% @private
-spec get_store_id(
    exception_store | automation:id(),
    undefined | atm_lane_execution:lane_run_selector() | atm_task_execution:id(),
    mock_call_ctx()
) ->
    atm_store:id().
get_store_id(exception_store, AtmLaneRunSelector, #atm_mock_call_ctx{
    provider = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    {ok, #document{value = AtmWorkflowExecution}} = ?rpc(
        ProviderSelector, atm_workflow_execution:get(AtmWorkflowExecutionId)
    ),
    {ok, AtmLaneRun} = atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution),
    AtmLaneRun#atm_lane_execution_run.exception_store_id;

get_store_id(?WORKFLOW_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID, _AtmTaskExecutionId, #atm_mock_call_ctx{
    provider = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    {ok, #document{value = #atm_workflow_execution{system_audit_log_store_id = AtmStoreId}}} = ?rpc(
        ProviderSelector, atm_workflow_execution:get(AtmWorkflowExecutionId)
    ),
    AtmStoreId;

get_store_id(?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID, AtmTaskExecutionId, #atm_mock_call_ctx{
    provider = ProviderSelector
}) ->
    {ok, #document{value = #atm_task_execution{system_audit_log_store_id = AtmStoreId}}} = ?rpc(
        ProviderSelector, atm_task_execution:get(AtmTaskExecutionId)
    ),
    AtmStoreId;

get_store_id(?CURRENT_TASK_TIME_SERIES_STORE_SCHEMA_ID, AtmTaskExecutionId, #atm_mock_call_ctx{
    provider = ProviderSelector
}) ->
    {ok, #document{value = #atm_task_execution{time_series_store_id = AtmStoreId}}} = ?rpc(
        ProviderSelector, atm_task_execution:get(AtmTaskExecutionId)
    ),
    AtmStoreId;

get_store_id(AtmStoreSchemaId, _AtmTaskExecutionId, #atm_mock_call_ctx{
    provider = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    {ok, #document{value = #atm_workflow_execution{store_registry = AtmStoreRegistry}}} = ?rpc(
        ProviderSelector, atm_workflow_execution:get(AtmWorkflowExecutionId)
    ),
    maps:get(AtmStoreSchemaId, AtmStoreRegistry).


%% @private
-spec browse_store(session:id(), od_space:id(), atm_workflow_execution:id(), atm_store:id()) ->
    json_utils:json_term().
browse_store(SessionId, SpaceId, AtmWorkflowExecutionId, AtmStoreId) ->
    {ok, AtmStore = #atm_store{container = AtmStoreContainer}} = atm_store_api:get(
        AtmStoreId
    ),
    atm_store_content_browse_result:to_json(atm_store_api:browse_content(
        atm_workflow_execution_auth:build(SpaceId, AtmWorkflowExecutionId, SessionId),
        build_browse_opts(atm_store_container:get_store_type(AtmStoreContainer)),
        AtmStore
    )).


%% @private
build_browse_opts(audit_log) ->
    #atm_audit_log_store_content_browse_options{listing_opts = ?INFINITE_LOG_BASED_STORES_LISTING_OPTS};

build_browse_opts(list) ->
    #atm_list_store_content_browse_options{listing_opts = ?INFINITE_LOG_BASED_STORES_LISTING_OPTS};

build_browse_opts(tree_forest) ->
    #atm_tree_forest_store_content_browse_options{listing_opts = ?INFINITE_LOG_BASED_STORES_LISTING_OPTS};

build_browse_opts(range) ->
    #atm_range_store_content_browse_options{};

build_browse_opts(single_value) ->
    #atm_single_value_store_content_browse_options{};

build_browse_opts(time_series) ->
    #atm_time_series_store_content_browse_options{
        request = #time_series_slice_get_request{
            layout = #{?ALL_TIME_SERIES => [?ALL_METRICS]},
            start_timestamp = undefined,
            window_limit = 10000000000000000000000000000000000000000000000000000
        }
    }.
