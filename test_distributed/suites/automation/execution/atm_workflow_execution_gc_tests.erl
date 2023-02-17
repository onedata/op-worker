%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning atm garbage collection mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_gc_tests).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    garbage_collect_atm_workflow_executions/0,
    massive_garbage_collect_atm_workflow_executions/0
]).


-define(ITERATED_STORE_SCHEMA_ID, <<"iterated_store_id">>).

-define(ATM_WORKFLOW_SCHEMA_DRAFT(__TESTCASE, __LAMBDAS), #atm_workflow_schema_dump_draft{
    name = str_utils:to_binary(__TESTCASE),
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [
            ?ATM_SV_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, ?ATM_NUMBER_DATA_SPEC, ?RAND_INT(100))
        ],
        lanes = [#atm_lane_schema_draft{
            parallel_boxes = [#atm_parallel_box_schema_draft{
                id = <<"pb1">>,
                tasks = [#atm_task_schema_draft{
                    id = <<"task1">>,
                    lambda_id = ?ECHO_LAMBDA_ID,
                    lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
                    argument_mappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
                    result_mappings = []
                }]
            }],
            store_iterator_spec = #atm_store_iterator_spec_draft{
                store_schema_id = ?ITERATED_STORE_SCHEMA_ID
            }
        }]
    },
    supplementary_lambdas = __LAMBDAS
}).

-define(ATM_WORKFLOW_SCHEMA_DRAFT(__TESTCASE), ?ATM_WORKFLOW_SCHEMA_DRAFT(__TESTCASE, #{
    ?ECHO_LAMBDA_ID => #{?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(
        ?ATM_NUMBER_DATA_SPEC, ?RAND_ELEMENT([return_value, file_pipe])
    )}
})).
-define(ATM_WORKFLOW_SCHEMA_DRAFT, ?ATM_WORKFLOW_SCHEMA_DRAFT(?FUNCTION_NAME)).

-define(ATM_WORKFLOW_SCHEMA_WITH_PAUSE_DRAFT, ?ATM_WORKFLOW_SCHEMA_DRAFT(?FUNCTION_NAME, #{
    ?ECHO_LAMBDA_ID => #{?ECHO_LAMBDA_REVISION_NUM => #atm_lambda_revision_draft{
        operation_spec = #atm_openfaas_operation_spec_draft{
            docker_image = ?ECHO_WITH_PAUSE_DOCKER_IMAGE_ID
        },
        argument_specs = [#atm_parameter_spec{
            name = ?ECHO_ARG_NAME,
            data_spec = ?ATM_NUMBER_DATA_SPEC,
            is_optional = false
        }],
        result_specs = [#atm_lambda_result_spec{
            name = ?ECHO_ARG_NAME,
            data_spec = ?ATM_NUMBER_DATA_SPEC,
            relay_method = ?RAND_ELEMENT([return_value, file_pipe])
        }]
    }}
})).

-define(TASK1_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb1">>, <<"task1">>}).

-define(ATTEMPTS, 300).  %% large num for slow bamboo -.-


%%%===================================================================
%%% Tests
%%%===================================================================


garbage_collect_atm_workflow_executions() ->
    set_env(atm_suspended_workflow_executions_expiration_sec, 4),
    set_env(atm_ended_workflow_executions_expiration_sec, 2),

    % Force early gc run (with original interval 1h) to ensure it will not launch
    % during following test setup
    run_gc(),

    Timestamp = time_test_utils:get_frozen_time_seconds(),

    ExpAtmWorkflowExecutionState1 = run_atm_workflow_execution(?FUNCTION_NAME, paused),

    time_test_utils:set_current_time_seconds(Timestamp + 2),
    ExpAtmWorkflowExecutionState2 = run_atm_workflow_execution(?FUNCTION_NAME, paused),
    ExpAtmWorkflowExecutionState3 = run_atm_workflow_execution(?FUNCTION_NAME, finished),

    time_test_utils:set_current_time_seconds(Timestamp + 6),
    ExpAtmWorkflowExecutionState4 = run_atm_workflow_execution(?FUNCTION_NAME, finished),
    ExpAtmWorkflowExecutionState5 = run_atm_workflow_execution(?FUNCTION_NAME, finished),

    AllExpAtmWorkflowExecutionStates = [
        ExpAtmWorkflowExecutionState1,
        ExpAtmWorkflowExecutionState2, ExpAtmWorkflowExecutionState3,
        ExpAtmWorkflowExecutionState4, ExpAtmWorkflowExecutionState5
    ],
    assert_all_match_with_backend(AllExpAtmWorkflowExecutionStates),

    % Move time backward and assert that no atm workflow execution is deleted
    time_test_utils:set_current_time_seconds(Timestamp - 2),

    set_env(atm_workflow_execution_garbage_collector_run_interval_sec, 1),
    % Manual gc run is needed for new interval to activate (original is 1h)
    run_gc(),

    assert_all_match_with_backend(AllExpAtmWorkflowExecutionStates),

    time_test_utils:set_current_time_seconds(Timestamp + 5),
    timer:sleep(timer:seconds(3)),
    assert_all_match_with_backend([
        ExpAtmWorkflowExecutionState2, ExpAtmWorkflowExecutionState4, ExpAtmWorkflowExecutionState5
    ]),
    assert_atm_workflow_executions_deleted([
        ExpAtmWorkflowExecutionState1, ExpAtmWorkflowExecutionState3
    ]),

    time_test_utils:set_current_time_seconds(Timestamp + 9),
    timer:sleep(timer:seconds(3)),
    assert_atm_workflow_executions_deleted(AllExpAtmWorkflowExecutionStates).


massive_garbage_collect_atm_workflow_executions() ->
    set_env(atm_suspended_workflow_executions_expiration_sec, 4),
    set_env(atm_ended_workflow_executions_expiration_sec, 2),

    % Force early gc run (with original interval 1h) to ensure it will not launch
    % during following test setup
    run_gc(),

    FinishedAtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(?ATM_WORKFLOW_SCHEMA_DRAFT),
    FinishedAtmWorkflowSchemaRevision = atm_test_inventory:get_workflow_schema_revision(
        1, FinishedAtmWorkflowSchemaId
    ),
    PausedAtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(
        ?ATM_WORKFLOW_SCHEMA_WITH_PAUSE_DRAFT
    ),
    PausedAtmWorkflowSchemaRevision = atm_test_inventory:get_workflow_schema_revision(
        1, PausedAtmWorkflowSchemaId
    ),

    ExpStatusesAndStates = lists_utils:pmap(fun(_) ->
        {StoppedStatus, SchemaId, SchemaRevision} = case ?RAND_BOOL() of
            true -> {paused, PausedAtmWorkflowSchemaId,PausedAtmWorkflowSchemaRevision};
            false -> {finished, FinishedAtmWorkflowSchemaId, FinishedAtmWorkflowSchemaRevision}
        end,
        ExecutionId = schedule_workflow_execution(SchemaId),

        await_workflow_execution_status(ExecutionId, StoppedStatus),

        ExpState = expect_workflow_execution(StoppedStatus, ExecutionId, SchemaRevision),
        ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpState)),

        {StoppedStatus, ExpState}
    end, lists:seq(1, 600)),

    {ExpPausedStates, ExpFinishedStates} = lists:foldl(fun
        ({paused, ExpState}, {Acc1, Acc2}) ->
            {[ExpState | Acc1], Acc2};
        ({finished, ExpState}, {Acc1, Acc2}) ->
            {Acc1, [ExpState | Acc2]}
    end, {[], []}, ExpStatusesAndStates),

    set_env(atm_workflow_execution_garbage_collector_run_interval_sec, 1),
    % Manual gc run is needed for new interval to activate (original is 1h)
    run_gc(),

    assert_all_match_with_backend(ExpPausedStates ++ ExpFinishedStates),

    time_test_utils:simulate_seconds_passing(3),
    ?assertEqual([], list_ended_workflows(), ?ATTEMPTS),
    ?assertEqual([], list_discarded_workflows(), ?ATTEMPTS),

    assert_all_match_with_backend(ExpPausedStates),
    assert_atm_workflow_executions_deleted(ExpFinishedStates),

    time_test_utils:simulate_seconds_passing(2),
    ?assertEqual([], list_suspended_workflows(), ?ATTEMPTS),
    ?assertEqual([], list_discarded_workflows(), ?ATTEMPTS),

    assert_atm_workflow_executions_deleted(ExpPausedStates),

    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec set_env(atom(), term()) -> ok.
set_env(EnvVar, EnvValue) ->
    ?rpc(?PROVIDER_SELECTOR, op_worker:set_env(EnvVar, EnvValue)).


%% @private
-spec run_atm_workflow_execution(term(), paused | finished) ->
    atm_workflow_execution_exp_state_builder:exp_state().
run_atm_workflow_execution(Testcase, paused) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        clock_status = frozen,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(Testcase),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                    before_step_exp_state_diff = [
                        {lane_run, {1, 1}, stopping},
                        workflow_stopping
                    ],
                    after_step_exp_state_diff = [{lane_run, {1, 1}, paused}]
                }
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [workflow_paused]
            }
        }],
        test_gc = false
    });

run_atm_workflow_execution(Testcase, finished) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        clock_status = frozen,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(Testcase),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{selector = {1, 1}}],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_finished
                ]
            }
        }],
        test_gc = false
    }).


%% @private
-spec schedule_workflow_execution(od_atm_workflow_schema:id()) ->
    atm_workflow_execution:id().
schedule_workflow_execution(AtmWorkflowSchemaId) ->
    SessionId = oct_background:get_user_session_id(?USER_SELECTOR, ?PROVIDER_SELECTOR),
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),

    {AtmWorkflowExecutionId, _} = ?rpc(?PROVIDER_SELECTOR, mi_atm:schedule_workflow_execution(
        SessionId, SpaceId, AtmWorkflowSchemaId, 1, #{}, undefined
    )),
    AtmWorkflowExecutionId.


%% @private
-spec list_suspended_workflows() -> [atm_workflow_execution:id()].
list_suspended_workflows() ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    element(2, lists:unzip(?rpc(?PROVIDER_SELECTOR, atm_suspended_workflow_executions:list(
        SpaceId, #{offset => 0, limit => 100000000000000000000000000000}
    )))).


%% @private
-spec list_ended_workflows() -> [atm_workflow_execution:id()].
list_ended_workflows() ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    element(2, lists:unzip(?rpc(?PROVIDER_SELECTOR, atm_ended_workflow_executions:list(
        SpaceId, #{offset => 0, limit => 100000000000000000000000000000}
    )))).


%% @private
-spec list_discarded_workflows() -> [atm_workflow_execution:id()].
list_discarded_workflows() ->
    ?rpc(
        ?PROVIDER_SELECTOR,
        atm_discarded_workflow_executions:list(<<>>, 100000000000000000000000000000)
    ).


%% @private
-spec await_workflow_execution_status(atm_workflow_execution:id(), atm_workflow_execution:status()) ->
    ok.
await_workflow_execution_status(AtmWorkflowExecutionId, ExpStatus) ->
    GetStatusFun = fun() ->
        case ?rpc(?PROVIDER_SELECTOR, atm_workflow_execution:get(AtmWorkflowExecutionId)) of
            {ok, #document{value = #atm_workflow_execution{status = Status}}} ->
                {ok, Status};
            {error, _} = Error ->
                Error
        end
    end,
    ?assertMatch({ok, ExpStatus}, GetStatusFun(), ?ATTEMPTS).


%% @private
-spec expect_workflow_execution(
    paused | finished,
    atm_workflow_execution:id(),
    atm_workflow_schema_revision:record()
) ->
    ok.
expect_workflow_execution(paused, AtmWorkflowExecutionId, AtmWorkflowSchemaRevision) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),

    ExpInitialState = atm_workflow_execution_exp_state_builder:init(
        ?PROVIDER_SELECTOR, SpaceId, frozen, AtmWorkflowExecutionId, AtmWorkflowSchemaRevision
    ),
    atm_workflow_execution_exp_state_builder:expect(ExpInitialState, [
        {lane_run, {1, 1}, started_preparing},
        {lane_run, {1, 1}, created},
        {lane_run, {1, 1}, enqueued},
        {task, ?TASK1_SELECTOR({1, 1}), items_scheduled, 1},
        {task, ?TASK1_SELECTOR({1, 1}), stopping},
        {lane_run, {1, 1}, stopping},
        workflow_stopping,
        {task, ?TASK1_SELECTOR({1, 1}), items_finished, 1},
        {task, ?TASK1_SELECTOR({1, 1}), paused},
        {lane_run, {1, 1}, paused},
        workflow_paused
    ]);

expect_workflow_execution(finished, AtmWorkflowExecutionId, AtmWorkflowSchemaRevision) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),

    ExpInitialState = atm_workflow_execution_exp_state_builder:init(
        ?PROVIDER_SELECTOR, SpaceId, frozen, AtmWorkflowExecutionId, AtmWorkflowSchemaRevision
    ),
    atm_workflow_execution_exp_state_builder:expect(ExpInitialState, [
        {lane_run, {1, 1}, started_preparing},
        {lane_run, {1, 1}, created},
        {lane_run, {1, 1}, enqueued},
        {task, ?TASK1_SELECTOR({1, 1}), items_scheduled, 1},
        {task, ?TASK1_SELECTOR({1, 1}), items_finished, 1},
        {task, ?TASK1_SELECTOR({1, 1}), finished},
        {lane_run, {1, 1}, finished},
        {lane_runs, [{1, 1}], rerunable},
        workflow_finished
    ]).


%% @private
-spec run_gc() -> ok.
run_gc() ->
    ?rpc(?PROVIDER_SELECTOR, atm_workflow_execution_garbage_collector:run()).


%% @private
-spec assert_all_match_with_backend([atm_workflow_execution_exp_state_builder:exp_state()]) ->
    ok.
assert_all_match_with_backend(ExpAtmWorkflowExecutionStates) ->
    lists_utils:pforeach(fun(ExpAtmWorkflowExecutionState) ->
        ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpAtmWorkflowExecutionState))
    end, ExpAtmWorkflowExecutionStates).


%% @private
-spec assert_atm_workflow_executions_deleted([atm_workflow_execution_exp_state_builder:exp_state()]) ->
    ok.
assert_atm_workflow_executions_deleted(ExpAtmWorkflowExecutionStates) ->
    lists_utils:pforeach(fun(ExpAtmWorkflowExecutionState) ->
        atm_workflow_execution_exp_state_builder:assert_deleted(ExpAtmWorkflowExecutionState)
    end, ExpAtmWorkflowExecutionStates).
