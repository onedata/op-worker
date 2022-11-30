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
    garbage_collect_atm_workflow_executions/0
]).


-define(ITERATED_STORE_SCHEMA_ID, <<"iterated_store_id">>).
-define(TARGET_STORE_SCHEMA_ID, <<"target_store_id">>).

-define(ATM_WORKFLOW_SCHEMA_DRAFT, #atm_workflow_schema_dump_draft{
    name = str_utils:to_binary(?FUNCTION_NAME),
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [
            ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, lists:seq(1, 10)),
            ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(?TARGET_STORE_SCHEMA_ID)
        ],
        lanes = [#atm_lane_schema_draft{
            parallel_boxes = [#atm_parallel_box_schema_draft{tasks = [#atm_task_schema_draft{
                id = <<"task1">>,
                lambda_id = ?ECHO_LAMBDA_ID,
                lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
                argument_mappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
                result_mappings = []
            }]}],
            store_iterator_spec = #atm_store_iterator_spec_draft{
                store_schema_id = ?ITERATED_STORE_SCHEMA_ID
            },
            max_retries = 2
        }]
    },
    supplementary_lambdas = #{
        ?ECHO_LAMBDA_ID => #{?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(
            ?ATM_INTEGER_DATA_SPEC, ?RAND_ELEMENT([return_value, file_pipe])
        )}
    }
}).


%%%===================================================================
%%% Tests
%%%===================================================================


garbage_collect_atm_workflow_executions() ->
    Timestamp = global_clock:timestamp_seconds(),

    ExpAtmWorkflowExecutionState1 = run_atm_workflow_execution(paused),

    time_test_utils:set_current_time_seconds(Timestamp + 2),
    ExpAtmWorkflowExecutionState2 = run_atm_workflow_execution(paused),
    ExpAtmWorkflowExecutionState3 = run_atm_workflow_execution(finished),

    time_test_utils:set_current_time_seconds(Timestamp + 6),
    ExpAtmWorkflowExecutionState4 = run_atm_workflow_execution(finished),
    ExpAtmWorkflowExecutionState5 = run_atm_workflow_execution(finished),

    AllExpAtmWorkflowExecutionStates = [
        ExpAtmWorkflowExecutionState1,
        ExpAtmWorkflowExecutionState2, ExpAtmWorkflowExecutionState3,
        ExpAtmWorkflowExecutionState4, ExpAtmWorkflowExecutionState5
    ],

    set_atm_suspended_workflow_executions_expiration_interval_sec(4),
    set_atm_ended_workflow_executions_expiration_interval_sec(2),

    % Move time backward and assert that no atm workflow execution is deleted
    time_test_utils:set_current_time_seconds(Timestamp - 2),
    assert_matches_with_backend(AllExpAtmWorkflowExecutionStates),

    set_atm_workflow_execution_garbage_collector_run_interval_sec(1),
    % Manual gc run is needed for new interval to activate (original is 1h)
    run_gc(),
    assert_matches_with_backend(AllExpAtmWorkflowExecutionStates),

    time_test_utils:set_current_time_seconds(Timestamp + 5),
    timer:sleep(timer:seconds(3)),
    assert_matches_with_backend([
        ExpAtmWorkflowExecutionState2, ExpAtmWorkflowExecutionState4, ExpAtmWorkflowExecutionState5
    ]),
    assert_atm_workflow_executions_deleted([
        ExpAtmWorkflowExecutionState1, ExpAtmWorkflowExecutionState3
    ]),

    time_test_utils:set_current_time_seconds(Timestamp + 9),
    timer:sleep(timer:seconds(3)),
    assert_atm_workflow_executions_deleted(AllExpAtmWorkflowExecutionStates).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec set_atm_workflow_execution_garbage_collector_run_interval_sec(non_neg_integer()) -> ok.
set_atm_workflow_execution_garbage_collector_run_interval_sec(Interval) ->
    set_env(atm_workflow_execution_garbage_collector_run_interval_sec, Interval).


%% @private
-spec set_atm_suspended_workflow_executions_expiration_interval_sec(non_neg_integer()) -> ok.
set_atm_suspended_workflow_executions_expiration_interval_sec(Interval) ->
    set_env(atm_suspended_workflow_executions_expiration_interval_sec, Interval).


%% @private
-spec set_atm_ended_workflow_executions_expiration_interval_sec(non_neg_integer()) -> ok.
set_atm_ended_workflow_executions_expiration_interval_sec(Interval) ->
    set_env(atm_ended_workflow_executions_expiration_interval_sec, Interval).


%% @private
-spec set_env(atom(), term()) -> ok.
set_env(EnvVar, EnvValue) ->
    ?rpc(?PROVIDER_SELECTOR, op_worker:set_env(EnvVar, EnvValue)).


%% @private
-spec run_atm_workflow_execution(paused | finished) ->
    atm_workflow_execution_exp_state_builder:exp_state().
run_atm_workflow_execution(paused) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT,
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

run_atm_workflow_execution(finished) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT,
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
-spec run_gc() -> ok.
run_gc() ->
    ?rpc(?PROVIDER_SELECTOR, atm_workflow_execution_garbage_collector:run()).


%% @private
-spec assert_matches_with_backend([atm_workflow_execution_exp_state_builder:exp_state()]) ->
    ok.
assert_matches_with_backend(ExpAtmWorkflowExecutionStates) ->
    lists:foreach(fun(ExpAtmWorkflowExecutionState) ->
        atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpAtmWorkflowExecutionState)
    end, ExpAtmWorkflowExecutionStates).


%% @private
-spec assert_atm_workflow_executions_deleted([atm_workflow_execution_exp_state_builder:exp_state()]) ->
    ok.
assert_atm_workflow_executions_deleted(ExpAtmWorkflowExecutionStates) ->
    lists:foreach(fun(ExpAtmWorkflowExecutionState) ->
        atm_workflow_execution_exp_state_builder:assert_deleted(ExpAtmWorkflowExecutionState)
    end, ExpAtmWorkflowExecutionStates).
