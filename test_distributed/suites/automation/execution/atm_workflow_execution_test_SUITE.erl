%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation workflow execution machinery.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_test_SUITE).
-author("Bartosz Walkowicz").

-include("atm_workflow_exeuction_test.hrl").
-include_lib("ctool/include/privileges.hrl").

%% exported for CT
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    atm_workflow_with_no_lanes_scheduling_should_fail_test/1,
    atm_workflow_with_empty_lane_scheduling_should_fail_test/1,
    atm_workflow_with_empty_parallel_box_scheduling_should_fail_test/1,

    prepare_first_lane_run_failure_test/1
]).

groups() -> [
    {non_executable_workflow_schema_scheduling, [parallel], [
        atm_workflow_with_no_lanes_scheduling_should_fail_test,
        atm_workflow_with_empty_lane_scheduling_should_fail_test,
        atm_workflow_with_empty_parallel_box_scheduling_should_fail_test
    ]},
    {execution_tests, [parallel], [
        prepare_first_lane_run_failure_test
    ]}
].

all() -> [
    {group, non_executable_workflow_schema_scheduling},
    {group, execution_tests}
].


-define(ECHO_LAMBDA_DRAFT, #atm_lambda_revision_draft{
    operation_spec = #atm_openfaas_operation_spec_draft{
        docker_image = <<"test/echo">>
    },
    argument_specs = [#atm_lambda_argument_spec{
        name = <<"val">>,
        data_spec = #atm_data_spec{type = atm_integer_type},
        is_optional = false
    }],
    result_specs = [#atm_lambda_result_spec{
        name = <<"val">>,
        data_spec = #atm_data_spec{type = atm_integer_type}
    }]
}).

-define(ECHO_ATM_WORKFLOW_SCHEMA_DRAFT, #atm_workflow_schema_dump_draft{
    name = <<"echo">>,
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [#atm_store_schema_draft{
            id = <<"st1">>,
            type = list,
            config = #atm_list_store_config{item_data_spec = #atm_data_spec{
                type = atm_integer_type
            }},
            requires_initial_content = false,
            default_initial_content = [1, 2, 3]
        }],
        lanes = [#atm_lane_schema_draft{
            parallel_boxes = [#atm_parallel_box_schema_draft{
                tasks = [#atm_task_schema_draft{
                    lambda_id = <<"echo">>,
                    lambda_revision_number = 1,
                    argument_mappings = [#atm_task_schema_argument_mapper{
                        argument_name = <<"val">>,
                        value_builder = #atm_task_argument_value_builder{
                            type = iterated_item,
                            recipe = undefined
                        }
                    }],
                    result_mappings = []
                }]
            }],
            store_iterator_spec = #atm_store_iterator_spec_draft{
                store_schema_id = <<"st1">>
            }
        }]
    },
    supplementary_lambdas = #{<<"echo">> => #{1 => ?ECHO_LAMBDA_DRAFT}}
}).


%%%===================================================================
%%% Test cases
%%%===================================================================


atm_workflow_with_no_lanes_scheduling_should_fail_test(_Config) ->
    atm_non_executable_workflow_schema_scheduling_test_base:atm_workflow_with_no_lanes_scheduling_should_fail_test().


atm_workflow_with_empty_lane_scheduling_should_fail_test(_Config) ->
    atm_non_executable_workflow_schema_scheduling_test_base:atm_workflow_with_empty_lane_scheduling_should_fail_test().


atm_workflow_with_empty_parallel_box_scheduling_should_fail_test(_Config) ->
    atm_non_executable_workflow_schema_scheduling_test_base:atm_workflow_with_empty_parallel_box_scheduling_should_fail_test().


prepare_first_lane_run_failure_test(_Config) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                create_run = #atm_step_mock_spec{
                    before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        {true, atm_workflow_execution_exp_state_builder:report_lane_run_started_preparing(
                            {1, 1}, ExpState0
                        )}
                    end,
                    mock_result = {true, ?ERROR_INTERNAL_SERVER_ERROR}
                },
                prepare_lane = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:report_lane_run_failed({1, 1}, ExpState0),
                        {true, atm_workflow_execution_exp_state_builder:report_workflow_execution_aborting(ExpState1)}
                    end
                }
            }],
            handle_workflow_execution_ended = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, atm_workflow_execution_exp_state_builder:report_workflow_execution_failed(ExpState0)}
                end
            }
        }]
    }).


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [
        atm_non_executable_workflow_schema_scheduling_test_base
        | ?ATM_WORKFLOW_EXECUTION_TEST_UTILS
    ],
    oct_background:init_per_suite(
        [{?LOAD_MODULES, ModulesToLoad} | Config],
        #onenv_test_config{
            onenv_scenario = "1op",
            envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
            posthook = fun(NewConfig) ->
                atm_test_inventory:init_per_suite(?PROVIDER_SELECTOR, user1),
                atm_test_inventory:add_member(?USER_SELECTOR),
                ozt_spaces:set_privileges(?SPACE_SELECTOR, ?USER_SELECTOR, [
                    ?SPACE_VIEW_ATM_WORKFLOW_EXECUTIONS,
                    ?SPACE_SCHEDULE_ATM_WORKFLOW_EXECUTIONS
                    | privileges:space_member()
                ]),
                NewConfig
            end
        }
    ).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(non_executable_workflow_schema_scheduling, Config) ->
    Config;

init_per_group(execution_tests, Config) ->
    atm_openfaas_task_executor_mock:init(?PROVIDER_SELECTOR, atm_openfaas_docker_mock),
    atm_workflow_execution_test_runner:init(?PROVIDER_SELECTOR),
    Config.


end_per_group(non_executable_workflow_schema_scheduling, Config) ->
    Config;

end_per_group(execution_tests, Config) ->
    atm_workflow_execution_test_runner:teardown(?PROVIDER_SELECTOR),
    atm_openfaas_task_executor_mock:teardown(?PROVIDER_SELECTOR),
    Config.


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
