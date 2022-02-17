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

-include("atm_workflow_exeuction_test_runner.hrl").
-include("atm_test_schema.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include_lib("ctool/include/privileges.hrl").

%% exported for CT
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    atm_workflow_with_empty_lane_scheduling_should_fail_test/1,
    prepare_first_lane_run_failure_test/1
]).

all() -> [
    atm_workflow_with_empty_lane_scheduling_should_fail_test,
    prepare_first_lane_run_failure_test
].


-define(EMPTY_LANE_ATM_WORKFLOW_SCHEMA_DRAFT, #atm_workflow_schema_dump_draft{
    name = <<"empty_lane">>,
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [
            #atm_store_schema_draft{
                id = <<"st1">>,
                type = list,
                data_spec = #atm_data_spec{type = atm_integer_type},
                requires_initial_value = false,
                default_initial_value = [1, 2, 3]
            }
        ],
        lanes = [
            #atm_lane_schema_draft{
                parallel_boxes = [],
                store_iterator_spec = #atm_store_iterator_spec_draft{
                    store_schema_id = <<"st1">>
                }
            }
        ]
    }
}).

-define(ECHO_ATM_WORKFLOW_SCHEMA_DRAFT, #atm_workflow_schema_dump_draft{
    name = <<"echo">>,
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [#atm_store_schema_draft{
            id = <<"st1">>,
            type = list,
            data_spec = #atm_data_spec{type = atm_integer_type},
            requires_initial_value = false,
            default_initial_value = [1, 2, 3]
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
    supplementary_lambdas = #{<<"echo">> => #{1 => #atm_lambda_revision_draft{
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
    }}}
}).


%%%===================================================================
%%% Test cases
%%%===================================================================


atm_workflow_with_empty_lane_scheduling_should_fail_test(_Config) ->
    SessionId = oct_background:get_user_session_id(user2, krakow),
    SpaceId = oct_background:get_space_id(space_krk),

    AtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(
        ?EMPTY_LANE_ATM_WORKFLOW_SCHEMA_DRAFT
    ),
    EmptyAtmLaneSchemaId = atm_workflow_schema_test_utils:query(
        atm_test_inventory:get_workflow_schema(AtmWorkflowSchemaId),
        [revision_registry, registry, 1, lanes, 1, id]
    ),

    ?assertEqual(
        ?ERROR_ATM_LANE_EMPTY(EmptyAtmLaneSchemaId),
        opt_atm:schedule_workflow_execution(krakow, SessionId, SpaceId, AtmWorkflowSchemaId, 1),
        30  % TODO is it necessary? maybe some force fetch? returns {error, forbidden}
    ).


prepare_first_lane_run_failure_test(_Config) ->
    AtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(
        ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT
    ),
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = krakow,
        user = user2,
        space = space_krk,
        workflow_schema_id = AtmWorkflowSchemaId,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                create_run = #atm_step_mock_spec{
                    before_step_test_view_diff = fun(#atm_mock_call_ctx{workflow_execution_test_view = TestView0}) ->
                        {true, atm_workflow_execution_test_view:report_lane_run_begin_preparing({1, 1}, TestView0)}
                    end,
                    mock_result = {true, ?ERROR_INTERNAL_SERVER_ERROR}
                },
                prepare_lane = #atm_step_mock_spec{
                    after_step_test_view_diff = fun(#atm_mock_call_ctx{workflow_execution_test_view = TestView0}) ->
                        TestView1 = atm_workflow_execution_test_view:report_lane_run_failed({1, 1}, TestView0),
                        {true, atm_workflow_execution_test_view:report_workflow_execution_aborting(TestView1)}
                    end
                }
            }],
            handle_workflow_execution_ended = #atm_step_mock_spec{
                after_step_test_view_diff = fun(#atm_mock_call_ctx{workflow_execution_test_view = TestView0}) ->
                    {true, atm_workflow_execution_test_view:report_workflow_execution_failed(TestView0)}
                end
            }
        }]
    }).


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [
        ?MODULE,
        atm_workflow_execution_test_runner,
        atm_openfaas_task_executor_mock,
        atm_openfaas_docker_mock,
        atm_test_inventory
    ],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
        posthook = fun(NewConfig) ->
            atm_test_inventory:init_per_suite(krakow, user1),
            atm_test_inventory:add_member(user2),
            ozt_spaces:set_privileges(
                oct_background:get_space_id(space_krk),
                oct_background:get_user_id(user2),
                [
                    ?SPACE_VIEW_ATM_WORKFLOW_EXECUTIONS,
                    ?SPACE_SCHEDULE_ATM_WORKFLOW_EXECUTIONS
                    | privileges:space_member()
                ]
            ),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(prepare_first_lane_run_failure_test, Config) ->
    atm_openfaas_task_executor_mock:init(krakow, atm_openfaas_docker_mock),
    atm_workflow_execution_test_runner:init(krakow),
    Config;

init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(prepare_first_lane_run_failure_test, _Config) ->
    atm_workflow_execution_test_runner:teardown(krakow),
    atm_openfaas_task_executor_mock:teardown(krakow);

end_per_testcase(_Case, _Config) ->
    ok.
