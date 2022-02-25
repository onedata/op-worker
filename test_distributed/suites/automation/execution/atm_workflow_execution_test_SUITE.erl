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

    create_first_lane_run_failure_test/1
]).

groups() -> [
    {workflow_execution_scheduling_tests, [parallel], [
        atm_workflow_with_no_lanes_scheduling_should_fail_test,
        atm_workflow_with_empty_lane_scheduling_should_fail_test,
        atm_workflow_with_empty_parallel_box_scheduling_should_fail_test
    ]},
    {workflow_execution_progress_tests, [parallel], [
        create_first_lane_run_failure_test
    ]}
].

all() -> [
    {group, workflow_execution_scheduling_tests},
    {group, workflow_execution_progress_tests}
].


%%%===================================================================
%%% Test cases
%%%===================================================================


atm_workflow_with_no_lanes_scheduling_should_fail_test(_Config) ->
    atm_workflow_execution_scheduling_test_base:atm_workflow_with_no_lanes_scheduling_should_fail_test().


atm_workflow_with_empty_lane_scheduling_should_fail_test(_Config) ->
    atm_workflow_execution_scheduling_test_base:atm_workflow_with_empty_lane_scheduling_should_fail_test().


atm_workflow_with_empty_parallel_box_scheduling_should_fail_test(_Config) ->
    atm_workflow_execution_scheduling_test_base:atm_workflow_with_empty_parallel_box_scheduling_should_fail_test().


create_first_lane_run_failure_test(_Config) ->
    atm_workflow_execution_preparation_test_base:create_first_lane_run_failure_test().


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [
        atm_workflow_execution_scheduling_test_base
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


init_per_group(workflow_execution_scheduling_tests, Config) ->
    Config;

init_per_group(workflow_execution_progress_tests, Config) ->
    atm_openfaas_task_executor_mock:init(?PROVIDER_SELECTOR, atm_openfaas_docker_mock),
    atm_workflow_execution_test_runner:init(?PROVIDER_SELECTOR),
    Config.


end_per_group(workflow_execution_scheduling_tests, Config) ->
    Config;

end_per_group(workflow_execution_progress_tests, Config) ->
    atm_workflow_execution_test_runner:teardown(?PROVIDER_SELECTOR),
    atm_openfaas_task_executor_mock:teardown(?PROVIDER_SELECTOR),
    Config.


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
