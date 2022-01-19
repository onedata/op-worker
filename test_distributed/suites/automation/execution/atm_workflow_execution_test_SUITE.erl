%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_schema_test_utils.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% exported for CT
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    atm_workflow_with_empty_lane_scheduling_should_fail_test/1
]).

all() -> [
    atm_workflow_with_empty_lane_scheduling_should_fail_test
].


-define(EMPTY_LANE_ATM_WORKFLOW, #atm_workflow_schema_dump_draft{
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


%%%===================================================================
%%% Test cases
%%%===================================================================


atm_workflow_with_empty_lane_scheduling_should_fail_test(_Config) ->
    SessionId = oct_background:get_user_session_id(user2, krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    AtmWorkflowSchemaId = get_workflow_schema_id(<<"empty_lane">>),

    ?assertMatch(
        ?ERROR_ATM_LANE_EMPTY(<<"1a916f36a6fdd628531beca8299f64bd238da5">>),
        opt_atm:schedule_workflow_execution(krakow, SessionId, SpaceId, AtmWorkflowSchemaId, 1)
    ).


%===================================================================
% Internal functions
%===================================================================


%% @private
-spec get_workflow_schema_id(string()) -> od_atm_workflow_schema:id().
get_workflow_schema_id(AtmWorkflowSchemaDumpName) ->
    maps:get(AtmWorkflowSchemaDumpName, node_cache:get(atm_workflow_schema_dump_name_to_id)).


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite([{?LOAD_MODULES, [?MODULE]} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
        posthook = fun(NewConfig) ->
            atm_test_inventory:ensure_exists(),
            atm_test_inventory:add_user(user2),

%%            atm_test_inventory:add_workflow(atm_test_schema_factory:create_from_draft(?EMPTY_LANE_ATM_WORKFLOW)),

            node_cache:put(atm_workflow_schema_dump_name_to_id, #{
                <<"empty_lane">> => atm_test_inventory:add_workflow(atm_test_schema_factory:create_from_draft(?EMPTY_LANE_ATM_WORKFLOW))
            }),  %% TODO

            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
