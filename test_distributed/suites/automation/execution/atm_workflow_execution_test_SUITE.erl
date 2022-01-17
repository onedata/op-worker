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


%%%===================================================================
%%% Test cases
%%%===================================================================


atm_workflow_with_empty_lane_scheduling_should_fail_test(_Config) ->
    SessionId = oct_background:get_user_session_id(user2, krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    AtmWorkflowSchemaId = get_workflow_schema_id("empty_lane.json"),

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
            AtmInventoryId = ozt_atm:create_inventory(str_utils:rand_hex(10)),

            UserId = oct_background:get_user_id(user2),
            ozt_atm:add_user_to_inventory(UserId, AtmInventoryId),

            load_atm_doc_json_dumps(AtmInventoryId, NewConfig),
            NewConfig
        end
    }).


%% @private
load_atm_doc_json_dumps(AtmInventoryId, Config) ->
    DataDirPath = test_utils:data_dir(Config),

    AtmWorkflowSchemaDumpNameToId = lists:foldl(fun(AtmWorkflowSchemaDumpFileName, Acc) ->
        AtmWorkflowSchemaDumpFilePath = filename:join(DataDirPath, AtmWorkflowSchemaDumpFileName),
        {ok, AtmWorkflowSchemaDumpBin} = file:read_file(AtmWorkflowSchemaDumpFilePath),
        AtmWorkflowSchemaDump = json_utils:decode(AtmWorkflowSchemaDumpBin),

        AtmWorkflowSchemaId = ozt_atm:create_workflow_schema(AtmWorkflowSchemaDump#{
            <<"atmInventoryId">> => AtmInventoryId
        }),
        Acc#{AtmWorkflowSchemaDumpFileName => AtmWorkflowSchemaId}
    end, #{}, element(2, file:list_dir(DataDirPath))),

    node_cache:put(atm_workflow_schema_dump_name_to_id, AtmWorkflowSchemaDumpNameToId).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
