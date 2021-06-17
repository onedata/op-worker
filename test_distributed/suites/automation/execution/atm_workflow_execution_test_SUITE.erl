%%%-------------------------------------------------------------------
%%% @author Piotr Duleba
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests for atm workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_test_SUITE).
-author("Piotr Duleba").

-include("modules/automation/atm_tmp.hrl").
-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").

-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% exported for CT
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    simple_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        simple_test
    ]}
].

all() -> [
    {group, all_tests}
].
-define(INVENTORY_SCHEMA(InventoryName), #{
    <<"name">> => InventoryName
}).

-define(WORKFLOW_SCHEMA(InventoryId, WorkflowName), #{
    <<"atmInventoryId">> => InventoryId,
    <<"description">> => <<"Some description">>,
    <<"name">> => WorkflowName
}).

-define(LAMBDA_SCHEMA(InventoryId), #{
    <<"atmInventoryId">> => InventoryId,
    <<"description">> => <<"Lambada description">>,
    <<"name">> => <<"lambda_1">>,
    <<"argumentSpecs">> => [
        #{
            <<"name">> => <<"arg1">>,
            <<"dataSpec">> => #{
                <<"type">> => <<"integer">>,
                <<"valueConstraints">> => #{}
            },
            <<"isBatch">> => false,
            <<"isOptional">> => false
        }
    ],
    <<"operationSpec">> => #{
        <<"engine">> => <<"openfaas">>,
        <<"dockerImage">> => <<"my_docker_image">>,
        <<"dockerExecutionOptions">> => #{
            <<"mountOneclient">> => true,
            <<"oneclientMountPoint">> => <<"mnt/onedata">>,
            <<"oneclientOptions">> => <<"">>,
            <<"readonly">> => true
        }
    },
    <<"resultSpecs">> => [
        #{
            <<"name">> => <<"res1">>,
            <<"isBatch">> => false,
            <<"dataSpec">> => #{
                <<"type">> => <<"integer">>,
                <<"valueConstraints">> => #{}
            }
        }
    ]
}).


-define(WORKFLOW_SCHEMA(AtmInventory, StoreId, LambdaId), #{
    <<"atmInventoryId">> => AtmInventory,
    <<"name">> => <<"simple_workflow">>,
    <<"description">> => <<"Workflow description">>,
    <<"lanes">> => [
        #{
            <<"id">> => <<"lane_1_id">>,
            <<"name">> => <<"lane_1">>,
            <<"parallelBoxes">> => [
                #{
                    <<"id">> => <<"box_1_id">>,
                    <<"name">> => <<"Parallel box">>,
                    <<"tasks">> => [#{
                        <<"id">> => <<"task_1_id">>,
                        <<"argumentMappings">> => [#{
                            <<"argumentName">> => <<"arg1">>,
                            <<"valueBuilder">> => #{
                                <<"valueBuilderRecipe">> => 1,
                                <<"valueBuilderType">> => <<"const">>
                            }
                        }],
                        <<"lambdaId">> => LambdaId,
                        <<"name">> => <<"lambda_1">>,
                        <<"resultMappings">> => [#{
                            <<"dispatchFunction">> => <<"append">>,
                            <<"resultName">> => <<"res1">>,
                            <<"storeSchemaId">> => StoreId
                        }]
                    }]
                }
            ],
            <<"storeIteratorSpec">> => #{
                <<"storeSchemaId">> => StoreId,
                <<"strategy">> => #{
                    <<"type">> => <<"serial">>
                }
            }
        }
    ],
    <<"stores">> => [#{
        <<"dataSpec">> => #{
            <<"type">> => <<"integer">>,
            <<"valueConstraints">> => #{}
        },
        <<"description">> => <<"Store description">>,
        <<"id">> => StoreId,
        <<"name">> => <<"someStore">>,
        <<"requiresInitialValue">> => true,
        <<"type">> => <<"list">>
    }]


}).

%%%===================================================================
%%% API functions
%%%===================================================================


simple_test(_Config) ->
    UserId = oct_background:get_user_id(user1),
    UserAuth = aai:user_auth(UserId),

    InventoryId = ozw_test_rpc:create_inventory(UserAuth, ?INVENTORY_SCHEMA(<<"my_inventory">>)),
    ct:pal("InventoryId: ~p", [InventoryId]),

    LambdaId = ozw_test_rpc:create_lambda(UserAuth, ?LAMBDA_SCHEMA(InventoryId)),
    ct:pal("LambdaId: ~p", [LambdaId]),

    StoreId = str_utils:rand_hex(15),
    WorkflowSchemaId = ozw_test_rpc:create_workflow_schema(UserAuth, ?WORKFLOW_SCHEMA(InventoryId, StoreId, LambdaId)),
    ct:pal("WorkflowSchemaId: ~p", [WorkflowSchemaId]),

    SpaceId = oct_background:get_space_id(space_krk),
    SessionId = oct_background:get_user_session_id(admin, krakow),

    StoreInitialVales = #{
        StoreId => [1,2,3,4]
    },
    Res6 = rpc:call(hd(oct_background:get_provider_nodes(krakow)), lfm, schedule_atm_workflow_execution, [SessionId, SpaceId,WorkflowSchemaId, StoreInitialVales]),

    ct:pal("Schedule: ~p", [Res6]),

    ok.


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
