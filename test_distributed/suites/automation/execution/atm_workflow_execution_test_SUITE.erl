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

-include_lib("ctool/include/test/assertions.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_and_execute_workflow_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_and_execute_workflow_test
    ]}
].

all() -> [
    {group, all_tests}
].

-define(ATTMEPTS, 30).

-define(LISTING_OPTS, #{
    limit => 300,
    start_index => <<"">>,
    offset => 0
}).

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
        <<"dockerImage">> => <<"inc_image">>,
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


create_and_execute_workflow_test(_Config) ->
    UserId = oct_background:get_user_id(user1),
    UserAuth = aai:user_auth(UserId),

    InventoryId = ozw_test_rpc:create_inventory_for_user(UserAuth, UserId, ?INVENTORY_SCHEMA(<<"my_inventory">>)),
    LambdaId = ozw_test_rpc:create_lambda(UserAuth, ?LAMBDA_SCHEMA(InventoryId)),

    StoreId = str_utils:rand_hex(15),
    WorkflowSchemaId = ozw_test_rpc:create_workflow_schema(UserAuth, ?WORKFLOW_SCHEMA(InventoryId, StoreId, LambdaId)),

    SpaceId = oct_background:get_space_id(space_krk),
    SessionId = oct_background:get_user_session_id(user1, krakow),

    StoreInitialVales = #{
        StoreId => [1, 2, 3, 4, 5, 6, 7, 8]
    },
    {ExecutionId, _ExecutionRecord} = opw_test_rpc:schedule_atm_workflow_execution(
        krakow, SessionId, SpaceId, WorkflowSchemaId, StoreInitialVales
    ),

    ?assert(workflow_is_waiting(ExecutionId, InventoryId)),
    ?assertEqual(true, workflow_is_ongoing(ExecutionId, InventoryId), ?ATTMEPTS).


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {openfaas_host, <<"host">>},
            {openfaas_port, 100},
            {openfaas_admin_username, <<"admin">>},
            {openfaas_admin_password, <<"password">>},
            {openfaas_function_namespace, <<"namespace">>}
        ]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    mock_openfaas(),
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.


%===================================================================
% Internal functions
%===================================================================


%% @private
-spec mock_openfaas() -> ok.
mock_openfaas() ->
    Worker = hd(oct_background:get_provider_nodes(krakow)),
    ok = test_utils:mock_new(Worker, atm_openfaas_task_executor),
    ok = test_utils:mock_expect(Worker, atm_openfaas_task_executor, prepare, fun(_) -> ok end),
    ok = test_utils:mock_expect(Worker, atm_openfaas_task_executor, run,
        fun(AtmJobExecutionCtx, Data, AtmTaskExecutor) ->
            Image = get_executor_image(AtmTaskExecutor),
            FinishCallbackUrl = atm_job_ctx:get_report_result_url(AtmJobExecutionCtx),

            spawn(fun() ->
                %% sleep, to simulate openfaas calculation delay
                timer:sleep(5000),

                Result = case Image of
                    <<"inc_image">> -> integer_to_binary(maps:get(<<"arg1">>, Data) + 1);
                    _ -> #{}
                end,
                http_client:post(FinishCallbackUrl, #{}, Result)
            end),
            ok
        end).


%% @private
-spec get_executor_image(term()) -> binary().
get_executor_image(AtmTaskExecutor) ->
    Record = element(3, AtmTaskExecutor),
    element(2, Record).


%% @private
-spec workflow_is_waiting(binary(), binary()) -> boolean().
workflow_is_waiting(ExecutionId, InventoryId) ->
    SpaceId = oct_background:get_space_id(space_krk),
    WaitingList = opw_test_rpc:list_waiting_atm_workflow_executions(krakow, SpaceId, InventoryId, ?LISTING_OPTS),
    lists:any(fun({_, ItemExecutionId}) ->
        ItemExecutionId == ExecutionId
    end, WaitingList).


%% @private
-spec workflow_is_ongoing(binary(), binary()) -> boolean().
workflow_is_ongoing(ExecutionId, InventoryId) ->
    SpaceId = oct_background:get_space_id(space_krk),
    WaitingList = opw_test_rpc:list_ongoing_atm_workflow_executions(krakow, SpaceId, InventoryId, ?LISTING_OPTS),
    lists:any(fun({_, ItemExecutionId}) ->
        ItemExecutionId == ExecutionId
    end, WaitingList).
