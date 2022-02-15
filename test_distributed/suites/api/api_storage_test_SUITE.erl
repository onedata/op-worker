%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning storage basic API (gs).
%%% @end
%%%-------------------------------------------------------------------
-module(api_storage_test_SUITE).
-author("Lukasz Opiola").

-include("api_file_test_utils.hrl").
-include("api_test_runner.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").

-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    get_shared_storage_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        get_shared_storage_test
    ]}
].

all() -> [
    {group, all_tests}
].


%%%===================================================================
%%% Test functions
%%%===================================================================


get_shared_storage_test(_Config) ->
    AllProviders = [krakow, paris],
    SpaceId = oct_background:get_space_id(space_krk_par),
    lists:foreach(fun(ProviderSelector) ->
        ProviderId = oct_background:get_provider_id(ProviderSelector),
        {ok, SupportingStorages} = ?opw_test_rpc(
            ProviderSelector, space_logic:get_storages_by_provider(SpaceId, ProviderId)
        ),
        lists:foreach(fun(StorageId) ->
            StorageName = ?opw_test_rpc(ProviderSelector, storage:fetch_name_of_local_storage(StorageId)),
            get_shared_storage_test_base(AllProviders, SpaceId, StorageId, StorageName, ProviderId)
        end, maps:keys(SupportingStorages))
    end, AllProviders).

get_shared_storage_test_base(TargetProviders, SpaceId, StorageId, StorageName, ProviderId) ->
    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = TargetProviders,
            client_spec = #client_spec{
                correct = [
                    user2, user3, user4 % all space members can access shared storage data
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1]
            },
            scenario_templates = [
                #scenario_template{
                    name = <<"Get shared storage using gs api">>,
                    type = gs,
                    prepare_args_fun = fun(_) ->
                        #gs_args{
                            operation = get,
                            gri = #gri{type = op_storage, id = StorageId, aspect = instance, scope = shared},
                            auth_hint = ?THROUGH_SPACE(SpaceId)
                        }
                    end,
                    validate_result_fun = fun(_, {ok, Result}) ->
                        ?assertEqual(#{
                            <<"revision">> => 1,
                            <<"gri">> => gri:serialize(#gri{
                                type = op_storage,
                                id = StorageId,
                                aspect = instance,
                                scope = shared
                            }),
                            <<"name">> => StorageName,
                            <<"provider">> => gri:serialize(#gri{
                                type = op_provider,
                                id = ProviderId,
                                aspect = instance,
                                scope = protected
                            })
                        }, Result)
                    end
                }
            ]
        }
    ])).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "api_tests",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    Config.


end_per_group(_Group, Config) ->
    Config.


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
