%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of db_sync and proxy
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_file_ops_test_SUITE).
-author("Michał Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    db_sync_basic_opts_test/1, db_sync_many_ops_test/1, db_sync_distributed_modification_test/1,
    proxy_basic_opts_test1/1, proxy_many_ops_test1/1, proxy_distributed_modification_test1/1,
    proxy_basic_opts_test2/1, proxy_many_ops_test2/1, proxy_distributed_modification_test2/1,
    db_sync_many_ops_test_base/1, proxy_many_ops_test1_base/1, proxy_many_ops_test2_base/1,
    file_consistency_test/1, file_consistency_test_base/1
]).

-define(TEST_CASES, [
    db_sync_basic_opts_test, db_sync_many_ops_test, db_sync_distributed_modification_test,
    proxy_basic_opts_test1, proxy_many_ops_test1, proxy_distributed_modification_test1,
    proxy_basic_opts_test2, proxy_many_ops_test2, proxy_distributed_modification_test2,
    file_consistency_test
]).

-define(PERFORMANCE_TEST_CASES, [
    db_sync_many_ops_test, proxy_many_ops_test1, proxy_many_ops_test2, file_consistency_test
]).

all() ->
    ?ALL(?TEST_CASES, ?PERFORMANCE_TEST_CASES).

%%%===================================================================
%%% Test functions
%%%===================================================================

-define(performance_description(Desc),
    [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, dirs_num}, {value, 5}, {description, "Numbers of directories used during test."}],
            [{name, files_num}, {value, 5}, {description, "Numbers of files used during test."}]
        ]},
        {description, Desc},
        {config, [{name, large_config},
            {parameters, [
                [{name, dirs_num}, {value, 50}],
                [{name, files_num}, {value, 100}]
            ]},
            {description, ""}
        ]}
    ]).

db_sync_basic_opts_test(Config) ->
    multi_provider_file_ops_test_base:basic_opts_test_base(Config, <<"user1">>, {4,0,0,2}, 10).

db_sync_many_ops_test(Config) ->
    ?PERFORMANCE(Config, ?performance_description("Tests working on dirs and files with db_sync")).
db_sync_many_ops_test_base(Config) ->
    DirsNum = ?config(dirs_num, Config),
    FilesNum = ?config(files_num, Config),
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user1">>, {4,0,0,2}, 10, DirsNum, FilesNum).

db_sync_distributed_modification_test(Config) ->
    multi_provider_file_ops_test_base:distributed_modification_test_base(Config, <<"user1">>, {4,0,0,2}, 10).

proxy_basic_opts_test1(Config) ->
    multi_provider_file_ops_test_base:basic_opts_test_base(Config, <<"user2">>, {0,4,1,2}, 0).

proxy_many_ops_test1(Config) ->
    ?PERFORMANCE(Config, ?performance_description("Tests working on dirs and files with db_sync")).
proxy_many_ops_test1_base(Config) ->
    DirsNum = ?config(dirs_num, Config),
    FilesNum = ?config(files_num, Config),
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user2">>, {0,4,1,2}, 0, DirsNum, FilesNum).

proxy_distributed_modification_test1(Config) ->
    multi_provider_file_ops_test_base:distributed_modification_test_base(Config, <<"user2">>, {0,4,1,2}, 0).

proxy_basic_opts_test2(Config) ->
    multi_provider_file_ops_test_base:basic_opts_test_base(Config, <<"user3">>, {0,4,1,2}, 0).

proxy_many_ops_test2(Config) ->
    ?PERFORMANCE(Config, ?performance_description("Tests working on dirs and files with db_sync")).
proxy_many_ops_test2_base(Config) ->
    DirsNum = ?config(dirs_num, Config),
    FilesNum = ?config(files_num, Config),
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user3">>, {0,4,1,2}, 0, DirsNum, FilesNum).

proxy_distributed_modification_test2(Config) ->
    multi_provider_file_ops_test_base:distributed_modification_test_base(Config, <<"user3">>, {0,4,1,2}, 0).

file_consistency_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, test_cases}, {value, [1,2,12,13]}, {description, "Number of test cases to be executed"}]
        ]},
        {description, "Tests file consistency"},
        {config, [{name, all_cases},
            {parameters, [
                [{name, test_cases}, {value, [1,2,3,4,5,6,7,8,9,10,11,12,13,14]}]
            ]},
            {description, ""}
        ]}
    ]).
file_consistency_test_base(Config) ->
    ConfigsNum = ?config(test_cases, Config),

    Workers = ?config(op_worker_nodes, Config),
    {Worker1, Worker2} = lists:foldl(fun(W, {Acc1, Acc2}) ->
        NAcc1 = case is_atom(Acc1) of
                    true ->
                        Acc1;
                    _ ->
                        case string:str(atom_to_list(W), "p1") of
                            0 -> Acc1;
                            _ -> W
                        end
                end,
        NAcc2 = case is_atom(Acc2) of
                    true ->
                        Acc2;
                    _ ->
                        case string:str(atom_to_list(W), "p2") of
                            0 -> Acc2;
                            _ -> W
                        end
                end,
        {NAcc1, NAcc2}
                                     end, {[], []}, Workers),

    multi_provider_file_ops_test_base:file_consistency_test_skeleton(Config, Worker1, Worker2, Worker1, ConfigsNum).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer, multi_provider_file_ops_test_base]).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    ct:timetrap({minutes, 60}),
    application:start(etls),
    hackney:start(),
    initializer:disable_quota_limit(Config),
    initializer:enable_grpca_based_communication(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_, Config) ->
    lfm_proxy:teardown(Config),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:disable_grpca_based_communication(Config),
    initializer:unload_quota_mocks(Config),
    hackney:stop(),
    application:stop(etls).