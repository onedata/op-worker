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

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([db_sync_test/1, proxy_test1/1, proxy_test2/1, file_consistency_test/1]).

all() ->
    ?ALL([
        proxy_test1, proxy_test2, db_sync_test, file_consistency_test
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================

db_sync_test(Config) ->
    % TODO change timeout after VFS-2197
    multi_provider_file_ops_test_base:synchronization_test_base(Config, <<"user1">>, {4,0,0,2}, 60, 10, 100).

proxy_test1(Config) ->
    multi_provider_file_ops_test_base:synchronization_test_base(Config, <<"user2">>, {0,4,1,2}, 0, 10, 100).

proxy_test2(Config) ->
    multi_provider_file_ops_test_base:synchronization_test_base(Config, <<"user3">>, {0,4,1,2}, 0, 10, 100).


file_consistency_test(Config) ->
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

    multi_provider_file_ops_test_base:file_consistency_test_base(Config, Worker1, Worker2, Worker1).


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
