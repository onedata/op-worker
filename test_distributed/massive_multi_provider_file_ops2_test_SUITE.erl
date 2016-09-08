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
-module(massive_multi_provider_file_ops2_test_SUITE).
-author("Michał Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    db_sync_test/1, file_consistency_test/1
]).

all() ->
    ?ALL([
        db_sync_test, file_consistency_test
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================

db_sync_test(Config) ->
    % TODO change timeout after VFS-2197
    multi_provider_file_ops_test_base:synchronization_test_base(Config, <<"user1">>, {4,2,0}, 150, 3, 10).
%%multi_provider_file_ops_test_base:synchronization_test_base(Config, <<"user1">>, {4,2,0}, 120, 3, 10).

file_consistency_test(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    {Worker1, Worker2, Worker3} = lists:foldl(fun(W, {Acc1, Acc2, Acc3}) ->
        Check = fun(Acc, Prov) ->
            case is_atom(Acc) of
                true ->
                    Acc;
                _ ->
                    case string:str(atom_to_list(W), Prov) of
                        0 -> Acc;
                        _ -> W
                    end
            end
        end,
        {Check(Acc1, "p1"), Check(Acc2, "p2"), Check(Acc3, "p3")}
    end, {[], [], []}, Workers),

    multi_provider_file_ops_test_base:file_consistency_test_base(Config, Worker1, Worker2, Worker3).

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
    initializer:enable_grpca_based_communication(Config),
    initializer:disable_quota_limit(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_, Config) ->
    lfm_proxy:teardown(Config),
     %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    initializer:disable_grpca_based_communication(Config),
    hackney:stop(),
    application:stop(etls).
