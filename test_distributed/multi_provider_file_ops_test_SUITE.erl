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
    db_sync_test/1, proxy_test1/1, proxy_test2/1, file_consistency_test/1,
    concurrent_create_test/1
]).

all() ->
    ?ALL([
        proxy_test1, proxy_test2, db_sync_test, file_consistency_test, concurrent_create_test
    ]).

-define(match(Expect, Expr, Attempts),
    case Attempts of
        0 ->
            ?assertMatch(Expect, Expr);
        _ ->
            ?assertMatch(Expect, Expr, Attempts)
    end
).

-define(rpc(W, Module, Function, Args), rpc:call(W, Module, Function, Args)).
-define(rpcTest(W, Function, Args), rpc:call(W, ?MODULE, Function, Args)).

%%%===================================================================
%%% Test functions
%%%===================================================================


concurrent_create_test(Config) ->
    FileCount = 3,

    Workers = ?config(op_worker_nodes, Config),
    ProvIDs0 = lists:map(fun(Worker) ->
        rpc:call(Worker, oneprovider, get_provider_id, [])
    end, Workers),

    ProvIdCount = length(lists:usort(ProvIDs0)),

    ProvIDs = lists:zip(Workers, ProvIDs0),
    ProvMap = lists:foldl(
        fun({Worker, ProvId}, Acc) ->
            maps:put(ProvId, [Worker | maps:get(ProvId, Acc, [])], Acc)
        end, #{}, ProvIDs),

    W = fun(N) ->
            [Worker | _] = maps:get(lists:nth(N, lists:usort(ProvIDs0)), ProvMap),
            Worker
        end,

    User = <<"user1">>,

    [{_SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),
    SessId = fun(W) -> ?config({session_id, {User, ?GET_DOMAIN(W)}}, Config) end,

    ct:print("WMap: ~p", [{W(1), W(2), ProvMap}]),

    DirBaseName = <<SpaceName/binary, "/concurrent_create_test_">>,

    TestMaster = self(),

    DirName = fun(N) ->
        NameSuffix = integer_to_binary(N),
        <<DirBaseName/binary, NameSuffix/binary>>
        end,

    AllFiles = lists:map(
        fun(N) ->

            Files = lists:foldl(fun
                (10, retry) ->
                    throw(unable_to_make_concurrent_create);
                (_, L) when is_list(L) ->
                    L;
                (_, _) ->
                    lists:foreach(
                        fun(WId) ->
                            lfm_proxy:unlink(W(WId), SessId(W(WId)), {path, DirName(N)})
                        end, lists:seq(1, ProvIdCount)),

                    lists:foreach(
                        fun(WId) ->
                            spawn(fun() ->
                                TestMaster ! {WId, lfm_proxy:mkdir(W(WId), SessId(W(WId)), DirName(N), 8#755)}
                            end)
                        end, lists:seq(1, ProvIdCount)),

                    try
                        lists:map(
                            fun(WId) ->
                                receive
                                    {WId, {ok, GUID}} ->
                                        {WId, GUID};
                                    {WId, {error, _}} ->
                                        throw(not_concurrent)
                                end
                            end, lists:seq(1, ProvIdCount))
                    catch
                        not_concurrent ->
                            retry
                    end


            end, start, lists:seq(1, 10)),
            {N, Files}
        end, lists:seq(1, FileCount)),

    ct:print("AllFiles ~p", [AllFiles]),

    timer:sleep(10000),

    lists:foreach(
        fun(WId) ->
            {ok, ChildList} = lfm_proxy:ls(W(WId), SessId(W(WId)), {path, SpaceName}, 0, 1000),
            ExpectedChildCount = ProvIdCount * FileCount,
            {FetchedIds, FetchedNames} = lists:unzip(ChildList),
            {_, IdsPerWorker} = lists:unzip(AllFiles),
            Ids = [GUID || {_, GUID} <- lists:flatten(IdsPerWorker)],
            ExpectedIds = lists:usort(Ids),

            ct:print("Check ~p", [{lists:usort(Ids), lists:usort(FetchedIds)}]),

            ?assertMatch(ExpectedChildCount, length(ChildList)),
            ?assertMatch(ExpectedChildCount, length(lists:usort(FetchedNames))),
            ?assertMatch(ExpectedIds, lists:usort(FetchedIds)),

            lists:foreach(
                fun(FileNo) ->
                    LocalIdsPerWorker = proplists:get_value(FileNo, AllFiles),
                    LocalGUID = proplists:get_value(WId, lists:flatten(LocalIdsPerWorker)),
                    LocalName = proplists:get_value(LocalGUID, ChildList),
                    ExpectedName = filename:basename(DirName(FileNo)),
                    ct:print("Local name test ~p", [{FileNo, LocalGUID, ExpectedName, LocalName}]),

                    ?assertMatch(ExpectedName, LocalName)
                end, lists:seq(1, FileCount))

        end, lists:seq(1, ProvIdCount)),

    ok.


db_sync_test(Config) ->
    % TODO change timeout after VFS-2197
    multi_provider_file_ops_base:synchronization_test_base(Config, <<"user1">>, {4,0,0,2}, 50, 10, 100).
%%synchronization_test_base(Config, <<"user1">>, {4,0,0,2}, 60, 10, 100).

proxy_test1(Config) ->
    multi_provider_file_ops_base:synchronization_test_base(Config, <<"user2">>, {0,4,1,2}, 0, 10, 100).

proxy_test2(Config) ->
    multi_provider_file_ops_base:synchronization_test_base(Config, <<"user3">>, {0,4,1,2}, 0, 10, 100).

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

    multi_provider_file_ops_base:file_consistency_test_base(Config, Worker1, Worker2, Worker1).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer, multi_provider_file_ops_base]).

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

%%%===================================================================
%%% Internal functions
%%%===================================================================