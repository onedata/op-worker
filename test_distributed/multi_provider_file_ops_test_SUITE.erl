%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
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
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    db_sync_basic_opts_test/1,
    db_sync_many_ops_test/1,
    db_sync_many_ops_test_base/1,
    db_sync_distributed_modification_test/1,
    proxy_basic_opts_test1/1,
    proxy_many_ops_test1/1,
    proxy_distributed_modification_test1/1,
    proxy_basic_opts_test2/1,
    proxy_many_ops_test2/1,
    proxy_distributed_modification_test2/1,
    proxy_many_ops_test1_base/1,
    proxy_many_ops_test2_base/1,
    file_consistency_test/1,
    file_consistency_test_base/1,
    concurrent_create_test/1,
    multi_space_test/1,
    mkdir_and_rmdir_loop_test/1,
    mkdir_and_rmdir_loop_test_base/1,
    create_and_delete_file_loop_test/1,
    create_and_delete_file_loop_test_base/1,
    echo_and_delete_file_loop_test/1,
    echo_and_delete_file_loop_test_base/1,
    distributed_delete_test/1,
    remote_driver_test/1,
    db_sync_with_delays_test/1
]).

-define(TEST_CASES, [
    db_sync_basic_opts_test,
    db_sync_many_ops_test,
    db_sync_distributed_modification_test,
    proxy_basic_opts_test1,
    proxy_many_ops_test1,
    proxy_distributed_modification_test1,
    proxy_basic_opts_test2,
    proxy_many_ops_test2,
    proxy_distributed_modification_test2,
    file_consistency_test,
    concurrent_create_test,
    multi_space_test,
    mkdir_and_rmdir_loop_test,
    create_and_delete_file_loop_test,
    echo_and_delete_file_loop_test,
    distributed_delete_test,
    remote_driver_test
]).

-define(PERFORMANCE_TEST_CASES, [
    db_sync_many_ops_test,
    proxy_many_ops_test1,
    proxy_many_ops_test2,
    mkdir_and_rmdir_loop_test,
    file_consistency_test,
    create_and_delete_file_loop_test,
    echo_and_delete_file_loop_test,
    db_sync_with_delays_test
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
                [{name, dirs_num}, {value, 200}],
                [{name, files_num}, {value, 300}]
            ]},
            {description, ""}
        ]}
    ]).

db_sync_basic_opts_test(Config) ->
    multi_provider_file_ops_test_base:basic_opts_test_base(Config, <<"user1">>, {4,0,0,2}, 60).

distributed_delete_test(Config) ->
    multi_provider_file_ops_test_base:distributed_delete_test_base(Config, <<"user1">>, {4,0,0,2}, 60).

db_sync_many_ops_test(Config) ->
    ?PERFORMANCE(Config, ?performance_description("Tests working on dirs and files with db_sync")).
db_sync_many_ops_test_base(Config) ->
    DirsNum = ?config(dirs_num, Config),
    FilesNum = ?config(files_num, Config),
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user1">>, {4,0,0,2}, 180, DirsNum, FilesNum).

db_sync_distributed_modification_test(Config) ->
    multi_provider_file_ops_test_base:distributed_modification_test_base(Config, <<"user1">>, {4,0,0,2}, 60).

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

concurrent_create_test(Config) ->
    FileCount = 3,
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    ProvIDs0 = lists:map(fun(Worker) ->
        rpc:call(Worker, oneprovider, get_id, [])
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

%%    ct:print("WMap: ~p", [{W(1), W(2), ProvMap}]),

    Dir0Name = <<"/", SpaceName/binary, "/concurrent_create_test_dir0">>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir0Name, 8#755)),
    lists:foreach(fun(W) ->
        ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}}, lfm_proxy:stat(W, SessId(W), {path, Dir0Name}), 15)
    end, Workers),

    DirBaseName = <<"/", Dir0Name/binary, "/concurrent_create_test_">>,

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
    {_, IdsPerWorker} = lists:unzip(AllFiles),
    Ids = [GUID || {_, GUID} <- lists:flatten(IdsPerWorker)],
    ExpectedIds = lists:usort(Ids),

    lists:foreach(
        fun(WId) ->
            Check = fun() ->
                {ok, CL} = lfm_proxy:ls(W(WId), SessId(W(WId)), {path, <<"/", Dir0Name/binary>>}, 0, 1000),
                _ExpectedChildCount = ProvIdCount * FileCount,
                {FetchedIds, FetchedNames} = lists:unzip(CL),

%%                ct:print("Check ~p", [{lists:usort(Ids), lists:usort(FetchedIds)}]),
%%                ct:print("Check ~p", [{ExpectedChildCount, CL}]),

                {length(CL), length(lists:usort(FetchedNames)), lists:usort(FetchedIds)}
            end,
            ?assertMatch({ExpectedChildCount, ExpectedChildCount, ExpectedIds}, Check(), 15),

            {ok, ChildList} = lfm_proxy:ls(W(WId), SessId(W(WId)), {path, <<"/", Dir0Name/binary>>}, 0, 1000),
            lists:foreach(
                fun(FileNo) ->
                    LocalIdsPerWorker = proplists:get_value(FileNo, AllFiles),
                    LocalGUID = proplists:get_value(WId, lists:flatten(LocalIdsPerWorker)),
                    LocalName = proplists:get_value(LocalGUID, ChildList),
                    ExpectedName = filename:basename(DirName(FileNo)),
%%                    ct:print("Local name test ~p", [{FileNo, LocalGUID, ExpectedName, LocalName}]),

                    ?assertMatch(ExpectedName, LocalName)
                end, lists:seq(1, FileCount))

        end, lists:seq(1, ProvIdCount)),

    ok.

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

multi_space_test(Config) ->
    User = <<"user1">>,
    Spaces = ?config({spaces, User}, Config),
    Attempts = 30,

    SpaceConfigs = lists:foldl(fun({_, SN}, Acc) ->
        {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider} = case SN of
            <<"space1">> ->
                {4,0,0,2};
            _ ->
                {0,4,1,2}
        end,
        EC = multi_provider_file_ops_test_base:extend_config(Config, User,
            {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts),
        [{SN, EC} | Acc]
    end, [], Spaces),

    multi_provider_file_ops_test_base:multi_space_test_base(Config, SpaceConfigs, User).


mkdir_and_rmdir_loop_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 3},
        {success_rate, 100},
        {parameters, [
            [{name, iterations}, {value, 100}, {description, "Number of times sequence mkdir, rmdir will be repeated"}]
        ]},
        {description, "Simulates loop of sequence mkdir and rmdir operations performed by clients"},
        {config, [{name, performance},
            {parameters, [
                [{name, iterations}, {value, 3000}]
            ]},
            {description, "Basic performance configuration"}
        ]}
    ]).
mkdir_and_rmdir_loop_test_base(Config) ->
    IterationsNum = ?config(iterations, Config),
    multi_provider_file_ops_test_base:mkdir_and_rmdir_loop_test_base(Config, IterationsNum, <<"user1">>).

create_and_delete_file_loop_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 3},
        {success_rate, 100},
        {parameters, [
            [{name, iterations}, {value, 100}, {description, "Number of times sequence create, remove file will be repeated"}]
        ]},
        {description, "Simulates loop of sequence create and remove file operations performed by clients"},
        {config, [{name, performance},
            {parameters, [
                [{name, iterations}, {value, 3000}]
            ]},
            {description, "Basic performance configuration"}
        ]}
    ]).
create_and_delete_file_loop_test_base(Config) ->
    IterationsNum = ?config(iterations, Config),
    multi_provider_file_ops_test_base:create_and_delete_file_loop_test_base(Config, IterationsNum, <<"user1">>).

echo_and_delete_file_loop_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 3},
        {success_rate, 100},
        {parameters, [
            [{name, iterations}, {value, 100}, {description, "Number of times sequence echo, remove file will be repeated"}]
        ]},
        {description, "Simulates loop of sequence echo and remove file operations performed by clients"},
        {config, [{name, performance},
            {parameters, [
                [{name, iterations}, {value, 3000}]
            ]},
            {description, "Basic performance configuration"}
        ]}
    ]).
echo_and_delete_file_loop_test_base(Config) ->
    IterationsNum = ?config(iterations, Config),
    multi_provider_file_ops_test_base:echo_and_delete_file_loop_test_base(Config, IterationsNum, <<"user1">>).

remote_driver_test(Config) ->
    Config2 = multi_provider_file_ops_test_base:extend_config(Config, <<"user1">>, {0, 0, 0, 0}, 0),
    Workers = ?config(op_worker_nodes, Config),
    [Worker1 | _] = ?config(workers1, Config2),
    [Worker2 | _] = ?config(workers_not1, Config2),
    Key = <<"someKey">>,
    LinkName = <<"someName">>,
    LinkTarget = <<"someTarget">>,
    Link = {LinkName, LinkTarget},
    TreeId1 = rpc:call(Worker1, oneprovider, get_id, []),
    Ctx1 = rpc:call(Worker1, datastore_model_default, get_ctx, [file_meta]),
    Ctx2 = rpc:call(Worker2, datastore_model_default, get_ctx, [file_meta]),
    ?assertMatch({ok, #link{}}, rpc:call(Worker1, datastore_model, add_links, [
        Ctx1, Key, TreeId1, Link
    ])),
    ?assertMatch({ok, [#link{
        name = LinkName,
        target = LinkTarget
    }]}, rpc:call(Worker2, datastore_model, get_links, [
        Ctx2, Key, TreeId1, LinkName
    ])).

db_sync_with_delays_test(Config) ->
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user1">>, {4,0,0,2}, 180, 200, 200).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> multi_provider_file_ops_test_base:init_env(NewConfig) end,
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config).

init_per_testcase(file_consistency_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, file_meta, [passthrough]),
    init_per_testcase(?DEFAULT_CASE(file_consistency_test), Config);
init_per_testcase(db_sync_with_delays_test, Config) ->
    Config2 = init_per_testcase(?DEFAULT_CASE(db_sync_with_delays_test), Config),

    Workers = ?config(op_worker_nodes, Config),
    {_Workers1, WorkersNot1} = lists:foldl(fun(W, {Acc2, Acc3}) ->
        case string:str(atom_to_list(W), "p1") of
            0 -> {Acc2, [W | Acc3]};
            _ -> {[W | Acc2], Acc3}
        end
    end, {[], []}, Workers),

    test_utils:mock_new(WorkersNot1, [
        datastore_throttling
    ]),
    test_utils:mock_expect(WorkersNot1, datastore_throttling, configure_throttling,
        fun() ->
            ok
        end
    ),
    test_utils:mock_expect(WorkersNot1, datastore_throttling, throttle_model, fun
        (file_meta) ->
            timer:sleep(100),
            ok;
        (_) ->
            ok
    end
    ),

    Config2;
init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 60}),
    lfm_proxy:init(Config).

end_per_testcase(file_consistency_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, file_meta),
    end_per_testcase(?DEFAULT_CASE(file_consistency_test), Config);
end_per_testcase(db_sync_with_delays_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [
        datastore_throttling
    ]),

    end_per_testcase(?DEFAULT_CASE(db_sync_with_delays_test), Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).