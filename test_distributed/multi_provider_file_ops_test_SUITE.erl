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

-include("fuse_utils.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

-include("transfers_test_mechanism.hrl").
%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([replicate_block/3]).

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
    db_sync_with_delays_test/1,
    db_sync_create_after_del_test/1,
    rtransfer_fetch_test/1,
    rtransfer_cancel_for_session_test/1,
    remove_file_during_transfers_test/1,
    remove_file_on_remote_provider_ceph/1,
    evict_on_ceph/1
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
    remote_driver_test,
    db_sync_create_after_del_test,
    rtransfer_fetch_test,
    rtransfer_cancel_for_session_test,
    remove_file_during_transfers_test,
    remove_file_on_remote_provider_ceph,
    evict_on_ceph
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

db_sync_create_after_del_test(Config) ->
    multi_provider_file_ops_test_base:create_after_del_test_base(Config, <<"user1">>, {4,0,0,2}, 60).

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
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user1">>, {4,0,0,2}, 300, 50, 50).

rtransfer_fetch_test(Config) ->
    [Worker1a, Worker1b, Worker2 | _] = ?config(op_worker_nodes, Config),
    Workers1 = [Worker1a, Worker1b],

    User1 = <<"user1">>,
    [{_SpaceId, SpaceName} | _] = ?config({spaces, User1}, Config),
    SessId = fun(W, User) ->
        ?config({session_id, {User, ?GET_DOMAIN(W)}}, Config)
    end,
    FilePath = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
    FileCtx = create_file(FilePath, 1, User1, Worker2, Worker1a, SessId),
    ct:pal("File created"),

    InitialBlock1 = {0, 100, 10},
    InitialBlocks2 = [{50, 150, 20}, {220, 5, 0}, {260, 6, 0}],
    FollowingBlocks = [
        % overlapping block, but with higher priority - should be fetched
        {90, 20, 0},

        % partially overlapping block - non-overlapping part should be fetched
        {190, 20, 32},

        % non overlapping block - should be fetched
        {300, 100, 255},

        % blocks overlapping/containing already started blocks;
        % taking into account min_hole_size 1st block should be wholly
        % replicated within 1 transfer but replication of 2nd one should be
        % split into 2 transfers
        {210, 30, 96}, {250, 30, 96},

        % overlapping blocks with lower priorities - should not be fetched
        {0, 5, 10}, {70, 20, 10}, {70, 130, 20},
        {90, 20, 32}, {5, 10, 32}, {150, 50, 32}, {110, 5, 32}, {190, 10, 32},
        {0, 15, 96}, {15, 15, 96}, {25, 15, 96}, {35, 15, 96}, {45, 15, 96},
        {55, 15, 96}, {65, 15, 96}, {75, 15, 96}, {85, 15, 96}, {95, 15, 96},
        {105, 15, 96}, {115, 15, 96}, {125, 15, 96}, {135, 15, 96},
        {145, 15, 96}, {155, 15, 96}, {165, 15, 96}, {175, 15, 96}
    ],
    ExpectedBlocks = lists:sort([
        {0, 100}, {100, 100}, {90, 20}, {200, 10}, {300, 100},
        {220, 5}, {210, 30}, {250, 10}, {260, 6}, {266, 14}
    ]),
    ExpectedBlocksNum = length(ExpectedBlocks),

    ok = test_utils:mock_new(Workers1, rtransfer_config, [passthrough]),
    ok = test_utils:mock_expect(Workers1, rtransfer_config, fetch,
        fun rtransfer_config_fetch_mock/6
    ),

    ct:timetrap(timer:minutes(5)),

    [Promise1] = async_replicate_blocks_start(
        Worker1a, SessId(Worker1a, User1), FileCtx, [InitialBlock1]
    ),
    timer:sleep(timer:seconds(5)),
    Promises2 = async_replicate_blocks_start(
        Worker1a, SessId(Worker1a, User1), FileCtx, InitialBlocks2
    ),
    timer:sleep(timer:seconds(5)),

    ExpectedResponses = lists:duplicate(length(FollowingBlocks), ok),
    ?assertMatch(ExpectedResponses, replicate_blocks(
        Worker1a, SessId(Worker1a, User1), FileCtx, FollowingBlocks
    )),
    ?assertMatch([ok, ok, ok, ok],
        async_replicate_blocks_end([Promise1 | Promises2])
    ),

    FetchRequests = lists:sum(meck_get_num_calls(
        Workers1, rtransfer_config, fetch, '_')
    ),
    ?assertMatch(ExpectedBlocksNum, FetchRequests),

    FetchedBlocks = lists:sort(get_fetched_blocks(Workers1)),
    ?assertMatch(ExpectedBlocks, FetchedBlocks),

    ok = test_utils:mock_unload(Workers1, rtransfer_config).

rtransfer_cancel_for_session_test(Config) ->
    [Worker1a, Worker1b, Worker2 | _] = ?config(op_worker_nodes, Config),
    Workers1 = [Worker1a, Worker1b],

    User1 = <<"user1">>,
    User2 = <<"user4">>,
    [{_SpaceId, SpaceName} | _] = ?config({spaces, User1}, Config),
    SessId = fun(W, User) ->
        ?config({session_id, {User, ?GET_DOMAIN(W)}}, Config)
    end,
    FilePath = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
    FileCtx = create_file(FilePath, 1, User1, Worker2, Worker1a, SessId),
    ct:pal("File created"),

    % Some blocks are overlapping so that more than 1 request would be made
    % for them, so that they should not be cancelled for others.
    Blocks1 = [
        {0, 15, 96}, {30, 15, 96}, {60, 15, 96}, {90, 30, 0}, {130, 30, 0},
        {170, 20, 0}
    ],
    Blocks2 = [
        {100, 30, 96}, {135, 15, 96}, {165, 15, 96}, {200, 100, 255}
    ],

    ok = test_utils:mock_new(Workers1, rtransfer_config, [passthrough]),
    ok = test_utils:mock_expect(Workers1, rtransfer_config, fetch,
        fun rtransfer_config_fetch_mock/6
    ),

    ct:timetrap(timer:minutes(5)),

    Promises1 = async_replicate_blocks_start(
        Worker1a, SessId(Worker1a, User1), FileCtx, Blocks1
    ),
    timer:sleep(timer:seconds(5)),

    Promises2 = async_replicate_blocks_start(
        Worker1a, SessId(Worker1a, User2), FileCtx, Blocks2
    ),
    timer:sleep(timer:seconds(5)),

    cancel_transfers_for_session_and_file(Worker1a, SessId(Worker1a, User1),
        FileCtx
    ),

    ExpectedResponses1 = lists:duplicate(length(Blocks1), {error, cancelled}),
    ?assertMatch(ExpectedResponses1, async_replicate_blocks_end(Promises1)),

    ExpectedResponses2 = lists:duplicate(length(Blocks2), ok),
    ?assertMatch(ExpectedResponses2, async_replicate_blocks_end(Promises2)),

    ok = test_utils:mock_unload(Workers1, rtransfer_config).

remove_file_during_transfers_test(Config0) ->
    User = <<"user2">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, User, {0, 0, 0, 0}, 0),
    [Worker1 | _] = ?config(workers1, Config),
    [Worker2 | _] = ?config(workers_not1, Config),
    % Create file
    SessId = fun(User, W) ->
        ?config({session_id, {User, ?GET_DOMAIN(W)}}, Config) 
    end,
    SpaceName = <<"space6">>,
    FilePath = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>, 
    BlocksCount = 10,
    BlockSize = 80 * 1024 * 1024,
    FileSize = BlocksCount * BlockSize,

    ?assertMatch({ok, _}, lfm_proxy:create(Worker2, SessId(User, Worker2),
        FilePath, 8#755)
    ),
    ?assertMatch(ok, lfm_proxy:truncate(Worker2, SessId(User, Worker2),
        {path, FilePath}, FileSize)
    ),

    {ok, #file_attr{guid = GUID}} =
        ?assertMatch({ok, #file_attr{size = FileSize}},
            lfm_proxy:stat(Worker1, SessId(User, Worker1), {path, FilePath}), 60
        ),

    Blocks = lists:map(
       fun(Num) -> 
           {Num*BlockSize, BlockSize, 96}
       end, lists:seq(0, BlocksCount -1)
    ),

    FileCtx = file_ctx:new_by_guid(GUID),

    Promises = async_replicate_blocks_start(Worker1, SessId(User, Worker1), FileCtx, Blocks),

    % delete file
    timer:sleep(timer:seconds(4)),
    lfm_proxy:unlink(Worker2, SessId(User, Worker2), {guid, GUID}),
    lists:foreach(fun(Promise) ->
        ?assertMatch({error, file_deleted}, rpc:yield(Promise))
    end, Promises).

remove_file_on_remote_provider_ceph(Config0) ->
    Config = multi_provider_file_ops_test_base:extend_config(Config0, <<"user2">>, {0, 0, 0, 0}, 0),
    [Worker1 | _] = ?config(workers1, Config),
    [Worker2 | _] = ?config(workers_not1, Config),

    SessionId = ?config(session, Config),
    
    SpaceName = <<"space7">>,
    FilePath = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
    
    [{_, Ceph} | _] = proplists:get_value(cephrados, ?config(storages, Config)),
    ContainerId = proplists:get_value(container_id, Ceph),
    
    {ok, Guid} = lfm_proxy:create(Worker1, SessionId(Worker1), FilePath, 8#755),
    {ok, Handle} = lfm_proxy:open(Worker1, SessionId(Worker1), {guid, Guid}, write),
    {ok, _} = lfm_proxy:write(Worker1, Handle, 0, crypto:strong_rand_bytes(100)),
    ok = lfm_proxy:close(Worker1, Handle),

    ?assertMatch({ok, _}, lfm_proxy:ls(Worker2, SessionId(Worker2), {guid, Guid}, 0, 0), 60),
    L = utils:cmd(["docker exec", atom_to_list(ContainerId), "rados -p onedata ls -"]),
    ?assertEqual(true, length(L) > 0),
       
    lfm_proxy:unlink(Worker2, SessionId(Worker2), {guid, Guid}),

    ?assertMatch({error, enoent}, lfm_proxy:ls(Worker1, SessionId(Worker1), {guid, Guid}, 0, 0), 60),
    ?assertMatch([], utils:cmd(["docker exec", atom_to_list(ContainerId), "rados -p onedata ls -"])).
    
evict_on_ceph(Config0) ->
    Config = multi_provider_file_ops_test_base:extend_config(Config0, <<"user2">>, {0, 0, 0, 0}, 0),
    Type = lfm,
    FileKeyType = path,
    [Worker1 | _] = ?config(workers1, Config),
    [Worker2 | _] = ?config(workers_not1, Config),
    Size = 100,
    User = <<"user2">>,
    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                user = User,
                setup_node = Worker2,
                assertion_nodes = [Worker1],
                files_structure = [{0, 1}],
                replicate_to_nodes = [Worker1],
                size = Size,
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(Worker1), <<"blocks">> => [[0, Size]]},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(Worker2), <<"blocks">> => [[0, Size]]}
                ]
            },
            scenario = #scenario{
                user = User,
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = Worker2,
                evicting_nodes = [Worker1],
                function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
            },
            expected = #expected{
                user = User,
                expected_transfer = #{
                    replication_status => skipped,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(Worker2),
                    evicting_provider => transfers_test_utils:provider_id(Worker1),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 1
                },
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(Worker2), <<"blocks">> => [[0, Size]]},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(Worker1), <<"blocks">> => []}
                ],
                assertion_nodes = [Worker1, Worker2]
            }
        }
    ),
    [{_, Ceph} | _] = proplists:get_value(cephrados, ?config(storages, Config)),
    ContainerId = proplists:get_value(container_id, Ceph),
    ?assertMatch([], utils:cmd(["docker exec", atom_to_list(ContainerId), "rados -p onedata ls -"])).

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_file(Path, Size, User, CreationNode, AssertionNode, SessionGetter) ->
    ChunkSize = 1024,
    ChunksNum = 1024,
    PartSize = ChunksNum * ChunkSize,
    FileSizeBytes = PartSize * Size, % size in MB

    % File creation
    ?assertMatch({ok, _}, lfm_proxy:create(
        CreationNode, SessionGetter(CreationNode, User), Path, 8#755)
    ),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(
        CreationNode, SessionGetter(CreationNode, User), {path, Path}, rdwr
    )),

    BytesChunk = crypto:strong_rand_bytes(ChunkSize),
    Bytes = lists:foldl(fun(_, Acc) ->
        <<Acc/binary, BytesChunk/binary>>
    end, <<>>, lists:seq(1, ChunksNum)),

    lists:foreach(fun(Num) ->
        ?assertEqual({ok, PartSize}, lfm_proxy:write(
            CreationNode, Handle, Num * PartSize, Bytes
        ))
    end, lists:seq(0, Size - 1)),

    ?assertEqual(ok, lfm_proxy:close(CreationNode, Handle)),

    {ok, #file_attr{guid = GUID}} =
        ?assertMatch({ok, #file_attr{size = FileSizeBytes}},
            lfm_proxy:stat(AssertionNode, SessionGetter(AssertionNode, User),
                {path, Path}), 60
        ),

    file_ctx:new_by_guid(GUID).

rtransfer_config_fetch_mock(Request, NotifyFun, CompleteFun, _, _, _) ->
    #{offset := O, size := S} = Request,
    Ref = make_ref(),
    spawn(fun() ->
        timer:sleep(timer:seconds(60)),
        NotifyFun(Ref, O, S),
        CompleteFun(Ref, {ok, ok})
    end),
    {ok, Ref}.

cancel_transfers_for_session_and_file(Node, SessionId, FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    rpc:call(Node, replica_synchronizer, cancel_transfers_of_session, [
        FileUuid, SessionId
    ]).

get_fetched_blocks(Nodes) ->
    Mod = rtransfer_config,
    Fun = fetch,
    lists:flatmap(fun(Node) ->
        [CallsNum] = meck_get_num_calls([Node], Mod, Fun, '_'),
        lists:foldl(fun(Num, Acc) ->
            case rpc:call(Node, meck, capture, [Num, Mod, Fun, '_', 1]) of
                #{offset := O, size := S} ->
                    [{O, S} | Acc];
                _ ->
                    Acc
            end
        end, [], lists:seq(1, CallsNum))
    end, Nodes).

replicate_blocks(Worker, SessionID, FileCtx, Blocks) ->
    Promises = async_replicate_blocks_start(Worker, SessionID, FileCtx, Blocks),
    async_replicate_blocks_end(Promises).

async_replicate_blocks_start(Worker, SessionID, FileCtx, Blocks) ->
    lists:map(fun(Block) ->
        rpc:async_call(Worker, ?MODULE, replicate_block, [
            SessionID, FileCtx, Block
        ])
    end, Blocks).

async_replicate_blocks_end(Promises) ->
    lists:map(fun(Promise) ->
        case rpc:yield(Promise) of
            {ok, _} -> ok;
            Error -> Error
        end
    end, Promises).

replicate_block(SessionID, FileCtx, {Offset, Size, Priority}) ->
    UserCtx = user_ctx:new(SessionID),
    replica_synchronizer:synchronize(UserCtx, FileCtx,
        #file_block{offset = Offset, size = Size}, false, undefined, Priority
    ).

meck_get_num_calls(Nodes, Module, Fun, Args) ->
    lists:map(fun(Node) ->
        rpc:call(Node, meck, num_calls, [Module, Fun, Args], timer:seconds(60))
    end, Nodes).

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
    ct:timetrap({hours, 3}),
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
            timer:sleep(50),
            ok;
        (_) ->
            ok
    end
    ),

    Config2;
init_per_testcase(rtransfer_fetch_test, Config) ->
    Config2 = init_per_testcase(?DEFAULT_CASE(rtransfer_fetch_test), Config),

    [Node | _ ] = Nodes = ?config(op_worker_nodes, Config),
    {ok, HoleSize} = rpc:call(Node, application, get_env, [
        ?APP_NAME, rtransfer_min_hole_size
    ]),
    rpc:multicall(Nodes, application, set_env, [
        ?APP_NAME, rtransfer_min_hole_size, 6
    ]),

    [{default_min_hole_size, HoleSize} | Config2];
init_per_testcase(evict_on_ceph, Config) ->
    init_per_testcase(all, [{?SPACE_ID_KEY, <<"space7">>} | Config]);
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
end_per_testcase(rtransfer_fetch_test, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    MinHoleSize = ?config(default_min_hole_size, Config),
    rpc:multicall(Nodes, application, set_env, [
        ?APP_NAME, rtransfer_min_hole_size, MinHoleSize
    ]),
    end_per_testcase(?DEFAULT_CASE(rtransfer_fetch_test), Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
