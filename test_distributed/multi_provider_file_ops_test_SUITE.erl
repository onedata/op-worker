%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of file operations in multi provider environment.
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_file_ops_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("transfers_test_mechanism.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("bp_tree/include/bp_tree.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([replicate_block/3]).

-export([
    dir_stats_collector_test/1,
    dir_stats_collector_trash_test/1,
    transfer_after_enabling_stats_test/1,
    dir_stats_collector_parallel_write_test/1,
    dir_stats_collector_parallel_override_test/1,
    dir_stats_collector_parallel_write_with_sleep_test/1,
    dir_stats_collector_parallel_write_to_empty_file_test/1,
    create_on_different_providers_test/1,
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
    remote_driver_get_test/1,
    remote_driver_fold_test/1,
    rtransfer_fetch_test/1,
    rtransfer_cancel_for_session_test/1,
    remove_file_during_transfers_test/1,
    remove_file_on_remote_provider_ceph/1,
    evict_on_ceph/1,
    read_dir_collisions_test/1,
    get_recursive_file_list/1,
    check_fs_stats_on_different_providers/1,
    remote_driver_internal_call_test/1,
    list_children_recreated_remotely/1,
    registered_user_opens_remotely_created_file_test/1,
    registered_user_opens_remotely_created_share_test/1,
    guest_user_opens_remotely_created_file_test/1,
    guest_user_opens_remotely_created_share_test/1,
    truncate_on_storage_does_not_block_synchronizer/1,
    recreate_file_on_storage/1,
    recreate_dir_on_storage/1,
    transfer_with_missing_documents/1,
    detect_stale_replica_synchronizer_jobs_test/1,
    tmp_files_posix_test/1,
    tmp_files_flat_storage_test/1,
    tmp_files_delete_test/1
]).

-define(TEST_CASES, [
    dir_stats_collector_test,
    dir_stats_collector_trash_test,
    transfer_after_enabling_stats_test,
    dir_stats_collector_parallel_write_test,
    dir_stats_collector_parallel_override_test,
    dir_stats_collector_parallel_write_with_sleep_test,
    dir_stats_collector_parallel_write_to_empty_file_test,
    transfer_with_missing_documents,
    create_on_different_providers_test,
    file_consistency_test,
    concurrent_create_test,
    multi_space_test,
    mkdir_and_rmdir_loop_test,
    create_and_delete_file_loop_test,
    echo_and_delete_file_loop_test,
    distributed_delete_test,
    remote_driver_get_test,
    remote_driver_fold_test,
    rtransfer_fetch_test,
    rtransfer_cancel_for_session_test,
    % TODO VFS-6618 Fix handling of file being removed during transfer and uncomment this test
    % remove_file_during_transfers_test,
    remove_file_on_remote_provider_ceph,
    evict_on_ceph,
    read_dir_collisions_test,
    get_recursive_file_list,
    check_fs_stats_on_different_providers,
    remote_driver_internal_call_test,
    list_children_recreated_remotely,
    registered_user_opens_remotely_created_file_test,
    registered_user_opens_remotely_created_share_test,
    guest_user_opens_remotely_created_file_test,
    guest_user_opens_remotely_created_share_test,
    truncate_on_storage_does_not_block_synchronizer,
    recreate_file_on_storage,
    recreate_dir_on_storage,
    detect_stale_replica_synchronizer_jobs_test,
    tmp_files_posix_test,
    tmp_files_flat_storage_test,
    tmp_files_delete_test
]).

-define(PERFORMANCE_TEST_CASES, [
    mkdir_and_rmdir_loop_test,
    file_consistency_test,
    create_and_delete_file_loop_test,
    echo_and_delete_file_loop_test
]).

all() ->
    ?ALL(
        ?TEST_CASES
%%        ?PERFORMANCE_TEST_CASES
    ).

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

create_on_different_providers_test(Config) ->
    multi_provider_file_ops_test_base:create_on_different_providers_test_base(Config).

distributed_delete_test(Config) ->
    multi_provider_file_ops_test_base:distributed_delete_test_base(Config, <<"user1">>, {4,0,0,2}, 60).

remote_driver_internal_call_test(Config0) ->
    User = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, User, {4,0,0,2}, 60),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),
    [Worker2 | _] = Workers2 = ?config(workers2, Config),

    % Init test
    Dir = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
    Level2Dir = <<Dir/binary, "/", (generator:gen_name())/binary>>,

    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Level2Dir)),
    DirUniqueKey = datastore_model:get_unique_key(file_meta, file_id:guid_to_uuid(DirGuid)),

    % Verify init and get link doc key
    Master = self(),
    test_utils:mock_expect(Workers2, datastore_doc, fetch, fun
        (Ctx, Key, Batch, true = LinkFetch) ->
            Ans = meck:passthrough([Ctx#{disc_driver => undefined}, Key, Batch, LinkFetch]),
            Master ! {link_node, Key, Ctx, Ans},
            Ans;
        (Ctx, Key, Batch, false = LinkFetch) ->
            meck:passthrough([Ctx, Key, Batch, LinkFetch])
        end),

    multi_provider_file_ops_test_base:verify_stats(Config, Dir, true),
    multi_provider_file_ops_test_base:verify_stats(Config, Level2Dir, true),

    RecAns = receive
        {link_node, Key, Ctx, {{ok, #document{value = #links_node{key = DirUniqueKey}}}, _}} -> {ok, Key, Ctx}
    after
        1000 -> receive_error
    end,
    {ok, LinkKey, LinkCtx} = ?assertMatch({ok, _, _}, RecAns),

    % Check remote driver usage
    delete_link_from_memory(Workers2, LinkKey, LinkCtx),
    test_utils:mock_expect(Workers2, datastore_remote_driver, get_async, fun(Ctx, Key) ->
        Master ! {link_remote_node, Key},
        put(tp_master, self()), % Mock code is executed in dedicated process so needed variables have to be set here
        meck:passthrough([Ctx, Key])
    end),
    test_utils:mock_expect(Workers2, datastore_remote_driver, wait, fun
        ([{{error, {badmatch, {error, internal_call}}}, _SessId}] = Future) ->
            Master ! internal_call,
            meck:passthrough([Future]);
        (Future) ->
            meck:passthrough([Future])
    end),

    multi_provider_file_ops_test_base:verify_stats(Config, Level2Dir, true),
    RecAns2 = receive
        {link_remote_node, LinkKey} -> ok
    after
        1000 -> receive_error
    end,
    ?assertEqual(ok, RecAns2),

    % Force datastore internal call in remote driver
    Worker1ProvId = rpc:call(Worker1, oneprovider, get_id, []),
    Worker2OutSessId = rpc:call(Worker2, session_utils, get_provider_session_id, [outgoing, Worker1ProvId]),
    ?assertEqual(ok, rpc:call(Worker2, session, delete, [Worker2OutSessId])),

    % Check remote driver usage
    delete_link_from_memory(Workers2, LinkKey, LinkCtx),
    multi_provider_file_ops_test_base:verify_stats(Config, Level2Dir, true),
    RecAns3 = receive
        {link_remote_node, LinkKey} -> ok
    after
        1000 -> receive_error
    end,
    ?assertEqual(ok, RecAns3),
    RecAns4 = receive
        internal_call -> ok
    after
        1000 -> receive_error
    end,
    ?assertEqual(ok, RecAns4),

    ok.

delete_link_from_memory([Worker | _] = Workers, LinkKey, LinkCtx) ->
    #{memory_driver_ctx := MemoryDriverCtx} = MemoryOnlyDocCtx =
        LinkCtx#{disc_driver => undefined, remote_driver := undefined},
    ?assertMatch({ok, _}, rpc:call(Worker, datastore_router, route, [get, [MemoryOnlyDocCtx, LinkKey]])),
    lists:foreach(fun(W) ->
        ?assertEqual(ok, rpc:call(W, ets_driver, delete, [MemoryDriverCtx, LinkKey]))
    end, Workers),
    ?assertEqual({error, not_found}, rpc:call(Worker, datastore_router, route,
        [get, [MemoryOnlyDocCtx#{include_deleted => false}, LinkKey]])).

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

    Worker = fun(N) ->
        [W | _] = maps:get(lists:nth(N, lists:usort(ProvIDs0)), ProvMap),
        W
    end,

    User = <<"user1">>,

    [{_SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),
    SessId = fun(W) -> ?config({session_id, {User, ?GET_DOMAIN(W)}}, Config) end,

%%    ct:print("WMap: ~p", [{W(1), W(2), ProvMap}]),

    Dir0Name = <<"/", SpaceName/binary, "/concurrent_create_test_dir0">>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir0Name)),
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
                            lfm_proxy:unlink(Worker(WId), SessId(Worker(WId)), {path, DirName(N)})
                        end, lists:seq(1, ProvIdCount)),

                    lists:foreach(
                        fun(WId) ->
                            spawn(fun() ->
                                TestMaster ! {WId, lfm_proxy:mkdir(Worker(WId), SessId(Worker(WId)), DirName(N))}
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
                {ok, CL} = lfm_proxy:get_children(Worker(WId), SessId(Worker(WId)), {path, <<"/", Dir0Name/binary>>}, 0, 1000),
                {FetchedIds, FetchedNames} = lists:unzip(CL),

%%                ExpectedChildCount = ProvIdCount * FileCount,
%%                ct:print("Check ~p", [{lists:usort(Ids), lists:usort(FetchedIds)}]),
%%                ct:print("Check ~p", [{ExpectedChildCount, CL}]),

                {length(CL), length(lists:usort(FetchedNames)), lists:usort(FetchedIds)}
            end,
            ?assertMatch({ExpectedChildCount, ExpectedChildCount, ExpectedIds}, Check(), 15),

            {ok, ChildList} = lfm_proxy:get_children(Worker(WId), SessId(Worker(WId)), {path, <<"/", Dir0Name/binary>>}, 0, 1000),
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

remote_driver_get_test(Config) ->
    remote_driver_test_base(Config, get_links).

remote_driver_fold_test(Config) ->
    remote_driver_test_base(Config, fold_links).

remote_driver_test_base(Config, TestFun) ->
    Config2 = multi_provider_file_ops_test_base:extend_config(Config, <<"user1">>, {0, 0, 0, 0}, 0),
    [Worker1 | _] = Workers1 = ?config(workers1, Config2),
    [Worker2 | _] = ?config(workers_not1, Config2),

    Key = generator:gen_name(),
    LinkName = <<"someName">>,
    LinkTarget = <<"someTarget">>,
    Link = {LinkName, LinkTarget},
    TreeId1 = rpc:call(Worker1, oneprovider, get_id, []),
    Ctx1 = rpc:call(Worker1, datastore_model_default, get_ctx, [file_meta]),
    Ctx2 = rpc:call(Worker2, datastore_model_default, get_ctx, [file_meta]),
    #{memory_driver_ctx := MemoryCtx} = datastore_model_default:set_defaults(
        datastore_model:get_unique_key(file_meta, Key), Ctx1),

    Master = self(),
    test_utils:mock_expect(Workers1, links_tree, update_node, fun(NodeId, Node, State) ->
        Master ! {link_node_id, Key, NodeId},
        meck:passthrough([NodeId, Node, State])
    end),

    ?assertMatch({ok, #link{}}, rpc:call(Worker1, datastore_model, add_links, [
        Ctx1, Key, TreeId1, Link
    ])),
    LinkNodeId = receive
        {link_node_id, Key, Id} -> Id
    after
        1000 -> timeout
    end,
    ?assertNotEqual(timeout, LinkNodeId),
    GetLinksCount = fun() ->
        case rpc:call(Worker1, couchbase_driver, get, [#{bucket => <<"onedata">>}, LinkNodeId]) of
            {ok, _, #document{value = #links_node{node = #bp_tree_node{children = {bp_tree_children, _, Data, _}}}}} ->
                gb_trees:size(Data);
            _ ->
                0
        end
    end,
    ?assertMatch(1, GetLinksCount(), 20),

    test_utils:mock_expect(Workers1, links_tree, update_node, fun(NodeId, Node, State) ->
        ets_driver:delete(MemoryCtx, NodeId),
        timer:sleep(timer:seconds(30)),
        meck:passthrough([NodeId, Node, State])
    end),

    spawn(fun() ->
        Master ! {add_links_ans, rpc:call(Worker1, datastore_model, add_links, [
            Ctx1, Key, TreeId1, {<<"l1">>, <<"v2">>}
        ])}
    end),

    timer:sleep(1000),
    spawn(fun() ->
        Args = case TestFun of
            get_links -> [LinkName];
            fold_links -> [fun(Link, Acc) -> {ok, [Link | Acc]} end, [], #{}]
        end,
        Master ! {test_ans, rpc:call(Worker2, datastore_model, TestFun, [Ctx2, Key, TreeId1] ++ Args)}
    end),
    % Use timeout smaller than 30s to check if TestFun is not blocked by sleep in update_node
    ?assertMatch({ok, [#link{
        name = LinkName,
        target = LinkTarget
    }]}, receive
        {test_ans, TestAns} -> TestAns
    after
        timer:seconds(15) -> timeout
    end),

    ?assertMatch({ok, #link{}}, receive
        {add_links_ans, AddLinksAns} -> AddLinksAns
    after
        timer:seconds(60) -> timeout
    end),

    ?assertMatch({ok, _, #document{}},
        rpc:call(Worker2, couchbase_driver, get, [
            #{bucket => <<"onedata">>}, LinkNodeId
        ]), 20
    ).

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
    % TODO VFS-6618 Fix handling of file being removed during transfer and uncomment this test
    User = <<"user2">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, User, {0, 0, 0, 0}, 0),
    [Worker1 | _] = ?config(workers1, Config),
    [Worker2 | _] = ?config(workers_not1, Config),
    % Create file
    SessId = fun(U, W) ->
        ?config({session_id, {U, ?GET_DOMAIN(W)}}, Config)
    end,
    SpaceName = <<"space6">>,
    FilePath = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>, 
    BlocksCount = 10,
    BlockSize = 80 * 1024 * 1024,
    FileSize = BlocksCount * BlockSize,

    ?assertMatch({ok, _}, lfm_proxy:create(Worker2, SessId(User, Worker2), FilePath)),
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
    lfm_proxy:unlink(Worker2, SessId(User, Worker2), ?FILE_REF(GUID)),
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
    
    {ok, Guid} = lfm_proxy:create(Worker1, SessionId(Worker1), FilePath),
    {ok, Handle} = lfm_proxy:open(Worker1, SessionId(Worker1), ?FILE_REF(Guid), write),
    {ok, _} = lfm_proxy:write(Worker1, Handle, 0, crypto:strong_rand_bytes(100)),
    ok = lfm_proxy:close(Worker1, Handle),

    ?assertMatch({ok, _}, lfm_proxy:get_children(Worker2, SessionId(Worker2), ?FILE_REF(Guid), 0, 0), 60),
    L = utils:cmd(["docker exec", atom_to_list(ContainerId), "rados -p onedata ls -"]),
    ?assertEqual(true, length(L) > 0),
       
    lfm_proxy:unlink(Worker2, SessionId(Worker2), ?FILE_REF(Guid)),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_children(Worker1, SessionId(Worker1), ?FILE_REF(Guid), 0, 0), 60),
    ?assertMatch([], utils:cmd(["docker exec", atom_to_list(ContainerId), "rados -p onedata ls -"])).

evict_on_ceph(Config0) ->
    Config = multi_provider_file_ops_test_base:extend_config(Config0, <<"user2">>, {0, 0, 0, 0}, 0),
    Type = rest,
    FileKeyType = guid,
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

read_dir_collisions_test(Config0) ->
    Config = multi_provider_file_ops_test_base:extend_config(Config0, <<"user2">>, {0, 0, 0, 0}, 0),
    User = <<"user2">>,
    SessionId = fun(Node) -> ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config) end,

    [Worker1 | _] = ?config(workers1, Config),
    Domain1 = ?GET_DOMAIN_BIN(Worker1),

    [Worker2 | _] = ?config(workers_not1, Config),
    Domain2 = ?GET_DOMAIN_BIN(Worker2),

    SpaceName = <<"space7">>,
    RootDirPath = <<"/", SpaceName/binary, "/read_dir_collisions_test">>,
    {ok, RootDirGuid} = ?assertMatch({ok, _} , lfm_proxy:mkdir(Worker1, SessionId(Worker1), RootDirPath)),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(Worker2, SessionId(Worker2), ?FILE_REF(RootDirGuid)), ?ATTEMPTS),
    ?assertMatch({ok, RootDirGuid}, lfm_proxy:resolve_guid(Worker2, SessionId(Worker2), RootDirPath), ?ATTEMPTS),

    GetNamesFromGetChildrenFun = fun(Node, Offset, Limit) ->
        {ok, Children} = ?assertMatch(
            {ok, _},
            lfm_proxy:get_children(Node, SessionId(Node), ?FILE_REF(RootDirGuid), Offset, Limit)
        ),
        lists:map(fun({_Guid, Name}) -> Name end, Children)
    end,
    GetNamesFromGetChildrenAttrsFun = fun(Node, Offset, Limit) ->
        {ok, ChildrenAttrs, _} = ?assertMatch(
            {ok, _, _},
            lfm_proxy:get_children_attrs(Node, SessionId(Node), ?FILE_REF(RootDirGuid), #{offset => Offset, limit => Limit, tune_for_large_continuous_listing => false})
        ),
        lists:map(fun(#file_attr{name = Name}) -> Name end, ChildrenAttrs)
    end,
    GetNamesFromGetChildrenDetailsFun = fun(Node, Offset, Limit) ->
        {ok, ChildrenDetails, _} = ?assertMatch(
            {ok, _, _},
            lfm_proxy:get_children_details(Node, SessionId(Node), ?FILE_REF(RootDirGuid), #{offset => Offset, limit => Limit, tune_for_large_continuous_listing => false})
        ),
        lists:map(fun(#file_details{file_attr = #file_attr{name = Name}}) ->
            Name
        end, ChildrenDetails)
    end,

    FileNames = lists:map(fun(Num) ->
        FileName = <<"file_", (integer_to_binary(Num))/binary>>,
        FilePath = <<RootDirPath/binary, "/", FileName/binary>>,
        {ok, Guid1} = ?assertMatch({ok, _} , lfm_proxy:create(Worker1, SessionId(Worker1), FilePath)),
        {ok, Guid2} = ?assertMatch({ok, _} , lfm_proxy:create(Worker2, SessionId(Worker2), FilePath)),

        % Wait for providers to synchronize state
        ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(Worker1, SessionId(Worker1), ?FILE_REF(Guid2)), ?ATTEMPTS),
        ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(Worker2, SessionId(Worker2), ?FILE_REF(Guid1)), ?ATTEMPTS),

        FileName
    end, lists:seq(0, 9)),

    FilesSeenOnWorker1 = lists:flatmap(fun(Name) ->
        [Name, <<Name/binary, "@", Domain2/binary>>]
    end, FileNames),
    FilesSeenOnWorker2 = lists:flatmap(fun(Name) ->
        [<<Name/binary, "@", Domain1/binary>>, Name]
    end, FileNames),

    % Assert proper listing when only one file from all conflicted is listed in batch
    % (it should still has suffix)
    lists:foreach(fun({Offset, Limit}) ->
        ExpBatchOnWorker1 = lists:sublist(FilesSeenOnWorker1, Offset+1, Limit),
        ?assertMatch(ExpBatchOnWorker1, GetNamesFromGetChildrenFun(Worker1, Offset, Limit)),
        ?assertMatch(ExpBatchOnWorker1, GetNamesFromGetChildrenAttrsFun(Worker1, Offset, Limit)),
        ?assertMatch(ExpBatchOnWorker1, GetNamesFromGetChildrenDetailsFun(Worker1, Offset, Limit)),

        ExpBatchOnWorker2 = lists:sublist(FilesSeenOnWorker2, Offset+1, Limit),
        ?assertMatch(ExpBatchOnWorker2, GetNamesFromGetChildrenFun(Worker2, Offset, Limit)),
        ?assertMatch(ExpBatchOnWorker2, GetNamesFromGetChildrenAttrsFun(Worker2, Offset, Limit)),
        ?assertMatch(ExpBatchOnWorker2, GetNamesFromGetChildrenDetailsFun(Worker2, Offset, Limit))
    end, [
        % Check listing all files
        {0, 100},

        % Check listing on batches containing only one of two conflicting files
        % (it should be suffixed nonetheless)
        {0, 3}, {1, 3},
        {2, 3}, {3, 3},
        {4, 3}, {5, 3},
        {6, 3}, {7, 3},
        {8, 3}, {9, 3},
        {10, 3}, {11, 3},
        {12, 3}, {13, 3},
        {14, 3}, {15, 3},
        {16, 3}, {17, 3},

        {0, 5}, {1, 5},
        {4, 5}, {5, 5},
        {8, 5}, {9, 5},
        {12, 5}, {13, 5},
        {16, 5}, {17, 5},

        {0, 7}, {1, 7},
        {6, 7}, {7, 7},
        {12, 7}, {13, 7},

        {0, 9}, {1, 9},
        {8, 9}, {9, 9},
        {16, 9}, {17, 9},

        {0, 11}, {1, 11},
        {10, 11}, {11, 11},

        {0, 13}, {1, 13},
        {12, 13}, {13, 13},

        {0, 15}, {1, 15},
        {14, 15}, {15, 15}
    ]).


get_recursive_file_list(Config0) ->
    Config = multi_provider_file_ops_test_base:extend_config(Config0, <<"user2">>, {0, 0, 0, 0}, 0),
    User = <<"user2">>,
    SessionId = fun(Node) -> ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config) end,
    
    [Worker1 | _] = ?config(workers1, Config),
    Domain1 = ?GET_DOMAIN_BIN(Worker1),
    
    [Worker2 | _] = ?config(workers_not1, Config),
    Domain2 = ?GET_DOMAIN_BIN(Worker2),
    
    lfm_ct:save_named_context(w1, Worker1, SessionId(Worker1)),
    lfm_ct:save_named_context(w2, Worker2, SessionId(Worker2)),
    
    MainDir = generator:gen_name(),
    MainDirPath = <<"/space7/", MainDir/binary, "/">>,
    MainDirGuid = lfm_ct:mkdir_with_ctx(w1, [MainDirPath]),
    file_test_utils:await_sync(Worker2, MainDirGuid),
    
    Dirname = generator:gen_name(),
    DirGuidW1 = lfm_ct:mkdir_with_ctx(w1, [<<MainDirPath/binary, Dirname/binary>>]),
    DirGuidW2 = lfm_ct:mkdir_with_ctx(w2, [<<MainDirPath/binary, Dirname/binary>>]),
    
    file_test_utils:await_sync(Worker2, DirGuidW1),
    file_test_utils:await_sync(Worker1, DirGuidW2),
    
    DirnameMapping = #{
        {Worker1, DirGuidW1} => Dirname,
        {Worker1, DirGuidW2} => <<Dirname/binary, "@", Domain2/binary>>,
        {Worker2, DirGuidW1} => <<Dirname/binary, "@", Domain1/binary>>,
        {Worker2, DirGuidW2} => Dirname
    },
    
    Files = lists:sort(lists_utils:generate(fun generator:gen_name/0, 8)),
    {AllExpectedFilesW1, AllExpectedFilesW2} = lists:foldl(fun(DG, Acc) ->
        lists:foldl(fun(Filename, {FilesTmpW1, FilesTmpW2}) ->
            GW1 = lfm_ct:create_with_ctx(w1, [DG, Filename, ?DEFAULT_FILE_MODE]),
            GW2 = lfm_ct:create_with_ctx(w2, [DG, Filename, ?DEFAULT_FILE_MODE]),
            {
                FilesTmpW1 ++ [
                    {GW1, filename:join([maps:get({Worker1, DG}, DirnameMapping), Filename])}, 
                    {GW2, filename:join([maps:get({Worker1, DG}, DirnameMapping), <<Filename/binary, "@", Domain2/binary>>])}
                ],
                FilesTmpW2 ++ [
                    {GW1, filename:join([maps:get({Worker2, DG}, DirnameMapping), <<Filename/binary, "@", Domain1/binary>>])}, 
                    {GW2, filename:join([maps:get({Worker2, DG}, DirnameMapping), Filename])}
                ]
            }
        end, Acc, Files)
    end, {[], []}, [DirGuidW1, DirGuidW2]),
    lists:foreach(fun({Guid, _}) ->
        file_test_utils:await_sync([Worker1, Worker2], Guid)
    end, AllExpectedFilesW1),
    
    lfm_files_test_base:check_list_recursive_start_after(Worker1, SessionId(Worker1), MainDirGuid, AllExpectedFilesW1),
    lfm_files_test_base:check_list_recursive_start_after(Worker2, SessionId(Worker2), MainDirGuid, AllExpectedFilesW2).


check_fs_stats_on_different_providers(Config) ->
    [P1, _, P2 | _] = ?config(op_worker_nodes, Config),

    UserId = <<"user3">>,
    GetSessId = fun(W) -> ?config({session_id, {UserId, ?GET_DOMAIN(W)}}, Config) end,

    SpaceId = <<"space9">>,
    SpaceRootDir = <<"/space9/">>,

    % Values set and taken from env_desc.json
    P1StorageId = <<"cephrados">>,
    P1SupportSize = 10000,
    P2StorageId = <<"mntst2">>,
    P2SupportSize = 50000,
    ProviderToStorage = #{
        P1 => {P1StorageId, P1SupportSize},
        P2 => {P2StorageId, P2SupportSize}
    },

    % create file on provider1
    [F1, F2] = Files = lists:map(fun(Node) ->
        SessId = ?config({session_id, {<<"user3">>, ?GET_DOMAIN(Node)}}, Config),
        FileName = generator:gen_name(),
        FilePath = <<SpaceRootDir/binary, FileName/binary>>,
        {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, FilePath)),
        FileGuid
    end, [P1, P2]),

    AssertEqualFSSTatsFun = fun(Node, Occupied) ->
        SessId = GetSessId(Node),
        {StorageId, SupportSize} = maps:get(Node, ProviderToStorage),

        lists:foreach(fun(FileGuid) ->
            ?assertEqual(
                {ok, #fs_stats{space_id = SpaceId, storage_stats = [
                    #storage_stats{
                        storage_id = StorageId,
                        size = SupportSize,
                        occupied = Occupied
                    }
                ]}},
                lfm_proxy:get_fs_stats(Node, SessId, ?FILE_REF(FileGuid)),
                ?ATTEMPTS
            )
        end, Files)
    end,
    WriteToFileFun = fun({Node, FileGuid, Offset, BytesNum}) ->
        Content = crypto:strong_rand_bytes(BytesNum),
        {ok, Handle} = lfm_proxy:open(Node, GetSessId(Node), ?FILE_REF(FileGuid), write),
        {ok, _} = lfm_proxy:write(Node, Handle, Offset, Content),
        ok = lfm_proxy:close(Node, Handle),
        Content
    end,

    % Assert empty storages at the beginning of test
    AssertEqualFSSTatsFun(P1, 0),
    AssertEqualFSSTatsFun(P2, 0),

    % Write to files and assert that quota was updated
    lists:foreach(WriteToFileFun, [{P1, F1, 0, 50}, {P2, F2, 0, 80}]),
    AssertEqualFSSTatsFun(P1, 50),
    AssertEqualFSSTatsFun(P2, 80),

    % Write to file on P2 and assert that only its quota was updated
    WrittenContent = WriteToFileFun({P2, F1, 100, 40}),
    AssertEqualFSSTatsFun(P1, 50),
    AssertEqualFSSTatsFun(P2, 120),

    % Read file on P1 (force rtransfer) and assert that its quota was updated
    ExpReadContent = binary:part(WrittenContent, 0, 20),
    {ok, ReadHandle} = lfm_proxy:open(P1, GetSessId(P1), ?FILE_REF(F1), read),
    ?assertMatch({ok, ExpReadContent}, lfm_proxy:read(P1, ReadHandle, 100, 20), ?ATTEMPTS),
    ok = lfm_proxy:close(P1, ReadHandle),

    % Quota should be updated not by read 20 bytes but by 40. That is due to
    % rtransfer fetching larger blocks at once (not only requested bytes)
    AssertEqualFSSTatsFun(P1, 90),
    AssertEqualFSSTatsFun(P2, 120).

% This test reproduces bug that appeared in Web GUI.
% Functions' arguments are the same as arguments generated by GUI as a result of user's activity.
list_children_recreated_remotely(Config0) ->
    User = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, User, {4,0,0,2}, 60),
    Worker1 = ?config(worker1, Config),
    [Worker2 | _] = ?config(workers2, Config),
    SessId = ?config(session, Config),
    SpaceId = <<"space1">>,
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),

    % Upload file on worker1
    {ok, G} = lfm_proxy:create(Worker1, SessId(Worker1), SpaceGuid, <<"file_name">>, undefined),
    {ok, _} = lfm_proxy:get_details(Worker1, SessId(Worker1), ?FILE_REF(G)),
    {ok, _, _} = lfm_proxy:get_children_details(Worker1, SessId(Worker1), ?FILE_REF(SpaceGuid),
        #{offset => 0, limit => 24, tune_for_large_continuous_listing => false}),
    {ok, _} = lfm_proxy:stat(Worker1, SessId(Worker1), ?FILE_REF(G)),
    {ok, _, _} = lfm_proxy:get_children_details(Worker1, SessId(Worker1), ?FILE_REF(SpaceGuid),
        #{offset => -24, limit => 24, index => file_listing:build_index(<<"file_name">>), tune_for_large_continuous_listing => false}),
    {ok, H} = lfm_proxy:open(Worker1, SessId(Worker1), ?FILE_REF(G), write),
    {ok, _} = lfm_proxy:write(Worker1, H, 0, <<>>),
    ok = lfm_proxy:close(Worker1, H),
    {ok, _} = lfm_proxy:get_details(Worker1, SessId(Worker1), ?FILE_REF(G)),

    % Check on worker2
    {ok, _, _} = lfm_proxy:get_children_details(Worker2, SessId(Worker2), ?FILE_REF(SpaceGuid),
        #{offset => -24, limit => 24, index => file_listing:build_index(<<"file_name">>), tune_for_large_continuous_listing => false}),

    % Delete file on worker2
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker2, SessId(Worker2), ?FILE_REF(G)), 30),
    ok = lfm_proxy:rm_recursive(Worker2, SessId(Worker2), ?FILE_REF(G)),
    ?assertMatch({error, ?EINVAL}, lfm_proxy:get_children_details(Worker2, SessId(Worker2), ?FILE_REF(SpaceGuid),
        #{offset => -24, limit => 24, tune_for_large_continuous_listing => false})),
    ?assertMatch({ok, _, _}, lfm_proxy:get_children_details(Worker2, SessId(Worker2), ?FILE_REF(SpaceGuid),
        #{offset => -24, limit => 24, index => file_listing:build_index(<<"file_name">>), tune_for_large_continuous_listing => false})),
    ?assertMatch({ok, _, _}, lfm_proxy:get_children_details(Worker2, SessId(Worker2), ?FILE_REF(SpaceGuid),
        #{offset => 0, limit => 24, tune_for_large_continuous_listing => false})),
    ?assertMatch({ok, _, _}, lfm_proxy:get_children_details(Worker2, SessId(Worker2), ?FILE_REF(SpaceGuid),
        #{offset => 0, limit => 24, index => file_listing:build_index(<<"file_name">>), tune_for_large_continuous_listing => false})),

    % Recreate file on worker2
    {ok, NewG} = lfm_proxy:create(Worker2, SessId(Worker2), SpaceGuid, <<"file_name">>, undefined),
    {ok, _} = lfm_proxy:get_details(Worker2, SessId(Worker2), ?FILE_REF(NewG)),
    {ok, _, _} = lfm_proxy:get_children_details(Worker2, SessId(Worker2), ?FILE_REF(SpaceGuid),
        #{offset => 0, limit => 24, tune_for_large_continuous_listing => false}),
    {ok, _} = lfm_proxy:stat(Worker2, SessId(Worker2), ?FILE_REF(NewG)),

    % Another check on worker2
    % The bug appeared here (badmatch)
    {ok, _, _} = lfm_proxy:get_children_details(Worker2, SessId(Worker2), ?FILE_REF(SpaceGuid),
        #{offset => -24, limit => 24, index => file_listing:build_index(<<"file_name">>), tune_for_large_continuous_listing => false}),

    {ok, H2} = lfm_proxy:open(Worker2, SessId(Worker2), ?FILE_REF(NewG), write),
    {ok, _} = lfm_proxy:write(Worker2, H2, 0, <<>>),
    ok = lfm_proxy:close(Worker2, H2),
    {ok, _} = lfm_proxy:get_details(Worker2, SessId(Worker2), ?FILE_REF(NewG)).


registered_user_opens_remotely_created_file_test(Config) ->
    user_opens_file_test_base(Config, true, false, ?FUNCTION_NAME).

registered_user_opens_remotely_created_share_test(Config) ->
    user_opens_file_test_base(Config, true, true, ?FUNCTION_NAME).

guest_user_opens_remotely_created_file_test(Config) ->
    user_opens_file_test_base(Config, false, false, ?FUNCTION_NAME).

guest_user_opens_remotely_created_share_test(Config) ->
    user_opens_file_test_base(Config, false, true, ?FUNCTION_NAME).

user_opens_file_test_base(Config0, IsRegisteredUser, UseShareGuid, TestCase) ->
    Config = multi_provider_file_ops_test_base:extend_config(Config0, <<"user2">>, {0, 0, 0, 0}, 0),
    User = <<"user2">>,
    SessionId = fun(Node) -> ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config) end,
    [Worker1 | _] = ?config(workers1, Config),
    [Worker2 | _] = ?config(workers_not1, Config),

    % create file on 1st provider
    SpaceName = <<"space7">>,
    DirPath = filepath_utils:join([<<"/">>, SpaceName, atom_to_binary(TestCase, utf8)]),
    FilePath = filepath_utils:join([DirPath, <<"file">>]),
    {ok, _} = ?assertMatch({ok, _} , lfm_proxy:mkdir(Worker1, SessionId(Worker1), DirPath)),
    {ok, FileGuid} = ?assertMatch({ok, _} , lfm_proxy:create(Worker1, SessionId(Worker1), FilePath)),
    {ok, ShareId} = opt_shares:create(Worker1, SessionId(Worker1), ?FILE_REF(FileGuid), <<"share">>),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    % verify share on 2nd provider
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(Worker2, ?ROOT_SESS_ID, ?FILE_REF(ShareFileGuid)), ?ATTEMPTS),
    case {IsRegisteredUser, UseShareGuid} of
        {true, false} ->
             ?assertMatch({ok, _}, lfm_proxy:open(Worker2, SessionId(Worker2), ?FILE_REF(FileGuid), read), ?ATTEMPTS);
        {true, true} ->
            ?assertMatch({ok, _}, lfm_proxy:open(Worker2, SessionId(Worker2), ?FILE_REF(ShareFileGuid), read), ?ATTEMPTS);
        {false, false} ->
            ?assertMatch({error, ?ENOENT}, lfm_proxy:open(Worker2, ?GUEST_SESS_ID, ?FILE_REF(FileGuid), read), ?ATTEMPTS);
        {false, true} ->
            ?assertMatch({ok, _}, lfm_proxy:open(Worker2, ?GUEST_SESS_ID, ?FILE_REF(ShareFileGuid), read), ?ATTEMPTS)
    end.

truncate_on_storage_does_not_block_synchronizer(Config0) ->
    User = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, User, {4,0,0,2}, 60),
    Worker1 = ?config(worker1, Config),
    [Worker2 | _] = ?config(workers2, Config),
    Workers = ?config(op_worker_nodes, Config),
    SessId = ?config(session, Config),
    SpaceId = <<"space1">>,
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    FileContent = <<"xxx">>,
    FileSize = byte_size(FileContent),

    % Create file on worker1
    {ok, Guid} = lfm_proxy:create(Worker1, SessId(Worker1), SpaceGuid, <<"synch_blocking_test_file">>, undefined),
    {ok, Handle} = lfm_proxy:open(Worker1, SessId(Worker1), ?FILE_REF(Guid), write),
    {ok, _} = lfm_proxy:write(Worker1, Handle, 0, FileContent),
    ok = lfm_proxy:close(Worker1, Handle),

    % Wait for file sync and read
    ?assertMatch({ok, #file_attr{size = FileSize}}, lfm_proxy:stat(Worker2, SessId(Worker2), ?FILE_REF(Guid)), 60),
    {ok, Handle2} = lfm_proxy:open(Worker2, SessId(Worker2), ?FILE_REF(Guid), read),
    ?assertEqual({ok, FileContent}, lfm_proxy:read(Worker2, Handle2, 0, FileSize)),
    ok = lfm_proxy:close(Worker2, Handle2),

    % Mock hanging of storage truncate operation
    test_utils:mock_expect(Workers, storage_driver, truncate,
        fun(Handle, Size, CurrentSize) ->
            timer:sleep(timer:hours(1)),
            meck:passthrough([Handle, Size, CurrentSize])
        end
    ),

    % Test if file operations are not blocked by hanging storage truncate operation triggered by replica deletion
    Uuid = file_id:guid_to_uuid(Guid),
    {ok, FileLocation} = ?assertMatch({ok, _}, rpc:call(Worker1, file_location, get_local, [Uuid])),
    VersionVector = file_location:get_version_vector(FileLocation),
    spawn(fun() ->
        rpc:call(Worker1, replica_synchronizer, delete_whole_file_replica,
            [file_ctx:new_by_guid(Guid), VersionVector], timer:seconds(60))
    end),
    ?assertMatch({ok, #file_attr{size = FileSize}}, lfm_proxy:stat(Worker1, SessId(Worker1), ?FILE_REF(Guid)), 60),

    % Test if size change (triggered by event) is not blocked by hanging storage truncate operation
    ?assertEqual(ok, rpc:call(Worker1, lfm_event_emitter, emit_file_truncated, [Guid, 0, SessId(Worker1)])),
    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(Worker1, SessId(Worker1), ?FILE_REF(Guid)), 60),
    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(Worker2, SessId(Worker2), ?FILE_REF(Guid)), 60),

    ok.

recreate_file_on_storage(Config0) ->
    User = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, User, {4,0,0,2}, 60),
    Worker1 = ?config(worker1, Config),
    [Worker2 | _] = ?config(workers2, Config),
    Workers = ?config(op_worker_nodes, Config),
    SessId = ?config(session, Config),
    SpaceId = <<"space1">>,
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    FileContent = <<"xxx">>,
    FileSize = byte_size(FileContent),

    % Mock to prevent storage file creation (only metadata will be set)
    mock_storage_driver_to_prevent_file_creation(Workers),

    % Create file on worker1
    {ok, {Guid, Handle0}} = ?assertMatch({ok, _},
        lfm_proxy:create_and_open(Worker1, SessId(Worker1), SpaceGuid, <<"recreate_file_on_storage">>, undefined)),
    ?assertEqual(ok, lfm_proxy:close(Worker1, Handle0)),

    % Unload mock - file is created according to metadata but it has not been created on storage
    ?assertEqual(ok, test_utils:mock_unload(Workers, storage_driver)),

    % Wait for file sync
    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(Worker2, SessId(Worker2), ?FILE_REF(Guid)), 60),

    % Add data to file
    {ok, Handle} = lfm_proxy:open(Worker2, SessId(Worker2), ?FILE_REF(Guid), write),
    {ok, _} = lfm_proxy:write(Worker2, Handle, 0, FileContent),
    ok = lfm_proxy:close(Worker2, Handle),

    % Wait for metadata sync and replicate file (replication should succeed even though file on storage is missing)
    ?assertMatch({ok, #file_attr{size = FileSize}}, lfm_proxy:stat(Worker1, SessId(Worker1), ?FILE_REF(Guid)), 60),
    ProviderId = rpc:call(Worker1, oneprovider, get_id_or_undefined, []),
    {ok, TransferID} = ?assertMatch({ok, _}, opt_transfers:schedule_file_replication(Worker1, SessId(Worker1),
        ?FILE_REF(Guid), ProviderId)),
    multi_provider_file_ops_test_base:await_replication_end(Worker1 ,TransferID, 60).


recreate_dir_on_storage(Config0) ->
    User = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, User, {4,0,0,2}, 60),
    Worker1 = ?config(worker1, Config),
    [Worker2 | _] = ?config(workers2, Config),
    Workers = ?config(op_worker_nodes, Config),
    SessId = ?config(session, Config),
    SpaceId = <<"space1">>,
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    FileContent = <<"xxx">>,
    FileSize = byte_size(FileContent),

    % Mock to prevent storage file creation (only metadata will be set)
    ?assertEqual(ok, test_utils:mock_new(Workers, storage_driver)),
    ?assertEqual(ok, test_utils:mock_expect(Workers, storage_driver, mkdir, fun(_SDHandle, _Mode) -> ok end)),
    ?assertEqual(ok, test_utils:mock_expect(Workers, storage_driver, mkdir, fun(_SDHandle, _Mode, _Recursive) -> ok end)),

    % Create dirs and file on worker1
    DirName = generator:gen_name(),
    {ok, DirGuid} = ?assertMatch({ok, _},
        lfm_proxy:mkdir(Worker1, SessId(Worker1), SpaceGuid, DirName, undefined)),
    {ok, Level2DirGuid} = ?assertMatch({ok, _},
        lfm_proxy:mkdir(Worker1, SessId(Worker1), DirGuid, generator:gen_name(), undefined)),
    {ok, Guid} = ?assertMatch({ok, _},
        lfm_proxy:create(Worker1, SessId(Worker1), Level2DirGuid, generator:gen_name(), undefined)),
    ?assertEqual({error, enoent}, lfm_proxy:open(Worker1, SessId(Worker1), ?FILE_REF(Guid), read)),

    % Check if dir location exists according to metadata
    {ok, DirLocation} = ?assertMatch({ok, _}, rpc:call(Worker1, dir_location, get, [file_id:guid_to_uuid(DirGuid)])),
    ?assert(dir_location:is_storage_file_created(DirLocation)),
    {ok, Level2DirLocation} = ?assertMatch({ok, _},
        rpc:call(Worker1, dir_location, get, [file_id:guid_to_uuid(Level2DirGuid)])),
    ?assert(dir_location:is_storage_file_created(Level2DirLocation)),
    % Check that dirs do not exist on storage (checking highest level dir is enough)
    ?assertEqual({error, enoent},
        storage_test_utils:read_file_info(Worker1, storage_test_utils:file_path(Worker1, SpaceId, DirName))),

    % Unload mock - dirs are created according to metadata but they have not been created on storage
    ?assertEqual(ok, test_utils:mock_unload(Workers, storage_driver)),

    % Wait for file sync
    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(Worker2, SessId(Worker2), ?FILE_REF(Guid)), 60),

    % Add data to file
    {ok, Handle} = lfm_proxy:open(Worker2, SessId(Worker2), ?FILE_REF(Guid), write),
    {ok, _} = lfm_proxy:write(Worker2, Handle, 0, FileContent),
    ok = lfm_proxy:close(Worker2, Handle),

    % Wait for metadata sync and replicate file (replication should succeed even though dirs on storage are missing)
    ?assertMatch({ok, #file_attr{size = FileSize}}, lfm_proxy:stat(Worker1, SessId(Worker1), ?FILE_REF(Guid)), 60),
    ProviderId = rpc:call(Worker1, oneprovider, get_id_or_undefined, []),
    {ok, TransferID} = ?assertMatch({ok, _}, opt_transfers:schedule_file_replication(Worker1, SessId(Worker1),
        ?FILE_REF(Guid), ProviderId)),
    multi_provider_file_ops_test_base:await_replication_end(Worker1 ,TransferID, 60).


transfer_with_missing_documents(Config) ->
    Worker1 = ?config(worker1, Config),
    [Worker2 | _] = Workers2 = ?config(workers2, Config),
    SessId = ?config(session, Config),
    SpaceId = <<"space1">>,
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    FileContent = <<"xxx">>,
    % Create dir and files on worker1
    {ok, DirGuid} = ?assertMatch({ok, _},
        lfm_proxy:mkdir(Worker1, SessId(Worker1), SpaceGuid, generator:gen_name(), undefined)),
    [FileWithoutLocationGuid, DeletedFileWithoutLocationGuid] = lists_utils:generate(fun() ->
        {ok, GuidWithoutLocation} = ?assertMatch({ok, _},
            lfm_proxy:create(Worker1, SessId(Worker1), DirGuid, generator:gen_name(), undefined)),
        GuidWithoutLocation
    end, 2),
    [FileWithNotSyncedLocationGuid, FileWithNotSyncedFileMetaAndTimesGuid, DeletedFileGuid] = lists_utils:generate(fun() ->
        {ok, {GuidWithLocation, Handle}} = ?assertMatch({ok, _},
            lfm_proxy:create_and_open(Worker1, SessId(Worker1), DirGuid, generator:gen_name(), undefined)),
        lfm_proxy:write(Worker1, Handle, 0, FileContent),
        ?assertEqual(ok, lfm_proxy:close(Worker1, Handle)),
        GuidWithLocation
    end, 3),
    lists:foreach(fun(GuidToDel) ->
        ?assertEqual(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), ?FILE_REF(GuidToDel)))
    end, [DeletedFileWithoutLocationGuid, DeletedFileGuid]),

    % Allow dbsync send metadata
    timer:sleep(timer:seconds(10)),

    % Resync all without chosen documents (dbsync was mocked and has not synced anything)
    FileWithNotSyncedLocationUuid = file_id:guid_to_uuid(FileWithNotSyncedLocationGuid),
    FileWithNotSyncedFileMetaAndTimesUuid = file_id:guid_to_uuid(FileWithNotSyncedFileMetaAndTimesGuid),
    test_utils:mock_expect(Workers2, dbsync_changes, apply, fun
        (#document{value = #file_location{uuid = Uuid}}) when Uuid =:= FileWithNotSyncedLocationUuid -> ok;
        (#document{key = Uuid, value = #file_meta{}}) when Uuid =:= FileWithNotSyncedFileMetaAndTimesUuid -> ok;
        (Doc) -> meck:passthrough([Doc])
    end),
    Provider1Id = rpc:call(Worker1, oneprovider, get_id_or_undefined, []),
    ?assertEqual(ok, rpc:call(Worker2, dbsync_worker, resynchronize_all, [SpaceId, Provider1Id])),
    ?assertEqual(rpc:call(Worker1, dbsync_state, get_seq, [SpaceId, Provider1Id]),
        rpc:call(Worker2, dbsync_state, get_seq, [SpaceId, Provider1Id]), 60),

    % Replicate dir - files should be processed but not replicated
    Provider2Id = rpc:call(Worker2, oneprovider, get_id_or_undefined, []),
    {ok, TransferId} = ?assertMatch({ok, _}, opt_transfers:schedule_file_replication(Worker2, SessId(Worker2),
        ?FILE_REF(DirGuid), Provider2Id)),
    ?assertMatch({ok, #document{value = #transfer{
        replication_status = completed,
        files_to_process = 2,
        files_processed = 2,
        files_replicated = 0
    }}}, rpc:call(Worker2, transfer, get, [TransferId]), 30),

    % Unload mock and resync all
    ?assertEqual(ok, test_utils:mock_unload(Workers2, dbsync_changes)),
    ?assertEqual(ok, rpc:call(Worker2, dbsync_worker, resynchronize_all, [SpaceId, Provider1Id])),
    ?assertEqual(rpc:call(Worker1, dbsync_state, get_seq, [SpaceId, Provider1Id]),
        rpc:call(Worker2, dbsync_state, get_seq, [SpaceId, Provider1Id]), 60),

    % Replicate dir - files should replicated
    {ok, Transfer2Id} = ?assertMatch({ok, _}, opt_transfers:schedule_file_replication(Worker2, SessId(Worker2),
        ?FILE_REF(DirGuid), Provider2Id)),
    ?assertMatch({ok, #document{value = #transfer{
        replication_status = completed,
        files_to_process = 3,
        files_processed = 3,
        files_replicated = 2,
        bytes_replicated = 6
    }}}, rpc:call(Worker2, transfer, get, [Transfer2Id]), 30),

    FilesToClean = [FileWithoutLocationGuid, FileWithNotSyncedLocationGuid, FileWithNotSyncedFileMetaAndTimesGuid, DirGuid],
    % Clean space
    lists:foreach(fun(GuidToDel) ->
        ?assertEqual(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), ?FILE_REF(GuidToDel)))
    end, FilesToClean),
    lists:foreach(fun(DeletedGuid) ->
        ?assertMatch({error, enoent}, lfm_proxy:stat(Worker2, SessId(Worker2), ?FILE_REF(DeletedGuid)), 60)
    end, FilesToClean).


detect_stale_replica_synchronizer_jobs_test(Config0) ->
    User = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, User, {4,0,0,2}, 60),
    [Worker1 | _] = Workers1 = ?config(workers1, Config),
    [Worker2 | _] = ?config(workers2, Config),
    Workers = ?config(op_worker_nodes, Config),
    SessId = ?config(session, Config),
    SpaceId = <<"space1">>,
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    FileContent = <<"xxx">>,
    FileSize = byte_size(FileContent),

    ?assertEqual(ok, test_utils:mock_new(Workers, rtransfer_config, [passthrough])),
    ?assertEqual(ok, test_utils:mock_expect(Workers, rtransfer_config, fetch, fun(_, _, _, _, _, _) ->
        {ok, make_ref()}
    end)),

    % Mock to prevent storage file creation (only metadata will be set)
    mock_storage_driver_to_prevent_file_creation(Workers),

    % Create file on worker1
    {ok, {Guid, Handle0}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        Worker1, SessId(Worker1), SpaceGuid, ?RAND_STR(), undefined
    )),
    ?assertEqual(ok, lfm_proxy:close(Worker1, Handle0)),

    % Unload mock - file is created according to metadata but it has not been created on storage
    ?assertEqual(ok, test_utils:mock_unload(Workers, storage_driver)),

    % Wait for file sync
    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(Worker2, SessId(Worker2), ?FILE_REF(Guid)), 60),

    % Add data to file
    {ok, Handle} = lfm_proxy:open(Worker2, SessId(Worker2), ?FILE_REF(Guid), write),
    {ok, _} = lfm_proxy:write(Worker2, Handle, 0, FileContent),
    ok = lfm_proxy:close(Worker2, Handle),

    % Wait for metadata sync and replicate file (replication should succeed even though file on storage is missing)
    ?assertMatch({ok, #file_attr{size = FileSize}}, lfm_proxy:stat(Worker1, SessId(Worker1), ?FILE_REF(Guid)), 60),
    ProviderId = rpc:call(Worker1, oneprovider, get_id_or_undefined, []),
    {ok, TransferId} = ?assertMatch({ok, _}, opt_transfers:schedule_file_replication(
        Worker1, SessId(Worker1), ?FILE_REF(Guid), ProviderId
    )),
    ?assertMatch(
        {ok, #document{value = #transfer{replication_status = failed}}},
        rpc:call(Worker1, transfer, get, [TransferId]),
        ?ATTEMPTS
    ),

    % Assert replica_synchronized tried to restart transfer 5 times
    FetchFunSignature = [
        rtransfer_config,
        fetch,
        [#{file_guid => Guid, space_id => SpaceId, offset => 0, size => FileSize}, '_', '_', '_', '_', '_']
    ],
    FetchCallsNum = lists:sum(lists:map(fun(Worker) ->
        rpc:call(Worker, meck, num_calls, FetchFunSignature, timer:seconds(1))
    end, Workers1)),

    ?assertEqual(6, FetchCallsNum).


tmp_files_posix_test(Config) ->
    tmp_files_test_base(Config, <<"user1">>, <<"space1">>).


tmp_files_flat_storage_test(Config) ->
    tmp_files_test_base(Config, <<"user5">>, <<"space10">>).


tmp_files_test_base(Config0, User, SpaceId) ->
    Config = multi_provider_file_ops_test_base:extend_config(Config0, User, {4,0,0,2}, 60),
    [Worker1 | _] = ?config(workers1, Config),
    [Worker2 | _] = ?config(workers2, Config),
    Workers = ?config(op_worker_nodes, Config),
    SessId = ?config(session, Config),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    TmpDirGuid = fslogic_file_id:spaceid_to_tmp_dir_guid(SpaceId),

    {ok, SyncedDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), SpaceGuid, ?RAND_STR(), undefined)),

    % Create files and dir directly in tmp_dir on worker1
    {[DirGuid, Dir2Guid] = TmpDirs, [FileGuid1 | _] = TmpRegFiles} = create_tmp_files(Config, SpaceId),
    TmpFiles = TmpDirs ++ TmpRegFiles,

    % test file_ctx functions connected to tmp dir
    ?assertNot(file_ctx:is_tmp_dir_const(file_ctx:new_by_guid(SyncedDirGuid))),
    ?assert(file_ctx:is_tmp_dir_const(file_ctx:new_by_guid(TmpDirGuid))),
    ?assertNot(file_ctx:is_tmp_dir_const(file_ctx:new_by_guid(SpaceGuid), <<"file_name">>)),
    ?assert(file_ctx:is_tmp_dir_const(file_ctx:new_by_guid(SpaceGuid), ?TMP_DIR_NAME)),
    ?assertMatch({true, _}, rpc:call(Worker1, file_ctx, is_synchronization_enabled, [file_ctx:new_by_guid(SyncedDirGuid)])),
    ?assertMatch({false, _}, rpc:call(Worker1, file_ctx, is_synchronization_enabled, [file_ctx:new_by_guid(TmpDirGuid)])),
    ?assertMatch({false, _}, rpc:call(Worker1, file_ctx, is_synchronization_enabled, [file_ctx:new_by_guid(DirGuid)])),

    wait_for_possible_sync(Config, SpaceId, TmpFiles, false),

    ?assertMatch({ok, [], _}, lfm_proxy:get_children(
        Worker2, SessId(Worker2), #file_ref{guid = TmpDirGuid}, #{tune_for_large_continuous_listing => false}
    )),
    lists:foreach(fun(G) ->
        ?assertMatch({error, enoent}, lfm_proxy:stat(Worker2, SessId(Worker2), #file_ref{guid = G}))
    end, TmpFiles),

    ?assertMatch({error, ?EINVAL}, lfm_proxy:mv(
        Worker1, SessId(Worker1), #file_ref{guid = SyncedDirGuid}, #file_ref{guid = TmpDirGuid}, ?RAND_STR()
    )),
    ?assertMatch({ok, _}, lfm_proxy:mv(
        Worker1, SessId(Worker1), #file_ref{guid = FileGuid1}, #file_ref{guid = SyncedDirGuid}, ?RAND_STR()
    )),
    ?assertMatch({ok, _}, lfm_proxy:mv(
        Worker1, SessId(Worker1), #file_ref{guid = DirGuid}, #file_ref{guid = SyncedDirGuid}, ?RAND_STR()
    )),

    lists:foreach(fun(Worker) ->
        lists:foreach(fun(G) ->
            ?assertMatch({ok,#file_attr{}}, lfm_proxy:stat(Worker2, SessId(Worker2), #file_ref{guid = G}), ?ATTEMPTS)
        end, TmpDirs),
        lists:foreach(fun(G) ->
            ?assertMatch({ok,#file_attr{size = 100}}, lfm_proxy:stat(Worker2, SessId(Worker2), #file_ref{guid = G}), ?ATTEMPTS)
        end, TmpRegFiles),

        ?assertMatch({ok, [_, _], _}, lfm_proxy:get_children(
            Worker, SessId(Worker), #file_ref{guid = SyncedDirGuid}, #{tune_for_large_continuous_listing => false}
        ), ?ATTEMPTS),
        ?assertMatch({ok, [_, _], _}, lfm_proxy:get_children(
            Worker, SessId(Worker), #file_ref{guid = DirGuid}, #{tune_for_large_continuous_listing => false}
        ), ?ATTEMPTS),
        ?assertMatch({ok, [_], _}, lfm_proxy:get_children(
            Worker, SessId(Worker), #file_ref{guid = Dir2Guid}, #{tune_for_large_continuous_listing => false}
        ), ?ATTEMPTS)
    end, Workers).

tmp_files_delete_test(Config0) ->
    User = <<"user1">>,
    SpaceId = <<"space1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, User, {4,0,0,2}, 60),
    [Worker1 | _] = ?config(workers1, Config),
    [Worker2 | _] = Workers2 = ?config(workers2, Config),
    SessId = ?config(session, Config),
    TmpDirGuid = fslogic_file_id:spaceid_to_tmp_dir_guid(SpaceId),

    Master = self(),
    test_utils:mock_expect(Workers2, dbsync_changes, apply, fun
        (Doc) ->
            Master ! {dbsync_changes_key, get_file_meta_unique_key(Doc)},
            meck:passthrough([Doc])
    end),

    % Create files and dir directly in tmp_dir on worker1
    {[DirGuid | _] = TmpDirs, [FileGuid1 | _] = TmpRegFiles} = create_tmp_files(Config, SpaceId),
    TmpFiles = TmpDirs ++ TmpRegFiles,

    wait_for_possible_sync(Config, SpaceId, TmpFiles, false),

    ?assertMatch({ok, [], _}, lfm_proxy:get_children(
        Worker2, SessId(Worker2), #file_ref{guid = TmpDirGuid}, #{tune_for_large_continuous_listing => false}
    )),
    lists:foreach(fun(G) ->
        ?assertMatch({error, enoent}, lfm_proxy:stat(Worker2, SessId(Worker2), #file_ref{guid = G}))
    end, TmpFiles),

    ?assertEqual(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), #file_ref{guid = FileGuid1})),
    ?assertEqual(ok, lfm_proxy:rm_recursive(Worker1, SessId(Worker1), #file_ref{guid = DirGuid})),

    wait_for_possible_sync(Config, SpaceId, TmpFiles, true),

    DbsyncChangesKeys = gather_dbsync_changes_keys(),
    lists:foreach(fun(Guid) ->
        Key = datastore_model:get_unique_key(file_meta, file_id:guid_to_uuid(Guid)),
        ?assertNot(lists:member(Key, DbsyncChangesKeys))
    end, TmpFiles).


dir_stats_collector_test(Config0) ->
    UserId = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, UserId, {4,0,0,2}, 60),
    dir_stats_collector_test_base:multiprovider_test(Config).


dir_stats_collector_trash_test(Config0) ->
    UserId = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, UserId, {4,0,0,2}, 60),
    dir_stats_collector_test_base:multiprovider_trash_test(Config).


transfer_after_enabling_stats_test(Config0) ->
    UserId = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, UserId, {4,0,0,2}, 60),
    dir_stats_collector_test_base:transfer_after_enabling_test(Config).


dir_stats_collector_parallel_write_test(Config0) ->
    UserId = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, UserId, {4,0,0,2}, 60),
    dir_stats_collector_test_base:parallel_write_test(Config, false, 10, false).


dir_stats_collector_parallel_override_test(Config0) ->
    UserId = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, UserId, {4,0,0,2}, 60),
    dir_stats_collector_test_base:parallel_write_test(Config, false, 10, true).


dir_stats_collector_parallel_write_with_sleep_test(Config0) ->
    UserId = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, UserId, {4,0,0,2}, 60),
    dir_stats_collector_test_base:parallel_write_test(Config, true, 10, false).


dir_stats_collector_parallel_write_to_empty_file_test(Config0) ->
    UserId = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, UserId, {4,0,0,2}, 60),
    dir_stats_collector_test_base:parallel_write_test(Config, true, 0, false).


%%%===================================================================
%%% Internal functions
%%%===================================================================

create_tmp_files(Config, SpaceId) ->
    [Worker1 | _] = ?config(workers1, Config),
    SessId = ?config(session, Config),
    TmpDirGuid = fslogic_file_id:spaceid_to_tmp_dir_guid(SpaceId),

    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), TmpDirGuid, ?RAND_STR(), undefined)),
    {ok, Dir2Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), DirGuid, ?RAND_STR(), undefined)),
    {ok, {FileGuid1, Handle1}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        Worker1, SessId(Worker1), TmpDirGuid, ?RAND_STR(), undefined
    )),
    {ok, {FileGuid2, Handle2}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        Worker1, SessId(Worker1), DirGuid, ?RAND_STR(), undefined
    )),
    {ok, {FileGuid3, Handle3}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        Worker1, SessId(Worker1), Dir2Guid, ?RAND_STR(), undefined
    )),

    lists:foreach(fun(Handle) ->
        ?assertEqual({ok, 100}, lfm_proxy:write(Worker1, Handle, 0, crypto:strong_rand_bytes(100))),
        ?assertEqual(ok, lfm_proxy:close(Worker1, Handle))
    end, [Handle1, Handle2, Handle3]),

    ?assertMatch({ok, [_, _], _}, lfm_proxy:get_children(
        Worker1, SessId(Worker1), #file_ref{guid = TmpDirGuid}, #{tune_for_large_continuous_listing => false}
    )),

    {[DirGuid, Dir2Guid], [FileGuid1, FileGuid2, FileGuid3]}.

wait_for_possible_sync(Config, SpaceId, Guids, AreDeleted) ->
    [Worker1 | _] = ?config(workers1, Config),
    [Worker2 | _] = ?config(workers2, Config),
    Provider1Id = rpc:call(Worker1, oneprovider, get_id_or_undefined, []),

    % Wait for disc save
    lists:foreach(fun(Guid) ->
        Key = datastore_model:get_unique_key(file_meta, file_id:guid_to_uuid(Guid)),
        ?assertMatch({ok, _, #document{deleted = AreDeleted}},
            rpc:call(Worker1, couchbase_driver, get, [#{bucket => <<"onedata">>}, Key]), 30)
    end, Guids),

    % Wait for sequences sync
    AreSeqsEqual = fun() ->
        rpc:call(Worker1, dbsync_state, get_seq, [SpaceId, Provider1Id]) =:=
            rpc:call(Worker2, dbsync_state, get_seq, [SpaceId, Provider1Id])
    end,
    ?assertEqual(true, AreSeqsEqual(), 60).

get_file_meta_unique_key(#document{value = #links_forest{key = Key}}) ->
    Key;
get_file_meta_unique_key(#document{value = #links_node{key = Key}}) ->
    Key;
get_file_meta_unique_key(#document{value = #links_mask{key = Key}}) ->
    Key;
get_file_meta_unique_key(#document{key = Key}) ->
    datastore_model:get_unique_key(file_meta, Key).


gather_dbsync_changes_keys() ->
    receive
        {dbsync_changes_key, Key} -> [Key | gather_dbsync_changes_keys()]
    after
        0 -> []
    end.


create_file(Path, Size, User, CreationNode, AssertionNode, SessionGetter) ->
    ChunkSize = 1024,
    ChunksNum = 1024,
    PartSize = ChunksNum * ChunkSize,
    FileSizeBytes = PartSize * Size, % size in MB

    % File creation
    ?assertMatch({ok, _}, lfm_proxy:create(
        CreationNode, SessionGetter(CreationNode, User), Path)
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
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
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
        #file_block{offset = Offset, size = Size}, false, undefined, Priority, transfer
    ).

meck_get_num_calls(Nodes, Module, Fun, Args) ->
    lists:map(fun(Node) ->
        rpc:call(Node, meck, num_calls, [Module, Fun, Args], timer:seconds(60))
    end, Nodes).

mock_storage_driver_to_prevent_file_creation(Workers) ->
    ?assertEqual(ok, test_utils:mock_expect(Workers, storage_driver, create, fun(_SDHandle, _Mode) -> ok end)),
    ?assertEqual(ok, test_utils:mock_expect(Workers, storage_driver, open, fun(SDHandle, _Flag) -> {ok, SDHandle} end)),
    ?assertEqual(ok, test_utils:mock_expect(Workers, storage_driver, release, fun(_SDHandle) -> ok end)).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        initializer:mock_auth_manager(NewConfig),
        multi_provider_file_ops_test_base:init_env(NewConfig)
    end,
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config),
    initializer:unmock_auth_manager(Config).


init_per_testcase(file_consistency_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, file_meta, [passthrough]),
    init_per_testcase(?DEFAULT_CASE(file_consistency_test), Config);
init_per_testcase(rtransfer_fetch_test, Config) ->
    Config2 = init_per_testcase(?DEFAULT_CASE(rtransfer_fetch_test), Config),

    [Node | _ ] = Nodes = ?config(op_worker_nodes, Config),
    {ok, HoleSize} = rpc:call(Node, application, get_env, [
        ?APP_NAME, rtransfer_min_hole_size
    ]),
    utils:rpc_multicall(Nodes, application, set_env, [
        ?APP_NAME, rtransfer_min_hole_size, 6
    ]),

    [{default_min_hole_size, HoleSize} | Config2];
init_per_testcase(TestCase, Config) when
    TestCase == evict_on_ceph;
    TestCase == read_dir_collisions_test
->
    init_per_testcase(all, [{?SPACE_ID_KEY, <<"space7">>} | Config]);
init_per_testcase(check_fs_stats_on_different_providers, Config) ->
    initializer:unload_quota_mocks(Config),
    init_per_testcase(all, Config);
init_per_testcase(remote_driver_internal_call_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [datastore_doc, datastore_remote_driver], [passthrough]),
    init_per_testcase(?DEFAULT_CASE(remote_driver_internal_call_test), Config);
init_per_testcase(Case, Config) when
    Case =:= registered_user_opens_remotely_created_file_test;
    Case =:= registered_user_opens_remotely_created_share_test;
    Case =:= guest_user_opens_remotely_created_file_test;
    Case =:= guest_user_opens_remotely_created_share_test
->
    initializer:mock_share_logic(Config),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(Case, Config)when
    Case =:= recreate_file_on_storage;
    Case =:= recreate_dir_on_storage;
    Case =:= truncate_on_storage_does_not_block_synchronizer
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, storage_driver),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(Case, Config) when
    Case =:= dir_stats_collector_test;
    Case =:= dir_stats_collector_trash_test;
    Case =:= dir_stats_collector_parallel_write_test;
    Case =:= dir_stats_collector_parallel_override_test;
    Case =:= dir_stats_collector_parallel_write_with_sleep_test;
    Case =:= dir_stats_collector_parallel_write_to_empty_file_test
->
    dir_stats_collector_test_base:init_and_enable_for_new_space(init_per_testcase(
        ?DEFAULT_CASE(Case), Config
    ));
init_per_testcase(transfer_after_enabling_stats_test = Case, Config) ->
    dir_stats_collector_test_base:init(init_per_testcase(?DEFAULT_CASE(Case), Config));
init_per_testcase(detect_stale_replica_synchronizer_jobs_test = Case, Config) ->
    Config2 = init_per_testcase(?DEFAULT_CASE(Case), Config),

    Nodes = ?config(op_worker_nodes, Config2),
    lists:foreach(fun({EnvVar, Value}) ->
        utils:rpc_multicall(Nodes, application, set_env, [
            ?APP_NAME, EnvVar, Value
        ])
    end, [
        {max_transfer_retries_per_file, 0},
        {minimal_sync_request, 1},
        {synchronizer_max_job_restarts, 5},
        {synchronizer_max_job_inactivity_period_sec, 1},
        {synchronizer_jobs_inactivity_check_interval_sec, 1}
    ]),
    ?assertEqual(ok, test_utils:mock_new(Nodes, storage_driver)),
    Config2;
init_per_testcase(transfer_with_missing_documents = Case, Config0) ->
    User = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, User, {4,0,0,2}, 60),

    % Mock to prevent sync of metadata
    Workers2 = ?config(workers2, Config),
    ?assertEqual(ok, test_utils:mock_new(Workers2, dbsync_changes)),
    test_utils:mock_expect(Workers2, dbsync_changes, apply, fun(_Doc) -> ok end),

    % Disable stats to prevent file_location creation before file opening
    Worker1 = ?config(worker1, Config),
    SpaceId = <<"space1">>,
    StatsState = rpc:call(Worker1, dir_stats_service_state, get_status, [SpaceId]),
    case StatsState of
        disabled -> ok;
        _ -> rpc:call(Worker1, dir_stats_service_state, disable, [SpaceId])
    end,

    init_per_testcase(?DEFAULT_CASE(Case), [{stats_state_before_test, StatsState} | Config]);
init_per_testcase(Case, Config) when
    Case =:= remote_driver_get_test orelse Case =:= remote_driver_fold_test
->
    Workers = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, test_utils:mock_new(Workers, links_tree)),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(tmp_files_delete_test = Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, test_utils:mock_new(Workers, dbsync_changes)),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 60}),
    lfm_proxy:init(Config).


end_per_testcase(file_consistency_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, file_meta),
    end_per_testcase(?DEFAULT_CASE(file_consistency_test), Config);
end_per_testcase(rtransfer_fetch_test, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    MinHoleSize = ?config(default_min_hole_size, Config),
    utils:rpc_multicall(Nodes, application, set_env, [
        ?APP_NAME, rtransfer_min_hole_size, MinHoleSize
    ]),
    end_per_testcase(?DEFAULT_CASE(rtransfer_fetch_test), Config);
end_per_testcase(check_fs_stats_on_different_providers, Config) ->
    initializer:disable_quota_limit(Config),
    end_per_testcase(all, Config);
end_per_testcase(remote_driver_internal_call_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [datastore_doc, datastore_remote_driver]),
    end_per_testcase(?DEFAULT_CASE(remote_driver_internal_call_test), Config);
end_per_testcase(Case, Config) when
    Case =:= registered_user_opens_remotely_created_file_test;
    Case =:= registered_user_opens_remotely_created_share_test;
    Case =:= guest_user_opens_remotely_created_file_test;
    Case =:= guest_user_opens_remotely_created_share_test
->
    initializer:unmock_share_logic(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(Case, Config) when
    Case =:= dir_stats_collector_test;
    Case =:= dir_stats_collector_trash_test;
    Case =:= transfer_after_enabling_stats_test;
    Case =:= dir_stats_collector_parallel_write_test;
    Case =:= dir_stats_collector_parallel_override_test;
    Case =:= dir_stats_collector_parallel_write_with_sleep_test;
    Case =:= dir_stats_collector_parallel_write_to_empty_file_test ->
    dir_stats_collector_test_base:teardown(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(Case = detect_stale_replica_synchronizer_jobs_test, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    lists:foreach(fun(EnvVar) ->
        utils:rpc_multicall(Nodes, application, unset_env, [
            ?APP_NAME, EnvVar
        ])
    end, [
        max_transfer_retries_per_file,
        minimal_sync_request,
        synchronizer_max_job_restarts,
        synchronizer_max_job_inactivity_period_sec,
        synchronizer_jobs_inactivity_check_interval_sec
    ]),
    ?assertEqual(ok, test_utils:mock_unload(Nodes, storage_driver)),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(Case, Config) when
    Case =:= recreate_file_on_storage;
    Case =:= recreate_dir_on_storage;
    Case =:= truncate_on_storage_does_not_block_synchronizer
->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Nodes, storage_driver),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(transfer_with_missing_documents = Case, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Nodes, dbsync_changes),

    case ?config(stats_state_before_test, Config) of
        enabled ->
            Worker1 = ?config(worker1, Config),
            rpc:call(Worker1, dir_stats_service_state, enable, [<<"space1">>]);
        _ ->
            ok
    end,

    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(Case, Config) when
    Case =:= remote_driver_get_test orelse Case =:= remote_driver_fold_test
->
    Workers = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, test_utils:mock_unload(Workers, links_tree)),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(tmp_files_delete_test = Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, dbsync_changes),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).