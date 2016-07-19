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
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    db_sync_test/1, proxy_test1/1, proxy_test2/1
]).
-export([synchronization_test_base/6]).

all() ->
    ?ALL([
        proxy_test1, proxy_test2, db_sync_test
    ]).

-define(match(Expect, Expr, Attempts),
    case Attempts of
        0 ->
            ?assertMatch(Expect, Expr);
        _ ->
            ?assertMatch(Expect, Expr, Attempts)
    end
).

%%%===================================================================
%%% Test functions
%%%===================================================================

db_sync_test(Config) ->
    % TODO change timeout after VFS-2197
    synchronization_test_base(Config, <<"user1">>, {4,0,0,2}, 10, 10, 100).
%%synchronization_test_base(Config, <<"user1">>, {4,0,0,2}, 60, 10, 100).

proxy_test1(Config) ->
    synchronization_test_base(Config, <<"user2">>, {0,4,1,2}, 0, 10, 100).

proxy_test2(Config) ->
    synchronization_test_base(Config, <<"user3">>, {0,4,1,2}, 0, 10, 100).

synchronization_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten}, Attempts, DirsNum, FilesNum) ->
    synchronization_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten, 1}, Attempts, DirsNum, FilesNum);

synchronization_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfWriteProvider},
    Attempts, DirsNum, FilesNum) ->
    ProxyNodesWritten = ProxyNodesWritten0 * NodesOfWriteProvider,
    Workers = ?config(op_worker_nodes, Config),
    Worker1 = lists:foldl(fun(W, Acc) ->
        case is_atom(Acc) of
            true ->
                Acc;
            _ ->
                case string:str(atom_to_list(W), "p1") of
                    0 -> Acc;
                    _ -> W
                end
        end
    end, [], Workers),
    timer:sleep(10000), % TODO - connection must appear after mock setup

    SessId = fun(W) -> ?config({session_id, {User, ?GET_DOMAIN(W)}}, Config) end,
    [{_SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),
    ProvIDs = lists:map(fun(Worker) ->
        rpc:call(Worker, oneprovider, get_provider_id, [])
    end, Workers),

    Verify = fun(TestFun) ->
        lists:foldl(fun(W, Acc) ->
            [TestFun(W) | Acc]
        end, [], Workers)
    end,

    Dir = <<SpaceName/binary, "/",  (generator:gen_name())/binary>>,
    Level2Dir = <<Dir/binary, "/", (generator:gen_name())/binary>>,
    Level2File = <<Dir/binary, "/", (generator:gen_name())/binary>>,

    Level3Dirs = lists:map(fun(_) ->
        <<Level2Dir/binary, "/", (generator:gen_name())/binary>>
    end, lists:seq(1,DirsNum)),
    Level3Dirs2 = lists:map(fun(_) ->
        <<Level2Dir/binary, "/", (generator:gen_name())/binary>>
    end, lists:seq(1,DirsNum)),

    Level3Dir = <<Level2Dir/binary, "/", (generator:gen_name())/binary>>,
    Level4Files = lists:map(fun(Num) ->
        {Num, <<Level3Dir/binary, "/", (generator:gen_name())/binary>>}
    end, lists:seq(1,FilesNum)),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir, 8#755)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Level2Dir, 8#755)),

    VerifyStats = fun(File, IsDir) ->
        VerAns = Verify(fun(W) ->
            ct:print("VerifyStats ~p", [{File, IsDir, W}]),
            case IsDir of
                true ->
                    ?match({ok, #file_attr{type = ?DIRECTORY_TYPE}},
                        lfm_proxy:stat(W, SessId(W), {path, File}), Attempts);
                _ ->
                    ?match({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
                        lfm_proxy:stat(W, SessId(W), {path, File}), Attempts)
            end,
            StatAns = lfm_proxy:stat(W, SessId(W), {path, File}),
            {ok, #file_attr{uuid = FileGUID}} = StatAns,
            FileUUID = fslogic_uuid:file_guid_to_uuid(FileGUID),
            {FileUUID, rpc:call(W, file_meta, get, [FileUUID])}
        end),

        NotFoundList = lists:filter(fun({_, {error, {not_found, _}}}) -> true; (_) -> false end, VerAns),
        OKList = lists:filter(fun({_, {ok, _}}) -> true; (_) -> false end, VerAns),

        ?assertEqual(ProxyNodes - ProxyNodesWritten, length(NotFoundList)),
        ?assertEqual(SyncNodes + ProxyNodesWritten, length(OKList)),
        [{FileUUIDAns, _} | _] = VerAns,
        FileUUIDAns
    end,
    VerifyStats(Dir, true),
    VerifyStats(Level2Dir, true),

    FileBeg = <<"1234567890abcd">>,
    CreateFileOnWorker = fun(Offset, File, WriteWorker) ->
        ?assertMatch({ok, _}, lfm_proxy:create(WriteWorker, SessId(WriteWorker), File, 8#755)),
        OpenAns = lfm_proxy:open(WriteWorker, SessId(WriteWorker), {path, File}, rdwr),
        ?assertMatch({ok, _}, OpenAns),
        {ok, Handle} = OpenAns,
        FileBegSize = size(FileBeg),
        ?assertMatch({ok, FileBegSize}, lfm_proxy:write(WriteWorker, Handle, 0, FileBeg)),
        Size = size(File),
        Offset2 = Offset rem 5 + 1,
        ?assertMatch({ok, Size}, lfm_proxy:write(WriteWorker, Handle, Offset2, File)),
        ?assertMatch(ok, lfm_proxy:truncate(WriteWorker, SessId(WriteWorker), {path, File}, 2*Offset2))
    end,
    CreateFile = fun({Offset, File}) ->
        CreateFileOnWorker(Offset, File, Worker1)
    end,
    CreateFile({2, Level2File}),

    VerifyFile = fun({Offset, File}) ->
        Offset2 = Offset rem 5 + 1,
        Size = 2*Offset2,
        FileUUID = VerifyStats(File, false),

        Beg = binary:part(FileBeg, 0, Offset2),
        End = binary:part(File, 0, Offset2),
        FileCheck = <<Beg/binary, End/binary>>,

        VerifyLocation = fun() ->
            Verify(fun(W) ->
                lists:map(fun(ProvID) ->
                    verify_locations(W, FileUUID, ProvID)
                end, ProvIDs)
            end)
        end,

        VerAns0 = VerifyLocation(),
        ct:print("Locations0 ~p", [{Offset, File, VerAns0}]),

        Verify(fun(W) ->
            ct:print("Verify file ~p", [{File, W}]),
            OpenAns = lfm_proxy:open(W, SessId(W), {path, File}, rdwr),
            ?assertMatch({ok, _}, OpenAns),
            {ok, Handle} = OpenAns,
            ?match({ok, FileCheck}, lfm_proxy:read(W, Handle, 0, Size), Attempts)
        end),

        AssertLocations = fun() ->
            VerAns = VerifyLocation(),
            Flattened = lists:flatten(VerAns),
            ct:print("Locations1 ~p", [{Offset, File, VerAns}]),

            ZerosList = lists:filter(fun(S) -> S == 0 end, Flattened),
            LocationsList = lists:filter(fun(S) -> S == 1 end, Flattened),
            {length(ZerosList), length(LocationsList)}
        end,
        ToMatch = {(SyncNodes+ProxyNodes)*(SyncNodes+ProxyNodes) - SyncNodes*SyncNodes
            - ProxyNodesWritten*NodesOfWriteProvider,
            SyncNodes*SyncNodes + ProxyNodesWritten*NodesOfWriteProvider},
        ?match(ToMatch, AssertLocations(), Attempts),

        LocToAns = Verify(fun(W) ->
            StatAns = lfm_proxy:stat(W, SessId(W), {path, File}),
            ?assertMatch({ok, #file_attr{}}, StatAns),
            {ok, #file_attr{uuid = FileGUID}} = StatAns,
            FileUUID = fslogic_uuid:file_guid_to_uuid(FileGUID),

            {W, lists:flatten(lists:foldl(fun(ProvID, Acc) ->
                [get_locations(W, FileUUID, ProvID) | Acc]
            end, [], ProvIDs))}
        end),
        {File, FileUUID, LocToAns}
    end,

    VerifyDel = fun({F,  FileUUID, Locations}) ->
        Verify(fun(W) ->
            ct:print("Del ~p", [{W, F,  FileUUID, Locations}]),
            ?match({error, ?ENOENT}, lfm_proxy:stat(W, SessId(W), {path, F}), Attempts)
            %,
%%            ?match({error, {not_found, _}}, rpc:call(W, file_meta, get, [FileUUID]), Attempts),
%%            lists:foreach(fun(ProvID) ->
%%                ?match(#{},
%%                    get_links(W, FileUUID, ProvID), Attempts)
%%            end, ProvIDs),
%%            lists:foreach(fun(Location) ->
%%                ?match({error, {not_found, _}},
%%                    rpc:call(W, file_meta, get, [Location]), Attempts)
%%            end, proplists:get_value(W, Locations, []))
        end)
    end,

    VerifyFile({2, Level2File}),

    lists:foreach(fun(W) ->
        Level2TmpDir = <<Dir/binary, "/", (generator:gen_name())/binary>>,
        ct:print("Verify dir ~p", [{Level2TmpDir, W}]),
        ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId(W), Level2TmpDir, 8#755)),
        VerifyStats(Level2TmpDir, true),

        lists:foreach(fun(W2) ->
            Level3TmpDir = <<Level2TmpDir/binary, "/", (generator:gen_name())/binary>>,
            ct:print("Verify dir2 ~p", [{Level3TmpDir, W}]),
            ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId(W2), Level3TmpDir, 8#755)),
            VerifyStats(Level3TmpDir, true)
        end, Workers)
    end, Workers),

    lists:map(fun(D) ->
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), D, 8#755))
    end, Level3Dirs),

    lists:map(fun(D) ->
        VerifyStats(D, true)
    end, Level3Dirs),

    lists:map(fun(D) ->
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), D, 8#755))
    end, Level3Dirs2),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Level3Dir, 8#755)),
    lists:map(fun(F) ->
        CreateFile(F)
    end, Level4Files),

    VerifyDirSize = fun(DirToCheck, DSize, Deleted) ->
        VerAns = Verify(fun(W) ->
            CountChilden = fun() ->
                LSAns = lfm_proxy:ls(W, SessId(W), {path, DirToCheck}, 0, 200),
                ?assertMatch({ok, _}, LSAns),
                {ok, ListerDirs} = LSAns,
                length(ListerDirs)
            end,
            ?match(DSize, CountChilden(), Attempts),

            StatAns = lfm_proxy:stat(W, SessId(W), {path, DirToCheck}),
            ?assertMatch({ok, #file_attr{}}, StatAns),
            {ok, #file_attr{uuid = FileGUID}} = StatAns,
            FileUUID = fslogic_uuid:file_guid_to_uuid(FileGUID),

            TmpAns = lists:map(fun(ProvID) ->
                count_links(W, FileUUID, ProvID)
            end, ProvIDs),
            TmpAns
        end),
        Flattened = lists:flatten(VerAns),
        ct:print("Links ~p", [{DSize, Deleted, VerAns}]),

        ZerosList = lists:filter(fun(S) -> S == 0 end, Flattened),
        SList = lists:filter(fun(S) -> S == 2*DSize + Deleted + 1 end, Flattened),

        ?assertEqual((SyncNodes+ProxyNodes)*(SyncNodes+ProxyNodes) - SyncNodes*NodesOfWriteProvider
            - ProxyNodesWritten*NodesOfWriteProvider, length(ZerosList)),
        ?assertEqual(SyncNodes * NodesOfWriteProvider + ProxyNodesWritten*NodesOfWriteProvider, length(SList))
    end,
    VerifyDirSize(Level2Dir, length(Level3Dirs) + length(Level3Dirs2) + 1, 0),

    Level3Dirs2Uuids = lists:map(fun(D) ->
        VerifyStats(D, true)
    end, Level3Dirs2),

    Level4FilesVerified = lists:map(fun(F) ->
        VerifyFile(F)
    end, Level4Files),
    VerifyDirSize(Level3Dir, length(Level4Files), 0),

    lists:map(fun({_, F}) ->
        ?assertMatch(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), {path, F}))
    end, Level4Files),
    lists:map(fun(D) ->
        ?assertMatch(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), {path, D}))
    end, Level3Dirs2),

    lists:map(fun(F) ->
        VerifyDel(F)
    end, Level4FilesVerified),
    lists:map(fun({D, Uuid}) ->
        VerifyDel({D, Uuid, []})
    end, lists:zip(Level3Dirs2, Level3Dirs2Uuids)),
    VerifyDirSize(Level3Dir, 0, length(Level4Files)),
    VerifyDirSize(Level2Dir, length(Level3Dirs) + 1, length(Level3Dirs2)),

    lists:foreach(fun(W) ->
        Level2TmpFile = <<Dir/binary, "/", (generator:gen_name())/binary>>,
        CreateFileOnWorker(4, Level2TmpFile, W),
        VerifyFile({4, Level2TmpFile})
    end, Workers),

    lists:foldl(fun(W, Acc) ->
        OpenAns = lfm_proxy:open(W, SessId(W), {path, Level2File}, rdwr),
        ?assertMatch({ok, _}, OpenAns),
        {ok, Handle} = OpenAns,
        WriteBuf = atom_to_binary(W, utf8),
        Offset = size(Acc),
        WriteSize = size(WriteBuf),
        ?assertMatch({ok, WriteSize}, lfm_proxy:write(W, Handle, Offset, WriteBuf)),
        NewAcc = <<Acc/binary, WriteBuf/binary>>,

        Verify(fun(W2) ->
            ct:print("Verify write ~p", [{Level2File, W2}]),
            OpenAns2 = lfm_proxy:open(W2, SessId(W2), {path, Level2File}, rdwr),
            ?assertMatch({ok, _}, OpenAns2),
            {ok, Handle2} = OpenAns2,
            ?match({ok, NewAcc}, lfm_proxy:read(W2, Handle2, 0, 500), Attempts)
        end),
        NewAcc
    end, <<>>, Workers),

    Master = self(),
    lists:foreach(fun(W) ->
        spawn_link(fun() ->
            Level2TmpDir = <<Dir/binary, "/", (generator:gen_name())/binary>>,
            MkAns = lfm_proxy:mkdir(W, SessId(W), Level2TmpDir, 8#755),
            Master ! {mkdir_ans, Level2TmpDir, MkAns}
        end)
    end, Workers),

    Level2TmpDirs = lists:foldl(fun(_, Acc) ->
        MkAnsCheck =
            receive
                {mkdir_ans, ReceivedLevel2TmpDir, MkAns} ->
                    {ReceivedLevel2TmpDir, MkAns}
            after timer:seconds(2*Attempts+2) ->
                {error, timeout}
            end,
        ?assertMatch({_, {ok, _}}, MkAnsCheck),
        {Level2TmpDir, _} = MkAnsCheck,
        ct:print("Verify spawn1 ~p", [{Level2TmpDir}]),
        VerifyStats(Level2TmpDir, true),
        [Level2TmpDir | Acc]
    end, [], Workers),

    lists:foreach(fun(Level2TmpDir) ->
        lists:foreach(fun(W2) ->
            spawn_link(fun() ->
                Level3TmpDir = <<Level2TmpDir/binary, "/", (generator:gen_name())/binary>>,
                MkAns = lfm_proxy:mkdir(W2, SessId(W2), Level3TmpDir, 8#755),
                Master ! {mkdir_ans, Level3TmpDir, MkAns}
            end)
        end, Workers)
    end, Level2TmpDirs),

    lists:foreach(fun(_) ->
        lists:foreach(fun(_) ->
            MkAnsCheck =
                receive
                    {mkdir_ans, ReceivedLevel2TmpDirLevel3TmpDir, MkAns} ->
                        {ReceivedLevel2TmpDirLevel3TmpDir, MkAns}
                after timer:seconds(2*Attempts+2) ->
                    {error, timeout}
                end,
            ?assertMatch({_, {ok, _}}, MkAnsCheck),
            {Level3TmpDir, _} = MkAnsCheck,
            ct:print("Verify spawn2 ~p", [{Level3TmpDir}]),
            VerifyStats(Level3TmpDir, true)
        end, Workers)
    end, Level2TmpDirs),

    Verify(fun(W) ->
        ?assertEqual(ok, lfm_proxy:close_all(W))
    end),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    ct:timetrap({minutes, 60}),
    application:start(ssl2),
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
    application:stop(ssl2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_links(W, FileUUID, ProvID) ->
    LinkDocKey = links_utils:links_doc_key(FileUUID, ProvID),
    TmpAns = get_links(W, FileUUID, LinkDocKey, couchdb_datastore_driver, #{}),
    get_links(W, FileUUID, LinkDocKey, mnesia_cache_driver, TmpAns).

get_links(W, FileUUID, LinkDocKey, Driver, Ans) ->
    ModelConfig = file_meta:model_init(),
    case rpc:call(W, Driver, get_link_doc, [ModelConfig, LinkDocKey]) of
        {error, {not_found, _}} ->
            Ans;
        GetAns ->
            ?assertMatch({ok, #document{value = #links{}}}, GetAns),
            {ok, #document{value = Links}} = GetAns,
            TmpAns = maps:fold(fun(K, V, Acc) ->
                case Driver of
                    mnesia_cache_driver ->
                        maps:put(K, V, Acc);
                    _ ->
                        case rpc:call(W, cache_controller, check_fetch, [{FileUUID, K}, file_meta, ?GLOBAL_ONLY_LEVEL]) of
                            ok ->
                                maps:put(K, V, Acc);
                            _ ->
                                Acc
                        end
                end
            end, Ans, Links#links.link_map),

            maps:fold(fun(_, DocKey, Acc) ->
                case DocKey of
                    <<"non">> -> Acc;
                    _ -> get_links(W, FileUUID, DocKey, Driver, Acc)
                end
            end, TmpAns, Links#links.children)
    end.

count_links(W, FileUUID, ProvID) ->
    Links = get_links(W, FileUUID, ProvID),
    maps:size(Links).

verify_locations(W, FileUUID, ProvID) ->
    IDs = get_locations(W, FileUUID, ProvID),
    lists:foldl(fun(ID, Acc) ->
        case rpc:call(W, file_location, get, [ID]) of
            {ok, _} -> Acc + 1;
            _ -> Acc
        end
    end, 0, IDs).

get_locations(W, FileUUID, ProvID) ->
    Links = get_links(W, FileUUID, ProvID),
    get_locations_from_map(Links).

get_locations_from_map(Map) ->
    maps:fold(fun(_, V, Acc) ->
        case V of
            {ID, file_location} ->
                [ID | Acc];
            _ ->
                Acc
        end
    end, [], Map).