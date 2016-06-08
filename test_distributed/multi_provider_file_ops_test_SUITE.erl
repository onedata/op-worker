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
    synchronization_test_base(Config, <<"user1">>, {2,0,0}, 15, 10, 100).

proxy_test1(Config) ->
    synchronization_test_base(Config, <<"user2">>, {0,2,1}, 0, 10, 100).

proxy_test2(Config) ->
    synchronization_test_base(Config, <<"user3">>, {0,2,1}, 0, 10, 100).

synchronization_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten}, Attempts, DirsNum, FilesNum) ->
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
    ProvIDs = lists:map(fun(Worker) ->
        rpc:call(Worker, oneprovider, get_provider_id, [])
    end, Workers),

    Verify = fun(TestFun) ->
        lists:foldl(fun(W, Acc) ->
            [TestFun(W) | Acc]
        end, [], Workers)
    end,

    Dir = generator:gen_name(),
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
%%            ct:print("xxxx ~p", [{File, IsDir, W}]),
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
            rpc:call(W, file_meta, get, [FileUUID])
        end),

        NotFoundList = lists:filter(fun({error, {not_found, _}}) -> true; (_) -> false end, VerAns),
        OKList = lists:filter(fun({ok, _}) -> true; (_) -> false end, VerAns),

        ?assertEqual(ProxyNodes - ProxyNodesWritten, length(NotFoundList)),
        ?assertEqual(SyncNodes + ProxyNodesWritten, length(OKList))
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
        VerifyStats(File, false),

        Beg = binary:part(FileBeg, 0, Offset2),
        End = binary:part(File, 0, Offset2),
        FileCheck = <<Beg/binary, End/binary>>,

        VerifyLocation = fun() ->
            Verify(fun(W) ->
                StatAns = lfm_proxy:stat(W, SessId(W), {path, File}),
                ?assertMatch({ok, #file_attr{}}, StatAns),
                {ok, #file_attr{uuid = FileGUID}} = StatAns,
                FileUUID = fslogic_uuid:file_guid_to_uuid(FileGUID),

                lists:map(fun(ProvID) ->
                    case rpc:call(W, file_meta, get, [links_utils:links_doc_key(FileUUID, ProvID)]) of
                        {error, {not_found, _}} ->
                            0;
                        GetAns ->
                            ?assertMatch({ok, #document{value = #links{}}}, GetAns),
                            {ok, #document{value = Links}} = GetAns,
                            verify_locations(W, Links)
                    end
                end, ProvIDs)
            end)
        end,

        VerAns0 = VerifyLocation(),
        ct:print("Locations0 ~p", [{Offset, File, VerAns0}]),

        Verify(fun(W) ->
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
        ToMatch = {(SyncNodes+ProxyNodes)*(SyncNodes+ProxyNodes) - SyncNodes*SyncNodes - ProxyNodesWritten,
            SyncNodes*SyncNodes + ProxyNodesWritten},
        ?match(ToMatch, AssertLocations(), Attempts)
    end,
    VerifyFile({2, Level2File}),

    lists:foreach(fun(W) ->
        Level2TmpDir = <<Dir/binary, "/", (generator:gen_name())/binary>>,
        ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId(W), Level2TmpDir, 8#755)),
        VerifyStats(Level2TmpDir, true),

        lists:foreach(fun(W2) ->
            Level3TmpDir = <<Level2TmpDir/binary, "/", (generator:gen_name())/binary>>,
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

            lists:map(fun(ProvID) ->
                case rpc:call(W, file_meta, get, [links_utils:links_doc_key(FileUUID, ProvID)]) of
                    {error, {not_found, _}} ->
                        0;
                    GetAns ->
                        ?assertMatch({ok, #document{value = #links{}}}, GetAns),
                        {ok, #document{value = Links}} = GetAns,
                        count_links(W, Links)
                end
            end, ProvIDs)
        end),
        Flattened = lists:flatten(VerAns),
        ct:print("Links ~p", [{DSize, Deleted, VerAns}]),

        ZerosList = lists:filter(fun(S) -> S == 0 end, Flattened),
        SList = lists:filter(fun(S) -> S == 2*DSize + Deleted + 1 end, Flattened),

        ?assertEqual((SyncNodes+ProxyNodes)*(SyncNodes+ProxyNodes) - SyncNodes - ProxyNodesWritten, length(ZerosList)),
        ?assertEqual(SyncNodes + ProxyNodesWritten, length(SList))
    end,
    VerifyDirSize(Level2Dir, length(Level3Dirs) + length(Level3Dirs2) + 1, 0),

    lists:map(fun(D) ->
        VerifyStats(D, true)
    end, Level3Dirs2),

    lists:map(fun(F) ->
        VerifyFile(F)
    end, Level4Files),
    VerifyDirSize(Level3Dir, length(Level4Files), 0),

    lists:map(fun({_, F}) ->
        ?assertMatch(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), {path, F}))
    end, Level4Files),
    lists:map(fun(D) ->
        ?assertMatch(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), {path, D}))
    end, Level3Dirs2),

    lists:map(fun({_, F}) ->
        Verify(fun(W) -> ?match({error, ?ENOENT}, lfm_proxy:stat(W, SessId(W), {path, F}), Attempts) end)
    end, Level4Files),
    lists:map(fun(D) ->
        Verify(fun(W) -> ?match({error, ?ENOENT}, lfm_proxy:stat(W, SessId(W), {path, D}), Attempts) end)
    end, Level3Dirs2),
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
            OpenAns2 = lfm_proxy:open(W2, SessId(W2), {path, Level2File}, rdwr),
            ?assertMatch({ok, _}, OpenAns2),
            {ok, Handle2} = OpenAns2,
            ?match({ok, NewAcc}, lfm_proxy:read(W2, Handle2, 0, 500), Attempts)
        end),
        NewAcc
    end, <<>>, Workers),

%%    ct:print("aaaaa1"),
%%
%%    Master = self(),
%%    lists:foreach(fun(W) ->
%%        spawn_link(fun() ->
%%            Level2TmpDir = <<Dir/binary, "/", (generator:gen_name())/binary>>,
%%            MkAns = lfm_proxy:mkdir(W, SessId(W), Level2TmpDir, 8#755),
%%            ct:print("bbbb1 ~p", [{W, MkAns}]),
%%            Master ! {mkdir_ans, Level2TmpDir, MkAns}
%%        end)
%%    end, Workers),
%%
%%    ct:print("aaaaa2"),
%%
%%    Level2TmpDirs = lists:foldl(fun(_, Acc) ->
%%        MkAnsCheck =
%%            receive
%%                {mkdir_ans, ReceivedLevel2TmpDir, MkAns} ->
%%                    {ReceivedLevel2TmpDir, MkAns}
%%            after timer:seconds(2*Attempts+2) ->
%%                {error, timeout}
%%            end,
%%        ct:print("bbbb2 ~p", [MkAnsCheck]),
%%        ?assertMatch({_, {ok, _}}, MkAnsCheck),
%%        {Level2TmpDir, _} = MkAnsCheck,
%%        VerifyStats(Level2TmpDir, true),
%%        [Level2TmpDir | Acc]
%%    end, [], Workers),
%%
%%    ct:print("aaaaa3"),
%%
%%    lists:foreach(fun(Level2TmpDir) ->
%%        lists:foreach(fun(W2) ->
%%            spawn_link(fun() ->
%%                Level3TmpDir = <<Level2TmpDir/binary, "/", (generator:gen_name())/binary>>,
%%                MkAns = lfm_proxy:mkdir(W2, SessId(W2), Level3TmpDir, 8#755),
%%                ct:print("bbbb3 ~p", [{W2, Level3TmpDir, MkAns}]),
%%                Master ! {mkdir_ans, Level3TmpDir, MkAns}
%%            end)
%%        end, Workers)
%%    end, Level2TmpDirs),
%%
%%    ct:print("aaaaa4"),
%%
%%    lists:foreach(fun(_) ->
%%        lists:foreach(fun(_) ->
%%            MkAnsCheck =
%%                receive
%%                    {mkdir_ans, ReceivedLevel2TmpDirLevel3TmpDir, MkAns} ->
%%                        {ReceivedLevel2TmpDirLevel3TmpDir, MkAns}
%%                after timer:seconds(2*Attempts+2) ->
%%                    {error, timeout}
%%                end,
%%            ct:print("bbbb4 ~p", [MkAnsCheck]),
%%            ?assertMatch({_, {ok, _}}, MkAnsCheck),
%%            {Level3TmpDir, _} = MkAnsCheck,
%%            VerifyStats(Level3TmpDir, true)
%%        end, Workers)
%%    end, Level2TmpDirs),
%%
%%    ct:print("aaaaa5"),
    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    ct:timetrap({minutes, 30}),
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

count_links(W, #links{link_map = Map, children = Children}) ->
    case maps:size(Children) of
        0 ->
            maps:size(Map);
        _ ->
            maps:fold(fun(_, Uuid, Acc) ->
                GetAns = rpc:call(W, file_meta, get, [Uuid]),
                ?assertMatch({ok, #document{value = #links{}}}, GetAns),
                {ok, #document{value = Links}} = GetAns,
                Acc + count_links(W, Links)
            end, maps:size(Map), Children)
    end.

verify_locations(W, #links{link_map = Map, children = Children}) ->
    case maps:size(Children) of
        0 ->
            check_locations(W, Map);
        _ ->
            maps:fold(fun(_, Uuid, Acc) ->
                GetAns = rpc:call(W, file_meta, get, [Uuid]),
                ?assertMatch({ok, #document{value = #links{}}}, GetAns),
                {ok, #document{value = Links}} = GetAns,
                Acc + verify_locations(W, Links)
            end, check_locations(W, Map), Children)
    end.

check_locations(W, Map) ->
    maps:fold(fun(_, V, Acc) ->
        case V of
            {ID, file_location} ->
                case rpc:call(W, file_location, get, [ID]) of
                    {ok, _} -> Acc + 1;
                    _ -> Acc
                end;
            _ ->
                Acc
        end
    end, 0, Map).