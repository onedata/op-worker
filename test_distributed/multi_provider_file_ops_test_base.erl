%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Module with base test functions for multi provider file
%%% operation tests
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_file_ops_test_base).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").


%% API
%% export for tests
-export([synchronization_test_base/6, file_consistency_test_base/4]).

-export([get_links/1]).

% for file consistency testing
-export([create_doc/4, set_parent_link/4, set_link_to_parent/4, create_location/4, set_link_to_location/4]).


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

synchronization_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten}, Attempts, DirsNum, FilesNum) ->
    synchronization_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten, 1}, Attempts, DirsNum, FilesNum);

synchronization_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider},
    Attempts, DirsNum, FilesNum) ->

%%    ct:print("Test ~p", [{User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts, DirsNum, FilesNum}]),

    SyncProvidersCount = max(1, round(SyncNodes / NodesOfProvider)),
    ProxyNodesWritten = ProxyNodesWritten0 * NodesOfProvider,
    Workers = ?config(op_worker_nodes, Config),

%%    ct:print("Workers: ~p", [Workers]),
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
%%            ct:print("VerifyStats ~p", [{File, IsDir, W}]),

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
%%        ct:print("CreateFileOnWorker ~p", [{Offset, File, WriteWorker}]),
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
                verify_locations(W, FileUUID)
            end)
        end,

%%        VerAns0 = VerifyLocation(),

        Verify(fun(W) ->
%%            ct:print("Verify file ~p", [{File, W}]),
            OpenAns = lfm_proxy:open(W, SessId(W), {path, File}, rdwr),
            ?assertMatch({ok, _}, OpenAns),
            {ok, Handle} = OpenAns,
            ?match({ok, FileCheck}, lfm_proxy:read(W, Handle, 0, Size), Attempts)
        end),

        ToMatch = {ProxyNodes - ProxyNodesWritten, SyncNodes + ProxyNodesWritten},
        AssertLocations = fun() ->
            VerAns = VerifyLocation(),
            Flattened = lists:flatten(VerAns),

            ZerosList = lists:filter(fun(S) -> S == 0 end, Flattened),
            LocationsList = lists:filter(fun(S) -> S == SyncProvidersCount end, Flattened),
            {length(ZerosList), length(LocationsList)}
        end,
        ?match(ToMatch, AssertLocations(), Attempts),


        LocToAns = Verify(fun(W) ->
            StatAns = lfm_proxy:stat(W, SessId(W), {path, File}),
            ?assertMatch({ok, #file_attr{}}, StatAns),
            {ok, #file_attr{uuid = FileGUID}} = StatAns,
            FileUUID = fslogic_uuid:file_guid_to_uuid(FileGUID),

            {W, get_locations(W, FileUUID)}
        end),
        {File, FileUUID, LocToAns}
    end,

    VerifyDel = fun({F,  FileUUID, Locations}) ->
        Verify(fun(W) ->
%%            ?match({error, ?ENOENT}, lfm_proxy:stat(W, SessId(W), {path, F}), Attempts)
            % TODO - match to chosen error (check perms may also result in ENOENT)
            ?match({error, _}, lfm_proxy:stat(W, SessId(W), {path, F}), Attempts)
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
        VerAns0 = Verify(fun(W) ->
            CountChilden = fun() ->
                LSAns = lfm_proxy:ls(W, SessId(W), {path, DirToCheck}, 0, 200),
                ?assertMatch({ok, _}, LSAns),
                {ok, ListedDirs} = LSAns,
                length(ListedDirs)
            end,
            CS = CountChilden(),
            ?match(DSize, CountChilden(), Attempts),

            StatAns = lfm_proxy:stat(W, SessId(W), {path, DirToCheck}),
            ?assertMatch({ok, #file_attr{}}, StatAns),
            {ok, #file_attr{uuid = FileGUID}} = StatAns,
            FileUUID = fslogic_uuid:file_guid_to_uuid(FileGUID),
            {W, FileUUID}
        end),

        AssertLinks = fun() ->
            VerAns = lists:map(fun({W, Uuid}) ->
                count_links(W, Uuid)
            end, VerAns0),

            ZerosList = lists:filter(fun(S) -> S == 0 end, VerAns),
            SList = lists:filter(fun(S) -> S == 2*DSize + 1 end, VerAns),
            {length(ZerosList), length(SList)}
        end,
        ToMatch = {ProxyNodes - ProxyNodesWritten, SyncNodes + ProxyNodesWritten},
        ?match(ToMatch, AssertLinks(), Attempts)
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
        WD = lists:nth(crypto:rand_uniform(1,length(Workers) + 1), Workers),
        ?assertMatch(ok, lfm_proxy:unlink(WD, SessId(WD), {path, F}))
    end, Level4Files),
    lists:map(fun(D) ->
        WD = lists:nth(crypto:rand_uniform(1,length(Workers) + 1), Workers),
        ?assertMatch(ok, lfm_proxy:unlink(WD, SessId(WD), {path, D}))
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
%%            ct:print("Verify write ~p", [{Level2File, W2}]),
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
%%        ct:print("Verify spawn1 ~p", [{Level2TmpDir}]),
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
%%            ct:print("Verify spawn2 ~p", [{Level3TmpDir}]),
            VerifyStats(Level3TmpDir, true)
        end, Workers)
    end, Level2TmpDirs),
    Verify(fun(W) ->
        ?assertEqual(ok, lfm_proxy:close_all(W))
    end),

    ok.

file_consistency_test_base(Config, Worker1, Worker2, Worker3) ->
    timer:sleep(10000), % TODO - connection must appear after mock setup
    Attempts = 15,
    User = <<"user1">>,

    SessId = fun(W) -> ?config({session_id, {User, ?GET_DOMAIN(W)}}, Config) end,
    [{_SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),
    A1 = ?rpc(Worker1, file_meta, get, [{path, <<"/", SpaceName/binary>>}]),
    ?assertMatch({ok, _}, A1),
    {ok, SpaceDoc} = A1,
    SpaceKey = SpaceDoc#document.key,
%%    ct:print("Space key ~p", [SpaceKey]),

    DoTest = fun(TaskList) ->
        ct:print("Do test ~p", [TaskList]),

        GenerateDoc = fun(Type) ->
            Name = generator:gen_name(),
            Uuid = datastore_utils:gen_uuid(),
            Doc = #document{key = Uuid, value =
            #file_meta{
                name = Name,
                type = Type,
                mode = 8#775,
                uid = User,
                scope = SpaceKey
            }
            },
            ct:print("Doc ~p ~p", [Uuid, Name]),
            {Doc, Name}
        end,

        {Doc1, Name1} = GenerateDoc(?DIRECTORY_TYPE),
        {Doc2, Name2} = GenerateDoc(?DIRECTORY_TYPE),
        {Doc3, Name3} = GenerateDoc(?REGULAR_FILE_TYPE),
        Loc3ID = datastore_utils:gen_uuid(),
        {Doc4, Name4} = GenerateDoc(?REGULAR_FILE_TYPE),
        Loc4ID = datastore_utils:gen_uuid(),

        D1Path = <<SpaceName/binary, "/",  Name1/binary>>,
        D2Path = <<D1Path/binary, "/",  Name2/binary>>,
        D3Path = <<D2Path/binary, "/",  Name3/binary>>,
        D4Path = <<D2Path/binary, "/",  Name4/binary>>,

        Doc1Args = [Doc1, SpaceDoc, non, D1Path],
        Doc2Args = [Doc2, Doc1, non, D2Path],
        Doc3Args = [Doc3, Doc2, Loc3ID, D3Path],
        Doc4Args = [Doc4, Doc2, Loc4ID, D4Path],

        DocsList = [{1, Doc1Args}, {2, Doc2Args}, {3, Doc3Args}, {4, Doc4Args}],
        DocsKeys = [Doc1#document.key, Doc2#document.key, Doc3#document.key, Doc4#document.key],

        % to allow adding location and link before document
        test_utils:mock_expect([Worker1], file_meta, get_scope,
            fun(Arg) ->
                case lists:member(Arg, DocsKeys) of
                    true ->
                        {ok, SpaceDoc};
                    _ ->
                        erlang:apply(meck_util:original_name(file_meta), get_scope, [Arg])
                end
            end),

        lists:foreach(fun(
            {sleep, Sek}) ->
            timer:sleep(timer:seconds(Sek));
            ({Fun, DocNum}) ->
                ?assertEqual(ok, ?rpcTest(Worker1, Fun, proplists:get_value(DocNum, DocsList)));
            ({Fun, DocNum, W}) ->
                ?assertEqual(ok, ?rpcTest(W, Fun, proplists:get_value(DocNum, DocsList)))
        end, TaskList),

        ?match({ok, #file_attr{}},
            lfm_proxy:stat(Worker2, SessId(Worker2), {path, D3Path}), Attempts),
        OpenAns = lfm_proxy:open(Worker2, SessId(Worker2), {path, D3Path}, rdwr),
        ?assertMatch({ok, _}, OpenAns),
        {ok, Handle} = OpenAns,
        ?match({ok, <<"abc">>}, lfm_proxy:read(Worker2, Handle, 0, 10), Attempts),
        ?assertMatch(ok, lfm_proxy:close(Worker2, Handle))
    end,

    {ok, CacheDelay} = test_utils:get_env(Worker1, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms),
    SleepTime = round(CacheDelay / 1000 + 5),

    T1 = [
        {create_doc, 1},
        {sleep, SleepTime},
        {set_parent_link, 1},
        {sleep, SleepTime},
        {set_link_to_parent, 1},
        {sleep, SleepTime},
        {create_doc, 2},
        {sleep, SleepTime},
        {set_parent_link, 2},
        {sleep, SleepTime},
        {set_link_to_parent, 2},
        {sleep, SleepTime},
        {create_doc, 3},
        {sleep, SleepTime},
        {set_parent_link, 3},
        {sleep, SleepTime},
        {set_link_to_parent, 3},
        {sleep, SleepTime},
        {create_location, 3},
        {sleep, SleepTime},
        {set_link_to_location, 3}
    ],
    DoTest(T1),

    T2 = [
        {create_doc, 1},
        {set_parent_link, 1},
        {set_link_to_parent, 1},
        {create_doc, 2},
        {set_parent_link, 2},
        {set_link_to_parent, 2},
        {create_doc, 3},
        {set_parent_link, 3},
        {set_link_to_parent, 3},
        {create_location, 3},
        {set_link_to_location, 3}
    ],
    DoTest(T2),

    T3 = [
        {create_doc, 1},
        {set_parent_link, 1},
        {set_link_to_parent, 1},
        {create_doc, 3},
        {set_parent_link, 3},
        {set_link_to_parent, 3},
        {create_location, 3},
        {set_link_to_location, 3},
        {sleep, SleepTime},
        {create_doc, 2},
        {set_parent_link, 2},
        {set_link_to_parent, 2}
    ],
    DoTest(T3),

    T4 = [
        {create_doc, 3},
        {set_parent_link, 3},
        {set_link_to_parent, 3},
        {create_location, 3},
        {set_link_to_location, 3},
        {sleep, SleepTime},
        {create_doc, 2},
        {set_parent_link, 2},
        {set_link_to_parent, 2},
        {sleep, SleepTime},
        {create_doc, 1},
        {set_parent_link, 1},
        {set_link_to_parent, 1}
    ],
    DoTest(T4),

    T5 = [
        {create_doc, 1},
        {set_parent_link, 1},
        {set_link_to_parent, 1},
        {create_doc, 2},
        {set_parent_link, 2},
        {set_link_to_parent, 2},
        {set_parent_link, 3},
        {set_link_to_parent, 3},
        {sleep, SleepTime},
        {create_doc, 3},
        {sleep, SleepTime},
        {create_location, 3},
        {set_link_to_location, 3}
    ],
    DoTest(T5),

    T6 = [
        {create_doc, 1},
        {set_parent_link, 1},
        {set_link_to_parent, 1},
        {create_doc, 2},
        {set_parent_link, 2},
        {set_link_to_parent, 2},
        {set_parent_link, 3},
        {set_link_to_parent, 3},
        {create_location, 3},
        {sleep, SleepTime},
        {create_doc, 3},
        {sleep, SleepTime},
        {set_link_to_location, 3}
    ],
    DoTest(T6),

    T7 = [
        {create_doc, 1},
        {set_parent_link, 1},
        {set_link_to_parent, 1},
        {create_doc, 2},
        {set_parent_link, 2},
        {set_link_to_parent, 2},
        {set_parent_link, 3},
        {set_link_to_parent, 3},
        {set_link_to_location, 3},
        {sleep, SleepTime},
        {create_doc, 3},
        {sleep, SleepTime},
        {create_location, 3}
    ],
    DoTest(T7),

    T8 = [
        {create_doc, 1},
        {set_parent_link, 1},
        {set_link_to_parent, 1},
        {create_doc, 2},
        {set_parent_link, 2},
        {set_link_to_parent, 2},
        {set_parent_link, 3},
        {set_link_to_parent, 3},
        {create_location, 3},
        {set_link_to_location, 3},
        {sleep, SleepTime},
        {create_doc, 3}
    ],
    DoTest(T8),

    T9 = [
        {create_doc, 1},
        {set_parent_link, 1},
        {set_link_to_parent, 1},
        {create_doc, 2},
        {set_parent_link, 2},
        {set_link_to_parent, 2},
        {create_doc, 3},
        {set_link_to_parent, 3},
        {create_location, 3},
        {set_link_to_location, 3},
        {sleep, SleepTime},
        {set_parent_link, 3}
    ],
    DoTest(T9),

    T10 = [
        {create_doc, 1},
        {set_parent_link, 1},
        {set_link_to_parent, 1},
        {create_doc, 2},
        {set_parent_link, 2},
        {set_link_to_parent, 2},
        {create_doc, 3},
        {set_parent_link, 3},
        {create_location, 3},
        {set_link_to_location, 3},
        {sleep, SleepTime},
        {set_link_to_parent, 3}
    ],
    DoTest(T10),

    T11 = [
        {create_doc, 1},
        {set_parent_link, 1},
        {set_link_to_parent, 1},
        {create_doc, 2},
        {set_parent_link, 2},
        {set_link_to_parent, 2},
        {create_doc, 3},
        {set_parent_link, 3},
        {set_link_to_parent, 3},
        {set_link_to_location, 3},
        {sleep, SleepTime},
        {create_location, 3}
    ],
    DoTest(T11),

    T12 = [
        {create_doc, 1},
        {set_parent_link, 1},
        {set_link_to_parent, 1},
        {create_doc, 2},
        {set_parent_link, 2},
        {set_link_to_parent, 2},
        {set_link_to_location, 3},
        {sleep, SleepTime},
        {create_location, 3},
        {sleep, SleepTime},
        {set_link_to_parent, 3},
        {sleep, SleepTime},
        {set_parent_link, 3},
        {sleep, SleepTime},
        {create_doc, 3}
    ],
    DoTest(T12),

    T13 = [
        {set_link_to_location, 3},
        {sleep, SleepTime},
        {create_location, 3},
        {sleep, SleepTime},
        {set_link_to_parent, 3},
        {sleep, SleepTime},
        {set_parent_link, 3},
        {sleep, SleepTime},
        {create_doc, 3},
        {sleep, SleepTime},
        {set_parent_link, 2},
        {sleep, SleepTime},
        {set_link_to_parent, 2},
        {sleep, SleepTime},
        {create_doc, 2},
        {sleep, SleepTime},
        {set_parent_link, 1},
        {sleep, SleepTime},
        {set_link_to_parent, 1},
        {sleep, SleepTime},
        {create_doc, 1}
    ],
    DoTest(T13),

    T14 = [
        {create_doc, 1},
        {set_parent_link, 1},
        {set_link_to_parent, 1},
        {create_doc, 2},
        {set_parent_link, 2},
        {set_link_to_parent, 2},
        {create_doc, 3},
        {sleep, SleepTime},
        {create_doc, 4, Worker3},
        {set_parent_link, 4, Worker3},
        {set_link_to_parent, 4, Worker3},
        {create_location, 4, Worker3},
        {set_link_to_location, 4, Worker3},
        {sleep, SleepTime},
        {set_parent_link, 3},
        {set_link_to_parent, 3},
        {create_location, 3},
        {set_link_to_location, 3}
    ],
    DoTest(T14),

    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_links(W, FileUUID) ->
    rpc:call(W, ?MODULE, get_links, [FileUUID]).

get_links(FileUUID) ->
    try
        AccFun = fun(LN, LV, Acc) ->
            maps:put(LN, LV, Acc)
        end,
        {ok, Scope1} = file_meta:get_scope({uuid, FileUUID}),
        file_meta:set_link_context(Scope1),
        {ok, Links} = datastore:foreach_link(?GLOBALLY_CACHED_LEVEL, FileUUID, file_meta, AccFun, #{}),
        Links
    catch
        _:_ ->
            #{}
    end.

count_links(W, FileUUID) ->
    Links = get_links(W, FileUUID),
    maps:size(Links).

verify_locations(W, FileUUID) ->
    IDs = get_locations(W, FileUUID),
    lists:foldl(fun(ID, Acc) ->
        case rpc:call(W, file_location, get, [ID]) of
            {ok, _} ->
                Acc + 1;
            DueTo ->
                Acc
        end
    end, 0, IDs).

get_locations(W, FileUUID) ->
    Links = get_links(W, FileUUID),
    get_locations_from_map(Links).

get_locations_from_map(Map) ->
    maps:fold(fun(_, V, Acc) ->
        case V of
            {_Version, [{_, _, ID, file_location}]} ->
                [ID | Acc];
            _D ->
                Acc
        end
    end, [], Map).

create_doc(Doc, _ParentDoc, _LocId, _Path) ->
    {ok, _} = file_meta:save(Doc),
    ok.

set_parent_link(Doc, ParentDoc, _LocId, _Path) ->
    FDoc = Doc#document.value,
    file_meta:set_link_context(ParentDoc),
    MC = file_meta:model_init(),
    LSL = MC#model_config.link_store_level,
    ok = datastore:add_links(LSL, ParentDoc, {FDoc#file_meta.name, Doc}).

set_link_to_parent(Doc, ParentDoc, _LocId, _Path) ->
    file_meta:set_link_context(ParentDoc),
    MC = file_meta:model_init(),
    LSL = MC#model_config.link_store_level,
    ok = datastore:add_links(LSL, Doc, {parent, ParentDoc}).

create_location(Doc, _ParentDoc, LocId, Path) ->
    FDoc = Doc#document.value,
    FileUuid = Doc#document.key,
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(FDoc#file_meta.scope),

    {ok, #document{key = StorageId}} = fslogic_storage:select_storage(SpaceId),
    FileId = fslogic_utils:gen_storage_file_id(FileUuid, Path, FDoc#file_meta.version),
    Location = #file_location{blocks = [#file_block{offset = 0, size = 3, file_id = FileId, storage_id = StorageId}],
        provider_id = oneprovider:get_provider_id(), file_id = FileId, storage_id = StorageId, uuid = FileUuid,
        space_id = SpaceId},

    MC = file_location:model_init(),
    LSL = MC#model_config.link_store_level,
    {ok, _} = datastore:save(LSL, #document{key = LocId, value = Location}),

    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    LeafLess = fslogic_path:dirname(FileId),
    {ok, #document{key = StorageId} = Storage} = fslogic_storage:select_storage(SpaceId),
    SFMHandle0 = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceDirUuid, FileUuid, Storage, LeafLess),
    case storage_file_manager:mkdir(SFMHandle0, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
        ok -> ok;
        {error, eexist} ->
            ok
    end,


    SFMHandle1 = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceDirUuid, FileUuid, Storage, FileId),
    storage_file_manager:unlink(SFMHandle1),
    ok = storage_file_manager:create(SFMHandle1, 8#775),
    {ok, SFMHandle2} = storage_file_manager:open(SFMHandle1, write),
    {ok, 3} = storage_file_manager:write(SFMHandle2, 0, <<"abc">>),
    storage_file_manager:fsync(SFMHandle2),
    ok.

set_link_to_location(Doc, ParentDoc, LocId, _Path) ->
    FileUuid = Doc#document.key,
    file_meta:set_link_context(ParentDoc),
    MC = file_meta:model_init(),
    LSL = MC#model_config.link_store_level,
    ok = datastore:add_links(LSL, Doc, {file_meta:location_ref(oneprovider:get_provider_id()), {LocId, file_location}}),
    ok = datastore:add_links(LSL, LocId, file_location, {file_meta, {FileUuid, file_meta}}).