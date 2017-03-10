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
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
%% export for tests
-export([
    basic_opts_test_base/4, many_ops_test_base/6, distributed_modification_test_base/4, multi_space_test_base/3,
    file_consistency_test_skeleton/5, permission_cache_invalidate_test_base/2, get_links/1,
    mkdir_and_rmdir_loop_test_base/3, echo_and_delete_file_loop_test_base/3,
    create_and_delete_file_loop_test_base/3
]).
-export([init_env/1, teardown_env/1]).

% for file consistency testing
-export([create_doc/4, set_parent_link/4, set_link_to_parent/4, create_location/4, set_link_to_location/4,
    add_dbsync_state/4]).

-export([extend_config/4]).

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

-define(deny_user(UserId),
    #access_control_entity{
        acetype = ?deny_mask,
        aceflags = ?no_flags_mask,
        identifier = UserId,
        acemask = (?read_mask bor ?write_mask bor ?execute_mask)
    }).

%%%===================================================================
%%% Test skeletons
%%%===================================================================

permission_cache_invalidate_test_base(Config, Attempts) ->
    InvalidateFun = fun(Worker, SessId, TestDir) ->
        ?assertMatch(ok, lfm_proxy:set_perms(Worker, SessId, {path, TestDir}, 0))
    end,

    InvalidateFun2 = fun(Worker, SessId, TestDir) ->
        ?assertEqual(ok, lfm_proxy:set_acl(Worker, SessId, {path, TestDir}, [?deny_user(<<"user1">>)]))
    end,

    permission_cache_invalidate_test_skeleton(Config, Attempts, file_meta, InvalidateFun),
    permission_cache_invalidate_test_skeleton(Config, Attempts, custom_metadata, InvalidateFun2).

permission_cache_invalidate_test_skeleton(Config, Attempts, CheckedModule, InvalidateFun) ->
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    [SessId1 | _] = SessIds = lists:map(fun(Worker) ->
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config)
    end, Workers),
    WS = lists:zip(Workers, SessIds),

    [LastTreeDir | _] = TreeDirsReversed = lists:foldl(fun(_, [H | _] = Acc) ->
        NewDir = <<H/binary, "/", (generator:gen_name())/binary>>,
        [NewDir | Acc]
    end, [<<"/space1">>], lists:seq(1,10)),
    [_ | TreeDirs] = lists:reverse(TreeDirsReversed),

    lists:foreach(fun(D) ->
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId1, D, 8#755))
    end, TreeDirs),

    [_, _, TestDir | _] = TreeDirs,

    lists:foreach(fun({Worker, SessId}) ->
        ?assertEqual({ok, []}, lfm_proxy:ls(Worker, SessId, {path, LastTreeDir}, 0, 10), Attempts)
    end, WS),

    StatAns = lfm_proxy:stat(Worker1, SessId1, {path, TestDir}),
    ?assertMatch({ok, #file_attr{}}, StatAns),
    {ok, #file_attr{guid = FileGuid}} = StatAns,
    FileUUID = fslogic_uuid:guid_to_uuid(FileGuid),
    ControllerUUID = base64:encode(term_to_binary({CheckedModule, FileUUID})),

    InvalidateFun(Worker1, SessId1, TestDir),
    lists:foreach(fun({Worker, SessId}) ->
        ?assertEqual({error, ?EACCES}, lfm_proxy:ls(Worker, SessId, {path, LastTreeDir}, 0, 10), Attempts)
    end, WS),

    lists:foreach(fun(Worker) ->
        ?assertMatch({error,{not_found, _}}, ?rpc(Worker, change_propagation_controller, get,
            [ControllerUUID]), Attempts * length(Workers)),
        % TODO - uncomment after VFS-2678
        ListFun = fun(LinkName, _LinkTarget, Acc) ->
            [LinkName | Acc]
        end,
        MC = change_propagation_controller:model_init(),
        LSL = MC#model_config.link_store_level,
        ?assertEqual({ok, []}, ?rpc(Worker, datastore, foreach_link,
            [LSL, ControllerUUID, change_propagation_controller, ListFun, []]), Attempts)
    end, Workers),

    ok.


basic_opts_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten}, Attempts) ->
    basic_opts_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten, 1}, Attempts);
basic_opts_test_base(Config0, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts) ->

%%    ct:print("Test ~p", [{User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts, DirsNum, FilesNum}]),

    Config = extend_config(Config0, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),
    Workers = ?config(op_worker_nodes, Config),

    Dir = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
    Level2Dir = <<Dir/binary, "/", (generator:gen_name())/binary>>,
    Level2File = <<Dir/binary, "/", (generator:gen_name())/binary>>,

    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir, 8#755)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Level2Dir, 8#755)),

    verify_stats(Config, Dir, true),
    verify_stats(Config, Level2Dir, true),
    ct:print("Dirs created"),

    FileBeg = <<"1234567890abcd">>,
    create_file(Config, FileBeg, {2, Level2File}),
    verify_file(Config, FileBeg, {2, Level2File}),
    ct:print("File verified"),

    lists:foreach(fun(W) ->
        Level2TmpDir = <<Dir/binary, "/", (generator:gen_name())/binary>>,
%%        ct:print("Verify dir ~p", [{Level2TmpDir, W}]),
        ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId(W), Level2TmpDir, 8#755)),
        verify_stats(Config, Level2TmpDir, true),

        lists:foreach(fun(W2) ->
            Level3TmpDir = <<Level2TmpDir/binary, "/", (generator:gen_name())/binary>>,
%%            ct:print("Verify dir2 ~p", [{Level3TmpDir, W}]),
            ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId(W2), Level3TmpDir, 8#755)),
            verify_stats(Config, Level3TmpDir, true)
        end, Workers),
        ct:print("Tree verification from node ~p done", [W])
    end, Workers),

    lists:foreach(fun(W) ->
        Level2TmpFile = <<Dir/binary, "/", (generator:gen_name())/binary>>,
        create_file_on_worker(Config, FileBeg, 4, Level2TmpFile, W),
        verify_file(Config, FileBeg, {4, Level2TmpFile}),
        ct:print("File from node ~p verified", [W])
    end, Workers),

    verify(Config, fun(W) ->
        ?assertEqual(ok, lfm_proxy:close_all(W))
    end),

    ok.

many_ops_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten}, Attempts, DirsNum, FilesNum) ->
    many_ops_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten, 1}, Attempts, DirsNum, FilesNum);
many_ops_test_base(Config0, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider},
    Attempts, DirsNum, FilesNum) ->

    Config = extend_config(Config0, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),
    Workers = ?config(op_worker_nodes, Config),
    FileBeg = <<"1234567890abcd">>,

    Dir = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
    Level2Dir = <<Dir/binary, "/", (generator:gen_name())/binary>>,

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

    verify_stats(Config, Dir, true),
    verify_stats(Config, Level2Dir, true),

    lists:map(fun(D) ->
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), D, 8#755))
    end, Level3Dirs),
    ct:print("Dirs created"),

    lists:map(fun(D) ->
        ct:print("Verify dir ~p", [D]),
        verify_stats(Config, D, true)
    end, Level3Dirs),

    lists:map(fun(D) ->
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), D, 8#755))
    end, Level3Dirs2),
    ct:print("Dirs created - second batch"),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Level3Dir, 8#755)),
    lists:map(fun(F) ->
        create_file(Config, FileBeg, F)
    end, Level4Files),
    ct:print("Files created"),

    verify_dir_size(Config, Level2Dir, length(Level3Dirs) + length(Level3Dirs2) + 1),

    Level3Dirs2Uuids = lists:map(fun(D) ->
        ct:print("Verify dir ~p", [D]),
        verify_stats(Config, D, true)
    end, Level3Dirs2),

    Level4FilesVerified = lists:map(fun(F) ->
        ct:print("Verify file ~p", [F]),
        verify_file(Config, FileBeg, F)
    end, Level4Files),
    verify_dir_size(Config, Level3Dir, length(Level4Files)),

    lists:map(fun({_, F}) ->
        WD = lists:nth(crypto:rand_uniform(1,length(Workers) + 1), Workers),
        ?assertMatch(ok, lfm_proxy:unlink(WD, SessId(WD), {path, F}))
    end, Level4Files),
    lists:map(fun(D) ->
        WD = lists:nth(crypto:rand_uniform(1,length(Workers) + 1), Workers),
        ?assertMatch(ok, lfm_proxy:unlink(WD, SessId(WD), {path, D}))
    end, Level3Dirs2),
    ct:print("Dirs and files deleted"),

    lists:map(fun(F) ->
        ct:print("Verify file del ~p", [F]),
        verify_del(Config, F)
    end, Level4FilesVerified),
    lists:map(fun({D, Uuid}) ->
        ct:print("Verify dir del ~p", [D]),
        verify_del(Config, {D, Uuid, []})
    end, lists:zip(Level3Dirs2, Level3Dirs2Uuids)),
    verify_dir_size(Config, Level3Dir, 0),
    ct:print("Level 3 dir size verified"),
    verify_dir_size(Config, Level2Dir, length(Level3Dirs) + 1),
    ct:print("Level 2 dir size verified"),

    verify(Config, fun(W) ->
        ?assertEqual(ok, lfm_proxy:close_all(W))
    end),

    ok.

distributed_modification_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten}, Attempts) ->
    distributed_modification_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten, 1}, Attempts);
distributed_modification_test_base(Config0, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts) ->

    Config = extend_config(Config0, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),
    Workers = ?config(op_worker_nodes, Config),

    Dir = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
    Level2File = <<Dir/binary, "/", (generator:gen_name())/binary>>,

    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir, 8#755)),
    verify_stats(Config, Dir, true),
    ct:print("Dir verified"),

    FileBeg = <<"1234567890abcd">>,
    create_file(Config, FileBeg, {2, Level2File}),
    verify_file(Config, FileBeg, {2, Level2File}),
    ct:print("File verified"),

    lists:foldl(fun(W, Acc) ->
        OpenAns = lfm_proxy:open(W, SessId(W), {path, Level2File}, rdwr),
        ?assertMatch({ok, _}, OpenAns),
        {ok, Handle} = OpenAns,
        WriteBuf = atom_to_binary(W, utf8),
        Offset = size(Acc),
        WriteSize = size(WriteBuf),
        ?assertMatch({ok, WriteSize}, lfm_proxy:write(W, Handle, Offset, WriteBuf)),
        ?assertEqual(ok, lfm_proxy:close(W, Handle)),
        NewAcc = <<Acc/binary, WriteBuf/binary>>,

        verify(Config, fun(W2) ->
            ct:print("Verify write ~p", [{Level2File, W2}]),
            ?match({ok, NewAcc},
                begin
                    {ok, Handle2} = lfm_proxy:open(W2, SessId(W2), {path, Level2File}, rdwr),
                    try
                        lfm_proxy:read(W2, Handle2, 0, 500)
                    after
                        lfm_proxy:close(W2, Handle2)
                    end
                end, Attempts)
        end),
        ct:print("Changes of file from node ~p verified", [W]),
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
    ct:print("Parallel dirs creation processed spawned"),

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
        verify_stats(Config, Level2TmpDir, true),
        ct:print("Dir verified"),
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
    ct:print("Parallel dirs creation processed spawned"),

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
            verify_stats(Config, Level3TmpDir, true),
            ct:print("Dir verified")
        end, Workers)
    end, Level2TmpDirs),

    verify(Config, fun(W) ->
        ?assertEqual(ok, lfm_proxy:close_all(W))
    end),

    ok.

file_consistency_test_skeleton(Config, Worker1, Worker2, Worker3, ConfigsNum) ->
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

    Workers = ?config(op_worker_nodes, Config),
    P1Workers = lists:foldl(fun(W, Acc) ->
        case string:str(atom_to_list(W), "p1") of
            0 -> Acc;
            _ -> [W | Acc]
        end
    end, [], Workers),

    DoTest = fun(TaskList) ->
%%        ct:print("Do test"),

        GenerateDoc = fun(Type) ->
            Name = generator:gen_name(),
            Uuid = datastore_utils:gen_uuid(),
            Doc = #document{key = Uuid, value =
            #file_meta{
                name = Name,
                type = Type,
                mode = 8#775,
                owner = User,
                scope = SpaceKey
            }
            },
%%            ct:print("Doc ~p ~p", [Uuid, Name]),
            {Doc, Name}
        end,

        {Doc1, Name1} = GenerateDoc(?DIRECTORY_TYPE),
        {Doc2, Name2} = GenerateDoc(?DIRECTORY_TYPE),
        {Doc3, Name3} = GenerateDoc(?REGULAR_FILE_TYPE),
        Loc3ID = datastore_utils:gen_uuid(),
        {Doc4, Name4} = GenerateDoc(?REGULAR_FILE_TYPE),
        Loc4ID = datastore_utils:gen_uuid(),

        D1Path = <<"/", SpaceName/binary, "/",  Name1/binary>>,
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
            ({add_dbsync_state = Fun, DocNum}) ->
                lists:foreach(fun(W) ->
                    ?assertEqual(ok, ?rpcTest(W, Fun, proplists:get_value(DocNum, DocsList)))
                end, P1Workers);
            ({Fun, DocNum}) ->
                ?assertEqual(ok, ?rpcTest(Worker1, Fun, proplists:get_value(DocNum, DocsList)));
            ({Fun, DocNum, W}) ->
                ?assertEqual(ok, ?rpcTest(W, Fun, proplists:get_value(DocNum, DocsList)))
        end, TaskList),

        ?match({ok, #file_attr{}},
            lfm_proxy:stat(Worker2, SessId(Worker2), {path, D3Path}), Attempts),

        ?match({ok, <<"abc">>},
            begin
                {ok, Handle} = lfm_proxy:open(Worker2, SessId(Worker2), {path, D3Path}, rdwr),
                try
                    lfm_proxy:read(Worker2, Handle, 0, 10)
                after
                    lfm_proxy:close(Worker2, Handle)
                end
            end, Attempts)
    end,

    {ok, CacheDelay} = test_utils:get_env(Worker1, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms),
    SleepTime = round(CacheDelay / 1000 + 5),

    TestConfigs = [
        _T1 = [
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

        _T2 = [
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

        _T3 = [
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

        _T4 = [
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

        _T5 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {set_link_to_parent, 1},
            {create_doc, 2},
            {set_parent_link, 2},
            {set_link_to_parent, 2},
            {add_dbsync_state, 3},
            {set_parent_link, 3},
            {set_link_to_parent, 3},
            {sleep, SleepTime},
            {create_doc, 3},
            {sleep, SleepTime},
            {create_location, 3},
            {set_link_to_location, 3}
        ],

        _T6 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {set_link_to_parent, 1},
            {create_doc, 2},
            {set_parent_link, 2},
            {set_link_to_parent, 2},
            {add_dbsync_state, 3},
            {set_parent_link, 3},
            {set_link_to_parent, 3},
            {create_location, 3},
            {sleep, SleepTime},
            {create_doc, 3},
            {sleep, SleepTime},
            {set_link_to_location, 3}
        ],

        _T7 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {set_link_to_parent, 1},
            {create_doc, 2},
            {set_parent_link, 2},
            {set_link_to_parent, 2},
            {add_dbsync_state, 3},
            {set_parent_link, 3},
            {set_link_to_parent, 3},
            {set_link_to_location, 3},
            {sleep, SleepTime},
            {create_doc, 3},
            {sleep, SleepTime},
            {create_location, 3}
        ],

        _T8 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {set_link_to_parent, 1},
            {create_doc, 2},
            {set_parent_link, 2},
            {set_link_to_parent, 2},
            {add_dbsync_state, 3},
            {set_parent_link, 3},
            {set_link_to_parent, 3},
            {create_location, 3},
            {set_link_to_location, 3},
            {sleep, SleepTime},
            {create_doc, 3}
        ],

        _T9 = [
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

        _T10 = [
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

        _T11 = [
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

        _T12 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {set_link_to_parent, 1},
            {create_doc, 2},
            {set_parent_link, 2},
            {set_link_to_parent, 2},
            {add_dbsync_state, 3},
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

        _T13 = [
            {add_dbsync_state, 3},
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
            {add_dbsync_state, 2},
            {set_parent_link, 2},
            {sleep, SleepTime},
            {set_link_to_parent, 2},
            {sleep, SleepTime},
            {create_doc, 2},
            {sleep, SleepTime},
            {add_dbsync_state, 1},
            {set_parent_link, 1},
            {sleep, SleepTime},
            {set_link_to_parent, 1},
            {sleep, SleepTime},
            {create_doc, 1}
        ],

        _T14 = [
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
        ]
    ],
    lists:foreach(fun(Num) ->
        ct:print("Consistency config number ~p", [Num]),
        DoTest(lists:nth(Num, TestConfigs))
    end, ConfigsNum),

    ok.

multi_space_test_base(Config0, SpaceConfigs, User) ->
    Workers = ?config(op_worker_nodes, Config0),
    Spaces = ?config({spaces, User}, Config0),

    CreateLog = lists:foldl(fun(W, Acc) ->
        lists:foldl(fun({_, SN}, Acc2) ->
            ct:print("Create dirs: node ~p, space ~p", [W, SN]),

            Config = proplists:get_value(SN, SpaceConfigs),
            SessId = ?config(session, Config),

            Dir = <<"/", SN/binary, "/",  (generator:gen_name())/binary>>,
            Level2Dir = <<Dir/binary, "/", (generator:gen_name())/binary>>,
            Level2File = <<Dir/binary, "/", (generator:gen_name())/binary>>,

            ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId(W), Dir, 8#755)),
            ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId(W), Level2Dir, 8#755)),

            [{Dir, Level2Dir, Level2File, W, SN} | Acc2]
        end, Acc, Spaces)
    end, [], Workers),

    lists:foreach(fun({Dir, Level2Dir, _Level2File, W, SN}) ->
        ct:print("Verify dirs, node ~p, space ~p ~p", [W, SN, Dir]),

        Config = proplists:get_value(SN, SpaceConfigs),
        verify_stats(Config, Dir, true),
        ct:print("Verify dirs, node ~p, space ~p ~p", [W, SN, Level2Dir]),
        verify_stats(Config, Level2Dir, true)
    end, CreateLog),

    FileCreateLog = lists:foldl(fun({_Dir, _Level2Dir, Level2File, W, SN}, Acc) ->
        ct:print("Create files: node ~p, space ~p", [W, SN]),

        FileBeg = <<"1234567890abcd",  (generator:gen_name())/binary>>,
        Config = proplists:get_value(SN, SpaceConfigs),
        create_file_on_worker(Config, FileBeg, 2, Level2File, W),
        [{Level2File, FileBeg, W, SN} | Acc]
    end, [], CreateLog),

    lists:foreach(fun({Level2File, FileBeg, W, SN}) ->
        ct:print("Verify file, node ~p, space ~p ~p", [W, SN, Level2File]),

        Config = proplists:get_value(SN, SpaceConfigs),
        verify_file(Config, FileBeg, {2, Level2File})
    end, FileCreateLog),

    verify(Config0, fun(W) ->
        ?assertEqual(ok, lfm_proxy:close_all(W))
    end),

    ok.


mkdir_and_rmdir_loop_test_base(Config0, IterationsNum, User) ->
    Config = extend_config(Config0, User, {0, 0, 0, 0}, 0),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),

    Dir = generator:gen_name(),
    DirPath = <<SpaceName/binary, "/",  Dir/binary>>,

    lists:foreach(fun(_N) ->
        MkdirAns = lfm_proxy:mkdir(Worker1, SessId(Worker1), DirPath),
        ?assertMatch({ok, _}, MkdirAns),
        {ok, Handle} = MkdirAns,
        ?assertMatch(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), {guid, Handle}))
    end, lists:seq(1, IterationsNum)),
    ok.


create_and_delete_file_loop_test_base(Config0, IterationsNum, User) ->
    Config = extend_config(Config0, User, {0, 0, 0, 0}, 0),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),

    File = generator:gen_name(),
    FilePath = <<"/", SpaceName/binary, "/",  File/binary>>,

    lists:foreach(fun(_N) ->
        ?assertMatch({ok, _} , lfm_proxy:create(Worker1, SessId(Worker1), FilePath, 8#755)),
        ?assertMatch(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), {path, FilePath}))
    end, lists:seq(1, IterationsNum)),
    ok.


echo_and_delete_file_loop_test_base(Config0, IterationsNum, User) ->
    Config = extend_config(Config0, User, {0, 0, 0, 0}, 0),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),

    File = generator:gen_name(),
    FilePath = <<"/", SpaceName/binary, "/",  File/binary>>,
    Text = <<"0123456789abcdef">>,
    BufSize = size(Text),

    lists:foreach(fun(_N) ->
        ?assertMatch({ok, _}, lfm_proxy:create(Worker1, SessId(Worker1), FilePath, 8#755)),
        OpenAns = lfm_proxy:open(Worker1, SessId(Worker1), {path, FilePath}, write),
        ?assertMatch({ok, _}, OpenAns),
        {ok, Handle} = OpenAns,
        ?assertMatch({ok, BufSize}, lfm_proxy:write(Worker1, Handle, 0, Text)),
        lfm_proxy:close(Worker1, Handle),
        ?assertMatch(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), {path, FilePath}))
    end, lists:seq(1, IterationsNum)),
    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_env(Config) ->
    lists:foreach(fun(Worker) ->
        test_utils:set_env(Worker, ?APP_NAME, dbsync_flush_queue_interval, timer:seconds(1)),
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
%%        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(2)),
        % TODO - change to 2 seconds
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(1)),
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, datastore_pool_queue_flush_delay, 1000)
    end, ?config(op_worker_nodes, Config)),

    application:start(etls),
    hackney:start(),
    initializer:enable_grpca_based_communication(Config),
    initializer:disable_quota_limit(Config),
    initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config).

teardown_env(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    initializer:disable_grpca_based_communication(Config),
    hackney:stop(),
    application:stop(etls).


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
            _ -> Acc
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
            _ ->
                Acc
        end
    end, [], Map).

create_doc(Doc, _ParentDoc, _LocId, _Path) ->
    {ok, FileUuid} = file_meta:save(Doc),
    {ok, _} = times:save(#document{key = FileUuid, value = #times{}}),
    ok.

set_parent_link(Doc, ParentDoc, _LocId, _Path) ->
    FDoc = Doc#document.value,
    MC = file_meta:model_init(),
    LSL = MC#model_config.link_store_level,
    ok = datastore:add_links(LSL, ParentDoc, {FDoc#file_meta.name, Doc}).

set_link_to_parent(Doc, ParentDoc, _LocId, _Path) ->
    MC = file_meta:model_init(),
    LSL = MC#model_config.link_store_level,
    ok = datastore:add_links(LSL, Doc, {parent, ParentDoc}).

create_location(Doc, _ParentDoc, LocId, Path) ->
    FDoc = Doc#document.value,
    FileUuid = Doc#document.key,
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(FDoc#file_meta.scope),

    {ok, #document{key = StorageId}} = fslogic_storage:select_storage(SpaceId),
    FileId = Path,
    Location = #file_location{blocks = [#file_block{offset = 0, size = 3}], size = 3,
        provider_id = oneprovider:get_provider_id(), file_id = FileId, storage_id = StorageId, uuid = FileUuid,
        space_id = SpaceId},

    MC = file_location:model_init(),
    LSL = MC#model_config.link_store_level,
    {ok, _} = datastore:save(LSL, #document{key = LocId, value = Location}),

    LeafLess = filename:dirname(FileId),
    {ok, #document{key = StorageId} = Storage} = fslogic_storage:select_storage(SpaceId),
    SFMHandle0 = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceId, FileUuid, Storage, LeafLess, undefined),
    case storage_file_manager:mkdir(SFMHandle0, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
        ok -> ok;
        {error, eexist} ->
            ok
    end,


    SFMHandle1 = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceId, FileUuid, Storage, FileId, undefined),
    storage_file_manager:unlink(SFMHandle1),
    ok = storage_file_manager:create(SFMHandle1, 8#775),
    {ok, SFMHandle2} = storage_file_manager:open(SFMHandle1, write),
    {ok, 3} = storage_file_manager:write(SFMHandle2, 0, <<"abc">>),
    storage_file_manager:fsync(SFMHandle2),
    ok.

set_link_to_location(Doc, _ParentDoc, LocId, _Path) ->
    FileUuid = Doc#document.key,
    MC = file_meta:model_init(),
    LSL = MC#model_config.link_store_level,
    ok = datastore:add_links(LSL, Doc, {file_meta:location_ref(oneprovider:get_provider_id()), {LocId, file_location}}),
    ok = datastore:add_links(LSL, LocId, file_location, {file_meta, {FileUuid, file_meta}}).

add_dbsync_state(Doc, _ParentDoc, _LocId, _Path) ->
    {ok, SID} = dbsync_worker:get_space_id(Doc),
    {ok, _} = dbsync_state:save(#document{key = dbsync_state:sid_doc_key(file_meta, Doc#document.key), value = #dbsync_state{entry = {ok, SID}}}),
    ok.

extend_config(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts) ->
    ProxyNodesWritten = ProxyNodesWritten0 * NodesOfProvider,
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

    SessId = fun(W) -> ?config({session_id, {User, ?GET_DOMAIN(W)}}, Config) end,
    [{_SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),
    [{worker1, Worker1}, {session, SessId}, {space_name, SpaceName}, {attempts, Attempts},
        {nodes_number, {SyncNodes, ProxyNodes, ProxyNodesWritten, ProxyNodesWritten0, NodesOfProvider}} | Config].

verify(Config, TestFun) ->
    Workers = ?config(op_worker_nodes, Config),

    process_flag(trap_exit, true),
    TestAns = verify_helper(Workers, TestFun),

    Error = lists:any(fun
        ({_W, error, _Reason}) -> true;
        (_) -> false
    end, TestAns),
    case Error of
        true -> ?assert(TestAns);
        _ -> ok
    end,

    lists:map(fun({_W, Ans}) -> Ans end, TestAns).

verify_helper([], _TestFun) ->
    [];
verify_helper([W | Workers], TestFun) ->
    Master = self(),
    Pid = spawn_link(fun() ->
        Ans = TestFun(W),
        Master ! {verify_ans, Ans}
    end),
    TmpAns = verify_helper(Workers, TestFun),
    receive
        {verify_ans, TestAns} ->
            [{W, TestAns} | TmpAns];
        {'EXIT', Pid , Error} when Error /= normal ->
            [{W, error, Error} | TmpAns]
    after
        timer:minutes(5) ->
            [{W, error, timeout} | TmpAns]
    end.

verify_stats(Config, File, IsDir) ->
    SessId = ?config(session, Config),
    Attempts = ?config(attempts, Config),
    {SyncNodes, ProxyNodes, ProxyNodesWritten, _ProxyNodesWritten0, _NodesOfProvider} = ?config(nodes_number, Config),

    VerAns = verify(Config, fun(W) ->
%%        ct:print("VerifyStats ~p", [{File, IsDir, W}]),
        case IsDir of
            true ->
                ?match({ok, #file_attr{type = ?DIRECTORY_TYPE}},
                    lfm_proxy:stat(W, SessId(W), {path, File}), Attempts);
            _ ->
                ?match({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
                    lfm_proxy:stat(W, SessId(W), {path, File}), Attempts)
        end,
        StatAns = lfm_proxy:stat(W, SessId(W), {path, File}),
        {ok, #file_attr{guid = FileGuid}} = StatAns,
        FileUUID = fslogic_uuid:guid_to_uuid(FileGuid),
        {FileUUID, rpc:call(W, file_meta, get, [FileUUID])}
    end),

    NotFoundList = lists:filter(fun({_, {error, {not_found, _}}}) -> true; (_) -> false end, VerAns),
    OKList = lists:filter(fun({_, {ok, _}}) -> true; (_) -> false end, VerAns),

    ?assertEqual(ProxyNodes - ProxyNodesWritten, length(NotFoundList)),
    ?assertEqual(SyncNodes + ProxyNodesWritten, length(OKList)),
    [{FileUUIDAns, _} | _] = VerAns,
    FileUUIDAns.

create_file_on_worker(Config, FileBeg, Offset, File, WriteWorker) ->
    SessId = ?config(session, Config),

    ?assertMatch({ok, _}, lfm_proxy:create(WriteWorker, SessId(WriteWorker), File, 8#755)),
    OpenAns = lfm_proxy:open(WriteWorker, SessId(WriteWorker), {path, File}, rdwr),
    ?assertMatch({ok, _}, OpenAns),
    {ok, Handle} = OpenAns,
    FileBegSize = size(FileBeg),
    ?assertMatch({ok, FileBegSize}, lfm_proxy:write(WriteWorker, Handle, 0, FileBeg)),
    Size = size(File),
    Offset2 = Offset rem 5 + 1,
    ?assertMatch({ok, Size}, lfm_proxy:write(WriteWorker, Handle, Offset2, File)),
    ?assertMatch(ok, lfm_proxy:truncate(WriteWorker, SessId(WriteWorker), {path, File}, 2*Offset2)),
    ?assertEqual(ok, lfm_proxy:close(WriteWorker, Handle)).

create_file(Config, FileBeg, {Offset, File}) ->
    Worker1 = ?config(worker1, Config),
    create_file_on_worker(Config, FileBeg, Offset, File, Worker1).

verify_file(Config, FileBeg, {Offset, File}) ->
    SessId = ?config(session, Config),
    Attempts = ?config(attempts, Config),
    {SyncNodes, ProxyNodes, ProxyNodesWritten, _ProxyNodesWritten0, NodesOfProvider} = ?config(nodes_number, Config),
    SyncProvidersCount = max(1, round(SyncNodes / NodesOfProvider)),

    Offset2 = Offset rem 5 + 1,
    Size = 2*Offset2,
    FileUUID = verify_stats(Config, File, false),

    Beg = binary:part(FileBeg, 0, Offset2),
    End = binary:part(File, 0, Offset2),
    FileCheck = <<Beg/binary, End/binary>>,

    VerifyLocation = fun() ->
        verify(Config, fun(W) ->
            verify_locations(W, FileUUID)
        end)
    end,

%%        VerAns0 = VerifyLocation(),
%%        ct:print("Locations0 ~p", [{Offset, File, VerAns0}]),

    verify(Config, fun(W) ->
%%            ct:print("Verify file ~p", [{File, W}]),
        ?match({ok, FileCheck},
            begin
                {ok, Handle} = lfm_proxy:open(W, SessId(W), {path, File}, rdwr),
                try
                    lfm_proxy:read(W, Handle, 0, Size)
                after
                    lfm_proxy:close(W, Handle)
                end
            end, Attempts)
    end),

    ToMatch = {ProxyNodes - ProxyNodesWritten, SyncNodes + ProxyNodesWritten},
    AssertLocations = fun() ->
        VerAns = VerifyLocation(),
%%            ct:print("Locations1 ~p", [{Offset, File, VerAns}]),
        Flattened = lists:flatten(VerAns),

        ZerosList = lists:filter(fun(S) -> S == 0 end, Flattened),
        LocationsList = lists:filter(fun(S) -> S == SyncProvidersCount end, Flattened),
%%            ct:print("Locations1 ~p", [{{length(ZerosList), length(LocationsList)}, ToMatch}]),
        {length(ZerosList), length(LocationsList)}
    end,
    ?match(ToMatch, AssertLocations(), Attempts),

    LocToAns = verify(Config, fun(W) ->
        StatAns = lfm_proxy:stat(W, SessId(W), {path, File}),
        ?assertMatch({ok, #file_attr{}}, StatAns),
        {ok, #file_attr{guid = FileGuid}} = StatAns,
        FileUUID = fslogic_uuid:guid_to_uuid(FileGuid),

        {W, get_locations(W, FileUUID)}
    end),
    {File, FileUUID, LocToAns}.

verify_del(Config, {F, FileUUID, Locations}) ->
    SessId = ?config(session, Config),
    Attempts = ?config(attempts, Config),

    verify(Config, fun(W) ->
%%            ct:print("Del ~p", [{W, F,  FileUUID, Locations}]),
%%            ?match({error, ?ENOENT}, lfm_proxy:stat(W, SessId(W), {path, F}), Attempts)
        % TODO - match to chosen error (check perms may also result in ENOENT)
        ?match({error, _}, lfm_proxy:stat(W, SessId(W), {path, F}), Attempts),

        ?match({error, {not_found, _}}, rpc:call(W, file_meta, get, [FileUUID]), Attempts)
%%        ?match(0, count_links(W, FileUUID), Attempts),
%%
%%        lists:foreach(fun(Location) ->
%%            ?match({error, {not_found, _}},
%%                rpc:call(W, file_meta, get, [Location]), Attempts)
%%        end, proplists:get_value(W, Locations, []))
    end).

verify_dir_size(Config, DirToCheck, DSize) ->
    SessId = ?config(session, Config),
    Attempts = ?config(attempts, Config),
    {SyncNodes, ProxyNodes, ProxyNodesWritten, _ProxyNodesWritten0, _NodesOfProvider} = ?config(nodes_number, Config),

    VerAns0 = verify(Config, fun(W) ->
        CountChilden = fun() ->
            LSAns = lfm_proxy:ls(W, SessId(W), {path, DirToCheck}, 0, 200),
            ?assertMatch({ok, _}, LSAns),
            {ok, ListedDirs} = LSAns,
            length(ListedDirs)
        end,
        ?match(DSize, CountChilden(), Attempts),

        StatAns = lfm_proxy:stat(W, SessId(W), {path, DirToCheck}),
        ?assertMatch({ok, #file_attr{}}, StatAns),
        {ok, #file_attr{guid = FileGuid}} = StatAns,
        FileUUID = fslogic_uuid:guid_to_uuid(FileGuid),
        {W, FileUUID}
    end),

    AssertLinks = fun() ->
        VerAns = lists:map(fun({W, Uuid}) ->
            count_links(W, Uuid)
        end, VerAns0),
%%        ct:print("Links ~lp", [{DSize, VerAns}]),

        ZerosList = lists:filter(fun(S) -> S == 0 end, VerAns),
        SList = lists:filter(fun(S) -> S == 2*DSize + 1 end, VerAns),
        {length(ZerosList), length(SList)}
    end,
    ToMatch = {ProxyNodes - ProxyNodesWritten, SyncNodes + ProxyNodesWritten},
    ?match(ToMatch, AssertLinks(), Attempts).
