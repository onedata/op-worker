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
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

%% API
%% export for tests
-export([
    basic_opts_test_base/4,
    rtransfer_test_base/11,
    many_ops_test_base/6,
    distributed_modification_test_base/4,
    multi_space_test_base/3,
    file_consistency_test_skeleton/5,
    get_links/1,
    mkdir_and_rmdir_loop_test_base/3,
    echo_and_delete_file_loop_test_base/3,
    create_and_delete_file_loop_test_base/3,
    distributed_delete_test_base/4,
    create_after_del_test_base/4
]).
-export([init_env/1, teardown_env/1]).

% for file consistency testing
-export([create_doc/4, set_parent_link/4, create_location/4]).

-export([extend_config/4]).

-define(match(Expect, Expr, Attempts),
    case Attempts of
        0 ->
            ?assertMatch(Expect, Expr);
        _ ->
            ?assertMatch(Expect, Expr, Attempts)
    end
).
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


rtransfer_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten},
    Attempts, Timeout, SmallFilesNum, MediumFilesNum, BigFilesNum,
    BigFilesChunks, TransfersNum, TransferFileParts) ->
    rtransfer_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten, 1},
        Attempts, Timeout, SmallFilesNum, MediumFilesNum, BigFilesNum,
        BigFilesChunks, TransfersNum, TransferFileParts);
rtransfer_test_base(Config0, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider},
    Attempts, Timeout, SmallFilesNum, MediumFilesNum, BigFilesNum, BigFileParts,
    TransfersNum, TransferFileParts) ->
    Config = extend_config(Config0, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),
    Workers1 = ?config(workers1, Config),
    Workers2 = ?config(workers2, Config),
    Workers = ?config(op_worker_nodes, Config),
    Workers3 = Workers -- Workers1 -- Workers2,

    Dir = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir, 8#755)),

    verify_stats(Config, Dir, true),
    ct:print("Dir created"),

    ChunkSize = 10240,
    SmallFiles = lists:map(fun(_) ->
        {1, 1, false}
    end, lists:seq(1, SmallFilesNum)),
    MediumFiles = lists:map(fun(_) ->
        {1024, 1, false}
    end, lists:seq(1, MediumFilesNum)),
    BigFiles = lists:map(fun(_) ->
        {1024, BigFileParts, false}
    end, lists:seq(1, BigFilesNum)),
    Transfers = lists:map(fun(_) ->
        {1024, TransferFileParts, true}
    end, lists:seq(1, TransfersNum)),
    Files = SmallFiles ++ MediumFiles ++ BigFiles ++ Transfers,

    Master = self(),
    lists:foreach(fun({ChunksNum, PartNum, Transfer}) ->
        spawn(fun() ->
            try
                PartSize = ChunkSize * ChunksNum,
                FileSize = PartSize * PartNum,
                Level2File = <<Dir/binary, "/", (generator:gen_name())/binary>>,
                Level2File2 = <<Dir/binary, "/", (generator:gen_name())/binary>>,
                create_big_file(Config, ChunkSize, ChunksNum, PartNum, Level2File, Worker1),

                T1 = lists:max(verify_workers(Workers2, fun(W) ->
                    read_big_file(Config, FileSize, Level2File, W, Transfer)
                end)),
                T2 = lists:max(verify_workers(Workers3, fun(W) ->
                    read_big_file(Config, FileSize, Level2File, W, Transfer)
                end)),

                create_big_file(Config, ChunkSize, ChunksNum, PartNum, Level2File2, Worker1),
                T3 = lists:max(verify(Config, fun(W) ->
                    read_big_file(Config, FileSize, Level2File2, W, Transfer)
                end)),

                Master ! {slave_ans, ChunksNum, PartNum, Transfer, {ok, lists:max([T1, T2, T3])}}
            catch
                E1:E2 ->
                    Master ! {slave_ans, ChunksNum, PartNum, Transfer, {error, E1, E2}}
            end
        end)
    end, Files),

    Answers = lists:foldl(fun({ChunksNum, PartNum, Transfer}, Acc) ->
        SlaveAns = receive
            {slave_ans, ChunksNum, PartNum, Transfer, Ans} ->
                Ans
        after
            Timeout ->
                timeout
        end,
        ?assertMatch({ok, _}, SlaveAns),
        {ok, Time} = SlaveAns,
        [{ChunksNum, PartNum, Transfer, Time} | Acc]
    end, [], Files),

    AnswersMap = lists:foldl(fun({ChunksNum, PartNum, Transfer, Time}, Acc) ->
        Key = {ChunksNum, PartNum, Transfer},
        V = maps:get(Key, Acc, 0),
        maps:put(Key, max(V, Time), Acc)
    end, #{}, Answers),

    ct:print("Read max times ~p", [AnswersMap]),

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

create_after_del_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten}, Attempts) ->
    create_after_del_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten, 1}, Attempts);
create_after_del_test_base(Config0, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts) ->

%%    ct:print("Test ~p", [{User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts, DirsNum, FilesNum}]),

    Config = extend_config(Config0, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts),
    Workers = ?config(op_worker_nodes, Config),

    lists:foreach(fun(Worker) ->
        test_utils:set_env(Worker, ?APP_NAME, unlink_on_create, false)
    end, Workers),

    delete_test_skeleton(Config, "Standard", true, false, false, false),
    delete_test_skeleton(Config, "Standard, write 1", false, false, false, false),

    delete_test_skeleton(Config, "Open and sleep", true, true, {true, 10}, false),
    delete_test_skeleton(Config, "Open and sleep, write 1", false, true, {true, 10}, false),

    delete_test_skeleton(Config, "Open and no sleep", true, true, false, false),
    delete_test_skeleton(Config, "Open and no sleep, write 1", false, true, false, false),

    delete_test_skeleton(Config, "Close after del", true, true, {true, 1}, true),
    delete_test_skeleton(Config, "Close after del, write 1", false, true, {true, 1}, true),

    lists:foreach(fun(Worker) ->
        test_utils:set_env(Worker, ?APP_NAME, unlink_on_create, true)
    end, Workers),

    ok.

delete_test_skeleton(Config, Desc, WriteOn1, OpenBeforeDel, SleepAfterVerify,
    CloseAfterVerify) ->
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),
    Workers = ?config(op_worker_nodes, Config),

    lists:foreach(fun(I) ->
        ct:print("~p ~p", [Desc, I]),
        DelFile = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
        lists:foldl(fun
            (W, undefined) ->
                ct:print("Before del ~p", [{DelFile, W}]),
                WriteWorker = case WriteOn1 of
                    true -> Worker1;
                    _ -> W
                end,
                Beg = crypto:strong_rand_bytes(8),
                BegSize = size(Beg),
                create_file_on_worker(Config, Beg, BegSize, DelFile, WriteWorker, 5),
                verify_file(Config, Beg, {BegSize, DelFile});
            (W, DelInfo) ->
                ct:print("Del ~p", [{DelFile, W}]),
                WriteWorker = case WriteOn1 of
                    true -> Worker1;
                    _ -> W
                end,

                case OpenBeforeDel of
                    true ->
                        {ok, Handle} = lfm_proxy:open(W, SessId(W), {path, DelFile}, rdwr),
                        ?assertMatch(ok, lfm_proxy:unlink(W, SessId(W), {path, DelFile})),

                        case CloseAfterVerify of
                            false ->
                                ok = timer:sleep(timer:seconds(1)),
                                lfm_proxy:close(W, Handle);
                            _ ->
                                ok
                        end,

                        verify_del(Config, DelInfo),

                        case SleepAfterVerify of
                            {true, Seconds} -> timer:sleep(timer:seconds(Seconds));
                            _ -> ok
                        end,

                        case CloseAfterVerify of
                            true ->
                                ok = timer:sleep(timer:seconds(1)),
                                lfm_proxy:close(W, Handle);
                            _ ->
                                ok
                        end;
                    _ ->
                        ?assertMatch(ok, lfm_proxy:unlink(W, SessId(W), {path, DelFile})),
                        verify_del(Config, DelInfo)
                end,

                Beg = crypto:strong_rand_bytes(8),
                BegSize = size(Beg),
                create_file_on_worker(Config, Beg, BegSize, DelFile, WriteWorker, 5),
                verify_file(Config, Beg, {BegSize, DelFile})
        end, undefined, Workers ++ Workers)
    end, lists:seq(1,5)).

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

distributed_delete_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten}, Attempts) ->
    distributed_delete_test_base(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten, 1}, Attempts);
distributed_delete_test_base(Config0, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts) ->

    Config = extend_config(Config0, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),
    Workers1 = ?config(workers1, Config),
    WorkersNot1 = ?config(workers_not1, Config),
    Workers = ?config(op_worker_nodes, Config),

    MainDirs = lists:foldl(fun(_, Acc) ->
        Dir = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir, 8#755)),
        verify_stats(Config, Dir, true),
        [Dir | Acc]
    end, [], lists:seq(1,6)),
    ct:print("Dirs verified"),

    ChildrenNodes = [
        [Worker1 || _ <- Workers],
        [Worker1 || _ <- Workers],
        [Worker1 || _ <- Workers],
        Workers,
        [Worker1 || _ <- Workers],
        Workers
    ],

    DelNodes = [
            Workers1 ++ Workers1,
            WorkersNot1 ++ WorkersNot1,
        Workers,
            Workers1 ++ Workers1,
        all,
        all
    ],

    ChildrenList = lists:foldl(fun({Dir, Nodes}, FinalAcc) ->
        TmpChildren = lists:foldl(fun(W, Acc) ->
            Level2TmpDir = <<Dir/binary, "/", (generator:gen_name())/binary>>,
            ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId(W), Level2TmpDir, 8#755)),
            [Level2TmpDir | Acc]
        end, [], Nodes),
        [{Dir, TmpChildren} | FinalAcc]
    end, [], lists:zip(MainDirs, ChildrenNodes)),
    ct:print("Children created"),

    Test = fun(MainDir, Children, Desc, DelWorkers) ->
        ct:print("Test ~p", [Desc]),

        ChildrenWithUuids = lists:map(fun(Child) ->
            {Child, verify_stats(Config, Child, true)}
        end, Children),
        ct:print("Children verified"),

        Zipped = case DelWorkers of
            all ->
                lists:foldl(fun(CWU, Acc) ->
                    Acc ++ [{W, CWU} || W <- Workers]
                end, [], ChildrenWithUuids);
            _ ->
                lists:zip(DelWorkers, ChildrenWithUuids)
        end,

        Master = self(),
        lists:foreach(fun({W, {D, Uuid}}) ->
            spawn_link(fun() ->
                RmAns = lfm_proxy:unlink(W, SessId(W), {path, D}),
                Master ! {rm_ans, W, D, Uuid, RmAns}
            end)
        end, Zipped),
        ct:print("Parallel dirs rm processed spawned"),

        lists:foreach(fun(_) ->
            RmAnsCheck =
                receive
                    {rm_ans, W, D, Uuid, {error, enoent}} ->
                        {rm_ans, W, D, Uuid, ok};
                    {rm_ans, W, D, Uuid, RmAns} ->
                        {rm_ans, W, D, Uuid, RmAns}
                after timer:seconds(2*Attempts+2) ->
                    {error, timeout}
                end,
            ?assertMatch({rm_ans, _, _, _, ok}, RmAnsCheck),
            {rm_ans, RecW, RecDir, RecUuid, ok} = RmAnsCheck,
            ct:print("Verify spawn ~p", [{RecW, RecDir, RecUuid}]),
            verify_del(Config, {RecDir, RecUuid, []}),
            ct:print("Dir rm verified")
        end, Zipped),

        verify_dir_size(Config, MainDir, 0)
    end,

    TestDescs = [
        "P1 create, P1 del",
        "P1 create, P2 del",
        "P1 create, single random del",
        "random create, P1 del",
        "P1 create, P1 and P2 del",
        "random create, P1 and P2 del"
    ],

    TestCases = lists:zip(lists:zip(lists:reverse(ChildrenList), DelNodes), TestDescs),
    lists:foreach(fun({{{MainDir, Children}, DelWorkers}, Desc}) ->
        Test(MainDir, Children, Desc, DelWorkers)
    end, TestCases),

    ok.

file_consistency_test_skeleton(Config, Worker1, Worker2, Worker3, ConfigsNum) ->
    timer:sleep(10000), % TODO - connection must appear after mock setup
    Attempts = 60,
    User = <<"user1">>,

    SessId = fun(W) -> ?config({session_id, {User, ?GET_DOMAIN(W)}}, Config) end,
    [{SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),
    A1 = rpc:call(Worker1, file_meta, get, [{path, <<"/", SpaceName/binary>>}]),
    ?assertMatch({ok, _}, A1),
    {ok, SpaceDoc} = A1,
    SpaceKey = SpaceDoc#document.key,
%%    ct:print("Space key ~p", [SpaceKey]),

    DoTest = fun(TaskList) ->
%%        ct:print("Do test"),

        GenerateDoc = fun(Type) ->
            Name = generator:gen_name(),
            Uuid = datastore_utils:gen_key(),
            Doc = #document{key = Uuid, value =
            #file_meta{
                name = Name,
                type = Type,
                mode = 8#775,
                owner = User,
                scope = SpaceKey
            },
                scope = SpaceId
            },
%%            ct:print("Doc ~p ~p", [Uuid, Name]),
            {Doc, Name}
        end,

        {Doc1, Name1} = GenerateDoc(?DIRECTORY_TYPE),
        {Doc2, Name2} = GenerateDoc(?DIRECTORY_TYPE),
        {Doc3, Name3} = GenerateDoc(?REGULAR_FILE_TYPE),
        Loc3ID = rpc:call(Worker1, file_location, local_id, [Doc3#document.key]),
        {Doc4, Name4} = GenerateDoc(?REGULAR_FILE_TYPE),
        Loc4ID = rpc:call(Worker1, file_location, local_id, [Doc4#document.key]),

        D1Path = <<"/", SpaceName/binary, "/",  Name1/binary>>,
        D2Path = <<D1Path/binary, "/",  Name2/binary>>,
        D3Path = <<D2Path/binary, "/",  Name3/binary>>,
        D4Path = <<D2Path/binary, "/",  Name4/binary>>,

        Doc1Args = [Doc1, SpaceDoc, undefined, D1Path],
        Doc2Args = [Doc2, Doc1, undefined, D2Path],
        Doc3Args = [Doc3, Doc2, Loc3ID, D3Path],
        Doc4Args = [Doc4, Doc2, Loc4ID, D4Path],

        DocsList = [{1, Doc1Args}, {2, Doc2Args}, {3, Doc3Args}, {4, Doc4Args}],
        DocsKeys = [Doc1#document.key, Doc2#document.key, Doc3#document.key, Doc4#document.key],

        % to allow adding location and link before document
        test_utils:mock_expect([Worker1], file_meta, get_scope_id,
            fun(Arg) ->
                case lists:member(Arg, DocsKeys) of
                    true ->
                        {ok, SpaceDoc#document.key};
                    _ ->
                        meck:passthrough([Arg])
                end
            end),

        lists:foreach(fun
            ({sleep, Sek}) ->
                timer:sleep(timer:seconds(Sek));
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
            {create_doc, 2},
            {sleep, SleepTime},
            {set_parent_link, 2},
            {sleep, SleepTime},
            {create_doc, 3},
            {sleep, SleepTime},
            {set_parent_link, 3},
            {sleep, SleepTime},
            {create_location, 3},
            {sleep, SleepTime}
        ],

        _T2 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {create_doc, 2},
            {set_parent_link, 2},
            {create_doc, 3},
            {set_parent_link, 3},
            {create_location, 3}
        ],

        _T3 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {create_doc, 3},
            {set_parent_link, 3},
            {create_location, 3},
            {sleep, SleepTime},
            {create_doc, 2},
            {set_parent_link, 2}
        ],

        _T4 = [
            {create_doc, 3},
            {set_parent_link, 3},
            {create_location, 3},
            {sleep, SleepTime},
            {create_doc, 2},
            {set_parent_link, 2},
            {sleep, SleepTime},
            {create_doc, 1},
            {set_parent_link, 1}
        ],

        _T5 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {create_doc, 2},
            {set_parent_link, 2},
            {set_parent_link, 3},
            {sleep, SleepTime},
            {create_doc, 3},
            {sleep, SleepTime},
            {create_location, 3}
        ],

        _T6 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {create_doc, 2},
            {set_parent_link, 2},
            {set_parent_link, 3},
            {create_location, 3},
            {sleep, SleepTime},
            {create_doc, 3},
            {sleep, SleepTime}
        ],

        _T7 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {create_doc, 2},
            {set_parent_link, 2},
            {set_parent_link, 3},
            {sleep, SleepTime},
            {create_doc, 3},
            {sleep, SleepTime},
            {create_location, 3}
        ],

        _T8 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {create_doc, 2},
            {set_parent_link, 2},
            {set_parent_link, 3},
            {create_location, 3},
            {sleep, SleepTime},
            {create_doc, 3}
        ],

        _T9 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {create_doc, 2},
            {set_parent_link, 2},
            {create_doc, 3},
            {create_location, 3},
            {sleep, SleepTime},
            {set_parent_link, 3}
        ],

        _T10 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {create_doc, 2},
            {set_parent_link, 2},
            {create_doc, 3},
            {set_parent_link, 3},
            {create_location, 3},
            {sleep, SleepTime}
        ],

        _T11 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {create_doc, 2},
            {set_parent_link, 2},
            {create_doc, 3},
            {set_parent_link, 3},
            {sleep, SleepTime},
            {create_location, 3}
        ],

        _T12 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {create_doc, 2},
            {set_parent_link, 2},
            {sleep, SleepTime},
            {create_location, 3},
            {sleep, SleepTime},
            {set_parent_link, 3},
            {sleep, SleepTime},
            {create_doc, 3}
        ],

        _T13 = [
            {sleep, SleepTime},
            {create_location, 3},
            {sleep, SleepTime},
            {set_parent_link, 3},
            {sleep, SleepTime},
            {create_doc, 3},
            {sleep, SleepTime},
            {set_parent_link, 2},
            {sleep, SleepTime},
            {create_doc, 2},
            {sleep, SleepTime},
            {set_parent_link, 1},
            {sleep, SleepTime},
            {create_doc, 1}
        ],

        _T14 = [
            {create_doc, 1},
            {set_parent_link, 1},
            {create_doc, 2},
            {set_parent_link, 2},
            {create_doc, 3},
            {sleep, SleepTime},
            {create_doc, 4, Worker3},
            {set_parent_link, 4, Worker3},
            {create_location, 4, Worker3},
            {sleep, SleepTime},
            {set_parent_link, 3},
            {create_location, 3}
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
        test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, datastore_links_tree_order, 100),
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1)),
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(1)) % TODO - change to 2 seconds
    end, ?config(op_worker_nodes, Config)),

    ssl:start(),
    hackney:start(),
    initializer:disable_quota_limit(Config),
    NewConfig = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),

    % time to react for changes done by initializer
    timer:sleep(5000),

    NewConfig.

teardown_env(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    hackney:stop(),
    ssl:stop().


%%%===================================================================
%%% Internal functions
%%%===================================================================

get_links(W, FileUuid) ->
    rpc:call(W, ?MODULE, get_links, [FileUuid]).

get_links(FileUuid) ->
    try
        Fun = fun(#link{name = Name, target = Target}, Acc) ->
            {ok, maps:put(Name, Target, Acc)}
        end,
        Ctx = datastore_model_default:get_ctx(file_meta),
        {ok, Links} = datastore_model:fold_links(Ctx, FileUuid, all, Fun, #{}, #{}),
        Links
    catch
        _:Reason ->
            ct:print("Get links failed: ~p~n~p", [Reason, erlang:get_stacktrace()]),
            #{}
    end.

count_links(W, FileUuid) ->
    Links = get_links(W, FileUuid),
    maps:size(Links).

verify_locations(W, FileUuid, SpaceId) ->
    IDs = get_locations(W, FileUuid, SpaceId),
    lists:foldl(fun(ID, Acc) ->
        case rpc:call(W, file_location, get, [ID]) of
            {ok, _} ->
                Acc + 1;
            _ -> Acc
        end
    end, 0, IDs).

get_locations(W, FileUuid, SpaceId) ->
    FileCtx = file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid, SpaceId)),
    {LocationIds, _} = rpc:call(W, file_ctx, get_file_location_ids, [FileCtx]),
    LocationIds.

create_doc(Doc = #document{value = FileMeta}, #document{key = ParentUuid}, _LocId, _Path) ->
    SpaceId = Doc#document.scope,
    DocWithParentUuid = Doc#document{value = FileMeta#file_meta{parent_uuid = ParentUuid}},
    {ok, FileUuid} = file_meta:save(DocWithParentUuid),
    {ok, _} = times:save(#document{key = FileUuid, value = #times{},
        scope = SpaceId}),
    ok.

set_parent_link(Doc, ParentDoc, _LocId, _Path) ->
    #document{key = ParentUuid} = ParentDoc,
    #document{key = FileUuid, scope = Scope, value = #file_meta{
        name = FileName
    }} = Doc,
    Ctx = datastore_model_default:get_ctx(file_meta),
    Ctx2 = Ctx#{scope => Scope},
    TreeId = oneprovider:get_id(),
    Link = {FileName, FileUuid},
    {ok, _} = datastore_model:add_links(Ctx2, ParentUuid, TreeId, Link),
    ok.

create_location(Doc, _ParentDoc, LocId, Path) ->
    FDoc = Doc#document.value,
    FileUuid = Doc#document.key,
    SpaceId = Doc#document.scope,
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(FDoc#file_meta.scope),

    {ok, #document{key = StorageId}} = fslogic_storage:select_storage(SpaceId),
    FileId = Path,
    Location = #file_location{
        blocks = [#file_block{offset = 0, size = 3}],
        size = 3,
        provider_id = oneprovider:get_id(),
        file_id = FileId,
        storage_id = StorageId,
        uuid = FileUuid,
        space_id = SpaceId,
        storage_file_created = true
    },

    Ctx = datastore_model_default:get_ctx(file_location),
    {ok, _} = datastore_model:save(Ctx, #document{
        key = LocId,
        value = Location,
        scope = SpaceId
    }),

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
    SFMHandle3 = storage_file_manager:set_size(SFMHandle2),
    {ok, 3} = storage_file_manager:write(SFMHandle3, 0, <<"abc">>),
    storage_file_manager:fsync(SFMHandle3, false),
    ok.

extend_config(Config, User, {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts) ->
    ProxyNodesWritten = ProxyNodesWritten0 * NodesOfProvider,
    Workers = ?config(op_worker_nodes, Config),
    {Worker1, Workers1, WorkersNot1, Workers2} = lists:foldl(fun(W, {Acc1, Acc2, Acc3, Acc4}) ->
        {NAcc2, NAcc3} = case string:str(atom_to_list(W), "p1") of
            0 -> {Acc2, [W | Acc3]};
            _ -> {[W | Acc2], Acc3}
        end,

        NAcc4 = case string:str(atom_to_list(W), "p2") of
            0 -> Acc4;
            _ -> [W | Acc4]
        end,

        case is_atom(Acc1) of
            true ->
                {Acc1, NAcc2, NAcc3, NAcc4};
            _ ->
                case string:str(atom_to_list(W), "p1") of
                    0 -> {Acc1, NAcc2, NAcc3, NAcc4};
                    _ -> {W, NAcc2, NAcc3, NAcc4}
                end
        end
    end, {[], [], [], []}, Workers),

    SessId = fun(W) -> ?config({session_id, {User, ?GET_DOMAIN(W)}}, Config) end,
    [{_SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),
    [{worker1, Worker1}, {workers1, Workers1}, {workers_not1, WorkersNot1}, {workers2, Workers2},
        {session, SessId}, {space_name, SpaceName}, {attempts, Attempts},
        {nodes_number, {SyncNodes, ProxyNodes, ProxyNodesWritten, ProxyNodesWritten0, NodesOfProvider}} | Config].

verify(Config, TestFun) ->
    Workers = ?config(op_worker_nodes, Config),
    verify_workers(Workers, TestFun).

verify_workers(Workers, TestFun) ->
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
        FileUuid = fslogic_uuid:guid_to_uuid(FileGuid),
        {FileUuid, rpc:call(W, file_meta, get, [FileUuid])}
    end),

    NotFoundList = lists:filter(fun({_, {error, not_found}}) -> true; (_) -> false end, VerAns),
    OKList = lists:filter(fun({_, {ok, _}}) -> true; (_) -> false end, VerAns),

    ?assertEqual(ProxyNodes - ProxyNodesWritten, length(NotFoundList)),
    ?assertEqual(SyncNodes + ProxyNodesWritten, length(OKList)),
    [{FileUuidAns, _} | _] = VerAns,
    FileUuidAns.

create_file_on_worker(Config, FileBeg, Offset, File, WriteWorker) ->
    create_file_on_worker(Config, FileBeg, Offset, File, WriteWorker, 0).

create_file_on_worker(Config, FileBeg, Offset, File, WriteWorker, CreateAttempts) ->
    SessId = ?config(session, Config),

    case CreateAttempts of
        0 ->
            ?assertMatch({ok, _}, lfm_proxy:create(WriteWorker,
                SessId(WriteWorker), File, 8#755));
        _ ->
            ?assertMatch({ok, _}, lfm_proxy:create(WriteWorker,
                SessId(WriteWorker), File, 8#755), CreateAttempts)
    end,
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

create_big_file(Config, ChunkSize, ChunksNum, PartNum, File, Worker) ->
    SessId = ?config(session, Config),

    ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId(Worker), File, 8#755)),
    OpenAns = lfm_proxy:open(Worker, SessId(Worker), {path, File}, rdwr),
    ?assertMatch({ok, _}, OpenAns),
    {ok, Handle} = OpenAns,

    BytesChunk = crypto:strong_rand_bytes(ChunkSize),
    Bytes = lists:foldl(fun(_, Acc) ->
        <<Acc/binary, BytesChunk/binary>>
    end, <<>>, lists:seq(1, ChunksNum)),

    PartSize = ChunksNum * ChunkSize,
    lists:foreach(fun(Num) ->
        ?assertEqual({ok, PartSize}, lfm_proxy:write(Worker, Handle, Num * PartSize, Bytes))
    end, lists:seq(0, PartNum - 1)),

    ?assertEqual(ok, lfm_proxy:close(Worker, Handle)).

read_big_file(Config, _FileSize, File, Worker, true) ->
    SessId = ?config(session, Config),
    Attempts = ?config(attempts, Config),
    Worker1 = ?config(worker1, Config),

    ProviderId = rpc:call(Worker, oneprovider, get_id_or_undefined, []),
    Start = os:timestamp(),
    {ok, TransferID} = ?assertMatch({ok, _},
        lfm_proxy:schedule_file_replication(Worker1, SessId(Worker1),
            {path, File}, ProviderId)),
    ?assertMatch({ok, #document{value = #transfer{status = completed}}},
        rpc:call(Worker1, transfer, get, [TransferID]), Attempts),
    timer:now_diff(os:timestamp(), Start);
read_big_file(Config, FileSize, File, Worker, _) ->
    SessId = ?config(session, Config),
    Attempts = ?config(attempts, Config),
    read_big_file_loop(FileSize, File, Worker, SessId, Attempts, undefined, 0).

read_big_file_loop(_FileSize, _File, _Worker, _SessId, _, {{ok, _Time, true}, ok}, TimeSum) ->
    TimeSum;
read_big_file_loop(_FileSize, _File, _Worker, _SessId, 0, LastAns, _TimeSum) ->
    LastAns;
read_big_file_loop(FileSize, File, Worker, SessId, Attempts, _LastAns, TimeSum) ->
    Ans = case lfm_proxy:open(Worker, SessId(Worker), {path, File}, rdwr) of
        {ok, Handle} ->
            ReadAns = try
                Start = os:timestamp(),
                {ok, Bytes} = lfm_proxy:read(Worker, Handle, 0, FileSize),
                {ok, timer:now_diff(os:timestamp(), Start), FileSize == size(Bytes)}
            catch
                E1:E2 ->
                    {read, E1, E2}
            end,

            {ReadAns, lfm_proxy:close(Worker, Handle)};
        OpenError ->
            {open_error, OpenError}
    end,
    case Ans of
        {{ok, Time, true}, _} ->
            read_big_file_loop(FileSize, File, Worker, SessId, Attempts - 1,
                Ans, TimeSum + Time);
        _ ->
            timer:sleep(timer:seconds(1)),
            read_big_file_loop(FileSize, File, Worker, SessId, Attempts - 1,
                Ans, TimeSum)
    end.

create_file(Config, FileBeg, {Offset, File}) ->
    Worker1 = ?config(worker1, Config),
    create_file_on_worker(Config, FileBeg, Offset, File, Worker1).

verify_file(Config, FileBeg, {Offset, File}) ->
    SessId = ?config(session, Config),
    Attempts = ?config(attempts, Config),
    {SyncNodes, ProxyNodes, ProxyNodesWritten, _ProxyNodesWritten0, _NodesOfProvider} =
        ?config(nodes_number, Config),

    Offset2 = Offset rem 5 + 1,
    Size = 2*Offset2,
    FileUuid = verify_stats(Config, File, false),

    Beg = binary:part(FileBeg, 0, Offset2),
    End = binary:part(File, 0, Offset2),
    FileCheck = <<Beg/binary, End/binary>>,

    VerifyLocation = fun() ->
        verify(Config, fun(W) ->
            [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
            verify_locations(W, FileUuid, SpaceId)
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
        LocationsList = lists:filter(fun(S) -> S >= 1 end, Flattened),
%%            ct:print("Locations1 ~p", [{{length(ZerosList), length(LocationsList)}, ToMatch}]),
        {length(ZerosList), length(LocationsList)}
    end,
    ?match(ToMatch, AssertLocations(), Attempts),

    LocToAns = verify(Config, fun(W) ->
        StatAns = lfm_proxy:stat(W, SessId(W), {path, File}),
        ?assertMatch({ok, #file_attr{}}, StatAns),
        {ok, #file_attr{guid = FileGuid}} = StatAns,
        FileUuid = fslogic_uuid:guid_to_uuid(FileGuid),

        [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
        {W, get_locations(W, FileUuid, SpaceId)}
    end),
    {File, FileUuid, LocToAns}.

verify_del(Config, {F, FileUuid, _Locations}) ->
    SessId = ?config(session, Config),
    Attempts = ?config(attempts, Config),

    verify(Config, fun(W) ->
%%            ct:print("Del ~p", [{W, F,  FileUuid, Locations}]),
%%            ?match({error, ?ENOENT}, lfm_proxy:stat(W, SessId(W), {path, F}), Attempts)
        % TODO - match to chosen error (check perms may also result in ENOENT)
        ?match({error, _}, lfm_proxy:stat(W, SessId(W), {path, F}), Attempts),

        ?match({error, not_found}, rpc:call(W, file_meta, get, [FileUuid]), Attempts)
%%        ?match(0, count_links(W, FileUuid), Attempts),
%%
%%        lists:foreach(fun(Location) ->
%%            ?match({error, not_found},
%%                rpc:call(W, file_meta, get, [Location]), Attempts)
%%        end, proplists:get_value(W, Locations, []))
    end).

verify_dir_size(Config, DirToCheck, DSize) ->
    SessId = ?config(session, Config),
    Attempts = ?config(attempts, Config),
    {SyncNodes, ProxyNodes, ProxyNodesWritten, _ProxyNodesWritten0, _NodesOfProvider} = ?config(nodes_number, Config),

    VerAns0 = verify(Config, fun(W) ->
        CountChilden = fun() ->
            LSAns = lfm_proxy:ls(W, SessId(W), {path, DirToCheck}, 0, 20000),
            ?assertMatch({ok, _}, LSAns),
            {ok, ListedDirs} = LSAns,
            length(ListedDirs)
        end,
        ?match(DSize, CountChilden(), Attempts),

        StatAns = lfm_proxy:stat(W, SessId(W), {path, DirToCheck}),
        ?assertMatch({ok, #file_attr{}}, StatAns),
        {ok, #file_attr{guid = FileGuid}} = StatAns,
        FileUuid = fslogic_uuid:guid_to_uuid(FileGuid),
        {W, FileUuid}
    end),

    AssertLinks = fun() ->
        VerAns = lists:map(fun({W, Uuid}) ->
            count_links(W, Uuid)
        end, VerAns0),
%%        ct:print("Links ~lp", [{DSize, VerAns}]),

        ZerosList = lists:filter(fun(S) -> S == 0 end, VerAns),
        SList = lists:filter(fun(S) -> S == DSize end, VerAns),

        {length(ZerosList), length(SList)}
    end,
    ToMatch = case DSize of
        0 ->
            {ProxyNodes + SyncNodes, ProxyNodes + SyncNodes};
        _ ->
            {ProxyNodes - ProxyNodesWritten, SyncNodes + ProxyNodesWritten}
    end,
    ?match(ToMatch, AssertLinks(), Attempts).
