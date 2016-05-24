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
-module(massive_multi_provider_file_ops_test_SUITE).
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
    db_sync_test/1
]).

all() ->
    ?ALL([
        db_sync_test
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
    synchronization_test_base(Config, <<"user1">>, true, 30).

synchronization_test_base(Config, User, Multisupport, Attempts) ->
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    timer:sleep(10000), % TODO - connection must appear after mock setup

    SessId = ?config({session_id, User}, Config),
    Prov1ID = rpc:call(Worker1, oneprovider, get_provider_id, []),
    Prov2ID = lists:foldl(fun(Worker, Acc) ->
        case rpc:call(Worker, oneprovider, get_provider_id, []) of
            Prov1ID ->
                Acc;
            ProvID ->
                ProvID
        end
    end, Prov1ID, Workers),

    Verify = fun(TestFun) ->
        lists:foldl(fun(W, Acc) ->
            ct:print("qqqq ~p", [W]),
            [TestFun(W) | Acc]
        end, [], Workers)
    end,

    Dir = generator:gen_name(),
    Level2Dir = <<Dir/binary, "/", (generator:gen_name())/binary>>,
    Level2File = <<Dir/binary, "/", (generator:gen_name())/binary>>,

    Level3Dirs = lists:map(fun(_) ->
        <<Level2Dir/binary, "/", (generator:gen_name())/binary>>
    end, lists:seq(1,10)),
    Level3Dirs2 = lists:map(fun(_) ->
        <<Level2Dir/binary, "/", (generator:gen_name())/binary>>
    end, lists:seq(1,10)),

    Level3Dir = <<Level2Dir/binary, "/", (generator:gen_name())/binary>>,
    Level4Files = lists:map(fun(Num) ->
        {Num, <<Level3Dir/binary, "/", (generator:gen_name())/binary>>}
    end, lists:seq(1,50)),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId, Dir, 8#755)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId, Level2Dir, 8#755)),

    VerifyStats = fun(File, IsDir) ->
        VerAns = Verify(fun(W) ->
            ct:print("aaaa ~p", [{File, IsDir, W}]),
            case IsDir of
                true ->
                    ?match({ok, #file_attr{type = ?DIRECTORY_TYPE}},
                        lfm_proxy:stat(W, SessId, {path, File}), Attempts);
                _ ->
                    ?match({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
                        lfm_proxy:stat(W, SessId, {path, File}), Attempts)
            end,
            StatAns = lfm_proxy:stat(W, SessId, {path, File}),
            {ok, #file_attr{uuid = FileGUID}} = StatAns,
            FileUUID = fslogic_uuid:file_guid_to_uuid(FileGUID),
            rpc:call(W, file_meta, get, [FileUUID])
        end),

        NotFoundList = lists:filter(fun({error, {not_found, _}}) -> true; (_) -> false end, VerAns),
        OKList = lists:filter(fun({ok, _}) -> true; (_) -> false end, VerAns),

%%        case Multisupport of
%%            true ->
%%                ?assertEqual(0, length(NotFoundList)),
%%                ?assertEqual(length(Workers), length(OKList));
%%            _ ->
%%                ?assertEqual(length(Workers) div 2, length(NotFoundList)),
%%                ?assertEqual(length(Workers) div 2, length(OKList))
%%        end
                  ok
    end,
    VerifyStats(Dir, true),
    VerifyStats(Level2Dir, true),

    FileBeg = <<"1234567890abcd">>,
    CreateFileOnWorker = fun(Offset, File, WriteWorker) ->
        ?assertMatch({ok, _}, lfm_proxy:create(WriteWorker, SessId, File, 8#755)),
        OpenAns = lfm_proxy:open(WriteWorker, SessId, {path, File}, rdwr),
        ?assertMatch({ok, _}, OpenAns),
        {ok, Handle} = OpenAns,
        FileBegSize = size(FileBeg),
        ?assertMatch({ok, FileBegSize}, lfm_proxy:write(WriteWorker, Handle, 0, FileBeg)),
        Size = size(File),
        Offset2 = Offset rem 5 + 1,
        ?assertMatch({ok, Size}, lfm_proxy:write(WriteWorker, Handle, Offset2, File)),
        ?assertMatch(ok, lfm_proxy:truncate(WriteWorker, SessId, {path, File}, 2*Offset2))
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
                StatAns = lfm_proxy:stat(W, SessId, {path, File}),
                ?assertMatch({ok, #file_attr{}}, StatAns),
                {ok, #file_attr{uuid = FileGUID}} = StatAns,
                FileUUID = fslogic_uuid:file_guid_to_uuid(FileGUID),

                S1 = case rpc:call(W, file_meta, get, [links_utils:links_doc_key(FileUUID, Prov1ID)]) of
                         {error, {not_found, _}} ->
                             0;
                         Get1Ans ->
                             ?assertMatch({ok, #document{value = #links{}}}, Get1Ans),
                             {ok, #document{value = Links1}} = Get1Ans,
                             verify_locations(W, Links1)
                     end,
                S2 = case rpc:call(W, file_meta, get, [links_utils:links_doc_key(FileUUID, Prov2ID)]) of
                         {error, {not_found, _}} ->
                             0;
                         Get2Ans ->
                             ?assertMatch({ok, #document{value = #links{}}}, Get2Ans),
                             {ok, #document{value = Links2}} = Get2Ans,
                             verify_locations(W, Links2)
                     end,
                [S1, S2]
            end)
        end,

        VerAns0 = VerifyLocation(),
        ct:print("Locations0 ~p", [{Offset, File, VerAns0}]),

        Verify(fun(W) ->
            OpenAns = lfm_proxy:open(W, SessId, {path, File}, rdwr),
            ?assertMatch({ok, _}, OpenAns),
            {ok, Handle} = OpenAns,
            ?match({ok, FileCheck}, lfm_proxy:read(W, Handle, 0, Size), Attempts)
        end),

        VerAns = VerifyLocation(),
        Flattened = lists:flatten(VerAns),
        ct:print("Locations1 ~p", [{Offset, File, VerAns}]),

        ZerosList = lists:filter(fun(S) -> S == 0 end, Flattened),
        LocationsList = lists:filter(fun(S) -> S == 1 end, Flattened),

%%        case Multisupport of
%%            true ->
%%                ?assertEqual(2*length(Workers), length(ZerosList) + length(LocationsList)),
%%                ?assert(length(Workers) * 3 div 2 =< length(LocationsList));
%%            _ ->
%%                ?assertEqual(length(Workers) * 3 div 2, length(ZerosList)),
%%                ?assertEqual(length(Workers) div 2, length(LocationsList))
%%        end
                 ok
    end,
    VerifyFile({2, Level2File}),

    ct:print("zzzzz ~p", [Workers]),
    lists:foreach(fun(W) ->
        ct:print("xxxxx ~p", [W]),
        Level2TmpDir = <<Dir/binary, "/", (generator:gen_name())/binary>>,
        ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, Level2TmpDir, 8#755)),
        VerifyStats(Level2TmpDir, true),

        lists:foreach(fun(W2) ->
            ct:print("yyyyy ~p", [W2]),
            Level3TmpDir = <<Level2TmpDir/binary, "/", (generator:gen_name())/binary>>,
            ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, Level3TmpDir, 8#755)),
            VerifyStats(Level3TmpDir, true)
        end, Workers)
    end, Workers),

    lists:map(fun(D) ->
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId, D, 8#755))
    end, Level3Dirs),

    lists:map(fun(D) ->
        VerifyStats(D, true)
    end, Level3Dirs),

    lists:map(fun(D) ->
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId, D, 8#755))
    end, Level3Dirs2),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId, Level3Dir, 8#755)),
    lists:map(fun(F) ->
        CreateFile(F)
    end, Level4Files),

    VerifyDirSize = fun(DirToCheck, DSize, Deleted) ->
        VerAns = Verify(fun(W) ->
            CountChilden = fun() ->
                LSAns = lfm_proxy:ls(W, SessId, {path, DirToCheck}, 0, 200),
                ?assertMatch({ok, _}, LSAns),
                {ok, ListerDirs} = LSAns,
                length(ListerDirs)
            end,
            ?match(DSize, CountChilden(), Attempts),

            StatAns = lfm_proxy:stat(W, SessId, {path, DirToCheck}),
            ?assertMatch({ok, #file_attr{}}, StatAns),
            {ok, #file_attr{uuid = FileGUID}} = StatAns,
            FileUUID = fslogic_uuid:file_guid_to_uuid(FileGUID),
            S1 = case rpc:call(W, file_meta, get, [links_utils:links_doc_key(FileUUID, Prov1ID)]) of
                {error, {not_found, _}} ->
                    0;
                Get1Ans ->
                    ?assertMatch({ok, #document{value = #links{}}}, Get1Ans),
                    {ok, #document{value = Links1}} = Get1Ans,
                    count_links(W, Links1)
            end,
            S2 = case rpc:call(W, file_meta, get, [links_utils:links_doc_key(FileUUID, Prov2ID)]) of
                {error, {not_found, _}} ->
                     0;
                Get2Ans ->
                    ?assertMatch({ok, #document{value = #links{}}}, Get2Ans),
                    {ok, #document{value = Links2}} = Get2Ans,
                    count_links(W, Links2)
            end,
            [S1, S2]
        end),
        Flattened = lists:flatten(VerAns),
        ct:print("Links ~p", [{DSize, Deleted, VerAns}]),

        ZerosList = lists:filter(fun(S) -> S == 0 end, Flattened),
        SList = lists:filter(fun(S) -> S == 2*DSize + Deleted + 1 end, Flattened),

%%        case Multisupport of
%%            true ->
%%                ?assertEqual(length(Workers), length(ZerosList)),
%%                ?assertEqual(length(Workers), length(SList));
%%            _ ->
%%                ?assertEqual(length(Workers) * 3 div 2, length(ZerosList)),
%%                ?assertEqual(length(Workers) div 2, length(SList))
%%        end
                    ok
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
        ?assertMatch(ok, lfm_proxy:unlink(Worker1, SessId, {path, F}))
    end, Level4Files),
    lists:map(fun(D) ->
        ?assertMatch(ok, lfm_proxy:unlink(Worker1, SessId, {path, D}))
    end, Level3Dirs2),

    lists:map(fun({_, F}) ->
        Verify(fun(W) -> ?match({error, ?ENOENT}, lfm_proxy:stat(W, SessId, {path, F}), Attempts) end)
    end, Level4Files),
    lists:map(fun(D) ->
        Verify(fun(W) -> ?match({error, ?ENOENT}, lfm_proxy:stat(W, SessId, {path, D}), Attempts) end)
    end, Level3Dirs2),
    VerifyDirSize(Level3Dir, 0, length(Level4Files)),
    VerifyDirSize(Level2Dir, length(Level3Dirs) + 1, length(Level3Dirs2)),

    lists:foreach(fun(W) ->
        Level2TmpFile = <<Dir/binary, "/", (generator:gen_name())/binary>>,
        CreateFileOnWorker(4, Level2TmpFile, W),
        VerifyFile({4, Level2TmpFile})
    end, Workers),

    lists:foldl(fun(W, Acc) ->
        OpenAns = lfm_proxy:open(W, SessId, {path, Level2File}, rdwr),
        ?assertMatch({ok, _}, OpenAns),
        {ok, Handle} = OpenAns,
        WriteBuf = atom_to_binary(W, utf8),
        Offset = size(Acc),
        ?assertMatch({ok, FileBegSize}, lfm_proxy:write(W, Handle, Offset, WriteBuf)),
        NewAcc = <<Acc/binary, WriteBuf/binary>>,

        Verify(fun(W2) ->
            OpenAns2 = lfm_proxy:open(W2, SessId, {path, Level2File}, rdwr),
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
            MkAns = lfm_proxy:mkdir(W, SessId, Level2TmpDir, 8#755),
            Master ! {mkdir_ans, Level2TmpDir, MkAns}
                   end)
                  end, Workers),

    Level2TmpDirs = lists:foldl(fun(_, Acc) ->
        MkAnsCheck =
            receive
                {mkdir_ans, ReceivedLevel2TmpDir, MkAns} ->
                    {ReceivedLevel2TmpDir, MkAns}
            after timer:seconds(2*Attempts) ->
                {error, timeout}
            end,
        ?assertMatch({_, {ok, _}}, MkAnsCheck),
        {Level2TmpDir, _} = MkAnsCheck,
        VerifyStats(Level2TmpDir, true),
        [Level2TmpDir | Acc]
                                end, [], Workers),

    lists:foreach(fun(Level2TmpDir) ->
        lists:foreach(fun(W2) ->
            spawn_link(fun() ->
                Level3TmpDir = <<Level2TmpDir/binary, "/", (generator:gen_name())/binary>>,
                MkAns = lfm_proxy:mkdir(W2, SessId, Level3TmpDir, 8#755),
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
                after timer:seconds(2*Attempts) ->
                    {error, timeout}
                end,
            ?assertMatch({_, {ok, _}}, MkAnsCheck),
            {Level3TmpDir, _} = MkAnsCheck,
            VerifyStats(Level3TmpDir, true)
                      end, Workers)
                  end, Level2TmpDirs),

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
    initializer:enable_grpca_based_communication(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_, Config) ->
    lfm_proxy:teardown(Config),
     %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:disable_grpca_based_communication(Config),
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
                ?assertMatch({ok, _}, rpc:call(W, file_location, get, [ID])),
                Acc + 1;
            _ ->
                Acc
        end
    end, 0, Map).