%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of db_sync
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
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    db_sync_test/1, proxy_test/1
]).

all() ->
    ?ALL([
        db_sync_test, proxy_test
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
    synchronization_test_base(Config, <<"user1">>, true, 15).

proxy_test(Config) ->
    synchronization_test_base(Config, <<"user2">>, false, 0),
    synchronization_test_base(Config, <<"user3">>, false, 0).

synchronization_test_base(Config, User, Multisupport, Attempts) ->
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, User}, Config),
ct:print("w1 ~p", [Worker1]),
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
    end, lists:seq(1,100)),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId, Dir, 8#755)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId, Level2Dir, 8#755)),

    VerifyStats = fun(File, IsDir) ->
        VerAns = Verify(fun(W) ->
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
        ct:print("ccc ~p", [{File, IsDir, VerAns, length(Workers)}]),
        NotFoundList = lists:filter(fun({error, {not_found, _}}) -> true; (_) -> false end, VerAns),
        OKList = lists:filter(fun({ok, _}) -> true; (_) -> false end, VerAns),

        case Multisupport of
            true ->
                ?assertEqual(0, length(NotFoundList)),
                ?assertEqual(length(Workers), length(OKList));
            _ ->
                ?assertEqual(length(Workers) div 2, length(NotFoundList)),
                ?assertEqual(length(Workers) div 2, length(OKList))
        end
    end,
    VerifyStats(Dir, true),
    VerifyStats(Level2Dir, true),

    FileBeg = <<"1234567890abcd">>,
    CreateFile = fun({Offset, File}) ->
        ?assertMatch({ok, _}, lfm_proxy:create(Worker1, SessId, File, 8#755)),
        OpenAns = lfm_proxy:open(Worker1, SessId, {path, File}, rdwr),
        ?assertMatch({ok, _}, OpenAns),
        {ok, Handle} = OpenAns,
        FileBegSize = size(FileBeg),
        ?assertMatch({ok, FileBegSize}, lfm_proxy:write(Worker1, Handle, 0, FileBeg)),
        Size = size(File),
        Offset2 = Offset rem 5 + 1,
        ?assertMatch({ok, Size}, lfm_proxy:write(Worker1, Handle, Offset2, File)),
        ?assertMatch(ok, lfm_proxy:truncate(Worker1, SessId, {path, File}, 2*Offset2))
    end,
    CreateFile({2, Level2File}),

    VerifyFile = fun({Offset, File}) ->
        Offset2 = Offset rem 5 + 1,
        Size = 2*Offset2,
        VerifyStats(File, false),

        Beg = binary:part(FileBeg, 0, Offset2),
        End = binary:part(File, 0, Offset2),
        FileCheck = <<Beg/binary, End/binary>>,

        VerAns = Verify(fun(W) ->
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
        end),
        Flattened = lists:flatten(VerAns),
        ct:print("bbb ~p", [{Offset, File, VerAns, Flattened, length(Workers)}]),
        ZerosList = lists:filter(fun(S) -> S == 0 end, Flattened),
        LocationsList = lists:filter(fun(S) -> S == 1 end, Flattened),
%TODO what if locations will not appear with file_meta?
        case Multisupport of
            true ->
                ?assertEqual(2*length(Workers), length(ZerosList) + length(LocationsList)),
                ?assert(length(Workers) * 3 div 2 =< length(LocationsList));
            _ ->
                ?assertEqual(length(Workers) * 3 div 2, length(ZerosList)),
                ?assertEqual(length(Workers) div 2, length(LocationsList))
        end,

        Verify(fun(W) ->
            ct:print("xxx ~p", [W]),
            OpenAns = lfm_proxy:open(W, SessId, {path, File}, rdwr),
            ?assertMatch({ok, _}, OpenAns),
            {ok, Handle} = OpenAns,
            ?assertMatch({ok, FileCheck}, lfm_proxy:read(W, Handle, 0, Size))
        end)
    end,
    VerifyFile({2, Level2File}),

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
        ct:print("aaa ~p", [{DSize, Deleted, VerAns, Flattened, length(Workers)}]),
        ZerosList = lists:filter(fun(S) -> S == 0 end, Flattened),
        SList = lists:filter(fun(S) -> S == 2*DSize + Deleted + 1 end, Flattened),

        case Multisupport of
            true ->
                ?assertEqual(length(Workers), length(ZerosList)),
                ?assertEqual(length(Workers), length(SList));
            _ ->
                ?assertEqual(length(Workers) * 3 div 2, length(ZerosList)),
                ?assertEqual(length(Workers) div 2, length(SList))
        end
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
    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    initializer:setup_storage(ConfigWithNodes).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    application:start(ssl2),
    hackney:start(),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    initializer:enable_grpca_based_communication(Config),
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