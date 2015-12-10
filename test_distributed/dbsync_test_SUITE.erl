%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of logical_file_manager API.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_test_SUITE).
-author("Rafal Slota").

-include("modules/datastore/datastore.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    global_stream_test/1
]).

-performance({test_cases, []}).
all() -> [
    global_stream_test
].

-define(TIMEOUT, timer:seconds(10)).
-define(req(W, SessId, FuseRequest), rpc:call(W, worker_proxy, call, [fslogic_worker, {fuse_request, SessId, FuseRequest}], ?TIMEOUT)).
-define(lfm_req(W, Method, Args), rpc:call(W, file_manager, Method, Args, ?TIMEOUT)).

%%%====================================================================
%%% Test function
%%%====================================================================


global_stream_test(Config) ->
    [WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, _UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},

    test_utils:mock_new([WorkerP1], dbsync_proto),
    test_utils:mock_expect([WorkerP1], dbsync_proto, send_batch,
        fun(global, BatchToSend) ->
            rpc:call(WorkerP2, dbsync_worker, apply_batch_changes, [undefined, BatchToSend])
        end),

    Dirs = lists:map(
        fun(N) ->
                    NBin = integer_to_binary(N),
                    D0 = <<"dbsync_test_", NBin/binary>>,
%%                    D1 = gen_filename(),
%%                    D2 = gen_filename(),
%%                    D3 = gen_filename(),

                    F = gen_filename(),

                    ct:print("Create ~p", [N]),

                    ok = mkdir(WorkerP1, SessId1, <<"/", D0/binary>>, 8#755),
                    ok = mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary>>, 8#755),
                    ok = mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
                    ok = mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
                    ok = mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
                    ok =    mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
                    ok =    mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
                    ok =    mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
                    ok =    mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
                    ok =    mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),

%%                    create(WorkerP1, SessId1, <<"/", F/binary>>, 8#755),

                    create(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", F/binary>>, 8#755),

            D0
        end, lists:seq(1, 100)),

    timer:sleep(timer:seconds(60)),

    RevPerPath = lists:map(
        fun(D0) ->

            Path1 = <<"/", D0/binary>>,
            Path2 = <<"/", D0/binary, "/", D0/binary>>,
            Path3 = <<"/", D0/binary, "/", D0/binary, "/", D0/binary>>,
            Path4 = <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>,
            Path5 = <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>,
            Path6 = <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>,
            Path7 = <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>,
            Path8 = <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>,
            Path9 = <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>,
            Path10 = <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>,
            Path11 = <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>,


            {ok, #file_attr{uuid = UUID1}} = stat(WorkerP1, SessId1, {path,     Path1}),
            {ok, #file_attr{uuid = UUID2}} = stat(WorkerP1, SessId1, {path,     Path2}),
            {ok, #file_attr{uuid = UUID3}} = stat(WorkerP1, SessId1, {path,     Path3}),
            {ok, #file_attr{uuid = UUID4}} = stat(WorkerP1, SessId1, {path,     Path4}),
            {ok, #file_attr{uuid = UUID5}} = stat(WorkerP1, SessId1, {path,     Path5}),
            {ok, #file_attr{uuid = UUID6}} = stat(WorkerP1, SessId1, {path,     Path6}),
            {ok, #file_attr{uuid = UUID7}} = stat(WorkerP1, SessId1, {path,     Path7}),
            {ok, #file_attr{uuid = UUID8}} = stat(WorkerP1, SessId1, {path,     Path8}),
            {ok, #file_attr{uuid = UUID9}} = stat(WorkerP1, SessId1, {path,     Path9}),
            {ok, #file_attr{uuid = UUID10}} = stat(WorkerP1, SessId1, {path,    Path10}),
            {ok, #file_attr{uuid = UUID11}} = stat(WorkerP1, SessId1, {path,    Path11}),

            {ok, #document{rev = Rev1}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID1]),
            {ok, #document{rev = Rev2}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID2]),
            {ok, #document{rev = Rev3}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID3]),
            {ok, #document{rev = Rev4}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID4]),
            {ok, #document{rev = Rev5}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID5]),
            {ok, #document{rev = Rev6}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID6]),
            {ok, #document{rev = Rev7}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID7]),
            {ok, #document{rev = Rev8}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID8]),
            {ok, #document{rev = Rev9}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID9]),
            {ok, #document{rev = Rev10}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID10]),
            {ok, #document{rev = Rev11}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID11]),

            {ok, #document{rev = LRev1}} = rpc:call(WorkerP1, file_meta, get, [<<UUID1/binary, "$$">>]),
            {ok, #document{rev = LRev2}} = rpc:call(WorkerP1, file_meta, get, [<<UUID2/binary, "$$">>]),
            {ok, #document{rev = LRev3}} = rpc:call(WorkerP1, file_meta, get, [<<UUID3/binary, "$$">>]),
            {ok, #document{rev = LRev4}} = rpc:call(WorkerP1, file_meta, get, [<<UUID4/binary, "$$">>]),
            {ok, #document{rev = LRev5}} = rpc:call(WorkerP1, file_meta, get, [<<UUID5/binary, "$$">>]),
            {ok, #document{rev = LRev6}} = rpc:call(WorkerP1, file_meta, get, [<<UUID6/binary, "$$">>]),
            {ok, #document{rev = LRev7}} = rpc:call(WorkerP1, file_meta, get, [<<UUID7/binary, "$$">>]),
            {ok, #document{rev = LRev8}} = rpc:call(WorkerP1, file_meta, get, [<<UUID8/binary, "$$">>]),
            {ok, #document{rev = LRev9}} = rpc:call(WorkerP1, file_meta, get, [<<UUID9/binary, "$$">>]),
            {ok, #document{rev = LRev10}} = rpc:call(WorkerP1, file_meta, get, [<<UUID10/binary, "$$">>]),
            {ok, #document{rev = LRev11}} = rpc:call(WorkerP1, file_meta, get, [<<UUID11/binary, "$$">>]),

            Map0 = #{},
            Map1 = maps:put(Path1, {UUID1,    Rev1,   LRev1}, Map0),
            Map2 = maps:put(Path2, {UUID2,    Rev2,   LRev2}, Map1),
            Map3 = maps:put(Path3, {UUID3,    Rev3,   LRev3}, Map2),
            Map4 = maps:put(Path4, {UUID4,    Rev4,   LRev4}, Map3),
            Map5 = maps:put(Path5, {UUID5,    Rev5,   LRev5}, Map4),
            Map6 = maps:put(Path6, {UUID6,    Rev6,   LRev6}, Map5),
            Map7 = maps:put(Path7, {UUID7,    Rev7,   LRev7}, Map6),
            Map8 = maps:put(Path8, {UUID8,    Rev8,   LRev8}, Map7),
            Map9 = maps:put(Path9, {UUID9,    Rev9,   LRev9}, Map8),
            Map10 = maps:put(Path10, {UUID10,    Rev10,   LRev10}, Map9),
            _Map11 = maps:put(Path11, {UUID11,    Rev11,   LRev11}, Map10)
        end, Dirs),

    lists:foreach(
        fun(PathMap) ->
            ct:print("                                                                         "),
            ct:print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"),
            ct:print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"),
            ct:print("                                                                         "),
            lists:foreach(
                fun({Path, {UUID, Rev, LRev}}) ->
                    LocalRev =
                        case rpc:call(WorkerP2, datastore, get, [disk_only, file_meta, UUID]) of
                            {ok, #document{rev = Rev1}} ->
                                Rev1;
                            {error, Reason1} ->
                              Reason1
                        end,
                    LocalLRev =
                        case rpc:call(WorkerP2, file_meta, get, [<<UUID/binary, "$$">>]) of
                            {ok, #document{rev = LRev2}} ->
                                LRev2;
                            {error, Reason2} ->
                                Reason2
                        end,
                    ct:print("Check UUID ~p: rev ~p should be ~p, link rev ~p should be ~p", [UUID, LocalRev, Rev, LocalLRev, LRev]),
                    ct:print("Check UUID ~p via path ~p: ~p", [UUID, Path, stat(WorkerP2, SessId1, {path, Path})])
                end, maps:to_list(PathMap))

%%            ct:print("F21: ~p~n", [stat(WorkerP2, SessId1, {path, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>})]),
%%            ct:print("F22: ~p~n", [stat(WorkerP2, SessId1, {path, <<"/", D0/binary>>})])
        end, RevPerPath),

    ct:print("ls1: ~p~n", [ls(WorkerP1, SessId1, {path, <<"/spaces/space_name1">>}, 0, 100)]),
    ct:print("ls2: ~p~n", [ls(WorkerP2, SessId1, {path, <<"/spaces/space_name1">>}, 0, 100)]),
    ct:print("ls1: ~p~n", [ls(WorkerP1, SessId1, {path, <<"/">>}, 0, 100)]),
    ct:print("ls2: ~p~n", [ls(WorkerP2, SessId1, {path, <<"/">>}, 0, 100)]),

    timer:sleep(20).

gen_filename() ->
    list_to_binary(ibrowse_lib:url_encode("dbsync_test_" ++ binary_to_list(base64:encode(crypto:rand_bytes(20))))).


%% Get uuid of given by path file. Possible as root to bypass permissions checks.
get_uuid_privileged(Worker, SessId, Path) ->
    get_uuid(Worker, SessId, Path).


get_uuid(Worker, SessId, Path) ->
    #fuse_response{fuse_response = #file_attr{uuid = UUID}} = ?assertMatch(
        #fuse_response{status = #status{code = ?OK}},
        ?req(Worker, SessId, #get_file_attr{entry = {path, Path}}),
        30
    ),
    UUID.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    try
    Config1 = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")),
    [Worker | _] = ?config(op_worker_nodes, Config1),

    TmpDir = gen_storage_dir(),

    %% @todo: use shared storage
    "" = rpc:call(Worker, os, cmd, ["mkdir -p " ++ TmpDir]),
    {ok, StorageId} = rpc:call(Worker, storage, create, [#document{value = fslogic_storage:new_storage(<<"Test">>,
        [fslogic_storage:new_helper_init(<<"DirectIO">>, #{<<"root_path">> => list_to_binary(TmpDir)})])}]),
    [{storage_id, StorageId}, {storage_dir, TmpDir} | Config1]
    catch
        _:Reason ->
            ct:print("~p ~p", [Reason, erlang:get_stacktrace()]),
            error(kill)
    end .

end_per_suite(Config) ->
    TmpDir = ?config(storage_dir, Config),
    [Worker | _] = ?config(op_worker_nodes, Config),
    "" = rpc:call(Worker, os, cmd, ["rm -rf " ++ TmpDir]),
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    [Worker, Worker2] = Workers = ?config(op_worker_nodes, Config),
    ct:print("Workers ~p", [Workers]),

    communicator_mock_setup(Workers),
    file_meta_mock_setup(Workers),
    Space1 = {<<"space_id1">>, <<"space_name1">>},
    Space2 = {<<"space_id2">>, <<"space_name2">>},
    Space3 = {<<"space_id3">>, <<"space_name3">>},
    Space4 = {<<"space_id4">>, <<"space_name4">>},
    gr_spaces_mock_setup(Workers, [Space1, Space2, Space3, Space4]),

    User1 = {1, [<<"space_id1">>, <<"space_id2">>, <<"space_id3">>, <<"space_id4">>]},
    User2 = {2, [<<"space_id2">>, <<"space_id3">>, <<"space_id4">>]},
    User3 = {3, [<<"space_id3">>, <<"space_id4">>]},
    User4 = {4, [<<"space_id4">>]},

    Host = self(),
    Servers = lists:map(fun(W) ->
        spawn_link(W, fun() ->
            lfm_handles = ets:new(lfm_handles, [public, set, named_table]),
            Host ! {self(), done},
            receive
                exit -> ok
            end
        end)
    end, ?config(op_worker_nodes, Config)),

    lists:foreach(fun(Server) ->
        receive
            {Server, done} -> ok
        after timer:seconds(5) ->
            error("Cannot setup lfm_handles ETS")
        end
    end, Servers),


    Config1 = session_setup(Worker, [User1, User2, User3, User4], Config),

    session_setup(Worker2, [User1, User2, User3, User4], Config),
    [{servers, Servers} | Config1].

end_per_testcase(_, Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),

    lists:foreach(fun(Pid) ->
        Pid ! exit
    end, ?config(servers, Config)),

    session_teardown(Worker, Config),
    test_utils:mock_validate(Workers, [communicator, file_meta, gr_spaces]),
    test_utils:mock_unload(Workers, [communicator, file_meta, gr_spaces]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new test session.
%% @end
%%--------------------------------------------------------------------
-spec session_setup(Worker :: node(), [{UserNum :: non_neg_integer(), [SpaceIds :: binary()]}], Config :: term()) -> NewConfig :: term().
session_setup(_Worker, [], Config) ->
    Config;
session_setup(Worker, [{UserNum, SpaceIds} | R], Config) ->
    Self = self(),

    Name = fun(Text, Num) -> name(Text, Num) end,

    SessId = Name("session_id", UserNum),
    UserId = Name("user_id", UserNum),
    Iden = #identity{user_id = UserId},
    UserName = Name("username", UserNum),

    ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, Iden, Self])),
    {ok, #document{value = Session}} = rpc:call(Worker, session, get, [SessId]),
    {ok, _} = rpc:call(Worker, onedata_user, create, [
        #document{key = UserId, value = #onedata_user{
            name = UserName, space_ids = SpaceIds
        }}
    ]),
    ?assertReceivedMatch(onedata_user_setup, ?TIMEOUT),
    [
        {{spaces, UserNum}, SpaceIds}, {{user_id, UserNum}, UserId}, {{session_id, UserNum}, SessId},
        {{fslogic_ctx, UserNum}, #fslogic_ctx{session = Session}}
        | session_setup(Worker, R, Config)
    ].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes existing test session.
%% @end
%%--------------------------------------------------------------------
-spec session_teardown(Worker :: node(), Config :: term()) -> NewConfig :: term().
session_teardown(Worker, Config) ->
    lists:foldl(fun
        ({{session_id, _}, SessId}, Acc) ->
            ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId])),
            Acc;
        ({{spaces, _}, SpaceIds}, Acc) ->
            lists:foreach(fun(SpaceId) ->
                ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [SpaceId]))
            end, SpaceIds),
            Acc;
        ({{user_id, _}, UserId}, Acc) ->
            ?assertEqual(ok, rpc:call(Worker, onedata_user, delete, [UserId])),
            ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [UserId])),
            ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [fslogic_path:spaces_uuid(UserId)])),
            Acc;
        ({{fslogic_ctx, _}, _}, Acc) ->
            Acc;
        (Elem, Acc) ->
            [Elem | Acc]
    end, [], Config).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks gr_spaces module, so that it returns default space details for default
%% space ID.
%% @end
%%--------------------------------------------------------------------
-spec gr_spaces_mock_setup(Workers :: node() | [node()],
    [{binary(), binary()}]) -> ok.
gr_spaces_mock_setup(Workers, Spaces) ->
    test_utils:mock_new(Workers, gr_spaces),
    test_utils:mock_expect(Workers, gr_spaces, get_details, fun
        (provider, SpaceId) ->
            {_, SpaceName} = lists:keyfind(SpaceId, 1, Spaces),
            {ok, #space_details{name = SpaceName}}
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks file_meta module, so that creation of onedata user sends notification.
%% @end
%%--------------------------------------------------------------------
-spec file_meta_mock_setup(Workers :: node() | [node()]) -> ok.
file_meta_mock_setup(Workers) ->
    Self = self(),
    test_utils:mock_new(Workers, file_meta),
    test_utils:mock_expect(Workers, file_meta, 'after', fun
        (onedata_user, create, _, _, {ok, UUID}) ->
            file_meta:setup_onedata_user(UUID),
            Self ! onedata_user_setup
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks communicator module, so that it ignores all messages.
%% @end
%%--------------------------------------------------------------------
-spec communicator_mock_setup(Workers :: node() | [node()]) -> ok.
communicator_mock_setup(Workers) ->
    test_utils:mock_new(Workers, communicator),
    test_utils:mock_expect(Workers, communicator, send,
        fun(_, _) -> ok end
    ).

name(Text, Num) ->
    list_to_binary(Text ++ "_" ++ integer_to_list(Num)).

gen_name() ->
    binary:replace(base64:encode(crypto:rand_bytes(12)), <<"/">>, <<"">>, [global]).


stat(Worker, SessId, FileKey) ->
    exec(Worker, fun(Host) ->
        Result =
            logical_file_manager:stat(SessId, FileKey),
        Host ! {self(), Result}
    end).


ls(Worker, SessId, FileKey, Offset, Limit) ->
    exec(Worker, fun(Host) ->
        Result =
            logical_file_manager:ls(SessId, FileKey, Limit, Offset),
        Host ! {self(), Result}
                 end).


truncate(Worker, SessId, FileKey, Size) ->
    exec(Worker, fun(Host) ->
        Result =
            logical_file_manager:truncate(SessId, FileKey, Size),
        Host ! {self(), Result}
    end).


create(Worker, SessId, FilePath, Mode) ->
    exec(Worker, fun(Host) ->
        Result =
            logical_file_manager:create(SessId, FilePath, Mode),
        Host ! {self(), Result}
    end).


mkdir(Worker, SessId, FilePath, Mode) ->
    exec(Worker, fun(Host) ->
        Result =
            logical_file_manager:mkdir(SessId, FilePath, Mode),
        Host ! {self(), Result}
                 end).

unlink(Worker, SessId, File) ->
    exec(Worker, fun(Host) ->
        Result =
            logical_file_manager:unlink(SessId, File),
        Host ! {self(), Result}
    end).


open(Worker, SessId, FileKey, OpenMode) ->
    exec(Worker, fun(Host) ->
        Result =
            case logical_file_manager:open(SessId, FileKey, OpenMode) of
                {ok, Handle} ->
                    TestHandle = crypto:rand_bytes(10),
                    ets:insert(lfm_handles, {TestHandle, Handle}),
                    {ok, TestHandle};
                Other -> Other
            end,
        Host ! {self(), Result}
    end).

read(Worker, TestHandle, Offset, Size) ->
    exec(Worker, fun(Host) ->
        [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
        Result =
            case logical_file_manager:read(Handle, Offset, Size) of
                {ok, NewHandle, Res} ->
                    ets:insert(lfm_handles, {TestHandle, NewHandle}),
                    {ok, Res};
                Other -> Other
            end,
        Host ! {self(), Result}
    end).

write(Worker, TestHandle, Offset, Bytes) ->
    exec(Worker, fun(Host) ->
        [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
        Result =
            case logical_file_manager:write(Handle, Offset, Bytes) of
                {ok, NewHandle, Res} ->
                    ets:insert(lfm_handles, {TestHandle, NewHandle}),
                    {ok, Res};
                Other -> Other
            end,
        Host ! {self(), Result}
    end).


exec(Worker, Fun) ->
    Host = self(),
    Pid = spawn_link(Worker, fun() ->
        try
            Fun(Host)
        catch
            _:Reason ->
                Host ! {self(), {error, {test_exec, Reason, erlang:get_stacktrace()}}}
        end
    end),
    receive
        {Pid, Result} -> Result
    after timer:seconds(30) ->
        {error, test_timeout}
    end.

for(From, To, Fun) ->
    for(From, To, 1, Fun).
for(From, To, Step, Fun) ->
    [Fun(I) || I <- lists:seq(From, To, Step)].

gen_storage_dir() ->
    ?TEMP_DIR ++ "/storage/" ++ erlang:binary_to_list(gen_name()).