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

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

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
        fun(global, SpaceId, BatchToSend) ->
            rpc:call(WorkerP2, dbsync_worker, apply_batch_changes, [undefined, SpaceId, BatchToSend])
        end),

    Dirs = lists:map(
        fun(N) ->
            NBin = integer_to_binary(N),
            D0 = <<"dbsync_test_", NBin/binary>>,

            F = gen_filename(),

            ct:print("Create ~p", [N]),

            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1, <<"/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:create(WorkerP1, SessId1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", D0/binary, "/", F/binary>>, 8#755),

            D0
        end, lists:seq(1, 10)),

    timer:sleep(timer:seconds(10)),

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

            {ok, #file_attr{uuid = UUID1}} = lfm_proxy:stat(WorkerP1, SessId1, {path, Path1}),
            {ok, #file_attr{uuid = UUID2}} = lfm_proxy:stat(WorkerP1, SessId1, {path, Path2}),
            {ok, #file_attr{uuid = UUID3}} = lfm_proxy:stat(WorkerP1, SessId1, {path, Path3}),
            {ok, #file_attr{uuid = UUID4}} = lfm_proxy:stat(WorkerP1, SessId1, {path, Path4}),
            {ok, #file_attr{uuid = UUID5}} = lfm_proxy:stat(WorkerP1, SessId1, {path, Path5}),
            {ok, #file_attr{uuid = UUID6}} = lfm_proxy:stat(WorkerP1, SessId1, {path, Path6}),
            {ok, #file_attr{uuid = UUID7}} = lfm_proxy:stat(WorkerP1, SessId1, {path, Path7}),
            {ok, #file_attr{uuid = UUID8}} = lfm_proxy:stat(WorkerP1, SessId1, {path, Path8}),
            {ok, #file_attr{uuid = UUID9}} = lfm_proxy:stat(WorkerP1, SessId1, {path, Path9}),
            {ok, #file_attr{uuid = UUID10}} = lfm_proxy:stat(WorkerP1, SessId1, {path, Path10}),

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

            Map0 = #{},
            Map1 = maps:put(Path1, {UUID1, Rev1, LRev1}, Map0),
            Map2 = maps:put(Path2, {UUID2, Rev2, LRev2}, Map1),
            Map3 = maps:put(Path3, {UUID3, Rev3, LRev3}, Map2),
            Map4 = maps:put(Path4, {UUID4, Rev4, LRev4}, Map3),
            Map5 = maps:put(Path5, {UUID5, Rev5, LRev5}, Map4),
            Map6 = maps:put(Path6, {UUID6, Rev6, LRev6}, Map5),
            Map7 = maps:put(Path7, {UUID7, Rev7, LRev7}, Map6),
            Map8 = maps:put(Path8, {UUID8, Rev8, LRev8}, Map7),
            Map9 = maps:put(Path9, {UUID9, Rev9, LRev9}, Map8),
            _Map10 = maps:put(Path10, {UUID10, Rev10, LRev10}, Map9)
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
                    ct:print("Check UUID ~p via path ~p: ~p", [UUID, Path, lfm_proxy:stat(WorkerP2, SessId1, {path, Path})])
                end, maps:to_list(PathMap))
        end, RevPerPath),

    ct:print("ls1: ~p~n", [lfm_proxy:ls(WorkerP1, SessId1, {path, <<"/spaces/space_name1">>}, 0, 100)]),
    ct:print("ls2: ~p~n", [lfm_proxy:ls(WorkerP2, SessId1, {path, <<"/spaces/space_name1">>}, 0, 100)]),
    ct:print("ls1: ~p~n", [lfm_proxy:ls(WorkerP1, SessId1, {path, <<"/">>}, 0, 100)]),
    ct:print("ls2: ~p~n", [lfm_proxy:ls(WorkerP2, SessId1, {path, <<"/">>}, 0, 100)]),

    ok.

gen_filename() ->
    list_to_binary(ibrowse_lib:url_encode("dbsync_test_" ++ binary_to_list(base64:encode(crypto:rand_bytes(20))))).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")),
    initializer:setup_storage(ConfigWithNodes).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    communicator_mock_setup(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


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