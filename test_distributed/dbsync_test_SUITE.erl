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
-include_lib("ctool/include/oz/oz_spaces.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    global_stream_test/1,
    global_stream_document_remove_test/1,
    global_stream_with_proto_test/1
]).

all() ->
    ?ALL([
        global_stream_test,
        global_stream_document_remove_test,
        global_stream_with_proto_test
    ]).

-define(TIMEOUT, timer:seconds(10)).
-define(req(W, SessId, FuseRequest), rpc:call(W, worker_proxy, call, [fslogic_worker, {fuse_request, SessId, FuseRequest}], ?TIMEOUT)).
-define(lfm_req(W, Method, Args), rpc:call(W, file_manager, Method, Args, ?TIMEOUT)).

%%%====================================================================
%%% Test function
%%%====================================================================


global_stream_test(MultiConfig) ->
    ConfigP1 = ?config(p1, MultiConfig),
    ConfigP2 = ?config(p1, MultiConfig),
    [WorkerP1 | _] = ?config(op_worker_nodes, ConfigP1),
    [WorkerP2 | _] = ?config(op_worker_nodes, ConfigP2),

    {SessId1P1, _} = {?config({session_id, 1}, ConfigP1), ?config({user_id, 1}, ConfigP1)},
    {SessId1P2, _} = {?config({session_id, 1}, ConfigP2), ?config({user_id, 1}, ConfigP2)},

    test_utils:mock_expect([WorkerP1], dbsync_proto, send_batch,
        fun(global, SpaceId, BatchToSend) ->
            rpc:call(WorkerP2, dbsync_worker, apply_batch_changes, [undefined, SpaceId, BatchToSend])
        end),

    Dirs = lists:map(
        fun(_N) ->
            D0 = gen_filename(),

            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1P1, <<"/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1P1, <<"/", D0/binary, "/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1P1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),

            D0
        end, lists:seq(1, 5)),

    timer:sleep(timer:seconds(10)),

    RevPerPath = lists:map(
        fun(D0) ->

            Path1 = <<"/", D0/binary>>,
            Path2 = <<"/", D0/binary, "/", D0/binary>>,
            Path3 = <<"/", D0/binary, "/", D0/binary, "/", D0/binary>>,

            {ok, #file_attr{uuid = UUID1}} = lfm_proxy:stat(WorkerP1, SessId1P1, {path, Path1}),
            {ok, #file_attr{uuid = UUID2}} = lfm_proxy:stat(WorkerP1, SessId1P1, {path, Path2}),
            {ok, #file_attr{uuid = UUID3}} = lfm_proxy:stat(WorkerP1, SessId1P1, {path, Path3}),

            {ok, #document{rev = Rev1}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID1]),
            {ok, #document{rev = Rev2}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID2]),
            {ok, #document{rev = Rev3}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID3]),

            {ok, #document{rev = LRev1}} = rpc:call(WorkerP1, file_meta, get, [links_utils:links_doc_key(UUID1)]),
            {ok, #document{rev = LRev2}} = rpc:call(WorkerP1, file_meta, get, [links_utils:links_doc_key(UUID2)]),
            {ok, #document{rev = LRev3}} = rpc:call(WorkerP1, file_meta, get, [links_utils:links_doc_key(UUID3)]),

            Map0 = #{},
            Map1 = maps:put(Path1, {UUID1, Rev1, LRev1}, Map0),
            Map2 = maps:put(Path2, {UUID2, Rev2, LRev2}, Map1),
            _Map3 = maps:put(Path3, {UUID3, Rev3, LRev3}, Map2)
        end, Dirs),

    lists:foreach(
        fun(PathMap) ->
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
                        case rpc:call(WorkerP2, file_meta, get, [links_utils:links_doc_key(UUID)]) of
                            {ok, #document{rev = LRev2}} ->
                                LRev2;
                            {error, Reason2} ->
                                Reason2
                        end,

                    ?assertMatch(LocalRev, Rev),
                    ?assertMatch(LocalLRev, LRev),

                    ?assertMatch({ok, #file_attr{uuid = UUID}}, lfm_proxy:stat(WorkerP2, SessId1P2, {path, Path}))

                end, maps:to_list(PathMap))
        end, RevPerPath),

    {ok, LS1P1} = lfm_proxy:ls(WorkerP1, SessId1P1, {path, <<"/spaces/space_name1">>}, 0, 100),
    {ok, LS1P2} = lfm_proxy:ls(WorkerP2, SessId1P2, {path, <<"/spaces/space_name1">>}, 0, 100),

    {ok, LS2P1} = lfm_proxy:ls(WorkerP1, SessId1P1, {path, <<"/">>}, 0, 100),
    {ok, LS2P2} = lfm_proxy:ls(WorkerP2, SessId1P2, {path, <<"/">>}, 0, 100),

    ?assertMatch(LS1P1, LS1P2),
    ?assertMatch(LS2P1, LS2P2),

    ok.


global_stream_document_remove_test(MultiConfig) ->
    ConfigP1 = ?config(p1, MultiConfig),
    ConfigP2 = ?config(p1, MultiConfig),
    [WorkerP1 | _] = ?config(op_worker_nodes, ConfigP1),
    [WorkerP2 | _] = ?config(op_worker_nodes, ConfigP2),

    {SessId1P1, _} = {?config({session_id, 1}, ConfigP1), ?config({user_id, 1}, ConfigP1)},
    {SessId1P2, _} = {?config({session_id, 1}, ConfigP2), ?config({user_id, 1}, ConfigP2)},

    test_utils:mock_expect([WorkerP1], dbsync_proto, send_batch,
        fun(global, SpaceId, BatchToSend) ->
            rpc:call(WorkerP2, dbsync_worker, apply_batch_changes, [undefined, SpaceId, BatchToSend])
        end),

    Dirs = lists:map(
        fun(_N) ->
            D0 = gen_filename(),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1P1, <<"/", D0/binary>>, 8#755),
            D0
        end, lists:seq(1, 5)),

    timer:sleep(timer:seconds(10)),

    RevPerPath = lists:map(
        fun(D0) ->

            Path1 = <<"/", D0/binary>>,
            {ok, #file_attr{uuid = UUID1}} = lfm_proxy:stat(WorkerP1, SessId1P1, {path, Path1}),

            {ok, #document{rev = Rev1}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID1]),

            {ok, #document{rev = LRev1}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, links_utils:links_doc_key(UUID1)]),

            Map0 = #{},
            _Map1 = maps:put(Path1, {UUID1, Rev1, LRev1}, Map0)
        end, Dirs),

    lists:foreach(
        fun(PathMap) ->
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
                        case rpc:call(WorkerP2, datastore, get, [disk_only, file_meta, links_utils:links_doc_key(UUID)]) of
                            {ok, #document{rev = LRev2}} ->
                                LRev2;
                            {error, Reason2} ->
                                Reason2
                        end,

                    ?assertMatch(LocalRev, Rev),
                    ?assertMatch(LocalLRev, LRev),

                    ?assertMatch({ok, #file_attr{uuid = UUID}}, lfm_proxy:stat(WorkerP2, SessId1P2, {path, Path}))

                end, maps:to_list(PathMap))
        end, RevPerPath),

    {ok, LS1P1} = lfm_proxy:ls(WorkerP1, SessId1P1, {path, <<"/spaces/space_name1">>}, 0, 100),
    {ok, LS1P2} = lfm_proxy:ls(WorkerP2, SessId1P2, {path, <<"/spaces/space_name1">>}, 0, 100),

    {ok, LS2P1} = lfm_proxy:ls(WorkerP1, SessId1P1, {path, <<"/">>}, 0, 100),
    {ok, LS2P2} = lfm_proxy:ls(WorkerP2, SessId1P2, {path, <<"/">>}, 0, 100),

    ?assertMatch(LS1P1, LS1P2),
    ?assertMatch(LS2P1, LS2P2),


    lists:foreach(
        fun(D0) ->
            ok = lfm_proxy:unlink(WorkerP1, SessId1P1, {path, <<"/", D0/binary>>})
        end, Dirs),

    timer:sleep(timer:seconds(5)),

    lists:foreach(
        fun(PathMap) ->
            lists:foreach(
                fun({Path, {UUID, _Rev, _LRev}}) ->

                    GlobalLinks = rpc:call(WorkerP2, datastore, foreach_link, [global_only, UUID, file_meta, fun(LN, LT, AccIn) ->
                        [{LN, LT} | AccIn]
                    end, []]),

                    DiskLinks = rpc:call(WorkerP2, datastore, foreach_link, [disk_only, UUID, file_meta, fun(LN, LT, AccIn) ->
                        [{LN, LT} | AccIn]
                    end, []]),

                    ?assertMatch({ok, []}, GlobalLinks),
                    ?assertMatch({ok, []}, DiskLinks),

                    ?assertMatch({error, {not_found, _}}, rpc:call(WorkerP2, file_meta, get, [UUID])),
                    ?assertMatch({error, {not_found, _}}, rpc:call(WorkerP2, datastore, get, [disk_only, file_meta, UUID])),

                    ?assertMatch({error, enoent}, lfm_proxy:stat(WorkerP2, SessId1P2, {path, Path})),
                    ?assertMatch({ok, [{_, <<"spaces">>}]}, lfm_proxy:ls(WorkerP2, SessId1P2, {path, <<"/">>}, 0, 10))
                end, maps:to_list(PathMap))
        end, RevPerPath),


    {ok, RemovedLS1P1} = lfm_proxy:ls(WorkerP1, SessId1P1, {path, <<"/spaces/space_name1">>}, 0, 100),
    {ok, RemovedLS1P2} = lfm_proxy:ls(WorkerP2, SessId1P2, {path, <<"/spaces/space_name1">>}, 0, 100),

    {ok, RemovedLS2P1} = lfm_proxy:ls(WorkerP1, SessId1P1, {path, <<"/">>}, 0, 100),
    {ok, RemovedLS2P2} = lfm_proxy:ls(WorkerP2, SessId1P2, {path, <<"/">>}, 0, 100),

    ?assertMatch(RemovedLS1P1, RemovedLS1P2),
    ?assertMatch(RemovedLS2P1, RemovedLS2P2),

    ok.


global_stream_with_proto_test(MultiConfig) ->
    ConfigP1 = ?config(p1, MultiConfig),
    ConfigP2 = ?config(p1, MultiConfig),
    [WorkerP1 | _] = ?config(op_worker_nodes, ConfigP1),
    [WorkerP2 | _] = ?config(op_worker_nodes, ConfigP2),

    {SessId1P1, _} = {?config({session_id, 1}, ConfigP1), ?config({user_id, 1}, ConfigP1)},
    {SessId1P2, _} = {?config({session_id, 1}, ConfigP2), ?config({user_id, 1}, ConfigP2)},


    Dirs = lists:map(
        fun(N) ->
            D0 = gen_filename(),

            F = gen_filename(),

            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1P1, <<"/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1P1, <<"/", D0/binary, "/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1P1, <<"/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:create(WorkerP1, SessId1P1, <<"/", D0/binary, "/", D0/binary, "/", F/binary>>, 8#755),

            D0
        end, lists:seq(1, 5)),

    timer:sleep(timer:seconds(10)),

    RevPerPath = lists:map(
        fun(D0) ->

            Path1 = <<"/", D0/binary>>,
            Path2 = <<"/", D0/binary, "/", D0/binary>>,
            Path3 = <<"/", D0/binary, "/", D0/binary, "/", D0/binary>>,

            {ok, #file_attr{uuid = UUID1}} = lfm_proxy:stat(WorkerP1, SessId1P1, {path, Path1}),
            {ok, #file_attr{uuid = UUID2}} = lfm_proxy:stat(WorkerP1, SessId1P1, {path, Path2}),
            {ok, #file_attr{uuid = UUID3}} = lfm_proxy:stat(WorkerP1, SessId1P1, {path, Path3}),

            {ok, #document{rev = Rev1}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID1]),
            {ok, #document{rev = Rev2}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID2]),
            {ok, #document{rev = Rev3}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, UUID3]),

            {ok, #document{rev = LRev1}} = rpc:call(WorkerP1, file_meta, get, [links_utils:links_doc_key(UUID1)]),
            {ok, #document{rev = LRev2}} = rpc:call(WorkerP1, file_meta, get, [links_utils:links_doc_key(UUID2)]),
            {ok, #document{rev = LRev3}} = rpc:call(WorkerP1, file_meta, get, [links_utils:links_doc_key(UUID3)]),

            Map0 = #{},
            Map1 = maps:put(Path1, {UUID1, Rev1, LRev1}, Map0),
            Map2 = maps:put(Path2, {UUID2, Rev2, LRev2}, Map1),
            _Map3 = maps:put(Path3, {UUID3, Rev3, LRev3}, Map2)
        end, Dirs),

    lists:foreach(
        fun(PathMap) ->
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
                        case rpc:call(WorkerP2, file_meta, get, [links_utils:links_doc_key(UUID)]) of
                            {ok, #document{rev = LRev2}} ->
                                LRev2;
                            {error, Reason2} ->
                                Reason2
                        end,

                    ?assertMatch(LocalRev, Rev),
                    ?assertMatch(LocalLRev, LRev),

                    ?assertMatch({ok, #file_attr{uuid = UUID}}, lfm_proxy:stat(WorkerP2, SessId1P2, {path, Path}))

                end, maps:to_list(PathMap))
        end, RevPerPath),

    {ok, LS1P1} = lfm_proxy:ls(WorkerP1, SessId1P1, {path, <<"/spaces/space_name1">>}, 0, 100),
    {ok, LS1P2} = lfm_proxy:ls(WorkerP2, SessId1P2, {path, <<"/spaces/space_name1">>}, 0, 100),

    {ok, LS2P1} = lfm_proxy:ls(WorkerP1, SessId1P1, {path, <<"/">>}, 0, 100),
    {ok, LS2P2} = lfm_proxy:ls(WorkerP2, SessId1P2, {path, <<"/">>}, 0, 100),

    ?assertMatch(LS1P1, LS1P2),
    ?assertMatch(LS2P1, LS2P2),

    ok.


gen_filename() ->
    generator:gen_name().


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
    [WorkerP1, WorkerP2] = Workers = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Workers, [dbsync_proto, oneprovider, dbsync_utils]),

    ConfigP1 = lists:keystore(op_worker_nodes, 1, Config, {op_worker_nodes, [WorkerP1]}),
    ConfigP2 = lists:keystore(op_worker_nodes, 1, Config, {op_worker_nodes, [WorkerP2]}),
    ConfigWithSessionInfoP1 = initializer:create_test_users_and_spaces(ConfigP1),
    ConfigWithSessionInfoP2 = initializer:create_test_users_and_spaces(ConfigP2),


    test_utils:mock_expect([WorkerP1], oneprovider, get_provider_id,
        fun() ->
            <<"provider_1">>
        end),
    test_utils:mock_expect([WorkerP2], oneprovider, get_provider_id,
        fun() ->
            <<"provider_2">>
        end),

    test_utils:mock_expect([WorkerP1, WorkerP2], dbsync_utils, get_providers_for_space,
        fun(_) ->
            [<<"provider_1">>, <<"provider_2">>]
        end),
    test_utils:mock_expect([WorkerP1, WorkerP2], dbsync_utils, get_spaces_for_provider,
        fun(_) ->
            [<<"space_id1">>, <<"space_id2">>, <<"space_id3">>, <<"space_id4">>, <<"space_id5">>]
        end),

    test_utils:mock_expect([WorkerP1, WorkerP2], dbsync_utils, communicate,
        fun
            (<<"provider_1">>, Message) ->
                rpc:call(WorkerP1, dbsync_proto, handle_impl, [<<"provider_2">>, Message]);
            (<<"provider_2">>, Message) ->
                rpc:call(WorkerP2, dbsync_proto, handle_impl, [<<"provider_1">>, Message])
        end),

    [{all, Config}, {p1, lfm_proxy:init(ConfigWithSessionInfoP1)}, {p2, lfm_proxy:init(ConfigWithSessionInfoP2)}].

end_per_testcase(_, MultiConfig) ->
    timer:sleep(timer:seconds(10)),
    Workers = ?config(op_worker_nodes, ?config(all, MultiConfig)),
    lfm_proxy:teardown(?config(p1, MultiConfig)),
    lfm_proxy:teardown(?config(p2, MultiConfig)),
    initializer:clean_test_users_and_spaces_no_validate(?config(p1, MultiConfig)),
    initializer:clean_test_users_and_spaces_no_validate(?config(p2, MultiConfig)),

    catch task_manager:kill_all(),

    test_utils:mock_unload(Workers, [dbsync_proto, oneprovider, dbsync_utils]).


%%%===================================================================
%%% Internal functions
%%%===================================================================
