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
%%        @todo Disabled until VFS-2306 is resolved
%%        global_stream_test,
%%        global_stream_document_remove_test,
%%        global_stream_with_proto_test
    ]).

-define(TIMEOUT, timer:seconds(10)).
-define(req(W, SessId, FuseRequest), rpc:call(W, worker_proxy, call, [fslogic_worker, {fuse_request, SessId, FuseRequest}], ?TIMEOUT)).
-define(lfm_req(W, Method, Args), rpc:call(W, file_manager, Method, Args, ?TIMEOUT)).

%%%====================================================================
%%% Test function
%%%====================================================================


global_stream_test(MultiConfig) ->
    ConfigP1 = ?config(p1, MultiConfig),
    ConfigP2 = ?config(p2, MultiConfig),
    [WorkerP1 | _] = ?config(op_worker_nodes, ConfigP1),
    [WorkerP2 | _] = ?config(op_worker_nodes, ConfigP2),

    {SessId1P1, _} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, ConfigP1), ?config({user_id, <<"user1">>}, ConfigP1)},
    {SessId1P2, _} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, ConfigP2), ?config({user_id, <<"user1">>}, ConfigP2)},

    Prov1ID = rpc:call(WorkerP1, oneprovider, get_provider_id, []),
    Prov2ID = rpc:call(WorkerP2, oneprovider, get_provider_id, []),

    test_utils:mock_expect([WorkerP1], dbsync_proto, send_batch,
        fun(global, SpaceId, BatchToSend) ->
            rpc:call(WorkerP2, dbsync_worker, apply_batch_changes, [undefined, SpaceId, BatchToSend])
        end),

    Dirs = lists:map(
        fun(_N) ->
            D0 = gen_filename(),

            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1P1, <<"/space_name1/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1P1, <<"/space_name1/", D0/binary, "/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1P1, <<"/space_name1/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),

            D0
        end, lists:seq(1, 5)),

    timer:sleep(timer:seconds(10)),

    RevPerPath = lists:map(
        fun(D0) ->

            Path1 = <<"/space_name1/", D0/binary>>,
            Path2 = <<"/space_name1/", D0/binary, "/", D0/binary>>,
            Path3 = <<"/space_name1/", D0/binary, "/", D0/binary, "/", D0/binary>>,

            {ok, #file_attr{uuid = FileGUID1}} = lfm_proxy:stat(WorkerP1, SessId1P1, {path, Path1}),
            {ok, #file_attr{uuid = FileGUID2}} = lfm_proxy:stat(WorkerP1, SessId1P1, {path, Path2}),
            {ok, #file_attr{uuid = FileGUID3}} = lfm_proxy:stat(WorkerP1, SessId1P1, {path, Path3}),

            FileUUID1 = fslogic_uuid:guid_to_uuid(FileGUID1),
            FileUUID2 = fslogic_uuid:guid_to_uuid(FileGUID2),
            FileUUID3 = fslogic_uuid:guid_to_uuid(FileGUID3),

            {ok, #document{rev = Rev1}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, FileUUID1]),
            {ok, #document{rev = Rev2}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, FileUUID2]),
            {ok, #document{rev = Rev3}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, FileUUID3]),

            {ok, #document{rev = LRev1}} = rpc:call(WorkerP1, file_meta, get, [links_utils:links_doc_key(FileUUID1, Prov1ID)]),
            {ok, #document{rev = LRev2}} = rpc:call(WorkerP1, file_meta, get, [links_utils:links_doc_key(FileUUID2, Prov1ID)]),
            {ok, #document{rev = LRev3}} = rpc:call(WorkerP1, file_meta, get, [links_utils:links_doc_key(FileUUID3, Prov1ID)]),

            Map0 = #{},
            Map1 = maps:put(Path1, {FileUUID1, Rev1, LRev1}, Map0),
            Map2 = maps:put(Path2, {FileUUID2, Rev2, LRev2}, Map1),
            _Map3 = maps:put(Path3, {FileUUID3, Rev3, LRev3}, Map2)
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
                        case rpc:call(WorkerP2, file_meta, get, [links_utils:links_doc_key(UUID, Prov2ID)]) of
                            {ok, #document{rev = LRev2}} ->
                                LRev2;
                            {error, Reason2} ->
                                Reason2
                        end,

                    ?assertMatch(LocalRev, Rev),
                    ?assertMatch(LocalLRev, LRev),

                    {ok, #file_attr{uuid = GUID}} = lfm_proxy:stat(WorkerP2, SessId1P2, {path, Path}),
                    ?assertMatch(UUID, fslogic_uuid:guid_to_uuid(GUID))

                end, maps:to_list(PathMap))
        end, RevPerPath),

    {ok, LS1P1} = lfm_proxy:ls(WorkerP1, SessId1P1, {path, <<"/space_name1">>}, 0, 100),
    {ok, LS1P2} = lfm_proxy:ls(WorkerP2, SessId1P2, {path, <<"/space_name1">>}, 0, 100),

    ?assertMatch(LS1P1, LS1P2),
    ok.


global_stream_document_remove_test(MultiConfig) ->
    ConfigP1 = ?config(p1, MultiConfig),
    ConfigP2 = ?config(p2, MultiConfig),
    [WorkerP1 | _] = ?config(op_worker_nodes, ConfigP1),
    [WorkerP2 | _] = ?config(op_worker_nodes, ConfigP2),

    {SessId1P1, _} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, ConfigP1), ?config({user_id, <<"user1">>}, ConfigP1)},
    {SessId1P2, _} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, ConfigP2), ?config({user_id, <<"user1">>}, ConfigP2)},

    Prov1ID = rpc:call(WorkerP1, oneprovider, get_provider_id, []),
    Prov2ID = rpc:call(WorkerP2, oneprovider, get_provider_id, []),

    test_utils:mock_expect([WorkerP1], dbsync_proto, send_batch,
        fun(global, SpaceId, BatchToSend) ->
            rpc:call(WorkerP2, dbsync_worker, apply_batch_changes, [undefined, SpaceId, BatchToSend])
        end),

    Dirs = lists:map(
        fun(_N) ->
            D0 = gen_filename(),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1P1, <<"/space_name1/", D0/binary>>, 8#755),
            D0
        end, lists:seq(1, 5)),

    timer:sleep(timer:seconds(10)),

    RevPerPath = lists:map(
        fun(D0) ->

            Path1 = <<"/space_name1/", D0/binary>>,
            {ok, #file_attr{uuid = FileGUID1}} = lfm_proxy:stat(WorkerP1, SessId1P1, {path, Path1}),
            FileUUID1 = fslogic_uuid:guid_to_uuid(FileGUID1),

            {ok, #document{rev = Rev1}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, FileUUID1]),

            {ok, #document{rev = LRev1}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, links_utils:links_doc_key(FileUUID1, Prov1ID)]),

            Map0 = #{},
            _Map1 = maps:put(Path1, {FileUUID1, Rev1, LRev1}, Map0)
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
                        case rpc:call(WorkerP2, datastore, get, [disk_only, file_meta, links_utils:links_doc_key(UUID, Prov2ID)]) of
                            {ok, #document{rev = LRev2}} ->
                                LRev2;
                            {error, Reason2} ->
                                Reason2
                        end,

                    ?assertMatch(LocalRev, Rev),
                    ?assertMatch(LocalLRev, LRev),

                    {ok, #file_attr{uuid = GUID}} = lfm_proxy:stat(WorkerP2, SessId1P2, {path, Path}),
                    ?assertMatch(UUID, fslogic_uuid:guid_to_uuid(GUID))

                end, maps:to_list(PathMap))
        end, RevPerPath),

    {ok, LS1P1} = lfm_proxy:ls(WorkerP1, SessId1P1, {path, <<"/space_name1">>}, 0, 100),
    {ok, LS1P2} = lfm_proxy:ls(WorkerP2, SessId1P2, {path, <<"/space_name1">>}, 0, 100),

    ?assertMatch(LS1P1, LS1P2),

    lists:foreach(
        fun(D0) ->
            ok = lfm_proxy:unlink(WorkerP1, SessId1P1, {path, <<"/space_name1/", D0/binary>>})
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
                    ?assertMatch({ok, []}, lfm_proxy:ls(WorkerP2, SessId1P2, {path, <<"/space_name1">>}, 0, 10))
                end, maps:to_list(PathMap))
        end, RevPerPath),


    {ok, RemovedLS1P1} = lfm_proxy:ls(WorkerP1, SessId1P1, {path, <<"/space_name1">>}, 0, 100),
    {ok, RemovedLS1P2} = lfm_proxy:ls(WorkerP2, SessId1P2, {path, <<"/space_name1">>}, 0, 100),

    ?assertMatch(RemovedLS1P1, RemovedLS1P2),
    ok.


global_stream_with_proto_test(MultiConfig) ->
    ConfigP1 = ?config(p1, MultiConfig),
    ConfigP2 = ?config(p2, MultiConfig),
    [WorkerP1 | _] = ?config(op_worker_nodes, ConfigP1),
    [WorkerP2 | _] = ?config(op_worker_nodes, ConfigP2),

    {SessId1P1, _} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, ConfigP1), ?config({user_id, <<"user1">>}, ConfigP1)},
    {SessId1P2, _} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, ConfigP2), ?config({user_id, <<"user1">>}, ConfigP2)},

    Prov1ID = rpc:call(WorkerP1, oneprovider, get_provider_id, []),
    Prov2ID = rpc:call(WorkerP2, oneprovider, get_provider_id, []),

    Dirs = lists:map(
        fun(N) ->
            D0 = gen_filename(),

            F = gen_filename(),

            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1P1, <<"/space_name1/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1P1, <<"/space_name1/", D0/binary, "/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:mkdir(WorkerP1, SessId1P1, <<"/space_name1/", D0/binary, "/", D0/binary, "/", D0/binary>>, 8#755),
            {ok, _} = lfm_proxy:create(WorkerP1, SessId1P1, <<"/space_name1/", D0/binary, "/", D0/binary, "/", F/binary>>, 8#755),

            D0
        end, lists:seq(1, 5)),

    MonitoringId = #monitoring_id{
        main_subject_type = space,
        main_subject_id = <<"space_name1">>,
        metric_type = storage_used,
        provider_id = Prov1ID
    },
    ?assertMatch({ok, _}, rpc:call(WorkerP1, monitoring_state, create,
        [#document{key = MonitoringId, value = #monitoring_state{}}])),

    timer:sleep(timer:seconds(10)),

    RevPerPath = lists:map(
        fun(D0) ->

            Path1 = <<"/space_name1/", D0/binary>>,
            Path2 = <<"/space_name1/", D0/binary, "/", D0/binary>>,
            Path3 = <<"/space_name1/", D0/binary, "/", D0/binary, "/", D0/binary>>,

            {ok, #file_attr{uuid = FileGUID1}} = lfm_proxy:stat(WorkerP1, SessId1P1, {path, Path1}),
            {ok, #file_attr{uuid = FileGUID2}} = lfm_proxy:stat(WorkerP1, SessId1P1, {path, Path2}),
            {ok, #file_attr{uuid = FileGUID3}} = lfm_proxy:stat(WorkerP1, SessId1P1, {path, Path3}),

            FileUUID1 = fslogic_uuid:guid_to_uuid(FileGUID1),
            FileUUID2 = fslogic_uuid:guid_to_uuid(FileGUID2),
            FileUUID3 = fslogic_uuid:guid_to_uuid(FileGUID3),

            {ok, #document{rev = Rev1}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, FileUUID1]),
            {ok, #document{rev = Rev2}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, FileUUID2]),
            {ok, #document{rev = Rev3}} = rpc:call(WorkerP1, datastore, get, [disk_only, file_meta, FileUUID3]),

            {ok, #document{rev = LRev1}} = rpc:call(WorkerP1, file_meta, get, [links_utils:links_doc_key(FileUUID1, Prov1ID)]),
            {ok, #document{rev = LRev2}} = rpc:call(WorkerP1, file_meta, get, [links_utils:links_doc_key(FileUUID2, Prov1ID)]),
            {ok, #document{rev = LRev3}} = rpc:call(WorkerP1, file_meta, get, [links_utils:links_doc_key(FileUUID3, Prov1ID)]),

            Map0 = #{},
            Map1 = maps:put(Path1, {FileUUID1, Rev1, LRev1}, Map0),
            Map2 = maps:put(Path2, {FileUUID2, Rev2, LRev2}, Map1),
            _Map3 = maps:put(Path3, {FileUUID3, Rev3, LRev3}, Map2)
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
                        case rpc:call(WorkerP2, file_meta, get, [links_utils:links_doc_key(UUID, Prov2ID)]) of
                            {ok, #document{rev = LRev2}} ->
                                LRev2;
                            {error, Reason2} ->
                                Reason2
                        end,

                    ?assertMatch(LocalRev, Rev),
                    ?assertMatch(LocalLRev, LRev),

                    {ok, #file_attr{uuid = GUID}} = lfm_proxy:stat(WorkerP2, SessId1P2, {path, Path}),
                    ?assertMatch(UUID, fslogic_uuid:guid_to_uuid(GUID))

                end, maps:to_list(PathMap))
        end, RevPerPath),

    ?assertEqual(true, rpc:call(WorkerP2, monitoring_state, exists, [MonitoringId])),

    {ok, LS1P1} = lfm_proxy:ls(WorkerP1, SessId1P1, {path, <<"/space_name1">>}, 0, 100),
    {ok, LS1P2} = lfm_proxy:ls(WorkerP2, SessId1P2, {path, <<"/space_name1">>}, 0, 100),

    ?assertMatch(LS1P1, LS1P2),
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
    ?TEST_STOP(Config).

init_per_testcase(Case, Config) ->
    ?CASE_START(Case),
    [WorkerP1, WorkerP2] = Workers = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Workers, [dbsync_proto, dbsync_utils]),

    ConfigP1 = lists:keystore(op_worker_nodes, 1, Config, {op_worker_nodes, [WorkerP1]}),
    ConfigP2 = lists:keystore(op_worker_nodes, 1, Config, {op_worker_nodes, [WorkerP2]}),
    ConfigWithSessionInfoP1 = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), ConfigP1),
    ConfigWithSessionInfoP2 = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), ConfigP2),

    ProviderId1 = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP1)),
    ProviderId2 = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP2)),

    test_utils:mock_expect([WorkerP1, WorkerP2], dbsync_utils, get_providers_for_space,
        fun(_) ->
            [ProviderId1, ProviderId2]
        end),
    test_utils:mock_expect([WorkerP1, WorkerP2], dbsync_utils, get_spaces_for_provider,
        fun(_) ->
            [<<"space_id1">>, <<"space_id2">>, <<"space_id3">>, <<"space_id4">>, <<"space_id5">>]
        end),

    test_utils:mock_expect([WorkerP1, WorkerP2], dbsync_utils, validate_space_access,
        fun(_, _) ->
            ok
        end),

    test_utils:mock_expect([WorkerP1, WorkerP2], dbsync_utils, communicate,
        fun
            (ProvId, Message) ->
                case ProvId of
                    ProviderId1 -> rpc:call(WorkerP1, dbsync_proto, handle_impl, [ProviderId2, Message]);
                    ProviderId2 -> rpc:call(WorkerP2, dbsync_proto, handle_impl, [ProviderId1, Message])
                end
        end),

    [{all, Config}, {p1, lfm_proxy:init(ConfigWithSessionInfoP1)}, {p2, lfm_proxy:init(ConfigWithSessionInfoP2)}].

end_per_testcase(Case, MultiConfig) ->
    ?CASE_STOP(Case),
    timer:sleep(timer:seconds(10)),
    Workers = ?config(op_worker_nodes, ?config(all, MultiConfig)),
    lfm_proxy:teardown(?config(p1, MultiConfig)),
    lfm_proxy:teardown(?config(p2, MultiConfig)),
    initializer:clean_test_users_and_spaces_no_validate(?config(p1, MultiConfig)),
    initializer:clean_test_users_and_spaces_no_validate(?config(p2, MultiConfig)),

    catch task_manager:kill_all(),

    test_utils:mock_unload(Workers, [dbsync_proto, dbsync_utils]).


%%%===================================================================
%%% Internal functions
%%%===================================================================
