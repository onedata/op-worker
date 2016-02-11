%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016, ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Replication tests.
%%% @end
%%%-------------------------------------------------------------------
-module(replication_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/dbsync/common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").


%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    dbsync_trigger_should_create_local_file_location/1,
    write_should_add_blocks_to_file_location/1,
    truncate_should_change_size_and_blocks/1,
    write_and_truncate_should_not_update_remote_file_location/1
]).


-performance({test_cases, []}).
all() ->
    [
        dbsync_trigger_should_create_local_file_location,
        write_should_add_blocks_to_file_location,
        truncate_should_change_size_and_blocks,
        write_and_truncate_should_not_update_remote_file_location
    ].



%%%===================================================================
%%% Test functions
%%%===================================================================

dbsync_trigger_should_create_local_file_location(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,
    UserId = <<"user_id1">>,
    SessionId = <<"session_id1">>,
    CTime = utils:time(),
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    FileMeta = #file_meta{
        mode = 8#777,
        name = <<"file">>,
        type = ?REGULAR_FILE_TYPE,
        mtime = CTime,
        atime = CTime,
        ctime = CTime,
        uid = UserId
    },
    {ok, FileUuid} = ?assertMatch({ok, _}, rpc:call(W1, file_meta, create, [{uuid, SpaceDirUuid}, FileMeta])),

    %when
    rpc:call(W1, dbsync_events, change_replicated, [SpaceId, #change{model = file_meta, doc = #document{key = FileUuid, value = FileMeta}}]),

    %then
    ?assertMatch({ok, [_]}, rpc:call(W1, file_meta, get_locations, [{uuid, FileUuid}])),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessionId, {uuid, FileUuid}, rdwr)),
    ?assertMatch({ok, 3}, lfm_proxy:write(W1, Handle, 0, <<"aaa">>)),
    ?assertMatch({ok, <<"aaa">>}, lfm_proxy:read(W1, Handle, 0, 3)).

write_should_add_blocks_to_file_location(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = <<"session_id1">>,
    {ok, FileUuid} = lfm_proxy:create(W1, SessionId, <<"test_file">>, 8#777),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {uuid, FileUuid}, rdwr),

    %when
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    %then
    {ok, [LocationId]} = ?assertMatch({ok, [_]}, rpc:call(W1, file_meta, get_locations, [{uuid, FileUuid}])),
    {ok, LocationDoc = #document{value = Location = #file_location{blocks = Blocks, size = Size, version_vector = VV, provider_id = ProviderId}}} =
        ?assertMatch({ok, _}, rpc:call(W1, file_location, get, [LocationId])),
    ?assertEqual(atom_to_binary(?GET_DOMAIN(W1), unicode), ProviderId),
    ?assertEqual(10, Size),
%%    ?assertEqual(#{?GET_DOMAIN(W1) => 1}, VV), %todo add VV and uncomment
    [Block] = ?assertMatch([#file_block{offset = 0, size = 10}], Blocks),

    % when
    LocationWithoutBeginning =
        LocationDoc#document{value = Location#file_location{blocks = [Block#file_block{offset = 5, size = 5}]}},
    ?assertMatch({ok, _},
        rpc:call(W1, file_location, save, [LocationWithoutBeginning])),
    ?assertMatch({ok, 5}, lfm_proxy:write(W1, Handle, 0, <<"11111">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % then
    {ok, [LocationId]} = ?assertMatch({ok, [_]}, rpc:call(W1, file_meta, get_locations, [{uuid, FileUuid}])),
    {ok, #document{value = #file_location{blocks = Blocks2, size = Size2, version_vector = VV2}}} =
        ?assertMatch({ok, _}, rpc:call(W1, file_location, get, [LocationId])),
    ?assertEqual(10, Size2),
%%    ?assertEqual(#{?GET_DOMAIN(W1) => 2}, VV2), %todo add VV and uncomment
    ?assertMatch([Block], Blocks2).

truncate_should_change_size_and_blocks(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = <<"session_id1">>,
    {ok, FileUuid} = lfm_proxy:create(W1, SessionId, <<"test_file">>, 8#777),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {uuid, FileUuid}, rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),

    %when
    ?assertMatch(ok, lfm_proxy:truncate(W1, SessionId, {uuid, FileUuid}, 6)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    %then
    {ok, [LocationId]} = ?assertMatch({ok, [_]}, rpc:call(W1, file_meta, get_locations, [{uuid, FileUuid}])),
    {ok, #document{value = #file_location{blocks = Blocks, size = Size, version_vector = VV}}} =
        ?assertMatch({ok, _}, rpc:call(W1, file_location, get, [LocationId])),
    ?assertEqual(6, Size),
%%    ?assertEqual(#{?GET_DOMAIN(W1) => 2}, VV), %todo add VV and uncomment
    ?assertMatch([#file_block{offset = 0, size = 6}], Blocks).

write_and_truncate_should_not_update_remote_file_location(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = <<"session_id1">>,
    SpaceId = <<"space_id1">>,
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,
    ExternalBlocks = [#file_block{offset = 0, size = 10, file_id = ExternalFileId, storage_id = <<"external_storage_id">>}],
    {ok, FileUuid} = lfm_proxy:create(W1, SessionId, <<"test_file">>, 8#777),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {uuid, FileUuid}, rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    RemoteLocation = #file_location{size = 10, space_id = SpaceId,
        storage_id = <<"external_storage_id">>, provider_id = ExternalProviderId,
        blocks = ExternalBlocks, file_id = ExternalFileId, uuid = FileUuid,
        version_vector = #{}},
    {ok, RemoteLocationId} = ?assertMatch({ok, _},
        rpc:call(W1, file_location, create, [#document{value = RemoteLocation}])),
    ?assertEqual(ok, rpc:call(W1, file_meta, attach_location,
        [{uuid, FileUuid}, RemoteLocationId, ExternalProviderId])),

    % when
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 1, <<"00">>)),
    ?assertMatch(ok, lfm_proxy:truncate(W1, SessionId, {uuid, FileUuid}, 8)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % then
    ?assertMatch({ok, #document{value = RemoteLocation}},
        rpc:call(W1, file_location, get, [RemoteLocationId])).

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
    application:start(ssl2),
    hackney:start(),
    mock_provider_id(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    tracer:start(Workers),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces(Config),
    unmock_provider_id(Config),
    hackney:stop(),
    application:stop(ssl2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

mock_provider_id(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [oneprovider]),
    test_utils:mock_expect(Workers, oneprovider, get_provider_id,
        fun() ->
            atom_to_binary(?GET_DOMAIN(node()), unicode)
        end
    ).

unmock_provider_id(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, oneprovider).