%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Multi provider rest tests
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_rest_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
-include("http/rest/cdmi/cdmi_capabilities.hrl").
-include("http/rest/http_status.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    get_simple_file_distribution/1,
    replicate_file/1,
    replicate_dir/1,
    posix_mode_get/1,
    posix_mode_put/1,
    xattr_get/1,
    xattr_put/1,
    xattr_list/1,
    metric_get/1,
    list_file/1,
    list_dir/1,
    list_dir_range/1,
    replicate_file_by_id/1,
    changes_stream_file_meta_test/1,
    changes_stream_xattr_test/1,
    list_spaces/1,
    get_space/1,
    set_get_json_metadata/1,
    set_get_json_metadata_id/1,
    set_get_rdf_metadata/1,
    set_get_rdf_metadata_id/1,
    changes_stream_json_metadata_test/1,
    create_list_index/1,
    set_get_json_metadata_inherited/1,
    set_get_xattr_inherited/1,
    set_get_json_metadata_using_filter/1
]).

all() ->
    ?ALL([
        get_simple_file_distribution,
        replicate_file,
        replicate_dir,
        posix_mode_get,
        posix_mode_put,
        xattr_get,
        xattr_put,
        xattr_list,
        metric_get,
        list_file,
        list_dir,
        list_dir_range,
        replicate_file_by_id,
        changes_stream_file_meta_test,
        changes_stream_xattr_test,
        list_spaces,
        get_space,
        set_get_json_metadata,
        set_get_json_metadata_id,
        set_get_rdf_metadata,
        set_get_rdf_metadata_id,
        changes_stream_json_metadata_test,
        create_list_index,
        set_get_json_metadata_inherited,
        set_get_xattr_inherited,
        set_get_json_metadata_using_filter
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_simple_file_distribution(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File =  list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file0"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"replicas", File/binary>>, get, [user_1_token_header(Config)], []),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertEqual([[{<<"providerId">>, domain(WorkerP1)}, {<<"blocks">>, [[0,4]]}]], DecodedBody).

replicate_file(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    File = <<"/space3/file">>,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    ?assertMatch(4, length(rpc:call(WorkerP2, file_consistency, check_missing_components,
        [fslogic_uuid:file_guid_to_uuid(FileGuid), <<"space3">>])), 15),
    timer:sleep(timer:seconds(2)), % for hooks
    {ok, 200, _, Body0} = do_request(WorkerP1, <<"replicas/space3/file?provider_id=", (domain(WorkerP2))/binary>>, post, [user_1_token_header(Config)], []),
    DecodedBody0 = json_utils:decode(Body0),
    [{<<"transferId">>, Tid}] = ?assertMatch([{<<"transferId">>, _}], DecodedBody0),

    % then
    ExpectedTransferStatus = erlang:iolist_to_binary([
        <<"{\"path\":\"/space3/file\",\"status\":\"completed\",\"targetProviderId\":\"">>,
        domain(WorkerP2),
        <<"\"}">>
    ]),
    ?assertMatch({ok, 200, _, ExpectedTransferStatus},
        do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []), 5),
    timer:sleep(timer:seconds(5)), % TODO - reorganize tests to remove sleeps
    {ok, 200, _, Body} = do_request(WorkerP2, <<"replicas/space3/file">>, get, [user_1_token_header(Config)], []),
    timer:sleep(timer:seconds(10)),
    {ok, 200, _, Body2} = do_request(WorkerP1, <<"replicas/space3/file">>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode(Body),
    DecodedBody2 = json_utils:decode(Body2),
    assertLists(
        [
            [{<<"providerId">>, domain(WorkerP1)}, {<<"blocks">>, [[0,4]]}],
            [{<<"providerId">>, domain(WorkerP2)}, {<<"blocks">>, [[0,4]]}]
        ], DecodedBody),
    assertLists(
        [
            [{<<"providerId">>, domain(WorkerP1)}, {<<"blocks">>, [[0,4]]}],
            [{<<"providerId">>, domain(WorkerP2)}, {<<"blocks">>, [[0,4]]}]
        ], DecodedBody2).

replicate_dir(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    Dir1 = <<"/space3/dir1">>,
    Dir2 = <<"/space3/dir1/dir2">>,
    File1 = <<"/space3/dir1/file1">>,
    File2 = <<"/space3/dir1/file2">>,
    File3 = <<"/space3/dir1/dir2/file3">>,
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir1),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir2),
    {ok, File1Guid} = lfm_proxy:create(WorkerP1, SessionId, File1, 8#700),
    {ok, Handle1} = lfm_proxy:open(WorkerP1, SessionId, {guid, File1Guid}, write),
    lfm_proxy:write(WorkerP1, Handle1, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP1, Handle1),
    {ok, File2Guid} = lfm_proxy:create(WorkerP1, SessionId, File2, 8#700),
    {ok, Handle2} = lfm_proxy:open(WorkerP1, SessionId, {guid, File2Guid}, write),
    lfm_proxy:write(WorkerP1, Handle2, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP1, Handle2),
    {ok, File3Guid} = lfm_proxy:create(WorkerP1, SessionId, File3, 8#700),
    {ok, Handle3} = lfm_proxy:open(WorkerP1, SessionId, {guid, File3Guid}, write),
    lfm_proxy:write(WorkerP1, Handle3, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP1, Handle3),

    % when
    ?assertMatch(4, length(rpc:call(WorkerP2, file_consistency, check_missing_components,
        [fslogic_uuid:file_guid_to_uuid(File1Guid), <<"space3">>])), 15),
    ?assertMatch(4, length(rpc:call(WorkerP2, file_consistency, check_missing_components,
        [fslogic_uuid:file_guid_to_uuid(File2Guid), <<"space3">>])), 15),
    ?assertMatch(4, length(rpc:call(WorkerP2, file_consistency, check_missing_components,
        [fslogic_uuid:file_guid_to_uuid(File3Guid), <<"space3">>])), 15),
    timer:sleep(timer:seconds(2)), % for hooks
    {ok, 200, _, Body} = do_request(WorkerP1, <<"replicas/space3/dir1?provider_id=", (domain(WorkerP2))/binary>>, post, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode(Body),
    [{<<"transferId">>, Tid}] = ?assertMatch([{<<"transferId">>, _}], DecodedBody),

    % then
    ExpectedTransferStatus = erlang:iolist_to_binary([
        <<"{\"path\":\"/space3/dir1\",\"status\":\"completed\",\"targetProviderId\":\"">>,
        domain(WorkerP2),
        <<"\"}">>
    ]),
    ?assertMatch({ok, 200, _, ExpectedTransferStatus},
        do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []), 5),

    {ok, 200, _, Body1} = do_request(WorkerP2, <<"replicas/space3/dir1/file1">>, get, [user_1_token_header(Config)], []),
    {ok, 200, _, Body2} = do_request(WorkerP2, <<"replicas/space3/dir1/file2">>, get, [user_1_token_header(Config)], []),
    {ok, 200, _, Body3} = do_request(WorkerP2, <<"replicas/space3/dir1/dir2/file3">>, get, [user_1_token_header(Config)], []),
    DecodedBody1 = json_utils:decode(Body1),
    DecodedBody2 = json_utils:decode(Body2),
    DecodedBody3 = json_utils:decode(Body3),
    Distribution = [
        [{<<"providerId">>, domain(WorkerP1)}, {<<"blocks">>, [[0,4]]}],
        [{<<"providerId">>, domain(WorkerP2)}, {<<"blocks">>, [[0,4]]}]
    ],
    assertLists(Distribution, DecodedBody1),
    assertLists(Distribution, DecodedBody2),
    assertLists(Distribution, DecodedBody3).

posix_mode_get(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File =  list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file1"])),
    Mode = 8#700,
    {ok, _FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"attributes", File/binary, "?attribute=mode">>, get, [user_1_token_header(Config)], []),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertEqual(
        [[
            {<<"value">>, <<"0", (integer_to_binary(Mode, 8))/binary>>},
            {<<"name">>, <<"mode">>}
        ]],
        DecodedBody
    ).

posix_mode_put(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File =  list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file2"])),
    Mode = 8#700,
    {ok, _FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    NewMode = 8#777,
    Body = json_utils:encode([{<<"name">>, <<"mode">>}, {<<"value">>, <<"0", (integer_to_binary(NewMode, 8))/binary>>}]),
    {ok, 204, _, _} = do_request(WorkerP1, <<"attributes", File/binary>>, put,
        [user_1_token_header(Config), {<<"Content-Type">>, <<"application/json">>}], Body),

    % then
    {ok, 200, _, RespBody} = do_request(WorkerP1, <<"attributes", File/binary, "?attribute=mode">>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode(RespBody),
    ?assertEqual(
        [[
            {<<"value">>, <<"0", (integer_to_binary(NewMode, 8))/binary>>},
            {<<"name">>, <<"mode">>}
        ]],
        DecodedBody
    ).

xattr_get(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File =  list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file1"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    ok = lfm_proxy:set_xattr(WorkerP1, SessionId, {guid, FileGuid}, #xattr{name = <<"k1">>, value = <<"v1">>}),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"attributes", File/binary, "?attribute=k1&extended=true">>, get, [user_1_token_header(Config)], []),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertEqual(
        [[
            {<<"value">>, <<"v1">>},
            {<<"name">>, <<"k1">>}
        ]],
        DecodedBody
    ).

xattr_put(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File =  list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file2"])),
    {ok, _FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),

    % when
    Body = json_utils:encode([{<<"name">>, <<"k1">>}, {<<"value">>, <<"v1">>}]),
    {ok, 204, _, _} = do_request(WorkerP1, <<"attributes", File/binary, "?extended=true">>, put,
        [user_1_token_header(Config), {<<"Content-Type">>, <<"application/json">>}], Body),

    % then
    {ok, 200, _, RespBody} = do_request(WorkerP1, <<"attributes", File/binary, "?attribute=k1&extended=true">>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode(RespBody),
    ?assertEqual(
        [[
            {<<"value">>, <<"v1">>},
            {<<"name">>, <<"k1">>}
        ]],
        DecodedBody
    ).

xattr_list(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File =  list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file1"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    ok = lfm_proxy:set_xattr(WorkerP1, SessionId, {guid, FileGuid}, #xattr{name = <<"k1">>, value = <<"v1">>}),
    ok = lfm_proxy:set_xattr(WorkerP1, SessionId, {guid, FileGuid}, #xattr{name = <<"k2">>, value = <<"v2">>}),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"attributes", File/binary, "?extended=true">>, get, [user_1_token_header(Config)], []),

    % then
    DecodedBody = jiffy:decode(Body, [return_maps]),
    ?assertMatch(
        [
            #{<<"name">> := <<"k1">>, <<"value">> := <<"v1">>},
            #{<<"name">> := <<"k2">>, <<"value">> := <<"v2">>}
        ],
        DecodedBody
    ).

metric_get(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    Prov1ID = rpc:call(WorkerP1, oneprovider, get_provider_id, []),
    Prov2ID = rpc:call(WorkerP2, oneprovider, get_provider_id, []),

    MonitoringId = #monitoring_id{
        main_subject_type = space,
        main_subject_id = <<"space3">>,
        metric_type = storage_quota,
        provider_id = Prov1ID
    },

    ?assertMatch(ok, rpc:call(WorkerP1, monitoring_utils, create, [<<"space3">>, MonitoringId, erlang:system_time(seconds)])),
    {ok, #document{value = State}} = rpc:call(WorkerP1, monitoring_state, get, [MonitoringId]),
    ?assertMatch({ok, _}, rpc:call(WorkerP1, monitoring_state, save,
        [#document{key = MonitoringId#monitoring_id{provider_id = Prov2ID}, value =  State}])),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"metrics/space/space3?metric=storage_quota">>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode(Body),

    % then
    [
        [{_, DecodedProv1ID}, _],
        [{_, DecodedProv2ID}, _]
    ] = DecodedBody,
    ?assertNotEqual(DecodedProv1ID, DecodedProv2ID),
    ?assertMatch([
        [{<<"providerId">>, _}, {<<"rrd">>, _}],
        [{<<"providerId">>, _}, {<<"rrd">>, _}]
    ], DecodedBody).

list_file(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    File = <<"/space3/file1">>,
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, Body},
        do_request(WorkerP1, <<"files/space3/file1">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode(Body),
    {ok, FileObjectId} = cdmi_id:uuid_to_objectid(FileGuid),
    ?assertEqual(
        [[{<<"id">>, FileObjectId}, {<<"path">>, File}]],
        DecodedBody
    ).

list_dir(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, Body},
        do_request(WorkerP1, <<"files">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(
        [
            [{<<"id">>, _}, {<<"path">>, <<"/space1">>}],
            [{<<"id">>, _}, {<<"path">>, <<"/space2">>}],
            [{<<"id">>, _}, {<<"path">>, <<"/space3">>}]
        ],
        DecodedBody
    ).

list_dir_range(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, Body},
        do_request(WorkerP1, <<"files?offset=1&limit=1">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(
        [
            [{<<"id">>, _}, {<<"path">>, <<"/space2">>}]
        ],
        DecodedBody
    ).

replicate_file_by_id(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    File = <<"/space3/file">>,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    ?assertMatch(4, length(rpc:call(WorkerP2, file_consistency, check_missing_components,
        [fslogic_uuid:file_guid_to_uuid(FileGuid), <<"space3">>])), 15),
    timer:sleep(timer:seconds(2)), % for hooks
    {ok, FileObjectId} = cdmi_id:uuid_to_objectid(FileGuid),
    {ok, 200, _, Body0} = do_request(WorkerP1, <<"replicas-id/", FileObjectId/binary,"?provider_id=", (domain(WorkerP2))/binary>>, post, [user_1_token_header(Config)], []),
    DecodedBody0 = json_utils:decode(Body0),
    [{<<"transferId">>, Tid}] = ?assertMatch([{<<"transferId">>, _}], DecodedBody0),

    % then
    ExpectedTransferStatus = erlang:iolist_to_binary([
        <<"{\"path\":\"/space3/file\",\"status\":\"completed\",\"targetProviderId\":\"">>,
        domain(WorkerP2),
        <<"\"}">>
    ]),
    ?assertMatch({ok, 200, _, ExpectedTransferStatus},
        do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []), 5),
    {ok, 200, _, Body} = do_request(WorkerP2, <<"replicas-id/", FileObjectId/binary>>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode(Body),
    assertLists(
        [
            [{<<"providerId">>, domain(WorkerP1)}, {<<"blocks">>, [[0,4]]}],
            [{<<"providerId">>, domain(WorkerP2)}, {<<"blocks">>, [[0,4]]}]
        ], DecodedBody).

changes_stream_file_meta_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File =  list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file3"])),
    File2 =  list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file3"])),
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, rdwr),

    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:write(WorkerP1, Handle, 0, <<"data">>),
        lfm_proxy:read(WorkerP1, Handle, 0, 2),
        lfm_proxy:set_perms(WorkerP1, SessionId, {guid, FileGuid}, 8#777),
        lfm_proxy:fsync(WorkerP1, Handle),
        lfm_proxy:create(WorkerP1, SessionId, File2, Mode)
    end),
    {ok, 200, _, Body} = do_request(WorkerP1, <<"changes/metadata/space1?timeout=10000">>,
        get, [user_1_token_header(Config)], []),

    ?assertNotEqual(<<>>, Body),
    ?assert(length(binary:split(Body, <<"\r\n">>, [global])) >= 2).

changes_stream_xattr_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File =  list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file4"])),
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:set_xattr(WorkerP1, SessionId, {guid, FileGuid}, #xattr{name = <<"name">>, value = <<"value">>})
    end),
    {ok, 200, _, Body} = do_request(WorkerP1, <<"changes/metadata/space1?timeout=10000">>,
        get, [user_1_token_header(Config)], []),

    ?assertNotEqual(<<>>, Body),
    Changes = binary:split(Body, <<"\r\n">>, [global]),
    ?assert(length(Changes) >= 1),
    [_, LastChangeJson | _] = lists:reverse(Changes),
    LastChange = json_utils:decode(LastChangeJson),
    Metadata = proplists:get_value(<<"changes">>, LastChange),
    ?assertEqual([{<<"name">>, <<"value">>}], proplists:get_value(<<"xattrs">>, Metadata)).

changes_stream_json_metadata_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File =  list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file4"])),
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),
    Json = #{<<"k1">> => <<"v1">>, <<"k2">> => [<<"v2">>, <<"v3">>], <<"k3">> => #{<<"k31">> => <<"v31">>}},
    % when
    spawn(fun() ->
        timer:sleep(5000),
        lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, FileGuid}, <<"json">>, Json, [])
    end),
    {ok, 200, _, Body} = do_request(WorkerP1, <<"changes/metadata/space1?timeout=10000">>,
        get, [user_1_token_header(Config)], []),

    ?assertNotEqual(<<>>, Body),
    Changes = binary:split(Body, <<"\r\n">>, [global]),
    ?assert(length(Changes) >= 1),
    [_ | AllChanges] = lists:reverse(Changes),
    DecodedChanges =
        lists:map(fun(Change) ->
            jiffy:decode(Change, [return_maps])
            end, AllChanges),

    ?assert(lists:any(fun(Change) ->
        Json == maps:get(<<"onedata_json">>, maps:get(<<"xattrs">>, maps:get(<<"changes">>, Change)))
    end, DecodedChanges)).

list_spaces(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, Body},
        do_request(WorkerP1, <<"spaces">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(
        [
            [{<<"name">>, <<"space1">>}, {<<"spaceId">>, <<"space1">>}],
            [{<<"name">>, <<"space2">>}, {<<"spaceId">>, <<"space2">>}],
            [{<<"name">>, <<"space3">>}, {<<"spaceId">>, <<"space3">>}]
        ],
        DecodedBody
    ).

get_space(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, Body},
        do_request(WorkerP1, <<"spaces/space3">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(
        [
            {<<"name">>, <<"space3">>},
            {<<"providers">>, [
                [
                    {<<"providerId">>, PID1},
                    {<<"providerName">>, PID1}
                ],
                [
                    {<<"providerId">>, PID2},
                    {<<"providerName">>, PID2}
                ]
            ]},
            {<<"spaceId">>, <<"space3">>}
        ],
        DecodedBody
    ).

set_get_json_metadata(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>,<<"application/json">>}], "{\"key\": \"value\"}")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, Body},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, get,
            [user_1_token_header(Config), {<<"accept">>,<<"application/json">>}], [])),
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(
        [
            {<<"key">>, <<"value">>}
        ],
        DecodedBody
    ),

    % then
    ?assertMatch({ok, 200, _, <<"\"value\"">>},
        do_request(WorkerP1, <<"metadata/space3?filter_type=keypath&filter=key">>, get,
            [user_1_token_header(Config), {<<"accept">>,<<"application/json">>}], [])).

set_get_json_metadata_id(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    {ok, Guid} = lfm_proxy:create(WorkerP1, SessionId, <<"/space3/file">>, 8#777),
    {ok, ObjectId} = cdmi_id:uuid_to_objectid(Guid),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata-id/", ObjectId/binary, "?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>,<<"application/json">>}], "{\"key\": \"value\"}")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, Body},
        do_request(WorkerP1, <<"metadata-id/", ObjectId/binary, "?metadata_type=json">>, get,
            [user_1_token_header(Config), {<<"accept">>,<<"application/json">>}], [])),
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(
        [
            {<<"key">>, <<"value">>}
        ],
        DecodedBody
    ),

    % then
    ?assertMatch({ok, 200, _, <<"\"value\"">>},
        do_request(WorkerP1, <<"metadata-id/", ObjectId/binary, "?filter_type=keypath&filter=key">>, get,
            [user_1_token_header(Config), {<<"accept">>,<<"application/json">>}], [])).


set_get_rdf_metadata(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=rdf">>, put,
            [user_1_token_header(Config), {<<"content-type">>,<<"application/rdf+xml">>}], "some_xml")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, Body},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=rdf">>, get,
            [user_1_token_header(Config), {<<"accept">>,<<"application/rdf+xml">>}], [])),
    ?assertMatch(<<"some_xml">>, Body).

set_get_rdf_metadata_id(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    {ok, Guid} = lfm_proxy:create(WorkerP1, SessionId, <<"/space3/file">>, 8#777),
    {ok, ObjectId} = cdmi_id:uuid_to_objectid(Guid),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata-id/", ObjectId/binary, "?metadata_type=rdf">>, put,
            [user_1_token_header(Config), {<<"content-type">>,<<"application/rdf+xml">>}], "some_xml")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, Body},
        do_request(WorkerP1, <<"metadata-id/", ObjectId/binary, "?metadata_type=rdf">>, get,
            [user_1_token_header(Config), {<<"accept">>,<<"application/rdf+xml">>}], [])),
    ?assertMatch(<<"some_xml">>, Body).

create_list_index(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Function =
        <<"function (meta) {
              if(meta['onedata_json'] && meta['onedata_json']['meta'] && meta['onedata_json']['meta']['color']) {
                  return meta['onedata_json']['meta']['color'];
              }
              return null;
        }">>,
    ?assertMatch({ok, 200, _, <<"[]">>}, do_request(WorkerP1, <<"index">>, get, [user_1_token_header(Config)], [])),

    % when
    {ok, 303, Headers, _} = ?assertMatch({ok, 303, _, _},
        do_request(WorkerP1, <<"index?space_id=space1&name=name">>, post, [user_1_token_header(Config), {<<"content-type">>,<<"text/javascript">>}], Function)),
    <<"/api/v3/oneprovider/index/", Id/binary>> = proplists:get_value(<<"location">>, Headers),

    % then
    {ok, _, _, ListBody} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"index">>, get, [user_1_token_header(Config)], [])),
    IndexList = jiffy:decode(ListBody, [return_maps]),
    ?assertMatch([#{<<"spaceId">> := <<"space1">>, <<"name">> := <<"name">>, <<"indexId">> := Id}], IndexList),
    ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"index/", Id/binary>>, get, [user_1_token_header(Config), {<<"accept">>, <<"text/javascript">>}], [])),

    % when
    {ok, 303, _, _} = ?assertMatch({ok, 303, _, _},
        do_request(WorkerP1, <<"index?space_id=space1&name=name2">>, post,
            [user_1_token_header(Config), {<<"content-type">>,<<"text/javascript">>}], Function)),

    % then
    {ok, _, _, ListBody2} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"index">>, get,
        [user_1_token_header(Config)], [])),
    IndexList2 = jiffy:decode(ListBody2, [return_maps]),
    ?assertMatch([#{}, #{}], IndexList2).

set_get_json_metadata_inherited(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>,<<"application/json">>}], <<"{\"a\": {\"a1\": \"b1\"}, \"b\": \"c\", \"e\": \"f\"}">>)),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, <<"/space3/dir">>),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3/dir?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>,<<"application/json">>}], <<"{\"a\": {\"a2\": \"b2\"}, \"b\": \"d\"}">>)),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, Body},
        do_request(WorkerP1, <<"metadata/space3/dir?metadata_type=json&inherited=true">>, get,
            [user_1_token_header(Config), {<<"accept">>,<<"application/json">>}], [])),
    DecodedBody = jiffy:decode(Body, [return_maps]),
    ?assertMatch(
        #{
            <<"a">> := #{<<"a1">> := <<"b1">>, <<"a2">> := <<"b2">>},
            <<"b">> := <<"d">>,
            <<"e">> := <<"f">>
        },
        DecodedBody
    ).

set_get_xattr_inherited(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, <<"/space3/dir_test">>),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, <<"/space3/dir_test/child">>),

    % when
    XattrSpace = json_utils:encode([{<<"name">>, <<"k1">>}, {<<"value">>, <<"v1">>}]),
    XattrDir = json_utils:encode([{<<"name">>, <<"k2">>}, {<<"value">>, <<"v2">>}]),
    XattrChild = json_utils:encode([{<<"name">>, <<"k2">>}, {<<"value">>, <<"v22">>}]),
    XattrChild2 = json_utils:encode([{<<"name">>, <<"k3">>}, {<<"value">>, <<"v3">>}]),

    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"attributes/space3?extended=true">>, put,
            [user_1_token_header(Config), {<<"content-type">>,<<"application/json">>}], XattrSpace)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"attributes/space3/dir_test?extended=true">>, put,
            [user_1_token_header(Config), {<<"content-type">>,<<"application/json">>}], XattrDir)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"attributes/space3/dir_test/child?extended=true">>, put,
            [user_1_token_header(Config), {<<"content-type">>,<<"application/json">>}], XattrChild)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"attributes/space3/dir_test/child?extended=true">>, put,
            [user_1_token_header(Config), {<<"content-type">>,<<"application/json">>}], XattrChild2)),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, Body},
        do_request(WorkerP1, <<"attributes/space3/dir_test/child?inherited=true&extended=true">>, get,
            [user_1_token_header(Config), {<<"accept">>,<<"application/json">>}], [])),
    tracer:stop(),
    DecodedBody = jiffy:decode(Body, [return_maps]),
    ?assertMatch(
        [
            #{<<"name">> := <<"k1">>, <<"value">> := <<"v1">>},
            #{<<"name">> := <<"k2">>, <<"value">> := <<"v22">>},
            #{<<"name">> := <<"k3">>, <<"value">> := <<"v3">>}
        ],
        DecodedBody
    ).

set_get_json_metadata_using_filter(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>,<<"application/json">>}], <<"{\"key1\": \"value1\", \"key2\": \"value2\"}">>)),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, Body},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json&filter_type=keypath&filter=key1">>, get,
            [user_1_token_header(Config), {<<"accept">>,<<"application/json">>}], [])),
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(<<"value1">>, DecodedBody),

    %when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json&filter_type=keypath&filter=key1">>, put,
            [user_1_token_header(Config), {<<"content-type">>,<<"application/json">>}], <<"\"value11\"">>)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json&filter_type=keypath&filter=key2">>, put,
            [user_1_token_header(Config), {<<"content-type">>,<<"application/json">>}], <<"{\"key22\": \"value22\"}">>)),

    %then
    {_, _, _, ReponseBody} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, get,
            [user_1_token_header(Config), {<<"accept">>,<<"application/json">>}], [])),
    ?assertMatch(
        #{
            <<"key1">> := <<"value11">>,
            <<"key2">> := #{<<"key22">> := <<"value22">>}
        },
        json_utils:decode_map(ReponseBody)).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    initializer:setup_storage(ConfigWithNodes).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    test_node_starter:clean_environment(Config).

init_per_testcase(metric_get, Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(WorkerP1, rrd_utils),
    test_utils:mock_expect(WorkerP1, rrd_utils, export_rrd, fun(_, _, _) ->
        {ok, <<"{\"test\":\"rrd\"}">>}
    end),
    init_per_testcase(all, Config);

init_per_testcase(_, Config) ->
    application:start(etls),
    hackney:start(),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    initializer:enable_grpca_based_communication(Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(metric_get, Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(WorkerP1, rrd_utils),
    end_per_testcase(all, Config);

end_per_testcase(_, Config) ->
    lfm_proxy:teardown(Config),
     %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:disable_grpca_based_communication(Config),
    hackney:stop(),
    application:stop(etls).

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_request(Node, URL, Method, Headers, Body) ->
    http_client:request(Method, <<(rest_endpoint(Node))/binary,  URL/binary>>, Headers, Body, [insecure, {recv_timeout, 15000}]).

rest_endpoint(Node) ->
    Port =
        case get(port) of
            undefined ->
                {ok, P} = test_utils:get_env(Node, ?APP_NAME, rest_port),
                PStr = integer_to_binary(P),
                PStr;
            P -> P
        end,
    <<"https://", (list_to_binary(utils:get_host(Node)))/binary, ":", Port/binary, "/api/v3/oneprovider/">>.


user_1_token_header(Config) ->
    #token_auth{macaroon = Macaroon} = ?config({auth, <<"user1">>}, Config),
    {ok, Srlzd} = token_utils:serialize62(Macaroon),
    {<<"X-Auth-Token">>, Srlzd}.

domain(Node) ->
    atom_to_binary(?GET_DOMAIN(Node), utf8).

assertLists(L1, L2) ->
    ?assertEqual(length(L1), length(L2)),
    lists:foreach(fun(E) ->
        ?assert(lists:member(E, L2))
    end, L1),
    lists:foreach(fun(E) ->
        ?assert(lists:member(E, L1))
    end, L2).