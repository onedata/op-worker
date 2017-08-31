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
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    get_simple_file_distribution/1,
    replicate_file/1,
    replicate_dir/1,
    replicate_file_by_id/1,
    replicate_to_missing_provider/1,
    replicate_to_nunsupporting_provider/1,
    invalidate_file_replica/1,
    invalidate_dir_replica/1,
    periodic_cleanup_should_invalidate_unpopular_files/1,
    posix_mode_get/1,
    posix_mode_put/1,
    attributes_list/1,
    xattr_get/1,
    xattr_put/1,
    xattr_list/1,
    metric_get/1,
    list_file/1,
    list_dir/1,
    list_dir_range/1,
    changes_stream_file_meta_test/1,
    changes_stream_xattr_test/1,
    changes_stream_json_metadata_test/1,
    changes_stream_times_test/1,
    changes_stream_file_location_test/1,
    changes_stream_on_multi_provider_test/1,
    list_spaces/1,
    get_space/1,
    set_get_json_metadata/1,
    set_get_json_metadata_id/1,
    set_get_rdf_metadata/1,
    set_get_rdf_metadata_id/1,
    create_list_index/1,
    remove_index/1,
    create_geospatial_index/1,
    query_geospatial_index/1,
    query_file_popularity_index/1,
    set_get_json_metadata_inherited/1,
    set_get_xattr_inherited/1,
    set_get_json_metadata_using_filter/1,
    primitive_json_metadata_test/1,
    empty_metadata_invalid_json_test/1,
    spatial_flag_test/1
]).

all() ->
    ?ALL([
        get_simple_file_distribution,
        replicate_file,
        replicate_dir,
        replicate_file_by_id,
        replicate_to_missing_provider,
        replicate_to_nunsupporting_provider,
        invalidate_file_replica,
        invalidate_dir_replica,
        periodic_cleanup_should_invalidate_unpopular_files,
        posix_mode_get,
        posix_mode_put,
        attributes_list,
        xattr_get,
        xattr_put,
        xattr_list,
        metric_get,
        list_file,
        list_dir,
        list_dir_range,
        changes_stream_file_meta_test,
        changes_stream_xattr_test,
        changes_stream_json_metadata_test,
        changes_stream_times_test,
        changes_stream_file_location_test,
        changes_stream_on_multi_provider_test, %todo fix VFS-2864
        list_spaces,
        get_space,
        set_get_json_metadata,
        set_get_json_metadata_id,
        set_get_rdf_metadata,
        set_get_rdf_metadata_id,
        remove_index,
        create_list_index,
        create_geospatial_index,
        query_geospatial_index,
        query_file_popularity_index,
        set_get_json_metadata_inherited,
        set_get_xattr_inherited,
        set_get_json_metadata_using_filter,
        primitive_json_metadata_test,
        empty_metadata_invalid_json_test,
        spatial_flag_test
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_simple_file_distribution(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file0_gsfd"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"replicas", File/binary>>, get, [user_1_token_header(Config)], []),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertEqual([
        #{
            <<"providerId">> => domain(WorkerP1),
            <<"blocks">> => [[0, 4]]
        }
    ], DecodedBody).

replicate_file(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    File = <<"/space3/file">>,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    timer:sleep(timer:seconds(20)), % for hooks todo VFS-3462
    {ok, 200, _, Body0} = do_request(WorkerP1, <<"replicas/space3/file?provider_id=", (domain(WorkerP2))/binary>>, post, [user_1_token_header(Config)], []),
    DecodedBody0 = json_utils:decode_map(Body0),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody0),

    % then
    ExpectedTransferStatus = erlang:iolist_to_binary([
        <<"{\"targetProviderId\":\"">>, domain(WorkerP2),
        <<"\",\"status\":\"completed\",\"path\":\"/space3/file\"}">>
    ]),
    ?assertMatch({ok, 200, _, ExpectedTransferStatus},
        do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []), 5),
    timer:sleep(timer:seconds(20)), % todo VFS-3462 - reorganize tests to remove sleeps
    {ok, 200, _, Body} = do_request(WorkerP2, <<"replicas/space3/file">>, get, [user_1_token_header(Config)], []),
    {ok, 200, _, Body2} = do_request(WorkerP1, <<"replicas/space3/file">>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode_map(Body),
    DecodedBody2 = json_utils:decode_map(Body2),
    ?assertEqual(
        lists:sort([
            #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
            #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
        ]), lists:sort(DecodedBody)),
    ?assertEqual(
        lists:sort([
            #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
            #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
        ]), lists:sort(DecodedBody2)).

replicate_dir(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    Dir1 = <<"/space3/dir1_rd">>,
    Dir2 = <<"/space3/dir1_rd/dir2">>,
    File1 = <<"/space3/dir1_rd/file1">>,
    File2 = <<"/space3/dir1_rd/file2">>,
    File3 = <<"/space3/dir1_rd/dir2/file3">>,
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
    timer:sleep(timer:seconds(20)), % for hooks todo  VFS-3462
    {ok, 200, _, Body} = do_request(WorkerP1, <<"replicas/space3/dir1_rd?provider_id=", (domain(WorkerP2))/binary>>, post, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode_map(Body),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),

    % then
    ExpectedTransferStatus = erlang:iolist_to_binary([
        <<"{\"targetProviderId\":\"">>, domain(WorkerP2),
        <<"\",\"status\":\"completed\",\"path\":\"/space3/dir1_rd\"}">>
    ]),
    ?assertMatch({ok, 200, _, ExpectedTransferStatus},
        do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []), 5),

    timer:sleep(timer:seconds(20)), % for hooks todo  VFS-3462
    {ok, 200, _, Body1} = do_request(WorkerP2, <<"replicas/space3/dir1_rd/file1">>, get, [user_1_token_header(Config)], []),
    {ok, 200, _, Body2} = do_request(WorkerP2, <<"replicas/space3/dir1_rd/file2">>, get, [user_1_token_header(Config)], []),
    {ok, 200, _, Body3} = do_request(WorkerP2, <<"replicas/space3/dir1_rd/dir2/file3">>, get, [user_1_token_header(Config)], []),
    DecodedBody1 = json_utils:decode_map(Body1),
    DecodedBody2 = json_utils:decode_map(Body2),
    DecodedBody3 = json_utils:decode_map(Body3),
    Distribution = lists:sort([
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
    ]),
    ?assertEqual(Distribution, lists:sort(DecodedBody1)),
    ?assertEqual(Distribution, lists:sort(DecodedBody2)),
    ?assertEqual(Distribution, lists:sort(DecodedBody3)).

replicate_file_by_id(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    File = <<"/space3/replicate_file_by_id">>,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    timer:sleep(timer:seconds(20)), % for hooks todo  VFS-3462
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    {ok, 200, _, Body0} = do_request(WorkerP1, <<"replicas-id/", FileObjectId/binary, "?provider_id=", (domain(WorkerP2))/binary>>, post, [user_1_token_header(Config)], []),
    DecodedBody0 = json_utils:decode_map(Body0),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody0),

    % then
    ExpectedTransferStatus = erlang:iolist_to_binary([
        <<"{\"targetProviderId\":\"">>, domain(WorkerP2),
        <<"\",\"status\":\"completed\",\"path\":\"/space3/replicate_file_by_id\"}">>
    ]),
    timer:sleep(timer:seconds(20)), % for hooks todo  VFS-3462
    ?assertMatch({ok, 200, _, ExpectedTransferStatus},
        do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []), 5),
    {ok, 200, _, Body} = do_request(WorkerP2, <<"replicas-id/", FileObjectId/binary>>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode_map(Body),
    ?assertEqual(
        lists:sort([
            #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
            #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
        ]), lists:sort(DecodedBody)).

replicate_to_missing_provider(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    File = <<"/space3/replicate_to_missing_provider">>,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    timer:sleep(timer:seconds(30)), % for hooks todo VFS-3462
    {ok, 200, _, Body0} = do_request(WorkerP1, <<"replicas/space3/replicate_to_missing_provider?provider_id=missing_id">>, post, [user_1_token_header(Config)], []),
    DecodedBody0 = json_utils:decode_map(Body0),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody0),

    % then
    ExpectedTransferStatus = erlang:iolist_to_binary([
        <<"{\"targetProviderId\":\"">>, <<"missing_id">>,
        <<"\",\"status\":\"failed\",\"path\":\"/space3/replicate_to_missing_provider\"}">>
    ]),
    ?assertMatch({ok, 200, _, ExpectedTransferStatus},
        do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []), 5).

replicate_to_nunsupporting_provider(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    File = <<"/space1/file">>,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    {ok, 200, _, Body0} = do_request(WorkerP1, <<"replicas/space1/file?provider_id=", (domain(WorkerP2))/binary>>, post, [user_1_token_header(Config)], []),
    DecodedBody0 = json_utils:decode_map(Body0),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody0),

    % then
    ExpectedTransferStatus = erlang:iolist_to_binary([
        <<"{\"targetProviderId\":\"">>, domain(WorkerP2),
        <<"\",\"status\":\"failed\",\"path\":\"/space1/file\"}">>
    ]),
    ?assertMatch({ok, 200, _, ExpectedTransferStatus},
        do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []), 5),
    {ok, 200, _, Body2} = do_request(WorkerP1, <<"replicas/space1/file">>, get, [user_1_token_header(Config)], []),
    DecodedBody2 = json_utils:decode_map(Body2),
    ?assertEqual(
        [
            #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]}
        ], DecodedBody2).

invalidate_file_replica(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    File = <<"/space3/file_invalidate">>,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    timer:sleep(timer:seconds(20)), % for hooks todo VFS-3462
    {ok, 200, _, Body0} = do_request(WorkerP1, <<"replicas/space3/file?provider_id=", (domain(WorkerP2))/binary>>, post, [user_1_token_header(Config)], []),
    DecodedBody0 = json_utils:decode_map(Body0),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody0),
    ExpectedTransferStatus = erlang:iolist_to_binary([
        <<"{\"targetProviderId\":\"">>, domain(WorkerP2),
        <<"\",\"status\":\"completed\",\"path\":\"/space3/file\"}">>
    ]),
    ?assertMatch({ok, 200, _, ExpectedTransferStatus},
        do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []), 5),
    {ok, 200, _, Body} = do_request(WorkerP2, <<"replicas/space3/file">>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode_map(Body),
    ?assertEqual(
        lists:sort([
            #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
            #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
        ]), lists:sort(DecodedBody)),

    % then
    {ok, 204, _, _} = do_request(WorkerP1, <<"replicas/space3/file?provider_id=", (domain(WorkerP2))/binary>>, delete, [user_1_token_header(Config)], []),
    {ok, 200, _, Body3} = do_request(WorkerP2, <<"replicas/space3/file">>, get, [user_1_token_header(Config)], []),
    DecodedBody3 = json_utils:decode_map(Body3),
    ?assertEqual(
        lists:sort([
            #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
            #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => []}
        ]), lists:sort(DecodedBody3)).

invalidate_dir_replica(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    Dir1 = <<"/space3/dir1_invalidate">>,
    Dir2 = <<"/space3/dir1_invalidate/dir2">>,
    File1 = <<"/space3/dir1_invalidate/file1">>,
    File2 = <<"/space3/dir1_invalidate/file2">>,
    File3 = <<"/space3/dir1_invalidate/dir2/file3">>,
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
    timer:sleep(timer:seconds(20)), % for hooks todo  VFS-3462
    {ok, 200, _, Body} = do_request(WorkerP1, <<"replicas/space3/dir1_rd?provider_id=", (domain(WorkerP2))/binary>>, post, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode_map(Body),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),
    ExpectedTransferStatus = erlang:iolist_to_binary([
        <<"{\"targetProviderId\":\"">>, domain(WorkerP2),
        <<"\",\"status\":\"completed\",\"path\":\"/space3/dir1_rd\"}">>
    ]),
    ?assertMatch({ok, 200, _, ExpectedTransferStatus},
        do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []), 5),
    timer:sleep(timer:seconds(20)), % for hooks todo  VFS-3462
    {ok, 200, _, Body1} = do_request(WorkerP2, <<"replicas/space3/dir1_rd/file1">>, get, [user_1_token_header(Config)], []),
    {ok, 200, _, Body2} = do_request(WorkerP2, <<"replicas/space3/dir1_rd/file2">>, get, [user_1_token_header(Config)], []),
    {ok, 200, _, Body3} = do_request(WorkerP2, <<"replicas/space3/dir1_rd/dir2/file3">>, get, [user_1_token_header(Config)], []),
    DecodedBody1 = json_utils:decode_map(Body1),
    DecodedBody2 = json_utils:decode_map(Body2),
    DecodedBody3 = json_utils:decode_map(Body3),
    Distribution = lists:sort([
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
    ]),
    ?assertEqual(Distribution, lists:sort(DecodedBody1)),
    ?assertEqual(Distribution, lists:sort(DecodedBody2)),
    ?assertEqual(Distribution, lists:sort(DecodedBody3)),
    {ok, 204, _, _} = do_request(WorkerP1, <<"replicas/space3/dir1_rd?provider_id=", (domain(WorkerP2))/binary>>, delete, [user_1_token_header(Config)], []),

    %then
    {ok, 200, _, NewBody1} = do_request(WorkerP2, <<"replicas/space3/dir1_rd/file1">>, get, [user_1_token_header(Config)], []),
    {ok, 200, _, NewBody2} = do_request(WorkerP2, <<"replicas/space3/dir1_rd/file2">>, get, [user_1_token_header(Config)], []),
    {ok, 200, _, NewBody3} = do_request(WorkerP2, <<"replicas/space3/dir1_rd/dir2/file3">>, get, [user_1_token_header(Config)], []),
    DecodedNewBody1 = json_utils:decode_map(NewBody1),
    DecodedNewBody2 = json_utils:decode_map(NewBody2),
    DecodedNewBody3 = json_utils:decode_map(NewBody3),
    Distribution2 = lists:sort([
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => []}
    ]),
    ?assertEqual(Distribution2, lists:sort(DecodedNewBody1)),
    ?assertEqual(Distribution2, lists:sort(DecodedNewBody2)),
    ?assertEqual(Distribution2, lists:sort(DecodedNewBody3)).

periodic_cleanup_should_invalidate_unpopular_files(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionIdP1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionIdP2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    File1 = <<"/space3/file1_popular">>,
    File2 = <<"/space3/file2_unpopular">>,
    File3 = <<"/space3/file3_unpopular">>,
    {ok, File1Guid} = lfm_proxy:create(WorkerP1, SessionIdP1, File1, 8#777),
    {ok, Handle1} = lfm_proxy:open(WorkerP1, SessionIdP1, {guid, File1Guid}, write),
    lfm_proxy:write(WorkerP1, Handle1, 0, <<"test">>),
    lfm_proxy:close(WorkerP1, Handle1),
    {ok, File2Guid} = lfm_proxy:create(WorkerP1, SessionIdP1, File2, 8#777),
    {ok, Handle2} = lfm_proxy:open(WorkerP1, SessionIdP1, {guid, File2Guid}, write),
    lfm_proxy:write(WorkerP1, Handle2, 0, <<"test">>),
    lfm_proxy:close(WorkerP1, Handle2),
    {ok, File3Guid} = lfm_proxy:create(WorkerP1, SessionIdP1, File3, 8#777),
    {ok, Handle3} = lfm_proxy:open(WorkerP1, SessionIdP1, {guid, File3Guid}, write),
    lfm_proxy:write(WorkerP1, Handle3, 0, <<"test">>),
    lfm_proxy:close(WorkerP1, Handle3),

    % synchronize files
    timer:sleep(timer:seconds(10)), % for hooks todo  VFS-3462
    {ok, ReadHandle1} = lfm_proxy:open(WorkerP2, SessionIdP2, {guid, File1Guid}, read),
    {ok, <<"test">>} = lfm_proxy:read(WorkerP2, ReadHandle1, 0, 4),
    lfm_proxy:close(WorkerP2, ReadHandle1),
    {ok, ReadHandle2} = lfm_proxy:open(WorkerP2, SessionIdP2, {guid, File2Guid}, read),
    {ok, <<"test">>} = lfm_proxy:read(WorkerP2, ReadHandle2, 0, 4),
    lfm_proxy:close(WorkerP2, ReadHandle2),
    {ok, ReadHandle3} = lfm_proxy:open(WorkerP2, SessionIdP2, {guid, File3Guid}, read),
    {ok, <<"test">>} = lfm_proxy:read(WorkerP2, ReadHandle3, 0, 4),
    lfm_proxy:close(WorkerP2, ReadHandle3),
    % open popular file
    Handles = [lfm_proxy:open(WorkerP2, SessionIdP2, {guid, File1Guid}, read) || _ <- lists:seq(0, 100)],
    [lfm_proxy:close(WorkerP2, Handle) || {ok, Handle} <- Handles],

    % trigger cleanup advancing 24 hours into future
    timer:sleep(timer:seconds(10)),
    test_utils:mock_new(WorkerP2, utils),
    test_utils:mock_expect(WorkerP2, utils, system_time_seconds, fun() ->
        meck:passthrough([]) + 25 * 3600
    end),
    rpc:call(WorkerP2, space_cleanup, periodic_cleanup, []),
    test_utils:mock_validate_and_unload(WorkerP2, utils),

    % then
    Provider1Id = rpc:call(WorkerP1, oneprovider, get_provider_id, []),
    Provider2Id = rpc:call(WorkerP2, oneprovider, get_provider_id, []),
    {ok, File1Blocks} = lfm_proxy:get_file_distribution(WorkerP2, SessionIdP2, {guid, File1Guid}),
    ?assertEqual(
        lists:sort([
            #{
                <<"providerId">> => Provider1Id,
                <<"blocks">> => [[0, 4]]
            },
            #{
                <<"providerId">> => Provider2Id,
                <<"blocks">> => [[0, 4]]
            }
        ]),
        lists:sort(File1Blocks)
    ),
    {ok, File2Blocks} = lfm_proxy:get_file_distribution(WorkerP2, SessionIdP2, {guid, File2Guid}),
    ?assertEqual(
        lists:sort([
            #{
                <<"providerId">> => Provider1Id,
                <<"blocks">> => [[0, 4]]
            },
            #{
                <<"providerId">> => Provider2Id,
                <<"blocks">> => []
            }
        ]),
        lists:sort(File2Blocks)
    ),
    {ok, File3Blocks} = lfm_proxy:get_file_distribution(WorkerP2, SessionIdP2, {guid, File3Guid}),
    ?assertEqual(
        lists:sort([
            #{
                <<"providerId">> => Provider1Id,
                <<"blocks">> => [[0, 4]]
            },
            #{
                <<"providerId">> => Provider2Id,
                <<"blocks">> => []
            }
        ]),
        lists:sort(File3Blocks)
    ).

posix_mode_get(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file1_pmg"])),
    Mode = 8#700,
    {ok, _FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"attributes", File/binary, "?attribute=mode">>, get, [user_1_token_header(Config)], []),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertEqual(
        #{
            <<"mode">> => <<"0", (integer_to_binary(Mode, 8))/binary>>
        },
        DecodedBody
    ).

posix_mode_put(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file2_pmp"])),
    Mode = 8#700,
    {ok, _FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    NewMode = 8#777,
    Body = json_utils:encode_map(#{<<"mode">> => <<"0", (integer_to_binary(NewMode, 8))/binary>>}),
    {ok, 204, _, _} = do_request(WorkerP1, <<"attributes", File/binary>>, put,
        [user_1_token_header(Config), {<<"Content-Type">>, <<"application/json">>}], Body),

    % then
    {ok, 200, _, RespBody} = do_request(WorkerP1, <<"attributes", File/binary, "?attribute=mode">>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode_map(RespBody),
    ?assertEqual(
        #{
            <<"mode">> => <<"0", (integer_to_binary(NewMode, 8))/binary>>
        },
        DecodedBody
    ).

attributes_list(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    UserId1 = ?config({user_id, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file1_al"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"attributes", File/binary>>, get, [user_1_token_header(Config)], []),

    % then
    {ok, #file_attr{
        atime = ATime,
        ctime = CTime,
        mtime = MTime,
        gid = Gid,
        uid = Uid
    }} = lfm_proxy:stat(WorkerP1, SessionId, {guid, FileGuid}),
    {ok, CdmiObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    DecodedBody = json_utils:decode_map(Body),
    ?assertEqual(
        #{
            <<"mode">> => <<"0700">>,
            <<"size">> => 0,
            <<"atime">> => ATime,
            <<"ctime">> => CTime,
            <<"mtime">> => MTime,
            <<"storage_group_id">> => Gid,
            <<"storage_user_id">> => Uid,
            <<"name">> => <<"file1_al">>,
            <<"owner_id">> => UserId1,
            <<"shares">> => [],
            <<"type">> => <<"reg">>,
            <<"file_id">> => CdmiObjectId
        },
        DecodedBody
    ).

xattr_get(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file1_xg"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    ok = lfm_proxy:set_xattr(WorkerP1, SessionId, {guid, FileGuid}, #xattr{name = <<"k1">>, value = <<"v1">>}),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"attributes", File/binary, "?attribute=k1&extended=true">>, get, [user_1_token_header(Config)], []),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertEqual(
        #{
            <<"k1">> => <<"v1">>
        },
        DecodedBody
    ).

xattr_put(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file2_xp"])),
    {ok, _FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),

    % when
    Body = json_utils:encode_map(#{<<"k1">> => <<"v1">>}),
    {ok, 204, _, _} = do_request(WorkerP1, <<"attributes", File/binary, "?extended=true">>, put,
        [user_1_token_header(Config), {<<"Content-Type">>, <<"application/json">>}], Body),

    % then
    {ok, 200, _, RespBody} = do_request(WorkerP1, <<"attributes", File/binary, "?attribute=k1&extended=true">>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode_map(RespBody),
    ?assertEqual(
        #{
            <<"k1">> => <<"v1">>
        },
        DecodedBody
    ).

xattr_list(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file1_xl"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    ok = lfm_proxy:set_xattr(WorkerP1, SessionId, {guid, FileGuid}, #xattr{name = <<"k1">>, value = <<"v1">>}),
    ok = lfm_proxy:set_xattr(WorkerP1, SessionId, {guid, FileGuid}, #xattr{name = <<"k2">>, value = <<"v2">>}),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"attributes", File/binary, "?extended=true">>, get, [user_1_token_header(Config)], []),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(#{
        <<"k1">> := <<"v1">>,
        <<"k2">> := <<"v2">>
    }, DecodedBody).

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

    ?assertMatch(ok, rpc:call(WorkerP1, monitoring_utils, create, [<<"space3">>, MonitoringId, utils:system_time_seconds()])),
    {ok, #document{value = State}} = rpc:call(WorkerP1, monitoring_state, get, [MonitoringId]),
    ?assertMatch({ok, _}, rpc:call(WorkerP1, monitoring_state, save, [
        #document{
            key = monitoring_state:encode_id(MonitoringId#monitoring_id{
                provider_id = Prov2ID
            }),
            value =  State
        }
    ])),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"metrics/space/space3?metric=storage_quota">>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode_map(Body),

    % then
    ?assertMatch([
        #{<<"providerId">> := _, <<"rrd">> := _},
        #{<<"providerId">> := _, <<"rrd">> := _}
    ], DecodedBody),
    [Elem1, Elem2] = DecodedBody,
    ?assertNotEqual(maps:get(<<"providerId">>, Elem1), maps:get(<<"providerId">>, Elem2)).

list_file(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    File = <<"/space3/file1_lf">>,
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"files/space3/file1_lf">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode_map(Body),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    ?assertEqual(
        [#{<<"id">> => FileObjectId, <<"path">> => File}],
        DecodedBody
    ).

list_dir(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"files">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(
        [
            #{<<"id">> := _, <<"path">> := <<"/space1">>},
            #{<<"id">> := _, <<"path">> := <<"/space2">>},
            #{<<"id">> := _, <<"path">> := <<"/space3">>}
        ],
        DecodedBody
    ).

list_dir_range(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"files?offset=1&limit=1">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(
        [
            #{<<"id">> := _, <<"path">> := <<"/space2">>}
        ],
        DecodedBody
    ).

changes_stream_file_meta_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file3_csfmt"])),
    File2 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file3_csfmt"])),
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
        get, [user_1_token_header(Config)], [], [insecure, {recv_timeout, 40000}]),

    ?assertNotEqual(<<>>, Body),
    ?assert(length(binary:split(Body, <<"\r\n">>, [global])) >= 2).

changes_stream_xattr_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file4_csxt"])),
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:set_xattr(WorkerP1, SessionId, {guid, FileGuid}, #xattr{name = <<"name">>, value = <<"value">>})
    end),
    {ok, 200, _, Body} = do_request(WorkerP1, <<"changes/metadata/space1?timeout=10000">>,
        get, [user_1_token_header(Config)], [], [insecure, {recv_timeout, 40000}]),

    ?assertNotEqual(<<>>, Body),
    Changes = binary:split(Body, <<"\r\n">>, [global]),
    ?assert(length(Changes) >= 1),
    [_, LastChangeJson | _] = lists:reverse(Changes),
    LastChange = json_utils:decode_map(LastChangeJson),
    Metadata = maps:get(<<"changes">>, LastChange),
    ?assertEqual(#{<<"name">> => <<"value">>}, maps:get(<<"xattrs">>, Metadata)).

changes_stream_json_metadata_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file4_csjmt"])),
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),
    Json = #{<<"k1">> => <<"v1">>, <<"k2">> => [<<"v2">>, <<"v3">>], <<"k3">> => #{<<"k31">> => <<"v31">>}},
    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, FileGuid}, json, Json, [])
    end),
    {ok, 200, _, Body} = do_request(WorkerP1, <<"changes/metadata/space1?timeout=10000">>,
        get, [user_1_token_header(Config)], [], [insecure, {recv_timeout, 40000}]),

    ?assertNotEqual(<<>>, Body),
    Changes = binary:split(Body, <<"\r\n">>, [global]),
    ?assert(length(Changes) >= 1),
    [_ | AllChanges] = lists:reverse(Changes),
    DecodedChanges =
        lists:map(fun(Change) ->
            json_utils:decode_map(Change)
        end, AllChanges),

    ?assert(lists:any(fun(Change) ->
        Json == maps:get(<<"onedata_json">>, maps:get(<<"xattrs">>, maps:get(<<"changes">>, Change)), undefined)
    end, DecodedChanges)).

changes_stream_times_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file4_cstt"])),
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),
    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:update_times(WorkerP1, SessionId, {guid, FileGuid}, 1000, 1000, 1000)
    end),
    {ok, 200, _, Body} = do_request(WorkerP1, <<"changes/metadata/space1?timeout=10000">>,
        get, [user_1_token_header(Config)], [], [insecure, {recv_timeout, 40000}]),

    ?assertNotEqual(<<>>, Body),
    Changes = binary:split(Body, <<"\r\n">>, [global]),
    ?assert(length(Changes) >= 1),
    [_ | AllChanges] = lists:reverse(Changes),
    DecodedChanges =
        lists:map(fun(Change) ->
            json_utils:decode_map(Change)
        end, AllChanges),
    ?assert(lists:any(fun(Change) ->
        1000 == maps:get(<<"atime">>, maps:get(<<"changes">>, Change)) andalso
            1000 == maps:get(<<"mtime">>, maps:get(<<"changes">>, Change)) andalso
            1000 == maps:get(<<"ctime">>, maps:get(<<"changes">>, Change))
    end, DecodedChanges)).

changes_stream_file_location_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file4_csflt"])),
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),
    % when
    spawn(fun() ->
        timer:sleep(500),
        {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
        {ok, 5} = lfm_proxy:write(WorkerP1, Handle, 0, <<"01234">>)
    end),
    {ok, 200, _, Body} = do_request(WorkerP1, <<"changes/metadata/space1?timeout=10000">>,
        get, [user_1_token_header(Config)], [], [insecure, {recv_timeout, 40000}]),

    ?assertNotEqual(<<>>, Body),
    Changes = binary:split(Body, <<"\r\n">>, [global]),
    ?assert(length(Changes) >= 1),
    [_ | AllChanges] = lists:reverse(Changes),
    DecodedChanges =
        lists:map(fun(Change) ->
            json_utils:decode_map(Change)
        end, AllChanges),
    ?assert(lists:any(fun(Change) ->
        5 == maps:get(<<"size">>, maps:get(<<"changes">>, Change))
    end, DecodedChanges)).

changes_stream_on_multi_provider_test(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionIdP2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    [_, _, {_SpaceId, SpaceName}] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file4_csompt"])),
    Mode = 8#700,
    % when
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),
    spawn(fun() ->
        timer:sleep(500),
        {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
        lfm_proxy:write(WorkerP1, Handle, 0, <<"data">>)
    end),
    ?assertMatch({ok, _}, lfm_proxy:open(WorkerP2, SessionIdP2, {guid, FileGuid}, write), 20),
    {ok, 200, _, Body} = do_request(WorkerP2, <<"changes/metadata/space3?timeout=20000">>,
        get, [user_1_token_header(Config)], [], [insecure, {recv_timeout, 60000}]),

    ?assertNotEqual(<<>>, Body),
    Changes = binary:split(Body, <<"\r\n">>, [global]),
    ?assert(length(Changes) >= 1),
    [_ | AllChanges] = lists:reverse(Changes),
    DecodedChanges =
        lists:map(fun(Change) ->
            json_utils:decode_map(Change)
        end, AllChanges),

    ?assert(lists:any(fun(Change) ->
        <<"file4_csompt">> == maps:get(<<"name">>, Change) andalso
            4 == maps:get(<<"size">>, maps:get(<<"changes">>, Change)) andalso
            0 < maps:get(<<"atime">>, maps:get(<<"changes">>, Change)) andalso
            0 < maps:get(<<"ctime">>, maps:get(<<"changes">>, Change)) andalso
            0 < maps:get(<<"mtime">>, maps:get(<<"changes">>, Change))
    end, DecodedChanges)).

list_spaces(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"spaces">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(
        [
            #{<<"name">> := <<"space1">>, <<"spaceId">> := <<"space1">>},
            #{<<"name">> := <<"space2">>, <<"spaceId">> := <<"space2">>},
            #{<<"name">> := <<"space3">>, <<"spaceId">> := <<"space3">>}
        ],
        DecodedBody
    ).

get_space(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"spaces/space3">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(
        #{
            <<"name">> := <<"space3">>,
            <<"providers">> := [
                #{
                    <<"providerId">> := PID1,
                    <<"providerName">> := PID1
                },
                #{
                    <<"providerId">> := PID2,
                    <<"providerName">> := PID2
                }
            ],
            <<"spaceId">> := <<"space3">>
        },
        DecodedBody
    ).

set_get_json_metadata(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], "{\"key\": \"value\"}")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])),
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(
        #{
            <<"key">> := <<"value">>
        },
        DecodedBody
    ),

    % then
    ?assertMatch({ok, 200, _, <<"\"value\"">>},
        do_request(WorkerP1, <<"metadata/space3?filter_type=keypath&filter=key">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])).

set_get_json_metadata_id(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    {ok, Guid} = lfm_proxy:create(WorkerP1, SessionId, <<"/space3/file_sgjmi">>, 8#777),
    {ok, ObjectId} = cdmi_id:guid_to_objectid(Guid),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata-id/", ObjectId/binary, "?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], "{\"key\": \"value\"}")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata-id/", ObjectId/binary, "?metadata_type=json">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])),
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(
        #{
            <<"key">> := <<"value">>
        },
        DecodedBody
    ),

    % then
    ?assertMatch({ok, 200, _, <<"\"value\"">>},
        do_request(WorkerP1, <<"metadata-id/", ObjectId/binary, "?filter_type=keypath&filter=key">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])).


set_get_rdf_metadata(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=rdf">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/rdf+xml">>}], "some_xml")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=rdf">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/rdf+xml">>}], [])),
    ?assertMatch(<<"some_xml">>, Body).

set_get_rdf_metadata_id(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    {ok, Guid} = lfm_proxy:create(WorkerP1, SessionId, <<"/space3/file_sgrmi">>, 8#777),
    {ok, ObjectId} = cdmi_id:guid_to_objectid(Guid),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata-id/", ObjectId/binary, "?metadata_type=rdf">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/rdf+xml">>}], "some_xml")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata-id/", ObjectId/binary, "?metadata_type=rdf">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/rdf+xml">>}], [])),
    ?assertMatch(<<"some_xml">>, Body).

remove_index(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Function =
        <<"function (meta) {
              if(meta['onedata_json'] && meta['onedata_json']['meta'] && meta['onedata_json']['meta']['color']) {
                  return meta['onedata_json']['meta']['color'];
              }
              return null;
        }">>,
    {ok, 303, Headers, _} = ?assertMatch({ok, 303, _, _},
        do_request(WorkerP1, <<"index?space_id=space1&name=name">>, post, [user_1_token_header(Config), {<<"content-type">>, <<"application/javascript">>}], Function)),
    <<"/api/v3/oneprovider/index/", Id/binary>> = proplists:get_value(<<"location">>, Headers),
    {ok, _, _, ListBody} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"index">>, get, [user_1_token_header(Config)], [])),
    IndexList = json_utils:decode_map(ListBody),
    ?assertMatch([_], IndexList),

    %when
    ?assertMatch({ok, 204, _, _}, do_request(WorkerP1, <<"index/", Id/binary>>, delete, [user_1_token_header(Config)], [])),

    %then
    ?assertMatch({ok, 200, _, <<"[]">>}, do_request(WorkerP1, <<"index">>, get, [user_1_token_header(Config)], [])).

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
        do_request(WorkerP1, <<"index?space_id=space1&name=name">>, post, [user_1_token_header(Config), {<<"content-type">>, <<"application/javascript">>}], Function)),
    <<"/api/v3/oneprovider/index/", Id/binary>> = proplists:get_value(<<"location">>, Headers),

    % then
    {ok, _, _, ListBody} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"index">>, get, [user_1_token_header(Config)], [])),
    IndexList = json_utils:decode_map(ListBody),
    ?assertMatch([#{<<"spaceId">> := <<"space1">>, <<"name">> := <<"name">>, <<"indexId">> := Id, <<"spatial">> := false}], IndexList),
    ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"index/", Id/binary>>, get, [user_1_token_header(Config), {<<"accept">>, <<"application/javascript">>}], [])),

    % when
    {ok, 303, _, _} = ?assertMatch({ok, 303, _, _},
        do_request(WorkerP1, <<"index?space_id=space1&name=name2">>, post,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/javascript">>}], Function)),

    % then
    {ok, _, _, ListBody2} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"index">>, get,
        [user_1_token_header(Config)], [])),
    IndexList2 = json_utils:decode_map(ListBody2),
    ?assertMatch([#{}, #{}], IndexList2).

create_geospatial_index(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Function =
        <<"function (meta) {
              if(meta['onedata_json'] && meta['onedata_json']['loc']) {
                  return meta['onedata_json']['loc'];
              }
              return null;
        }">>,

    % when
    {ok, 303, Headers, _} = ?assertMatch({ok, 303, _, _},
        do_request(WorkerP1, <<"index?space_id=space1&name=name&spatial=true">>, post, [user_1_token_header(Config), {<<"content-type">>, <<"application/javascript">>}], Function)),
    <<"/api/v3/oneprovider/index/", Id/binary>> = proplists:get_value(<<"location">>, Headers),

    % then
    {ok, _, _, ListBody} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"index">>, get, [user_1_token_header(Config)], [])),
    IndexList = json_utils:decode_map(ListBody),
    ?assert(lists:member(#{<<"spaceId">> => <<"space1">>, <<"name">> => <<"name">>, <<"indexId">> => Id, <<"spatial">> => true}, IndexList)),
    ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"index/", Id/binary>>, get, [user_1_token_header(Config), {<<"accept">>, <<"application/javascript">>}], [])).

query_geospatial_index(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    Function =
        <<"function (meta) {
              if(meta['onedata_json'] && meta['onedata_json']['loc']) {
                  return meta['onedata_json']['loc'];
              }
              return null;
        }">>,
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Path1 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "f1"])),
    Path2 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "f2"])),
    Path3 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "f3"])),
    {ok, Guid1} = lfm_proxy:create(WorkerP1, SessionId, Path1, 8#777),
    {ok, Guid2} = lfm_proxy:create(WorkerP1, SessionId, Path2, 8#777),
    {ok, Guid3} = lfm_proxy:create(WorkerP1, SessionId, Path3, 8#777),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, Guid1}, json, #{<<"type">> => <<"Point">>, <<"coordinates">> => [5.1, 10.22]}, [<<"loc">>]),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, Guid2}, json, #{<<"type">> => <<"Point">>, <<"coordinates">> => [0, 0]}, [<<"loc">>]),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, Guid3}, json, #{<<"type">> => <<"Point">>, <<"coordinates">> => [10, 5]}, [<<"loc">>]),
    {ok, 303, Headers, _} = ?assertMatch({ok, 303, _, _},
        do_request(WorkerP1, <<"index?space_id=space1&name=name&spatial=true">>, post,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/javascript">>}], Function)),
    <<"/api/v3/oneprovider/index/", Id/binary>> = proplists:get_value(<<"location">>, Headers),
    timer:sleep(timer:seconds(5)), % let the data be stored in db todo VFS-3462

    % when
    {ok, 200, _, Body} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"query-index/", Id/binary, "?spatial=true&stale=false">>, get, [user_1_token_header(Config)], [])),

    % then
    Guids = lists:map(fun(X) ->
        {ok, ObjId} = cdmi_id:objectid_to_guid(X),
        ObjId
    end, json_utils:decode_map(Body)),
    ?assertEqual(lists:sort([Guid1, Guid2, Guid3]), lists:sort(Guids)),

    % when
    {ok, 200, _, Body2} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"query-index/", Id/binary, "?spatial=true&stale=false&start_range=[0,0]&end_range=[5.5,10.5]">>, get, [user_1_token_header(Config)], [])),

    % then
    Guids2 = lists:map(fun(X) -> {ok, ObjId} = cdmi_id:objectid_to_guid(X),
        ObjId end, json_utils:decode_map(Body2)),
    ?assertEqual(lists:sort([Guid1, Guid2]), lists:sort(Guids2)).

query_file_popularity_index(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"query-index/file-popularity-", SpaceId/binary, "?spatial=true&stale=false">>, get, [user_1_token_header(Config)], [])).

set_get_json_metadata_inherited(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], <<"{\"a\": {\"a1\": \"b1\"}, \"b\": \"c\", \"e\": \"f\"}">>)),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, <<"/space3/dir">>),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3/dir?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], <<"{\"a\": {\"a2\": \"b2\"}, \"b\": \"d\"}">>)),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata/space3/dir?metadata_type=json&inherited=true">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])),
    DecodedBody = json_utils:decode_map(Body),
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
    XattrSpace = json_utils:encode_map(#{<<"k1">> => <<"v1">>}),
    XattrDir = json_utils:encode_map(#{<<"k2">> => <<"v2">>}),
    XattrChild = json_utils:encode_map(#{<<"k2">> => <<"v22">>}),
    XattrChild2 = json_utils:encode_map(#{<<"k3">> => <<"v3">>}),

    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], <<"{\"a\":5}">>)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"attributes/space3?extended=true">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], XattrSpace)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"attributes/space3/dir_test?extended=true">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], XattrDir)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"attributes/space3/dir_test/child?extended=true">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], XattrChild)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"attributes/space3/dir_test/child?extended=true">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], XattrChild2)),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"attributes/space3/dir_test/child?inherited=true&extended=true">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])),
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(#{
        <<"k1">> := <<"v1">>,
        <<"k2">> := <<"v22">>,
        <<"k3">> := <<"v3">>,
        <<"onedata_json">> := #{<<"a">> := 5}
    }, DecodedBody).

set_get_json_metadata_using_filter(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], <<"{\"key1\": \"value1\", \"key2\": \"value2\", \"key3\": [\"v1\", \"v2\"]}">>)),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json&filter_type=keypath&filter=key1">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])),
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(<<"value1">>, DecodedBody),
    {_, _, _, Body2} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json&filter_type=keypath&filter=key3.[1]">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])),
    DecodedBody2 = json_utils:decode_map(Body2),
    ?assertMatch(<<"v2">>, DecodedBody2),

    %when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json&filter_type=keypath&filter=key1">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], <<"\"value11\"">>)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json&filter_type=keypath&filter=key2">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], <<"{\"key22\": \"value22\"}">>)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json&filter_type=keypath&filter=key3.[0]">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], <<"\"v11\"">>)),

    %then
    {_, _, _, ReponseBody} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])),
    ?assertMatch(
        #{
            <<"key1">> := <<"value11">>,
            <<"key2">> := #{<<"key22">> := <<"value22">>},
            <<"key3">> := [<<"v11">>, <<"v2">>]
        },
        json_utils:decode_map(ReponseBody)).


primitive_json_metadata_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    Primitives = [<<"{}">>, <<"[]">>, <<"true">>, <<"0">>, <<"0.1">>,
        <<"null">>, <<"\"string\"">>],

    lists:foreach(fun(Primitive) ->
        ?assertMatch({ok, 204, _, _},
            do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, put,
                [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], Primitive)),
        ?assertMatch({ok, 200, _, Primitive},
            do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, get,
                [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], []))
    end, Primitives).

empty_metadata_invalid_json_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    InvalidJsons = [<<"">>, <<"aaa">>, <<"{">>, <<"{\"aaa\": aaa}">>],

    lists:foreach(fun(InvalidJson) ->
        ?assertMatch({ok, 400, _, _},
            do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, put,
                [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], InvalidJson))
    end, InvalidJsons).

spatial_flag_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    ?assertMatch({ok, 404, _, _}, do_request(WorkerP1, <<"query-index/file-popularity-", SpaceId/binary>>, get, [user_1_token_header(Config)], [])),
    ?assertMatch({ok, 400, _, _}, do_request(WorkerP1, <<"query-index/file-popularity-", SpaceId/binary, "?spatial">>, get, [user_1_token_header(Config)], [])),
    ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"query-index/file-popularity-", SpaceId/binary, "?spatial=true">>, get, [user_1_token_header(Config)], [])).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig2 = initializer:setup_storage(NewConfig),
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(1)) % TODO - change to 2 seconds
        end, ?config(op_worker_nodes, NewConfig2)),

        application:start(ssl),
        hackney:start(),
        initializer:enable_grpca_based_communication(NewConfig2),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig2, "env_desc.json"), NewConfig2)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:disable_grpca_based_communication(Config),
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).

init_per_testcase(metric_get, Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    test_utils:mock_new(WorkerP1, rrd_utils),
    test_utils:mock_expect(WorkerP1, rrd_utils, export_rrd, fun(_, _, _) ->
        {ok, <<"{\"test\":\"rrd\"}">>}
    end),
    init_per_testcase(all, Config);

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).

end_per_testcase(metric_get = Case, Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(WorkerP1, rrd_utils),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_request(Node, URL, Method, Headers, Body) ->
    do_request(Node, URL, Method, Headers, Body, [insecure, {recv_timeout, 15000}]).

do_request(Node, URL, Method, Headers, Body, Opts) ->
    Result = http_client:request(
        Method, <<(rest_endpoint(Node))/binary, URL/binary>>,
        maps:from_list(Headers), Body, Opts
    ),
    case Result of
        {ok, RespCode, RespHeaders, RespBody} ->
            {ok, RespCode, maps:to_list(RespHeaders), RespBody};
        Other ->
            Other
    end.

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
    #macaroon_auth{macaroon = Macaroon} = ?config({auth, <<"user1">>}, Config),
    {<<"Macaroon">>, Macaroon}.

domain(Node) ->
    atom_to_binary(?GET_DOMAIN(Node), utf8).