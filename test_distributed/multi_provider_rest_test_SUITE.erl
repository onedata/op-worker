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
    read_event_subscription_test/1,
    metric_get/1,
    list_file/1,
    list_dir/1,
    replicate_file_by_id/1
]).

all() ->
    ?ALL([
        get_simple_file_distribution,
        replicate_file,
        replicate_dir,
        posix_mode_get,
        posix_mode_put,
        read_event_subscription_test,
        metric_get,
        list_file,
        list_dir,
        replicate_file_by_id
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_simple_file_distribution(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, <<"user1">>}, Config),
    File = <<"/file">>,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"replicas/file">>, get, [user_1_token_header(Config)], []),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertEqual([[{<<"provider">>, domain(WorkerP1)}, {<<"blocks">>, [[0,4]]}]], DecodedBody).

replicate_file(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, <<"user1">>}, Config),
    File = <<"/spaces/space3/file">>,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    timer:sleep(timer:seconds(10)),
    {ok, 200, _, Body0} = do_request(WorkerP1, <<"replicas/spaces/space3/file?provider_id=", (domain(WorkerP2))/binary>>, post, [user_1_token_header(Config)], []),
    DecodedBody0 = json_utils:decode(Body0),
    ?assertMatch([{<<"transferId">>, _}], DecodedBody0),

    % then
    timer:sleep(timer:seconds(5)),
    {ok, 200, _, Body} = do_request(WorkerP1, <<"replicas/spaces/space3/file">>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode(Body),
    ?assertEqual(
        [
            [{<<"provider">>, domain(WorkerP2)}, {<<"blocks">>, [[0,4]]}],
            [{<<"provider">>, domain(WorkerP1)}, {<<"blocks">>, [[0,4]]}]
        ], DecodedBody).

replicate_dir(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, <<"user1">>}, Config),
    Dir1 = <<"/spaces/space3/dir1">>,
    Dir2 = <<"/spaces/space3/dir1/dir2">>,
    File1 = <<"/spaces/space3/dir1/file1">>,
    File2 = <<"/spaces/space3/dir1/file2">>,
    File3 = <<"/spaces/space3/dir1/dir2/file3">>,
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
    timer:sleep(timer:seconds(10)),
    {ok, 200, _, Body} = do_request(WorkerP1, <<"replicas/spaces/space3/dir1?provider_id=", (domain(WorkerP2))/binary>>, post, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode(Body),
    ?assertMatch([{<<"transferId">>, _}], DecodedBody),

    % then
    timer:sleep(timer:seconds(5)),
    {ok, 200, _, Body1} = do_request(WorkerP1, <<"replicas/spaces/space3/dir1/file1">>, get, [user_1_token_header(Config)], []),
    {ok, 200, _, Body2} = do_request(WorkerP1, <<"replicas/spaces/space3/dir1/file2">>, get, [user_1_token_header(Config)], []),
    {ok, 200, _, Body3} = do_request(WorkerP1, <<"replicas/spaces/space3/dir1/dir2/file3">>, get, [user_1_token_header(Config)], []),
    DecodedBody1 = json_utils:decode(Body1),
    DecodedBody2 = json_utils:decode(Body2),
    DecodedBody3 = json_utils:decode(Body3),
    Distribution = [
        [{<<"provider">>, domain(WorkerP2)}, {<<"blocks">>, [[0,4]]}],
        [{<<"provider">>, domain(WorkerP1)}, {<<"blocks">>, [[0,4]]}]
    ],
    ?assertEqual(Distribution, DecodedBody1),
    ?assertEqual(Distribution, DecodedBody2),
    ?assertEqual(Distribution, DecodedBody3).

posix_mode_get(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, <<"user1">>}, Config),
    File = <<"/file1">>,
    Mode = 8#700,
    {ok, _FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"attributes/file1?attribute=posix_mode">>, get, [user_1_token_header(Config)], []),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertEqual(
        [{<<"name">>, <<"posix_mode">>}, {<<"value">>, <<"0", (integer_to_binary(Mode, 8))/binary>>}],
        DecodedBody
    ).

posix_mode_put(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, <<"user1">>}, Config),
    File = <<"/file2">>,
    Mode = 8#700,
    {ok, _FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    NewMode = 8#777,
    Body = json_utils:encode([{<<"name">>, <<"posix_mode">>}, {<<"value">>, <<"0", (integer_to_binary(NewMode, 8))/binary>>}]),
    {ok, 204, _, _} = do_request(WorkerP1, <<"attributes/file2">>, put,
        [user_1_token_header(Config), {<<"Content-Type">>, <<"application/json">>}], Body),

    % then
    {ok, 200, _, RespBody} = do_request(WorkerP1, <<"attributes/file2?attribute=posix_mode">>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode(RespBody),
    ?assertEqual(
        [{<<"name">>, <<"posix_mode">>},
        {<<"value">>, <<"0", (integer_to_binary(NewMode, 8))/binary>>}],
        DecodedBody
    ).

read_event_subscription_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, <<"user1">>}, Config),
    File = <<"/file3">>,
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, rdwr),
    lfm_proxy:write(WorkerP1, Handle, 0, <<"data">>),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:read(WorkerP1, Handle, 0, 2),
        lfm_proxy:read(WorkerP1, Handle, 0, 2),
        lfm_proxy:read(WorkerP1, Handle, 2, 2),
        lfm_proxy:fsync(WorkerP1, Handle)
    end),
    {ok, 200, _, RespBody} = do_request(WorkerP1, <<"read_event/file3?timeout=3000">>, get, [user_1_token_header(Config)], []),
    ?assertEqual(<<"{\"type\":\"read_event\",\"count\":3,\"size\":0,\"blocks\":[[0,4]]}\r\n">>, % todo check size
        RespBody).

metric_get(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"metrics/user/id?metric=storage_quota">>, get, [user_1_token_header(Config)], []),

    % then
    ?assertEqual(<<"gzip_data">>, Body).

list_file(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, <<"user1">>}, Config),
    File = <<"/spaces/space3/file1">>,
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"files/spaces/space3/file1">>, get, [user_1_token_header(Config)], []),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertEqual(
        [[{<<"id">>, FileGuid}, {<<"path">>, File}]],
        DecodedBody
    ).

list_dir(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"files/spaces">>, get, [user_1_token_header(Config)], []),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(
        [
            [{<<"id">>, _}, {<<"path">>, <<"/spaces/space1">>}],
            [{<<"id">>, _}, {<<"path">>, <<"/spaces/space2">>}],
            [{<<"id">>, _}, {<<"path">>, <<"/spaces/space3">>}]
        ],
        DecodedBody
    ).

replicate_file_by_id(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, <<"user1">>}, Config),
    File = <<"/spaces/space3/file">>,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    timer:sleep(timer:seconds(10)),
    {ok, 200, _, Body0} = do_request(WorkerP1, <<"replicas-id/", FileGuid/binary,"?provider_id=", (domain(WorkerP2))/binary>>, post, [user_1_token_header(Config)], []),
    DecodedBody0 = json_utils:decode(Body0),
    ?assertMatch([{<<"transferId">>, _}], DecodedBody0),

    % then
    timer:sleep(timer:seconds(5)),
    {ok, 200, _, Body} = do_request(WorkerP1, <<"replicas-id/", FileGuid/binary>>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode(Body),
    ?assertEqual(
        [
            [{<<"provider">>, domain(WorkerP2)}, {<<"blocks">>, [[0,4]]}],
            [{<<"provider">>, domain(WorkerP1)}, {<<"blocks">>, [[0,4]]}]
        ], DecodedBody).


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

do_request(Node, URL, Method, Headers, Body) ->
    http_client:request(Method, <<(rest_endpoint(Node))/binary,  URL/binary>>, Headers, Body, [insecure]).

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
    #auth{macaroon = Macaroon} = ?config({auth, <<"user1">>}, Config),
    {ok, Srlzd} = macaroon:serialize(Macaroon),
    {<<"X-Auth-Token">>, Srlzd}.

domain(Node) ->
    atom_to_binary(?GET_DOMAIN(Node), utf8).