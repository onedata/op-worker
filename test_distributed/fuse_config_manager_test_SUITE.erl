%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of fuse_config_manager API.
%%% @end
%%%-------------------------------------------------------------------
-module(fuse_config_manager_test_SUITE).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    get_configuration_test/1,
    create_storage_test_file_test/1,
    verify_storage_test_file_test/1
]).

all() ->
    ?ALL([
        get_configuration_test,
        create_storage_test_file_test,
        verify_storage_test_file_test
    ]).

-define(TIMEOUT, timer:seconds(10)).

-define(req(W, SessId, FuseRequest), rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}])).

-define(fcm_req(W, Method, Args), rpc:call(W, fuse_config_manager, Method, Args, ?TIMEOUT)).

%%%====================================================================
%%% Test function
%%%====================================================================

get_configuration_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    ?assertMatch(#configuration{subscriptions = [_ | _]},
        ?fcm_req(Worker, get_configuration, [])).

create_storage_test_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageId = ?config({storage_id, ?GET_DOMAIN(Worker)}, Config),
    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    {ok, FileGUID} = ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId1, FilePath, 8#600)),

    Response1 = ?req(Worker, SessId1, #create_storage_test_file{
        storage_id = StorageId,
        file_uuid = FileGUID
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?OK},
        fuse_response = #storage_test_file{}}, Response1),

    Response2 = ?req(Worker, SessId1, #create_storage_test_file{
        storage_id = StorageId,
        file_uuid = <<"unknown_id">>
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, Response2),

    Response3 = ?req(Worker, SessId1, #create_storage_test_file{
        storage_id = <<"unknown_id">>,
        file_uuid = FileGUID
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?EAGAIN}}, Response3).

verify_storage_test_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageId = ?config({storage_id, ?GET_DOMAIN(Worker)}, Config),
    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    test_utils:set_env(Worker, ?APP_NAME, verify_storage_test_file_delay_seconds, 1),
    test_utils:set_env(Worker, ?APP_NAME, remove_storage_test_file_attempts, 1),

    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId1, FilePath, 8#600)),
    FileUuid = fslogic_uuid:guid_to_uuid(FileGuid),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId1, {guid, FileGuid}, write)),
    ?assertMatch({ok, _}, lfm_proxy:write(Worker, Handle, 0, <<"test">>)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle)),

    FileId = rpc:call(Worker, fslogic_utils, gen_storage_file_id, [{uuid, FileUuid}]),
    SpaceUuid = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid, [<<"space_id1">>]),

    Response1 = ?req(Worker, SessId1, #verify_storage_test_file{
        storage_id = StorageId, space_uuid = SpaceUuid,
        file_id = FileId, file_content = <<"test2">>
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?EINVAL}}, Response1),

    Response2 = ?req(Worker, SessId1, #verify_storage_test_file{
        storage_id = StorageId, space_uuid = SpaceUuid,
        file_id = <<"unknown_id">>, file_content = <<"test">>
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, Response2),

    Response3 = ?req(Worker, SessId1, #verify_storage_test_file{
        storage_id = <<"unknown_id">>, space_uuid = SpaceUuid,
        file_id = FileId, file_content = <<"test">>
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?EAGAIN}}, Response3),

    Response4 = ?req(Worker, SessId1, #verify_storage_test_file{
        storage_id = StorageId, space_uuid = SpaceUuid,
        file_id = FileId, file_content = <<"test">>
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, Response4).

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
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    initializer:disable_quota_limit(ConfigWithSessionInfo),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_, Config) ->
    lfm_proxy:teardown(Config),
    initializer:unload_quota_mocks(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config).

