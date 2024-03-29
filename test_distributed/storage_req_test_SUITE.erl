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
-module(storage_req_test_SUITE).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/logical_file_manager/lfm.hrl").
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
    get_helper_params_test/1,
    create_storage_test_file_test/1,
    verify_storage_test_file_test/1
]).

all() ->
    ?ALL([
        get_configuration_test,
        get_helper_params_test,
        create_storage_test_file_test,
        verify_storage_test_file_test
    ]).

-define(TIMEOUT, timer:seconds(10)).

-define(req(W, SessId, FuseRequest), element(2, rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}]))).

-define(fcm_req(W, Method, Args), rpc:call(W, storage_req, Method, Args, ?TIMEOUT)).

%%%====================================================================
%%% Test function
%%%====================================================================

get_configuration_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    UserId = ?config({user_id, <<"user1">>}, Config),

    UserRootGuid = fslogic_file_id:user_root_dir_guid(UserId),

    ?assertMatch(#configuration{subscriptions = [_ | _], root_guid = UserRootGuid},
        ?fcm_req(Worker, get_configuration, [SessId])).

get_helper_params_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageId = ?config({storage_id, ?GET_DOMAIN(Worker)}, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId, FilePath)),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    SpaceId = file_ctx:get_space_id_const(FileCtx),

    %% Test forced proxy mode
    Response1 = ?req(Worker, SessId, #get_helper_params{
        storage_id = StorageId,
        space_id = SpaceId,
        helper_mode = ?FORCE_PROXY_HELPER_MODE
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?OK},
        fuse_response = #helper_params{}}, Response1),
    HelperName1 = Response1#fuse_response.fuse_response#helper_params.helper_name,
    ?assertMatch(<<"proxy">>, HelperName1),

    %% Test forced direct mode
    Response2 = ?req(Worker, SessId, #get_helper_params{
        storage_id = StorageId,
        space_id = SpaceId,
        helper_mode = ?FORCE_DIRECT_HELPER_MODE
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?OK},
        fuse_response = #helper_params{}}, Response2),
    HelperName2 = Response2#fuse_response.fuse_response#helper_params.helper_name,
    ?assertMatch(<<"posix">>, HelperName2),

    %% Test auto mode
    Response3 = ?req(Worker, SessId, #get_helper_params{
        storage_id = StorageId,
        space_id = SpaceId,
        helper_mode = ?AUTO_HELPER_MODE
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?OK},
        fuse_response = #helper_params{}}, Response3),
    HelperName3 = Response3#fuse_response.fuse_response#helper_params.helper_name,
    ?assertMatch(<<"proxy">>, HelperName3).

create_storage_test_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageId = ?config({storage_id, ?GET_DOMAIN(Worker)}, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId, FilePath)),

    Response1 = ?req(Worker, SessId, #create_storage_test_file{
        storage_id = StorageId,
        file_guid = FileGuid
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?OK},
        fuse_response = #storage_test_file{}}, Response1),

    Response2 = ?req(Worker, SessId, #create_storage_test_file{
        storage_id = StorageId,
        file_guid = <<"unknown_id">>
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, Response2),

    Response3 = ?req(Worker, SessId, #create_storage_test_file{
        storage_id = <<"unknown_id">>,
        file_guid = FileGuid
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, Response3),

    StorageDir = ?config({storage_dir, ?GET_DOMAIN(Worker)}, Config),
    ok = rpc:call(Worker, file, change_mode, [StorageDir, 8#444]),

    Response5 = ?req(Worker, SessId, #create_storage_test_file{
        storage_id = StorageId,
        file_guid = FileGuid
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?EACCES}}, Response5).

verify_storage_test_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageId = ?config({storage_id, ?GET_DOMAIN(Worker)}, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    test_utils:set_env(Worker, ?APP_NAME, verify_storage_test_file_delay_seconds, 1),
    test_utils:set_env(Worker, ?APP_NAME, remove_storage_test_file_attempts, 1),

    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId, FilePath)),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(FileGuid), write)),
    ?assertMatch({ok, _}, lfm_proxy:write(Worker, Handle, 0, <<"test">>)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle)),

    FileCtx = rpc:call(Worker, file_ctx, new_by_guid, [FileGuid]),
    {FileId, _} = rpc:call(Worker, file_ctx, get_storage_file_id, [FileCtx]),
    SpaceId = <<"space_id1">>,

    Response1 = ?req(Worker, SessId, #verify_storage_test_file{
        storage_id = StorageId, space_id = SpaceId,
        file_id = FileId, file_content = <<"test2">>
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?EINVAL}}, Response1),

    Response2 = ?req(Worker, SessId, #verify_storage_test_file{
        storage_id = StorageId, space_id = SpaceId,
        file_id = <<"unknown_id">>, file_content = <<"test">>
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, Response2),

    Response3 = ?req(Worker, SessId, #verify_storage_test_file{
        storage_id = <<"unknown_id">>, space_id = SpaceId,
        file_id = FileId, file_content = <<"test">>
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, Response3),

    Response4 = ?req(Worker, SessId, #verify_storage_test_file{
        storage_id = StorageId, space_id = SpaceId,
        file_id = FileId, file_content = <<"test">>
    }),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, Response4).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(_Case, Config) ->
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    initializer:disable_quota_limit(ConfigWithSessionInfo),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(create_storage_test_file_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDir = ?config({storage_dir, ?GET_DOMAIN(Worker)}, Config),
    ok = rpc:call(Worker, file, change_mode, [StorageDir, 8#777]),
    end_per_testcase(default, Config);

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    initializer:unload_quota_mocks(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config).
