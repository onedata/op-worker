%%%-------------------------------------------------------------------
%%% @author Rafal Slota
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

-include("proto/oneclient/diagnostic_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    get_configuration_test/1,
    create_storage_test_file_test/1,
    verify_storage_test_file_test/1
]).

-performance({test_cases, []}).
all() -> [
    get_configuration_test,
    create_storage_test_file_test,
    verify_storage_test_file_test
].

-define(TIMEOUT, timer:seconds(10)).
-define(req(W, SessId, FuseRequest), rpc:call(W, worker_proxy, call, [fslogic_worker, {fuse_request, SessId, FuseRequest}], ?TIMEOUT)).
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
    StorageId = ?config(storage_id, Config),
    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},

    FilePath = <<"/spaces/space_name1/", (generator:gen_name())/binary>>,
    {ok, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId1, FilePath, 8#600)),

    ?fcm_req(Worker, create_storage_test_file, [SessId1, StorageId, FileUuid]),
    ?assertReceivedMatch(#fuse_response{status = #status{code = ?OK}}, ?TIMEOUT),

    ?fcm_req(Worker, create_storage_test_file, [SessId1, StorageId, FileUuid]),
    ?assertReceivedMatch(#fuse_response{status = #status{code = ?OK}}, ?TIMEOUT).

verify_storage_test_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageId = ?config(storage_id, Config),
    {SessId1, _UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},

    FilePath = <<"/spaces/space_name1/", (generator:gen_name())/binary>>,
    {ok, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId1, FilePath, 8#600)),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId1, {uuid, FileUuid}, write)),
    ?assertMatch({ok, _}, lfm_proxy:write(Worker, Handle, 0, <<"test">>)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle)),

    FileId = rpc:call(Worker, fslogic_utils, gen_storage_file_id, [{uuid, FileUuid}]),
    SpaceUuid = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid, [<<"space_id1">>]),

    ?fcm_req(Worker, verify_storage_test_file, [SessId1, StorageId, SpaceUuid, FileId, <<"test">>]),
    ?assertReceivedMatch(#fuse_response{status = #status{code = ?OK}}, ?TIMEOUT),

    ?fcm_req(Worker, verify_storage_test_file, [SessId1, StorageId, SpaceUuid, FileId, <<"test2">>]),
    ?assertReceivedMatch(#fuse_response{status = #status{code = ?EINVAL}}, ?TIMEOUT),

    ?fcm_req(Worker, verify_storage_test_file, [SessId1, StorageId, SpaceUuid, <<"unknown_id">>, <<"test">>]),
    ?assertReceivedMatch(#fuse_response{status = #status{code = ?ENOENT}}, ?TIMEOUT),

    ?fcm_req(Worker, verify_storage_test_file, [SessId1, <<"unknown_id">>, SpaceUuid, FileId, <<"test">>]),
    ?assertReceivedMatch(#fuse_response{status = #status{code = ?EAGAIN}}, ?TIMEOUT).

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
%% Mocks communicator module, so that it forwards all messages to the test process.
%% @end
%%--------------------------------------------------------------------
-spec communicator_mock_setup(Workers :: node() | [node()]) -> ok.
communicator_mock_setup(Workers) ->
    Self = self(),
    test_utils:mock_new(Workers, communicator),
    test_utils:mock_expect(Workers, communicator, send,
        fun(Msg, _) -> Self ! Msg end
    ).

