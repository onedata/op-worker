%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of lfm_attrs API.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_attrs_test_SUITE).
-author("Tomasz Lichon").

-include("modules/datastore/datastore.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([lfm_empty_xattr_test/1, lfm_set_and_get_xattr_test/1]).

-performance({test_cases, []}).
all() -> [
    lfm_empty_xattr_test, lfm_set_and_get_xattr_test
].

%%%====================================================================
%%% Test function
%%%====================================================================

lfm_empty_xattr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    Path = <<"/file">>,
    Name1 = <<"name1">>,
    {ok, Uuid} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual({error, ?ENOATTR}, lfm_proxy:get_xattr(Worker, SessId, {uuid, Uuid}, Name1)),
    ?assertEqual({ok, []}, lfm_proxy:list_xattr(Worker, SessId, {uuid, Uuid})).

lfm_set_and_get_xattr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    Path = <<"/file">>,
    Name1 = <<"name1">>,
    Value1 = <<"value1">>,
    Xattr1 = #xattr{name = Name1, value = Value1},
    {ok, Uuid} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {uuid, Uuid}, Xattr1)),
    ?assertEqual({ok, Xattr1}, lfm_proxy:get_xattr(Worker, SessId, {uuid, Uuid}, Name1)).

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
    test_utils:mock_validate(Workers, [communicator]),
    test_utils:mock_unload(Workers, [communicator]).

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