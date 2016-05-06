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

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([empty_xattr_test/1, crud_xattr_test/1, list_xattr_test/1, remove_file_test/1,
    modify_cdmi_attrs/1]).

all() ->
    ?ALL([
        empty_xattr_test,
        crud_xattr_test,
        list_xattr_test,
        remove_file_test,
        modify_cdmi_attrs
    ]).

%%%====================================================================
%%% Test function
%%%====================================================================

empty_xattr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, <<"user1">>}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/t1_file">>,
    Name1 = <<"t1_name1">>,
    {ok, GUID} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual({error, ?ENOATTR}, lfm_proxy:get_xattr(Worker, SessId, {guid, GUID}, Name1)),
    ?assertEqual({ok, []}, lfm_proxy:list_xattr(Worker, SessId, {guid, GUID})).

crud_xattr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, <<"user1">>}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/t2_file">>,
    Name1 = <<"t2_name1">>,
    Value1 = <<"t2_value1">>,
    Value2 = <<"t2_value2">>,
    Xattr1 = #xattr{name = Name1, value = Value1},
    UpdatedXattr1 = #xattr{name = Name1, value = Value2},
    {ok, GUID} = lfm_proxy:create(Worker, SessId, Path, 8#600),
    WholeCRUD = fun() ->
        ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, GUID}, Xattr1)),
        ?assertEqual({ok, Xattr1}, lfm_proxy:get_xattr(Worker, SessId, {guid, GUID}, Name1)),
        ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, GUID}, UpdatedXattr1)),
        ?assertEqual({ok, UpdatedXattr1}, lfm_proxy:get_xattr(Worker, SessId, {guid, GUID}, Name1)),
        ?assertEqual(ok, lfm_proxy:remove_xattr(Worker, SessId, {guid, GUID}, Name1)),
        ?assertEqual({error, ?ENOATTR}, lfm_proxy:get_xattr(Worker, SessId, {guid, GUID}, Name1))
    end,

    WholeCRUD(),
    WholeCRUD().

list_xattr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, <<"user1">>}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/t3_file">>,
    Name1 = <<"t3_name1">>,
    Value1 = <<"t3_value1">>,
    Xattr1 = #xattr{name = Name1, value = Value1},
    Name2 = <<"t3_name2">>,
    Value2 = <<"t3_value2">>,
    Xattr2 = #xattr{name = Name2, value = Value2},
    {ok, GUID} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, GUID}, Xattr1)),
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, GUID}, Xattr2)),
    ?assertEqual({ok, [Name2, Name1]}, lfm_proxy:list_xattr(Worker, SessId, {guid, GUID})),
    ?assertEqual(ok, lfm_proxy:remove_xattr(Worker, SessId, {guid, GUID}, Name1)),
    ?assertEqual({ok, [Name2]}, lfm_proxy:list_xattr(Worker, SessId, {guid, GUID})).

remove_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, <<"user1">>}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/t4_file">>,
    Name1 = <<"t4_name1">>,
    Value1 = <<"t4_value1">>,
    Xattr1 = #xattr{name = Name1, value = Value1},
    {ok, GUID} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, GUID}, Xattr1)),
    ?assertEqual({ok, [Name1]}, lfm_proxy:list_xattr(Worker, SessId, {guid, GUID})),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, {guid, GUID})),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:list_xattr(Worker, SessId, {guid, GUID})),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:get_xattr(Worker, SessId, {guid, GUID}, Name1)),
    {ok, GUID2} = lfm_proxy:create(Worker, SessId, Path, 8#600),
    ?assertEqual({ok, []}, lfm_proxy:list_xattr(Worker, SessId, {guid, GUID2})).

modify_cdmi_attrs(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, <<"user1">>}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/t5_file">>,
    Name1 = <<"cdmi_attr">>,
    Value1 = <<"t5_value1">>,
    Xattr1 = #xattr{name = Name1, value = Value1},
    {ok, GUID} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual({error, ?EPERM}, lfm_proxy:set_xattr(Worker, SessId, {guid, GUID}, Xattr1)),
    ?assertEqual({ok, []}, lfm_proxy:list_xattr(Worker, SessId, {guid, GUID})).

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
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).