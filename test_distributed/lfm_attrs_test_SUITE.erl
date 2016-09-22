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
-export([
    empty_xattr_test/1,
    crud_xattr_test/1,
    list_xattr_test/1,
    remove_file_test/1,
    modify_cdmi_attrs/1,
    create_and_get_view/1,
    get_empty_json/1,
    get_empty_rdf/1
]).

all() ->
    ?ALL([
        empty_xattr_test,
        crud_xattr_test,
        list_xattr_test,
        remove_file_test,
        modify_cdmi_attrs,
        create_and_get_view,
        get_empty_json,
        get_empty_rdf
    ]).

%%%====================================================================
%%% Test function
%%%====================================================================

empty_xattr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t1_file">>,
    Name1 = <<"t1_name1">>,
    {ok, GUID} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual({error, ?ENOATTR}, lfm_proxy:get_xattr(Worker, SessId, {guid, GUID}, Name1)),
    ?assertEqual({ok, []}, lfm_proxy:list_xattr(Worker, SessId, {guid, GUID}, false, true)).

crud_xattr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t2_file">>,
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
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t3_file">>,
    Name1 = <<"t3_name1">>,
    Value1 = <<"t3_value1">>,
    Xattr1 = #xattr{name = Name1, value = Value1},
    Name2 = <<"t3_name2">>,
    Value2 = <<"t3_value2">>,
    Xattr2 = #xattr{name = Name2, value = Value2},
    {ok, GUID} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, GUID}, Xattr1)),
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, GUID}, Xattr2)),
    ?assertEqual({ok, [Name1, Name2]}, lfm_proxy:list_xattr(Worker, SessId, {guid, GUID}, false, true)),
    ?assertEqual(ok, lfm_proxy:remove_xattr(Worker, SessId, {guid, GUID}, Name1)),
    ?assertEqual({ok, [Name2]}, lfm_proxy:list_xattr(Worker, SessId, {guid, GUID}, false, true)).

remove_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t4_file">>,
    Name1 = <<"t4_name1">>,
    Value1 = <<"t4_value1">>,
    Xattr1 = #xattr{name = Name1, value = Value1},
    {ok, GUID} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, GUID}, Xattr1)),
    ?assertEqual({ok, [Name1]}, lfm_proxy:list_xattr(Worker, SessId, {guid, GUID}, false, true)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, {guid, GUID})),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:list_xattr(Worker, SessId, {guid, GUID}, false, true)),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:get_xattr(Worker, SessId, {guid, GUID}, Name1)),
    {ok, GUID2} = lfm_proxy:create(Worker, SessId, Path, 8#600),
    ?assertEqual({ok, []}, lfm_proxy:list_xattr(Worker, SessId, {guid, GUID2}, false, true)).

modify_cdmi_attrs(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t5_file">>,
    Name1 = <<"cdmi_attr">>,
    Value1 = <<"t5_value1">>,
    Xattr1 = #xattr{name = Name1, value = Value1},
    {ok, GUID} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual({error, ?EPERM}, lfm_proxy:set_xattr(Worker, SessId, {guid, GUID}, Xattr1)),
    ?assertEqual({ok, []}, lfm_proxy:list_xattr(Worker, SessId, {guid, GUID}, false, true)).

create_and_get_view(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path1 = <<"/space_name1/t6_file">>,
    Path2 = <<"/space_name1/t7_file">>,
    Path3 = <<"/space_name1/t8_file">>,
    MetaBlue = #{<<"meta">> => #{<<"color">> => <<"blue">>}},
    MetaRed = #{<<"meta">> => #{<<"color">> => <<"red">>}},
    ViewFunction =
        <<"function (meta) {
              if(meta['onedata_json'] && meta['onedata_json']['meta'] && meta['onedata_json']['meta']['color']) {
                  return meta['onedata_json']['meta']['color'];
              }
              return null;
        }">>,
    {ok, GUID1} = lfm_proxy:create(Worker, SessId, Path1, 8#600),
    {ok, GUID2} = lfm_proxy:create(Worker, SessId, Path2, 8#600),
    {ok, GUID3} = lfm_proxy:create(Worker, SessId, Path3, 8#600),
    ?assertEqual(ok, lfm_proxy:set_metadata(Worker, SessId, {guid, GUID1}, json, MetaBlue, [])),
    ?assertEqual(ok, lfm_proxy:set_metadata(Worker, SessId, {guid, GUID2}, json, MetaRed, [])),
    ?assertEqual(ok, lfm_proxy:set_metadata(Worker, SessId, {guid, GUID3}, json, MetaBlue, [])),
    {ok, ViewId} = rpc:call(Worker, indexes, add_index, [<<"user1">>, <<"name">>, ViewFunction, <<"space_id1">>]),
    ?assertMatch({ok, #{name := <<"name">>, space_id := <<"space_id1">>, function := _}},
        rpc:call(Worker, indexes, get_index, [<<"user1">>, ViewId])),
    {ok, GuidsBlue} = ?assertMatch({ok, [_ | _]}, rpc:call(Worker, indexes, query_view, [ViewId, [{key, <<"blue">>}]]), 5, timer:seconds(3)),
    {ok, GuidsRed} = rpc:call(Worker, indexes, query_view, [ViewId, [{key, <<"red">>}]]),
    {ok, GuidsOrange} = rpc:call(Worker, indexes, query_view, [ViewId, [{key, <<"orange">>}]]),

    ?assert(lists:member(GUID1, GuidsBlue)),
    ?assertNot(lists:member(GUID2, GuidsBlue)),
    ?assert(lists:member(GUID3, GuidsBlue)),
    ?assertEqual([GUID2], GuidsRed),
    ?assertEqual([], GuidsOrange).

get_empty_json(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t6_file">>,
    {ok, GUID} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual({error, ?ENOATTR}, lfm_proxy:get_metadata(Worker, SessId, {guid, GUID}, json, [], false)).

get_empty_rdf(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t6_file">>,
    {ok, GUID} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual({error, ?ENOATTR}, lfm_proxy:get_metadata(Worker, SessId, {guid, GUID}, rdf, [], false)).

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
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(Case, Config) ->
    ?CASE_STOP(Case),
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).