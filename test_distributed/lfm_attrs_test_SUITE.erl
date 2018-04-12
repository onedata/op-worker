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
    xattr_create_flag/1,
    xattr_replace_flag/1,
    xattr_replace_and_create_flag_in_conflict/1,
    remove_file_test/1,
    modify_cdmi_attrs/1,
    create_and_get_view/1,
    get_empty_json/1,
    get_empty_rdf/1,
    has_custom_metadata_test/1,
    resolve_guid_of_root_should_return_root_guid/1,
    resolve_guid_of_space_should_return_space_guid/1,
    resolve_guid_of_dir_should_return_dir_guid/1,
    custom_metadata_doc_should_contain_file_objectid/1
]).

all() ->
    ?ALL([
        empty_xattr_test,
        crud_xattr_test,
        list_xattr_test,
        xattr_create_flag,
        xattr_replace_flag,
        xattr_replace_and_create_flag_in_conflict,
        remove_file_test,
        modify_cdmi_attrs,
        create_and_get_view,
        get_empty_json,
        get_empty_rdf,
        has_custom_metadata_test,
        resolve_guid_of_root_should_return_root_guid,
        resolve_guid_of_space_should_return_space_guid,
        resolve_guid_of_dir_should_return_dir_guid,
        custom_metadata_doc_should_contain_file_objectid
    ]).

%%%====================================================================
%%% Test function
%%%====================================================================

empty_xattr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t1_file">>,
    Name1 = <<"t1_name1">>,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual({error, ?ENOATTR}, lfm_proxy:get_xattr(Worker, SessId, {guid, Guid}, Name1)),
    ?assertEqual({ok, []}, lfm_proxy:list_xattr(Worker, SessId, {guid, Guid}, false, true)).

crud_xattr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t2_file">>,
    Name1 = <<"t2_name1">>,
    Value1 = <<"t2_value1">>,
    Value2 = <<"t2_value2">>,
    Xattr1 = #xattr{name = Name1, value = Value1},
    UpdatedXattr1 = #xattr{name = Name1, value = Value2},
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path, 8#600),
    WholeCRUD = fun() ->
        ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1)),
        ?assertEqual({ok, Xattr1}, lfm_proxy:get_xattr(Worker, SessId, {guid, Guid}, Name1)),
        ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, UpdatedXattr1)),
        ?assertEqual({ok, UpdatedXattr1}, lfm_proxy:get_xattr(Worker, SessId, {guid, Guid}, Name1)),
        ?assertEqual(ok, lfm_proxy:remove_xattr(Worker, SessId, {guid, Guid}, Name1)),
        ?assertEqual({error, ?ENOATTR}, lfm_proxy:get_xattr(Worker, SessId, {guid, Guid}, Name1))
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
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1)),
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr2)),
    ?assertEqual({ok, [Name1, Name2]}, lfm_proxy:list_xattr(Worker, SessId, {guid, Guid}, false, true)),
    ?assertEqual(ok, lfm_proxy:remove_xattr(Worker, SessId, {guid, Guid}, Name1)),
    ?assertEqual({ok, [Name2]}, lfm_proxy:list_xattr(Worker, SessId, {guid, Guid}, false, true)).

xattr_create_flag(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/xattr_create_flag_file">>,
    Name1 = <<"name">>,
    Value1 = <<"value">>,
    Xattr1 = #xattr{name = Name1, value = Value1},
    Name2 = <<"name">>,
    Value2 = <<"value2">>,
    Xattr2 = #xattr{name = Name2, value = Value2},
    OtherName = <<"other_name">>,
    OtherValue = <<"other_value">>,
    OtherXattr = #xattr{name = OtherName, value = OtherValue},
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    % create first xattr
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1, true, false)),
    ?assertEqual({ok, Xattr1}, lfm_proxy:get_xattr(Worker, SessId, {guid, Guid}, Name1)),

    % fail to replace xattr
    ?assertEqual({error, ?EEXIST}, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr2, true, false)),

    % create second xattr
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, OtherXattr, true, false)),
    ?assertEqual({ok, OtherXattr}, lfm_proxy:get_xattr(Worker, SessId, {guid, Guid}, OtherName)).

xattr_replace_flag(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/xattr_create_flag_file">>,
    Name1 = <<"name">>,
    Value1 = <<"value">>,
    Xattr1 = #xattr{name = Name1, value = Value1},
    Name2 = <<"name">>,
    Value2 = <<"value2">>,
    Xattr2 = #xattr{name = Name2, value = Value2},
    OtherName = <<"other_name">>,
    OtherValue = <<"other_value">>,
    OtherXattr = #xattr{name = OtherName, value = OtherValue},
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    % fail to create first xattr with replace flag
    ?assertEqual({error, ?ENODATA}, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1, false, true)),

    % create first xattr
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1, false, false)),
    ?assertEqual({ok, Xattr1}, lfm_proxy:get_xattr(Worker, SessId, {guid, Guid}, Name1)),

    % replace first xattr
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr2, false, true)),
    ?assertEqual({ok, Xattr2}, lfm_proxy:get_xattr(Worker, SessId, {guid, Guid}, Name1)),

    % fail to create second xattr
    ?assertEqual({error, ?ENODATA}, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, OtherXattr, false, true)).

xattr_replace_and_create_flag_in_conflict(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/xattr_create_flag_file">>,
    Name1 = <<"name">>,
    Value1 = <<"value">>,
    Xattr1 = #xattr{name = Name1, value = Value1},
    Name2 = <<"name">>,
    Value2 = <<"value2">>,
    Xattr2 = #xattr{name = Name2, value = Value2},
    OtherName = <<"other_name">>,
    OtherValue = <<"other_value">>,
    OtherXattr = #xattr{name = OtherName, value = OtherValue},
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    % fail to create first xattr due to replace flag
    ?assertEqual({error, ?ENODATA}, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1, true, true)),

    % create first xattr
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1, false, false)),
    ?assertEqual({ok, Xattr1}, lfm_proxy:get_xattr(Worker, SessId, {guid, Guid}, Name1)),

    % fail to set xattr due to create flag
    ?assertEqual({error, ?EEXIST}, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr2, true, true)),

    % fail to create second xattr due to replace flag
    ?assertEqual({error, ?ENODATA}, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, OtherXattr, true, true)).

remove_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t4_file">>,
    Name1 = <<"t4_name1">>,
    Value1 = <<"t4_value1">>,
    Xattr1 = #xattr{name = Name1, value = Value1},
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1)),
    ?assertEqual({ok, [Name1]}, lfm_proxy:list_xattr(Worker, SessId, {guid, Guid}, false, true)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, {guid, Guid})),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:list_xattr(Worker, SessId, {guid, Guid}, false, true)),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:get_xattr(Worker, SessId, {guid, Guid}, Name1)),
    {ok, Guid2} = lfm_proxy:create(Worker, SessId, Path, 8#600),
    ?assertEqual({ok, []}, lfm_proxy:list_xattr(Worker, SessId, {guid, Guid2}, false, true)).

modify_cdmi_attrs(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t5_file">>,
    Name1 = <<"cdmi_attr">>,
    Value1 = <<"t5_value1">>,
    Xattr1 = #xattr{name = Name1, value = Value1},
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual({error, ?EPERM}, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1)),
    ?assertEqual({ok, []}, lfm_proxy:list_xattr(Worker, SessId, {guid, Guid}, false, true)).

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
    {ok, Guid1} = lfm_proxy:create(Worker, SessId, Path1, 8#600),
    {ok, Guid2} = lfm_proxy:create(Worker, SessId, Path2, 8#600),
    {ok, Guid3} = lfm_proxy:create(Worker, SessId, Path3, 8#600),
    ?assertEqual(ok, lfm_proxy:set_metadata(Worker, SessId, {guid, Guid1}, json, MetaBlue, [])),
    ?assertEqual(ok, lfm_proxy:set_metadata(Worker, SessId, {guid, Guid2}, json, MetaRed, [])),
    ?assertEqual(ok, lfm_proxy:set_metadata(Worker, SessId, {guid, Guid3}, json, MetaBlue, [])),
    {ok, ViewId} = rpc:call(Worker, indexes, add_index, [<<"user1">>, <<"name">>, ViewFunction, <<"space_id1">>, false]),
    ?assertMatch({ok, #{name := <<"name">>, space_id := <<"space_id1">>, function := _}},
        rpc:call(Worker, indexes, get_index, [<<"user1">>, ViewId])),

    FinalCheck = fun() ->
        try
            {ok, GuidsBlue} = {ok, [_ | _]} = rpc:call(Worker, indexes, query_view, [ViewId, [{key, <<"blue">>}, {stale, false}]]),
            {ok, GuidsRed} = rpc:call(Worker, indexes, query_view, [ViewId, [{key, <<"red">>}]]),
            {ok, GuidsOrange} = rpc:call(Worker, indexes, query_view, [ViewId, [{key, <<"orange">>}]]),

            true = lists:member(Guid1, GuidsBlue),
            false = lists:member(Guid2, GuidsBlue),
            true = lists:member(Guid3, GuidsBlue),
            [Guid2] = GuidsRed,
            [] = GuidsOrange,
            ok
        catch
            E1:E2 ->
                {error, E1, E2}
        end
    end,
    ?assertEqual(ok, FinalCheck(), 10, timer:seconds(3)).

get_empty_json(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t6_file">>,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual({error, ?ENOATTR}, lfm_proxy:get_metadata(Worker, SessId, {guid, Guid}, json, [], false)).

get_empty_rdf(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t6_file">>,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    ?assertEqual({error, ?ENOATTR}, lfm_proxy:get_metadata(Worker, SessId, {guid, Guid}, rdf, [], false)).

has_custom_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t6_file">>,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path, 8#600),

    % json
    ?assertEqual({ok, false}, lfm_proxy:has_custom_metadata(Worker, SessId, {guid, Guid})),
    ?assertEqual(ok, lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, #{}, [])),
    ?assertEqual({ok, true}, lfm_proxy:has_custom_metadata(Worker, SessId, {guid, Guid})),
    ?assertEqual(ok, lfm_proxy:remove_metadata(Worker, SessId, {guid, Guid}, json)),

    % rdf
    ?assertEqual({ok, false}, lfm_proxy:has_custom_metadata(Worker, SessId, {guid, Guid})),
    ?assertEqual(ok, lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, rdf, <<"<xml>">>, [])),
    ?assertEqual({ok, true}, lfm_proxy:has_custom_metadata(Worker, SessId, {guid, Guid})),
    ?assertEqual(ok, lfm_proxy:remove_metadata(Worker, SessId, {guid, Guid}, rdf)),

    % xattr
    ?assertEqual({ok, false}, lfm_proxy:has_custom_metadata(Worker, SessId, {guid, Guid})),
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, #xattr{name = <<"name">>, value = <<"value">>})),
    ?assertEqual({ok, true}, lfm_proxy:has_custom_metadata(Worker, SessId, {guid, Guid})),
    ?assertEqual(ok, lfm_proxy:remove_xattr(Worker, SessId, {guid, Guid}, <<"name">>)),
    ?assertEqual({ok, false}, lfm_proxy:has_custom_metadata(Worker, SessId, {guid, Guid})).

resolve_guid_of_root_should_return_root_guid(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    RootGuid = rpc:call(Worker, fslogic_uuid, user_root_dir_guid, [UserId]),

    ?assertEqual({ok, RootGuid}, lfm_proxy:resolve_guid(Worker, SessId, <<"/">>)).

resolve_guid_of_space_should_return_space_guid(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    SpaceDirGuid = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),

    ?assertEqual({ok, SpaceDirGuid}, lfm_proxy:resolve_guid(Worker, SessId, <<"/", SpaceName/binary>>)).

resolve_guid_of_dir_should_return_dir_guid(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    DirPath = <<"/", SpaceName/binary, "/dir">>,
    {ok, Guid} = lfm_proxy:mkdir(Worker, SessId, DirPath),

    ?assertEqual({ok, Guid}, lfm_proxy:resolve_guid(Worker, SessId, DirPath)).

custom_metadata_doc_should_contain_file_objectid(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    Path = <<"/", SpaceName/binary, "/custom_meta_file">>,
    Xattr1 = #xattr{name = <<"name">>, value = <<"value">>},
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path, 8#600),
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1)),
    FileUuid = fslogic_uuid:guid_to_uuid(Guid),

    {ok, #document{value = #custom_metadata{file_objectid = FileObjectid}}} =
        rpc:call(Worker, custom_metadata, get, [FileUuid]),

    {ok, ExpectedFileObjectid} = cdmi_id:guid_to_objectid(Guid),
    ?assertEqual(ExpectedFileObjectid, FileObjectid).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).