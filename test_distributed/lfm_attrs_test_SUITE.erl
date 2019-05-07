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
    create_and_query_view/1,
    get_empty_json/1,
    get_empty_rdf/1,
    has_custom_metadata_test/1,
    resolve_guid_of_root_should_return_root_guid/1,
    resolve_guid_of_space_should_return_space_guid/1,
    resolve_guid_of_dir_should_return_dir_guid/1,
    custom_metadata_doc_should_contain_file_objectid/1,
    create_and_query_view_mapping_one_file_to_many_rows/1,
    effective_value_test/1,
    traverse_test/1]).

%% Pool callbacks
-export([do_master_job/1, do_slave_job/1, task_finished/1, save_job/2]).

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
        create_and_query_view,
        get_empty_json,
        get_empty_rdf,
        has_custom_metadata_test,
        resolve_guid_of_root_should_return_root_guid,
        resolve_guid_of_space_should_return_space_guid,
        resolve_guid_of_dir_should_return_dir_guid,
        custom_metadata_doc_should_contain_file_objectid,
        effective_value_test,
        traverse_test
    ]).

-define(CACHE, test_cache).
-define(CALL_CACHE(Worker, Op, Args), rpc:call(Worker, effective_value, Op, [?CACHE | Args])).
-define(POOL, test_pool).

%%%====================================================================
%%% Test function
%%%====================================================================

traverse_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    Dir1 = <<"/", SpaceName/binary, "/1">>,
    {ok, Guid1} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, Dir1)),
    build_traverse_tree(Worker, SessId, Dir1, 1),

    ?assertEqual(ok, rpc:call(Worker, tree_traverse, run, [?POOL, ?MODULE, file_ctx:new_by_guid(Guid1),
        <<"1">>, <<"1">>, false, 1, self()])),

    Expected = [2,3,4,
        11,12,13,16,17,18,
        101,102,103,106,107,108,
        151,152,153,156,157,158,
        1001,1002,1003,1006,1007,1008,
        1051,1052,1053,1056,1057,1058,
        1501,1502, 1503,1506,1507,1508,
        1551,1552,1553,1556,1557, 1558],
    Ans = get_slave_ans(),

    SJobsNum = length(Expected),
    MJobsNum = SJobsNum * 4 div 3 - 1,
    Description = #{
        slave_jobs_delegated => SJobsNum,
        master_jobs_delegated => MJobsNum,
        slave_jobs_done => SJobsNum,
        master_jobs_done => MJobsNum,
        master_jobs_failed => 0
    },

    ?assertEqual(Expected, lists:sort(Ans)),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, traverse_task, get, [<<"1">>])),
    ok.

effective_value_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Dir1 = <<"/", SpaceName/binary, "/dir1">>,
    Dir2 = <<Dir1/binary, "/dir2">>,
    Dir3 = <<Dir2/binary, "/dir3">>,

    {ok, Guid1} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, Dir1)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, Dir2)),
    {ok, Guid3} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, Dir3)),

    Uid1 = fslogic_uuid:guid_to_uuid(Guid1),
    Uid3 = fslogic_uuid:guid_to_uuid(Guid3),

    {ok, Doc1} = ?assertMatch({ok, _}, rpc:call(Worker, file_meta, get, [{uuid, Uid1}])),
    {ok, Doc3} = ?assertMatch({ok, _}, rpc:call(Worker, file_meta, get, [{uuid, Uid3}])),

    Callback = fun([#document{value = #file_meta{name = Name}}, ParentValue, CalculationInfo]) ->
        {ok, Name, [{Name, ParentValue} | CalculationInfo]}
    end,

    ?assertEqual({ok, <<"dir3">>, [{<<"dir3">>, <<"dir2">>}, {<<"dir2">>, <<"dir1">>},
        {<<"dir1">>, <<"space_id1">>}, {<<"space_id1">>, undefined}]},
        ?CALL_CACHE(Worker, get_or_calculate, [Doc3, Callback, [], []])),
    ?assertEqual({ok, <<"dir3">>, []},
        ?CALL_CACHE(Worker, get_or_calculate, [Doc3, Callback, [], []])),

    ?assertEqual(ok, ?CALL_CACHE(Worker, invalidate, [])),
    ?assertEqual({ok, <<"dir1">>, [{<<"dir1">>, <<"space_id1">>}, {<<"space_id1">>, undefined}]},
        ?CALL_CACHE(Worker, get_or_calculate, [Doc1, Callback, [], []])),
    ?assertEqual({ok, <<"dir3">>, [{<<"dir3">>, <<"dir2">>}, {<<"dir2">>, <<"dir1">>}]},
        ?CALL_CACHE(Worker, get_or_calculate, [Doc3, Callback, [], []])),

    Timestamp = tmp_cache:get_timestamp(),
    ?assertEqual(ok, ?CALL_CACHE(Worker, invalidate, [])),
    ?assertEqual({ok, <<"dir3">>, [{<<"dir3">>, <<"dir2">>}, {<<"dir2">>, <<"dir1">>},
        {<<"dir1">>, <<"space_id1">>}, {<<"space_id1">>, undefined}]},
        ?CALL_CACHE(Worker, get_or_calculate, [Doc3, Callback, [], [], Timestamp])),
    ?assertEqual({ok, <<"dir3">>, [{<<"dir3">>, <<"dir2">>}, {<<"dir2">>, <<"dir1">>},
        {<<"dir1">>, <<"space_id1">>}, {<<"space_id1">>, undefined}]},
        ?CALL_CACHE(Worker, get_or_calculate, [Doc3, Callback, [], []])),

    ok.

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
    Uuid = fslogic_uuid:guid_to_uuid(Guid),

    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1)),
    ?assertEqual({ok, [Name1]}, lfm_proxy:list_xattr(Worker, SessId, {guid, Guid}, false, true)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, {guid, Guid})),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:list_xattr(Worker, SessId, {guid, Guid}, false, true)),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:get_xattr(Worker, SessId, {guid, Guid}, Name1)),
    {ok, Guid2} = lfm_proxy:create(Worker, SessId, Path, 8#600),
    ?assertEqual({ok, []}, lfm_proxy:list_xattr(Worker, SessId, {guid, Guid2}, false, true)),
    ?assertEqual({error, not_found}, rpc:call(Worker, custom_metadata, get, [Uuid])),
    ?assertEqual({error, not_found}, rpc:call(Worker, times, get, [Uuid])).

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

create_and_query_view(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    SpaceId = <<"space_id1">>,
    ViewName = <<"view_name">>,
    ProviderId = rpc:call(Worker, oneprovider, get_id, []),
    Path1 = <<"/space_name1/t6_file">>,
    Path2 = <<"/space_name1/t7_file">>,
    Path3 = <<"/space_name1/t8_file">>,
    MetaBlue = #{<<"meta">> => #{<<"color">> => <<"blue">>}},
    MetaRed = #{<<"meta">> => #{<<"color">> => <<"red">>}},
    ViewFunction =
        <<"function (id, type, meta, ctx) {
             if(type == 'custom_metadata'
                && meta['onedata_json']
                && meta['onedata_json']['meta']
                && meta['onedata_json']['meta']['color'])
             {
                 return [meta['onedata_json']['meta']['color'], id];
             }
             return null;
       }">>,
    {ok, Guid1} = lfm_proxy:create(Worker, SessId, Path1, 8#600),
    {ok, Guid2} = lfm_proxy:create(Worker, SessId, Path2, 8#600),
    {ok, Guid3} = lfm_proxy:create(Worker, SessId, Path3, 8#600),
    {ok, FileId1} = cdmi_id:guid_to_objectid(Guid1),
    {ok, FileId2} = cdmi_id:guid_to_objectid(Guid2),
    {ok, FileId3} = cdmi_id:guid_to_objectid(Guid3),
    ?assertEqual(ok, lfm_proxy:set_metadata(Worker, SessId, {guid, Guid1}, json, MetaBlue, [])),
    ?assertEqual(ok, lfm_proxy:set_metadata(Worker, SessId, {guid, Guid2}, json, MetaRed, [])),
    ?assertEqual(ok, lfm_proxy:set_metadata(Worker, SessId, {guid, Guid3}, json, MetaBlue, [])),
    ok = rpc:call(Worker, index, save, [SpaceId, ViewName, ViewFunction, undefined, [], false, [ProviderId]]),
    ?assertMatch({ok, [ViewName]}, rpc:call(Worker, index, list, [SpaceId])),
    FinalCheck = fun() ->
        try
            {ok, QueryResultBlue} = query_index(Worker, SpaceId, ViewName, [{key, <<"blue">>}, {stale, false}]),
            {ok, QueryResultRed} = query_index(Worker, SpaceId, ViewName, [{key, <<"red">>}]),
            {ok, QueryResultOrange} = query_index(Worker, SpaceId, ViewName, [{key, <<"orange">>}]),

            IdsBlue = extract_query_values(QueryResultBlue),
            IdsRed = extract_query_values(QueryResultRed),
            IdsOrange = extract_query_values(QueryResultOrange),
            true = lists:member(FileId1, IdsBlue),
            false = lists:member(FileId2, IdsBlue),
            true = lists:member(FileId3, IdsBlue),
            [FileId2] = IdsRed,
            [] = IdsOrange,
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

create_and_query_view_mapping_one_file_to_many_rows(Config) ->
    FilePrefix = str_utils:to_binary(?FUNCTION_NAME),
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    SpaceId = <<"space_id1">>,
    ViewName = <<"view_name">>,
    ProviderId = rpc:call(Worker, oneprovider, get_id, []),
    Path1 = <<"/space_name1/", FilePrefix/binary, "_file1">>,
    Path2 = <<"/space_name1/", FilePrefix/binary, "_file2">>,
    Path3 = <<"/space_name1/", FilePrefix/binary, "_file3">>,

    Xattr1 = #xattr{name = <<"jobId.1">>},
    Xattr2 = #xattr{name = <<"jobId.2">>},
    Xattr3 = #xattr{name = <<"jobId.3">>},

    ViewFunction =
        <<"
        function (id, type, meta, ctx) {
            if (type == 'custom_metadata') {
                const JOB_PREFIX = 'jobId.'
                var results = [];
                for (var key of Object.keys(meta)) {
                    if (key.startsWith(JOB_PREFIX)) {
                        var jobId = key.slice(JOB_PREFIX.length);
                        results.push([jobId, id]);
                    }
                }
                return {'list': results};
            }
       }">>,

    {ok, Guid1} = lfm_proxy:create(Worker, SessId, Path1, 8#600),
    {ok, Guid2} = lfm_proxy:create(Worker, SessId, Path2, 8#600),
    {ok, Guid3} = lfm_proxy:create(Worker, SessId, Path3, 8#600),

    {ok, FileId1} = cdmi_id:guid_to_objectid(Guid1),
    {ok, FileId2} = cdmi_id:guid_to_objectid(Guid2),
    {ok, FileId3} = cdmi_id:guid_to_objectid(Guid3),

    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid1}, Xattr1)),
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid1}, Xattr2)),
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid1}, Xattr3)),

    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid2}, Xattr1)),
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid2}, Xattr2)),

    ok = rpc:call(Worker, index, save, [SpaceId, ViewName, ViewFunction, undefined, [], false, [ProviderId]]),
    ?assertMatch({ok, [ViewName]}, rpc:call(Worker, index, list, [SpaceId])),

    FinalCheck = fun() ->
        try
            {ok, QueryResult1} = ?assertMatch({ok, _},
                query_index(Worker, SpaceId, ViewName, [{key, <<"1">>}, {stale, false}])),
            {ok, QueryResult2} = ?assertMatch({ok, _},
                query_index(Worker, SpaceId, ViewName, [{key, <<"2">>}])),
            {ok, QueryResult3} = ?assertMatch({ok, _},
                query_index(Worker, SpaceId, ViewName, [{key, <<"3">>}])),
            {ok, QueryResult4} = ?assertMatch({ok, _},
                query_index(Worker, SpaceId, ViewName, [{key, <<"4">>}])),

            Ids1 = extract_query_values(QueryResult1),
            Ids2 = extract_query_values(QueryResult2),
            Ids3 = extract_query_values(QueryResult3),
            Ids4 = extract_query_values(QueryResult4),

            ?assertEqual(true, lists:member(FileId1, Ids1)),
            ?assertEqual(true, lists:member(FileId1, Ids2)),
            ?assertEqual(true, lists:member(FileId1, Ids3)),

            ?assertEqual(true, lists:member(FileId2, Ids1)),
            ?assertEqual(true, lists:member(FileId2, Ids2)),
            ?assertEqual(false, lists:member(FileId2, Ids3)),

            ?assertEqual(false, lists:member(FileId3, Ids1)),
            ?assertEqual(false, lists:member(FileId3, Ids2)),
            ?assertEqual(false, lists:member(FileId3, Ids3)),

            ?assertEqual([FileId1],  Ids3),
            ?assertEqual([], Ids4),

            ok
        catch
            E1:E2 ->
                {error, E1, E2}
        end
    end,
    ?assertEqual(ok, FinalCheck(), 10, timer:seconds(3)).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(traverse_test = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, traverse, init_pool, [?POOL, 3, 3, 10])),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(effective_value_test = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    CachePid = spawn(Worker, fun() -> cache_proc(
        #{check_frequency => timer:minutes(5), size => 100}) end),
    init_per_testcase(?DEFAULT_CASE(Case), [{cache_pid, CachePid} | Config]);
init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(effective_value_test = Case, Config) ->
    CachePid = ?config(cache_pid, Config),
    CachePid ! {finish, self()},
    ok = receive
             finished -> ok
         after
             1000 -> timeout
         end,
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

query_index(Worker, SpaceId, ViewName, Options) ->
    rpc:call(Worker, index, query, [SpaceId, ViewName, Options]).

extract_query_values(QueryResult) ->
    {Rows} = QueryResult,
    lists:map(fun(Row) ->
        {<<"value">>, Value} = lists:keyfind(<<"value">>, 1, Row),
        Value
    end, Rows).

cache_proc(Options) ->
    tmp_cache:init_cache(?CACHE, Options),
    cache_proc().

cache_proc() ->
    receive
        {tmp_cache_timer, Options} ->
            tmp_cache:check_cache_size(Options),
            cache_proc();
        {finish, Pid} ->
            tmp_cache:terminate_cache(?CACHE, #{}),
            Pid ! finished
    end.

get_slave_ans() ->
    receive
        {slave, Num} ->
            [Num | get_slave_ans()]
    after
        10000 ->
            []
    end.

build_traverse_tree(Worker, SessId, Dir, Num) ->
    NumBin = integer_to_binary(Num),
    Dirs = case Num < 1000 of
               true -> [10 * Num, 10 * Num + 5];
               _ -> []
           end,
    Files = [Num + 1, Num + 2, Num + 3],

    DirsPaths = lists:map(fun(DirNum) ->
        DirNumBin = integer_to_binary(DirNum),
        NewDir = <<Dir/binary, "/", DirNumBin/binary>>,
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, NewDir)),
        {NewDir, DirNum}
                          end, Dirs),

    lists:foreach(fun(FileNum) ->
        FileNumBin = integer_to_binary(FileNum),
        NewFile = <<Dir/binary, "/", FileNumBin/binary>>,
        ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId, NewFile, 8#600))
                  end, Files),

    NumBin = integer_to_binary(Num),
    Files ++ lists:flatten(lists:map(fun({D, N}) ->
        build_traverse_tree(Worker, SessId, D, N) end, DirsPaths)).

%%%===================================================================
%%% Pool callbacks
%%%===================================================================

do_master_job(Job) ->
    tree_traverse:do_master_job(Job).

do_slave_job({#document{value = #file_meta{name = Name}}, TraverseInfo}) ->
    TraverseInfo ! {slave, binary_to_integer(Name)},
    ok.

task_finished(_) ->
    ok.

save_job(_, _) ->
    ok.