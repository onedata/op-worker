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

-behaviour(traverse_behaviour).

-include("tree_traverse.hrl").
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
    traverse_test/1,
    file_traverse_job_test/1,
    do_not_overwrite_space_dir_attrs_on_make_space_exist_test/1,
    listing_file_attrs_should_work_properly_in_open_handle_mode/1
]).

%% Pool callbacks
-export([do_master_job/2, do_slave_job/2, update_job_progress/5, get_job/1, get_sync_info/1]).

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
        traverse_test,
        file_traverse_job_test,
        do_not_overwrite_space_dir_attrs_on_make_space_exist_test,
        listing_file_attrs_should_work_properly_in_open_handle_mode
    ]).

-define(CACHE, test_cache).
-define(CALL_CACHE(Worker, Op, Args), rpc:call(Worker, effective_value, Op, [?CACHE | Args])).

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

    RunOptions = #{
        batch_size => 1,
        traverse_info => #{pid => self()}
    },
    {ok, ID} = ?assertMatch({ok, _}, rpc:call(Worker, tree_traverse, run, [?MODULE, file_ctx:new_by_guid(Guid1), RunOptions])),

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
        slave_jobs_done => SJobsNum,
        slave_jobs_failed => 0,
        master_jobs_delegated => MJobsNum,
        master_jobs_done => MJobsNum
    },

    ?assertEqual(Expected, lists:sort(Ans)),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, tree_traverse, get_task, [?MODULE, ID])),
    ok.

file_traverse_job_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    File = <<"/", SpaceName/binary, "/1000000">>,
    {ok, Guid} = ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId, File)),

    RunOptions = #{
        traverse_info => #{pid => self()}
    },
    {ok, ID} = ?assertMatch({ok, _}, rpc:call(Worker, tree_traverse, run, [?MODULE, file_ctx:new_by_guid(Guid), RunOptions])),

    Expected = [1000000],
    Ans = get_slave_ans(),
    Description = #{
        slave_jobs_delegated => 1,
        slave_jobs_done => 1,
        slave_jobs_failed => 0,
        master_jobs_delegated => 1,
        master_jobs_done => 1
    },

    ?assertEqual(Expected, Ans),
    ?assertMatch({ok, #document{value = #traverse_task{description = Description}}},
        rpc:call(Worker, tree_traverse, get_task, [?MODULE, ID])),
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

    Uid1 = file_id:guid_to_uuid(Guid1),
    Uid3 = file_id:guid_to_uuid(Guid3),

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
    % Sleep to be sure that next operations are perceived as following after invalidation,
    % not parallel to invalidation (millisecond clock is used by effective value)
    timer:sleep(5),
    ?assertEqual({ok, <<"dir1">>, [{<<"dir1">>, <<"space_id1">>}, {<<"space_id1">>, undefined}]},
        ?CALL_CACHE(Worker, get_or_calculate, [Doc1, Callback, [], []])),
    ?assertEqual({ok, <<"dir3">>, [{<<"dir3">>, <<"dir2">>}, {<<"dir2">>, <<"dir1">>}]},
        ?CALL_CACHE(Worker, get_or_calculate, [Doc3, Callback, [], []])),

    Timestamp = rpc:call(Worker, bounded_cache, get_timestamp, []),
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
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),

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
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),
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
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),

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
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),

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
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),

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
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),

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
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),
    Uuid = file_id:guid_to_uuid(Guid),

    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1)),
    ?assertEqual({ok, [Name1]}, lfm_proxy:list_xattr(Worker, SessId, {guid, Guid}, false, true)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, {guid, Guid})),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:list_xattr(Worker, SessId, {guid, Guid}, false, true)),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:get_xattr(Worker, SessId, {guid, Guid}, Name1)),
    {ok, Guid2} = lfm_proxy:create(Worker, SessId, Path),
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
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),

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
    {ok, Guid1} = lfm_proxy:create(Worker, SessId, Path1),
    {ok, Guid2} = lfm_proxy:create(Worker, SessId, Path2),
    {ok, Guid3} = lfm_proxy:create(Worker, SessId, Path3),
    {ok, FileId1} = file_id:guid_to_objectid(Guid1),
    {ok, FileId2} = file_id:guid_to_objectid(Guid2),
    {ok, FileId3} = file_id:guid_to_objectid(Guid3),
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
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),

    ?assertEqual({error, ?ENOATTR}, lfm_proxy:get_metadata(Worker, SessId, {guid, Guid}, json, [], false)).

get_empty_rdf(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t6_file">>,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),

    ?assertEqual({error, ?ENOATTR}, lfm_proxy:get_metadata(Worker, SessId, {guid, Guid}, rdf, [], false)).

has_custom_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t6_file">>,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),

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
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1)),
    FileUuid = file_id:guid_to_uuid(Guid),

    {ok, #document{value = #custom_metadata{file_objectid = FileObjectid}}} =
        rpc:call(Worker, custom_metadata, get, [FileUuid]),

    {ok, ExpectedFileObjectid} = file_id:guid_to_objectid(Guid),
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

    {ok, Guid1} = lfm_proxy:create(Worker, SessId, Path1),
    {ok, Guid2} = lfm_proxy:create(Worker, SessId, Path2),
    {ok, Guid3} = lfm_proxy:create(Worker, SessId, Path3),

    {ok, FileId1} = file_id:guid_to_objectid(Guid1),
    {ok, FileId2} = file_id:guid_to_objectid(Guid2),
    {ok, FileId3} = file_id:guid_to_objectid(Guid3),

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

do_not_overwrite_space_dir_attrs_on_make_space_exist_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    {ok, ShareId} = lfm_proxy:create_share(Worker, SessId, {guid, SpaceGuid}, <<"szer">>),
    {ok, SpaceAttrs} = ?assertMatch(
        {ok, #file_attr{shares = [ShareId]}},
        lfm_proxy:stat(Worker, SessId, {guid, SpaceGuid})
    ),

    lists:foreach(fun(_) ->
        ?assertEqual(ok, rpc:call(Worker, file_meta, make_space_exist, [SpaceId])),
        ?assertMatch({ok, SpaceAttrs}, lfm_proxy:stat(Worker, SessId, {guid, SpaceGuid}))
    end, lists:seq(1, 10)).


listing_file_attrs_should_work_properly_in_open_handle_mode(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    User = <<"user1">>,
    [{SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    NormalSessId = ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config),
    OpenHandleSessId = permissions_test_utils:create_session(
        Worker, User, initializer:create_access_token(User), open_handle
    ),

    {ok, File1Guid} = lfm_proxy:create(Worker, NormalSessId, <<"/", SpaceName/binary, "/file1">>),
    {ok, File2Guid} = lfm_proxy:create(Worker, NormalSessId, <<"/", SpaceName/binary, "/file2">>),
    {ok, File3Guid} = lfm_proxy:create(Worker, NormalSessId, <<"/", SpaceName/binary, "/file3">>),
    {ok, DirGuid} = lfm_proxy:mkdir(Worker, NormalSessId, <<"/", SpaceName/binary, "/dir">>, 8#770),
    {ok, File4Guid} = lfm_proxy:create(Worker, NormalSessId, <<"/", SpaceName/binary, "/dir/file4">>),
    mock_space_get_shares(Worker, []),

    Content = <<"content">>,
    ContentSize = size(Content),
    {ok, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, NormalSessId, {guid, File4Guid}, write)),
    ?assertMatch({ok, ContentSize}, lfm_proxy:write(Worker, Handle1, 0, Content)),
    ?assertMatch(ok, lfm_proxy:close(Worker, Handle1)),

    % Assert that when listing in normal mode all space files are returned
    ?assertMatch(
        {ok, [{DirGuid, _}, {File1Guid, _}, {File2Guid, _}, {File3Guid, _}]},
        lfm_proxy:get_children(Worker, NormalSessId, {guid, SpaceGuid}, 0, 100)
    ),

    % Assert that listing in open_handle mode should return nothing as there are no shares with open handle
    ?assertMatch(
        {ok, []},
        lfm_proxy:get_children(Worker, OpenHandleSessId, {guid, SpaceGuid}, 0, 100)
    ),

    BuildShareRootDirFun = fun(ShareId) ->
        file_id:pack_share_guid(fslogic_uuid:shareid_to_share_root_dir_uuid(ShareId), SpaceId, ShareId)
    end,

    SpaceShareId = <<"spaceshare">>,
    SpaceShareRootDirGuid = BuildShareRootDirFun(SpaceShareId),
    SpaceShareGuid = file_id:guid_to_share_guid(SpaceGuid, SpaceShareId),
    create_share(Worker, SpaceShareId, <<"szer">>, SpaceId, SpaceShareGuid, ?DIRECTORY_TYPE, <<"handle">>),

    Share1Id = <<"share1">>,
    Share1RootDirGuid = BuildShareRootDirFun(Share1Id),
    File1ShareGuid = file_id:guid_to_share_guid(File1Guid, Share1Id),
    create_share(Worker, Share1Id, <<"szer">>, SpaceId, File1ShareGuid, ?REGULAR_FILE_TYPE, <<"handle">>),

    Share3Id = <<"share3">>,
    File3ShareGuid = file_id:guid_to_share_guid(File3Guid, Share3Id),
    create_share(Worker, Share3Id, <<"szer">>, SpaceId, File3ShareGuid, ?REGULAR_FILE_TYPE, undefined),

    DirShareId = <<"dirshare">>,
    DirShareRootDirGuid = BuildShareRootDirFun(DirShareId),
    DirShareGuid = file_id:guid_to_share_guid(DirGuid, DirShareId),
    create_share(Worker, DirShareId, <<"szer">>, SpaceId, DirShareGuid, ?DIRECTORY_TYPE, <<"handle">>),

    Share4Id = <<"share4">>,
    Share4RootDirGuid = BuildShareRootDirFun(Share4Id),
    File4ShareGuid = file_id:guid_to_share_guid(File4Guid, Share4Id),
    create_share(Worker, Share4Id, <<"szer">>, SpaceId, File4ShareGuid, ?REGULAR_FILE_TYPE, <<"handle">>),

    mock_space_get_shares(Worker, [Share1Id, Share3Id, Share4Id, DirShareId, SpaceShareId]),

    % Assert proper virtual share root dirs attrs (for all shares with open handle existing in space
    % - file3 share has no handle so it shouldn't be listed) when listing space in 'open_handle' mode
    ?assertMatch(
        {ok, [
            #file_attr{
                guid = DirShareRootDirGuid, name = DirShareId, mode = 8#005, parent_guid = SpaceGuid,
                uid = ?SHARE_UID, gid = ?SHARE_GID, type = ?DIRECTORY_TYPE, size = 0,
                shares = [], provider_id = <<"unknown">>, owner_id = <<"unknown">>
            },
            #file_attr{
                guid = Share1RootDirGuid, name = Share1Id, mode = 8#005, parent_guid = SpaceGuid,
                uid = ?SHARE_UID, gid = ?SHARE_GID, type = ?DIRECTORY_TYPE, size = 0,
                shares = [], provider_id = <<"unknown">>, owner_id = <<"unknown">>
            },
            #file_attr{
                guid = Share4RootDirGuid, name = Share4Id, mode = 8#005, parent_guid = SpaceGuid,
                uid = ?SHARE_UID, gid = ?SHARE_GID, type = ?DIRECTORY_TYPE, size = 0,
                shares = [], provider_id = <<"unknown">>, owner_id = <<"unknown">>
            },
            #file_attr{
                guid = SpaceShareRootDirGuid, name = SpaceShareId, mode = 8#005, parent_guid = SpaceGuid,
                uid = ?SHARE_UID, gid = ?SHARE_GID, type = ?DIRECTORY_TYPE, size = 0,
                shares = [], provider_id = <<"unknown">>, owner_id = <<"unknown">>
            }
        ], #{is_last := true}},
        lfm_proxy:get_children_attrs(Worker, OpenHandleSessId, {guid, SpaceGuid},  #{offset => 0, size => 100})
    ),

    % Assert listing virtual share root dir returns only share root file
    lists:foreach(fun({ShareRootFileGuid, ShareRootFileName, ShareRootDirGuid}) ->
        ?assertMatch(
            {ok, [{ShareRootFileGuid, ShareRootFileName}]},
            lfm_proxy:get_children(Worker, OpenHandleSessId, {guid, ShareRootDirGuid}, 0, 100)
        )
    end, [
        {DirShareGuid, <<"dir">>, DirShareRootDirGuid},
        {File1ShareGuid, <<"file1">>, Share1RootDirGuid},
        {File4ShareGuid, <<"file4">>, Share4RootDirGuid},
        {SpaceShareGuid, SpaceName, SpaceShareRootDirGuid}
    ]),

    % Assert listing space using space share guid returns all space files
    % (but with share guids containing space share id)
    File1SpaceShareGuid = file_id:guid_to_share_guid(File1Guid, SpaceShareId),
    File2SpaceShareGuid = file_id:guid_to_share_guid(File2Guid, SpaceShareId),
    File3SpaceShareGuid = file_id:guid_to_share_guid(File3Guid, SpaceShareId),
    DirSpaceShareGuid = file_id:guid_to_share_guid(DirGuid, SpaceShareId),

    ?assertMatch(
        {ok, [{DirSpaceShareGuid, _}, {File1SpaceShareGuid, _}, {File2SpaceShareGuid, _}, {File3SpaceShareGuid, _}]},
        lfm_proxy:get_children(Worker, OpenHandleSessId, {guid, SpaceShareGuid}, 0, 100)
    ),

    % Assert it is possible to operate on file4 using Share4Id (direct share of file4)
    % but is not possible via space/dir share (those are shares created on parents so
    % permissions check must assert traverse ancestors for them too - dir has 8#770 perms
    % and in 'open_handle' mode 'other' bits are checked so permissions will be denied)
    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, OpenHandleSessId, {guid, File4ShareGuid}, read)),
    ?assertMatch({ok, Content}, lfm_proxy:read(Worker, Handle2, 0, 100)),
    ?assertMatch(ok, lfm_proxy:close(Worker, Handle2)),

    File4DirShareGuid = file_id:guid_to_share_guid(File4Guid, DirShareId),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(Worker, OpenHandleSessId, {guid, File4DirShareGuid}, read)),

    File4SpaceShareGuid = file_id:guid_to_share_guid(File4Guid, SpaceShareId),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(Worker, OpenHandleSessId, {guid, File4SpaceShareGuid}, read)),

    ok.


%% @private
create_share(Worker, ShareId, Name, SpaceId, ShareFileGuid, FileType, Handle) ->
    Doc = #document{key = ShareId, value = Record = #od_share{
        name = Name,
        description = <<>>,
        space = SpaceId,
        root_file = ShareFileGuid,
        public_url = <<ShareId/binary, "_public_url">>,
        file_type = FileType,
        handle = Handle
    }},
    ?assertMatch({ok, _}, rpc:call(Worker, od_share, update_cache, [
        ShareId, fun(_) -> {ok, Record} end, Doc
    ])),
    ?assertMatch({ok, _}, rpc:call(Worker, file_meta, add_share, [
        file_ctx:new_by_guid(ShareFileGuid), ShareId
    ])),
    ok.


%% @private
mock_space_get_shares(Workers, ShareList) ->
    test_utils:mock_expect(Workers, space_logic, get_shares, fun(_, _) ->
        {ok, ShareList}
    end),
    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(Case, Config) when Case =:= traverse_test ; Case =:= file_traverse_job_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, tree_traverse, init, [?MODULE, 3, 3, 10])),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(effective_value_test = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    CachePid = spawn(Worker, fun() -> cache_proc(
        #{check_frequency => timer:minutes(5), size => 100}) end),
    init_per_testcase(?DEFAULT_CASE(Case), [{cache_pid, CachePid} | Config]);
init_per_testcase(Case, Config) when
    Case == do_not_overwrite_space_dir_attrs_on_make_space_exist_test;
    Case == listing_file_attrs_should_work_properly_in_open_handle_mode
->
    initializer:mock_share_logic(Config),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(Case, Config) when Case =:= traverse_test ; Case =:= file_traverse_job_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, tree_traverse, stop, [?MODULE])),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(effective_value_test = Case, Config) ->
    CachePid = ?config(cache_pid, Config),
    CachePid ! {finish, self()},
    ok = receive
             finished -> ok
         after
             1000 -> timeout
         end,
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(do_not_overwrite_space_dir_attrs_on_make_space_exist_test = Case, Config) ->
    initializer:unmock_share_logic(Config),
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
    #{<<"rows">> := Rows} = QueryResult,
    [maps:get(<<"value">>, Row) || Row <- Rows].

cache_proc(Options) ->
    bounded_cache:init_cache(?CACHE, Options),
    cache_proc().

cache_proc() ->
    receive
        {bounded_cache_timer, Options} ->
            bounded_cache:check_cache_size(Options),
            cache_proc();
        {finish, Pid} ->
            bounded_cache:terminate_cache(?CACHE),
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
        ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId, NewFile))
    end, Files),

    NumBin = integer_to_binary(Num),
    Files ++ lists:flatten(lists:map(fun({D, N}) ->
        build_traverse_tree(Worker, SessId, D, N) end, DirsPaths)).

%%%===================================================================
%%% Pool callbacks
%%%===================================================================

do_master_job(Job, TaskID) ->
    tree_traverse:do_master_job(Job, TaskID).

do_slave_job(#tree_traverse_slave{
    file_ctx = FileCtx,
    traverse_info = #{pid := Pid}
}, _TaskID) ->
    {#document{value = #file_meta{name = Name}}, _} = file_ctx:get_file_doc(FileCtx),
    Pid ! {slave, binary_to_integer(Name)},
    ok.

update_job_progress(ID, Job, Pool, TaskID, Status) ->
    tree_traverse:update_job_progress(ID, Job, Pool, TaskID, Status, ?MODULE).

get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

get_sync_info(Job) ->
    tree_traverse:get_sync_info(Job).