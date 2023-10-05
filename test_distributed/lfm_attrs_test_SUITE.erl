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

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("tree_traverse.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
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
    multiple_references_effective_value_simple_test/1,
    multiple_references_effective_value_advanced_test/1,
    deleted_reference_effective_value_test/1,
    concurent_multiple_references_effective_value_test/1,
    concurent_multiple_references_effective_value_in_critical_section_test/1,
    concurent_multiple_references_effective_value_parent_critical_section_test/1,
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
        multiple_references_effective_value_simple_test,
        multiple_references_effective_value_advanced_test,
        deleted_reference_effective_value_test,
        concurent_multiple_references_effective_value_test,
        concurent_multiple_references_effective_value_in_critical_section_test,
        concurent_multiple_references_effective_value_parent_critical_section_test,
        traverse_test,
        file_traverse_job_test,
        do_not_overwrite_space_dir_attrs_on_make_space_exist_test,
        listing_file_attrs_should_work_properly_in_open_handle_mode
    ]).

-define(CACHE, test_cache).
-define(CALL_CACHE(Worker, Op, Args), rpc:call(Worker, effective_value, Op, [?CACHE | Args])).

-define(assertReceivedMergeMessage(Expected), ?assertReceivedEqual({merge_callback, Expected}, 0)).
-define(assertNotReceivedMergeMessage(), ?assertNotReceivedMatch({merge_callback, _})).

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

    {Callback, _MergeCallback} = prepare_effective_value_callbacks(),

    ?assertEqual({ok, <<"dir3">>, [{<<"dir3">>, <<"dir2">>}, {<<"dir2">>, <<"dir1">>},
        {<<"dir1">>, <<"space_id1">>}, {<<"space_id1">>, undefined}]},
        ?CALL_CACHE(Worker, get_or_calculate, [Doc3, Callback, #{initial_calculation_info => []}])),
    ?assertEqual({ok, <<"dir3">>, []},
        ?CALL_CACHE(Worker, get_or_calculate, [Doc3, Callback, #{initial_calculation_info => []}])),

    invalidate_effective_value_cache(Worker),
    ?assertEqual({ok, <<"dir1">>, [{<<"dir1">>, <<"space_id1">>}, {<<"space_id1">>, undefined}]},
        ?CALL_CACHE(Worker, get_or_calculate, [Doc1, Callback, #{initial_calculation_info => []}])),
    ?assertEqual({ok, <<"dir3">>, [{<<"dir3">>, <<"dir2">>}, {<<"dir2">>, <<"dir1">>}]},
        ?CALL_CACHE(Worker, get_or_calculate, [Doc3, Callback, #{initial_calculation_info => []}])),

    Timestamp = rpc:call(Worker, bounded_cache, get_timestamp, []),
    invalidate_effective_value_cache(Worker),
    ?assertEqual({ok, <<"dir3">>, [{<<"dir3">>, <<"dir2">>}, {<<"dir2">>, <<"dir1">>},
        {<<"dir1">>, <<"space_id1">>}, {<<"space_id1">>, undefined}]},
        ?CALL_CACHE(Worker, get_or_calculate, [Doc3, Callback, #{initial_calculation_info => [], timestamp => Timestamp}])),
    ?assertEqual({ok, <<"dir3">>, [{<<"dir3">>, <<"dir2">>}, {<<"dir2">>, <<"dir1">>},
        {<<"dir1">>, <<"space_id1">>}, {<<"space_id1">>, undefined}]},
        ?CALL_CACHE(Worker, get_or_calculate, [Doc3, Callback, #{initial_calculation_info => []}])),

    invalidate_effective_value_cache(Worker),
    % Calculation should work with critical_section_level set to direct
    ?assertEqual({ok, <<"dir3">>, [{<<"dir3">>, <<"dir2">>}, {<<"dir2">>, <<"dir1">>},
        {<<"dir1">>, <<"space_id1">>}, {<<"space_id1">>, undefined}]}, ?CALL_CACHE(Worker, get_or_calculate,
        [Doc3, Callback, #{initial_calculation_info => [], critical_section_level => direct}])),
    ?assertEqual({ok, <<"dir3">>, []}, ?CALL_CACHE(Worker, get_or_calculate,
        [Doc3, Callback, #{initial_calculation_info => [], critical_section_level => direct}])),

    % Invalidate cache for further tests
    invalidate_effective_value_cache(Worker),

    % Test calculation when dir is deleted
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Guid3))),
    ?assertEqual({ok, <<"dir3">>, [{<<"dir3">>, <<"dir2">>}, {<<"dir2">>, <<"dir1">>},
        {<<"dir1">>, <<"space_id1">>}, {<<"space_id1">>, undefined}]},
        ?CALL_CACHE(Worker, get_or_calculate, [Doc3, Callback, #{initial_calculation_info => []}])),

    ok.

multiple_references_effective_value_simple_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {_FileGuid, FileDoc, LinkDoc} = prepare_effective_value_test_tree(Config, "mr_ev"),
    {Callback, MergeCallback} = prepare_effective_value_callbacks(),

    % Calculate value for file and hardlink - different paths should be used during calculation
    ?assertEqual({ok, <<"file">>,
        [{<<"file">>, <<"mr_ev_dir1">>}, {<<"mr_ev_dir1">>, <<"mr_ev_dir0">>},
            {<<"mr_ev_dir0">>, <<"space_id1">>}, {<<"space_id1">>, undefined}]},
        ?CALL_CACHE(Worker, get_or_calculate, [FileDoc, Callback, #{initial_calculation_info => []}])),
    ?assertEqual({ok, <<"link">>, [{<<"link">>, <<"mr_ev_dir3">>}, {<<"mr_ev_dir3">>, <<"space_id1">>}]},
        ?CALL_CACHE(Worker, get_or_calculate, [LinkDoc, Callback, #{initial_calculation_info => []}])),

    % Test usage of use_referenced_key parameter - value for file should be read from cache
    % (calculation info should be empty)
    ?assertEqual({ok, <<"file">>, []}, ?CALL_CACHE(Worker, get_or_calculate, [LinkDoc, Callback,
            #{initial_calculation_info => [], use_referenced_key => true}])),

    % Invalidate cache for further tests
    invalidate_effective_value_cache(Worker),

    % Test calculation using more than one path (path of file and hardlink should be used)
    CalculationInfo1 = [{<<"link">>, <<"mr_ev_dir3">>}, {<<"mr_ev_dir3">>, <<"space_id1">>}, {<<"space_id1">>, undefined},
        {<<"file">>, <<"mr_ev_dir1">>}, {<<"mr_ev_dir1">>, <<"mr_ev_dir0">>}, {<<"mr_ev_dir0">>, <<"space_id1">>}],
    ?assertEqual({ok, {<<"link">>, <<"file">>}, CalculationInfo1}, ?CALL_CACHE(Worker, get_or_calculate, [LinkDoc, Callback,
            #{initial_calculation_info => [], use_referenced_key => true, merge_callback => MergeCallback}])),
    ?assertReceivedMergeMessage({<<"link">>, <<"file">>, CalculationInfo1}),
    ?assertNotReceivedMergeMessage(),

    % Value calculated using multiple paths should be provided from cache for both file and link
    ?assertEqual({ok, {<<"link">>, <<"file">>}, []}, ?CALL_CACHE(Worker, get_or_calculate, [LinkDoc, Callback,
            #{initial_calculation_info => [], use_referenced_key => true, merge_callback => MergeCallback}])),
    ?assertNotReceivedMergeMessage(),
    ?assertEqual({ok, {<<"link">>, <<"file">>}, []}, ?CALL_CACHE(Worker, get_or_calculate, [FileDoc, Callback,
            #{initial_calculation_info => [], use_referenced_key => true, merge_callback => MergeCallback}])),
    ?assertNotReceivedMergeMessage(),

    % Cached parent values should be used without use_referenced_key parameter set to true
    CalculationInfo2 = [{<<"link">>, <<"mr_ev_dir3">>}, {<<"file">>, <<"mr_ev_dir1">>}],
    ?assertEqual({ok, {<<"link">>, <<"file">>}, CalculationInfo2}, ?CALL_CACHE(Worker, get_or_calculate, [LinkDoc, Callback,
            #{initial_calculation_info => [], merge_callback => MergeCallback}])),
    ?assertReceivedMergeMessage({<<"link">>, <<"file">>, CalculationInfo2}),
    ?assertNotReceivedMergeMessage(),

    % Value calculated without use_referenced_key parameter set to true should be cached
    ?assertEqual({ok, {<<"link">>, <<"file">>}, []}, ?CALL_CACHE(Worker, get_or_calculate, [LinkDoc, Callback,
            #{initial_calculation_info => [], merge_callback => MergeCallback}])),
    ?assertNotReceivedMergeMessage(),

    % Value for space dir should be calculated 2 times because timestamp is before invalidation
    Timestamp = rpc:call(Worker, bounded_cache, get_timestamp, []),
    invalidate_effective_value_cache(Worker),
    CalculationInfo3 = [{<<"link">>, <<"mr_ev_dir3">>}, {<<"mr_ev_dir3">>, <<"space_id1">>}, {<<"space_id1">>, undefined},
        {<<"file">>, <<"mr_ev_dir1">>}, {<<"mr_ev_dir1">>, <<"mr_ev_dir0">>},
        {<<"mr_ev_dir0">>, <<"space_id1">>}, {<<"space_id1">>, undefined}],
    ?assertEqual({ok, {<<"link">>, <<"file">>}, CalculationInfo3}, ?CALL_CACHE(Worker, get_or_calculate, [LinkDoc, Callback,
            #{initial_calculation_info => [], timestamp => Timestamp, merge_callback => MergeCallback}])),
    ?assertReceivedMergeMessage({<<"link">>, <<"file">>, CalculationInfo3}),
    ?assertNotReceivedMergeMessage(),

    % Value for space dir should be calculated only once as we are after invalidation
    ?assertEqual({ok, {<<"link">>, <<"file">>}, CalculationInfo1}, ?CALL_CACHE(Worker, get_or_calculate, [LinkDoc, Callback,
            #{initial_calculation_info => [], merge_callback => MergeCallback}])),
    ?assertReceivedMergeMessage({<<"link">>, <<"file">>, CalculationInfo1}),
    ?assertNotReceivedMergeMessage(),

    ok.

multiple_references_effective_value_advanced_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {_FileGuid, FileDoc, LinkDoc} = prepare_effective_value_test_tree(Config, "ev_params"),
    {Callback, MergeCallback} = prepare_effective_value_callbacks(),

    % Test order of references used to calculate value (reference connected with argument should be used last)
    % Test if force_execution_on_referenced_key does not affect returned value
    % (inode is not deleted so no additional actions should be executed)
    CalculationInfo1 = [{<<"file">>, <<"ev_params_dir1">>}, {<<"ev_params_dir1">>, <<"ev_params_dir0">>},
        {<<"ev_params_dir0">>, <<"space_id1">>}, {<<"space_id1">>, undefined},
        {<<"link">>, <<"ev_params_dir3">>}, {<<"ev_params_dir3">>, <<"space_id1">>}],
    ?assertEqual({ok, {<<"file">>, <<"link">>}, CalculationInfo1}, ?CALL_CACHE(Worker, get_or_calculate, [FileDoc,
        Callback, #{initial_calculation_info => [], merge_callback => MergeCallback, force_execution_on_referenced_key => true}])),
    ?assertReceivedMergeMessage({<<"file">>, <<"link">>, CalculationInfo1}),
    ?assertNotReceivedMergeMessage(),

    % Invalidate cache for further tests
    invalidate_effective_value_cache(Worker),

    % Calculation should work with critical_section_level set to direct
    CalculationInfo2 = [{<<"link">>, <<"ev_params_dir3">>}, {<<"ev_params_dir3">>, <<"space_id1">>},
        {<<"space_id1">>, undefined},
        {<<"file">>, <<"ev_params_dir1">>}, {<<"ev_params_dir1">>, <<"ev_params_dir0">>},
        {<<"ev_params_dir0">>, <<"space_id1">>}],
    ?assertEqual({ok, {<<"link">>, <<"file">>}, CalculationInfo2}, ?CALL_CACHE(Worker, get_or_calculate, [LinkDoc,
        Callback, #{initial_calculation_info => [], merge_callback => MergeCallback, critical_section_level => direct}])),
    ?assertReceivedMergeMessage({<<"link">>, <<"file">>, CalculationInfo2}),
    ?assertNotReceivedMergeMessage(),

    % Calculation should work with critical_section_level set to direct and no caching because of timestamp
    Timestamp2 = rpc:call(Worker, bounded_cache, get_timestamp, []),
    invalidate_effective_value_cache(Worker),
    CalculationInfo3 = [{<<"link">>, <<"ev_params_dir3">>}, {<<"ev_params_dir3">>, <<"space_id1">>}, {<<"space_id1">>, undefined},
        {<<"file">>, <<"ev_params_dir1">>}, {<<"ev_params_dir1">>, <<"ev_params_dir0">>},
        {<<"ev_params_dir0">>, <<"space_id1">>}, {<<"space_id1">>, undefined}],
    ?assertEqual({ok, {<<"link">>, <<"file">>}, CalculationInfo3}, ?CALL_CACHE(Worker, get_or_calculate, [LinkDoc,
        Callback, #{initial_calculation_info => [], timestamp => Timestamp2, merge_callback => MergeCallback,
            critical_section_level => direct}])),
    ?assertReceivedMergeMessage({<<"link">>, <<"file">>, CalculationInfo3}),
    ?assertNotReceivedMergeMessage(),

    % Invalidate cache for further tests
    invalidate_effective_value_cache(Worker),

    % Test postprocessing callback that allow caching of different values for different paths
    DifferentiateCallback = fun(Value, Acc, _CalculationInfo) ->
        {ok, {Value, Acc}}
    end,
    ?assertEqual({ok, {<<"link">>, {<<"link">>, <<"file">>}}, CalculationInfo2}, ?CALL_CACHE(Worker, get_or_calculate,
        [LinkDoc, Callback, #{initial_calculation_info => [], merge_callback => MergeCallback,
            differentiate_callback => DifferentiateCallback}])),
    ?assertReceivedMergeMessage({<<"link">>, <<"file">>, CalculationInfo2}),
    ?assertNotReceivedMergeMessage(),

    % Value calculated using postprocessing callback should be provided from cache for both file and link
    ?assertEqual({ok, {<<"link">>, {<<"link">>, <<"file">>}}, []}, ?CALL_CACHE(Worker, get_or_calculate, [LinkDoc,
        Callback, #{initial_calculation_info => [], merge_callback => MergeCallback,
            differentiate_callback => DifferentiateCallback}])),
    ?assertNotReceivedMergeMessage(),
    ?assertEqual({ok, {<<"file">>, {<<"link">>, <<"file">>}}, []}, ?CALL_CACHE(Worker, get_or_calculate, [FileDoc,
        Callback, #{initial_calculation_info => [], merge_callback => MergeCallback,
            differentiate_callback => DifferentiateCallback}])),
    ?assertNotReceivedMergeMessage(),

    ok.

deleted_reference_effective_value_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    {FileGuid, FileDoc, LinkDoc} = prepare_effective_value_test_tree(Config, "del_ev"),
    {Callback, MergeCallback} = prepare_effective_value_callbacks(),

    % Test calculation for hardlink when file is deleted but hardlink is not deleted
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(FileGuid))),
    CalculationInfo1 = [{<<"link">>, <<"del_ev_dir3">>}, {<<"del_ev_dir3">>, <<"space_id1">>},
        {<<"space_id1">>, undefined}],
    ?assertEqual({ok, <<"link">>, CalculationInfo1}, ?CALL_CACHE(Worker, get_or_calculate, [LinkDoc, Callback,
        #{initial_calculation_info => [], use_referenced_key => true, merge_callback => MergeCallback}])),
    ?assertNotReceivedMergeMessage(),

    % Calculated value should be provided from cache for both link and deleted file
    ?assertEqual({ok, <<"link">>, []}, ?CALL_CACHE(Worker, get_or_calculate, [LinkDoc, Callback,
        #{initial_calculation_info => [], use_referenced_key => true, merge_callback => MergeCallback}])),
    ?assertNotReceivedMergeMessage(),
    ?assertEqual({ok, <<"link">>, []}, ?CALL_CACHE(Worker, get_or_calculate, [FileDoc, Callback,
        #{initial_calculation_info => [], use_referenced_key => true, merge_callback => MergeCallback}])),
    ?assertNotReceivedMergeMessage(),

    % Invalidate cache for further tests
    invalidate_effective_value_cache(Worker),

    % Test calculation using force_execution_on_referenced_key flag
    CalculationInfo2 = [{<<"link">>, <<"del_ev_dir3">>}, {<<"del_ev_dir3">>, <<"space_id1">>}, {<<"space_id1">>, undefined},
        {<<"file">>, undefined}],
    ?assertEqual({ok, {<<"link">>, <<"file">>}, CalculationInfo2}, ?CALL_CACHE(Worker, get_or_calculate, [LinkDoc, Callback,
        #{initial_calculation_info => [], use_referenced_key => true, merge_callback => MergeCallback,
            force_execution_on_referenced_key => true}])),
    ?assertReceivedMergeMessage({<<"link">>, <<"file">>, CalculationInfo2}),
    ?assertNotReceivedMergeMessage(),

    % Value calculated using force_execution_on_referenced_key flag should be provided from cache for both link and deleted file
    ?assertEqual({ok, {<<"link">>, <<"file">>}, []}, ?CALL_CACHE(Worker, get_or_calculate, [LinkDoc, Callback,
        #{initial_calculation_info => [], use_referenced_key => true, merge_callback => MergeCallback,
            force_execution_on_referenced_key => true}])),
    ?assertNotReceivedMergeMessage(),
    ?assertEqual({ok, {<<"link">>, <<"file">>}, []}, ?CALL_CACHE(Worker, get_or_calculate, [FileDoc, Callback,
        #{initial_calculation_info => [], use_referenced_key => true, merge_callback => MergeCallback,
            force_execution_on_referenced_key => true}])),
    ?assertNotReceivedMergeMessage(),

    % Invalidate cache for further tests
    invalidate_effective_value_cache(Worker),

    % Value should be calculated for deleted file
    CalculationInfo3 = [{<<"file">>, <<"del_ev_dir1">>}, {<<"del_ev_dir1">>, <<"del_ev_dir0">>},
        {<<"del_ev_dir0">>, <<"space_id1">>}, {<<"space_id1">>, undefined},
        {<<"link">>, <<"del_ev_dir3">>}, {<<"del_ev_dir3">>, <<"space_id1">>}],
    ?assertEqual({ok, {<<"file">>, <<"link">>}, CalculationInfo3}, ?CALL_CACHE(Worker, get_or_calculate, [FileDoc,
        Callback, #{initial_calculation_info => [], use_referenced_key => true, merge_callback => MergeCallback}])),
    ?assertReceivedMergeMessage({<<"file">>, <<"link">>, CalculationInfo3}),
    ?assertNotReceivedMergeMessage(),

    ok.

concurent_multiple_references_effective_value_test(Config) ->
    concurent_multiple_references_effective_value_test_base(Config, <<"concurent_ev">>, no).

concurent_multiple_references_effective_value_in_critical_section_test(Config) ->
    concurent_multiple_references_effective_value_test_base(Config, <<"concurent_ev_in_section">>, direct).

concurent_multiple_references_effective_value_parent_critical_section_test(Config) ->
    concurent_multiple_references_effective_value_test_base(Config, <<"concurent_ev_in_section">>, parent).

concurent_multiple_references_effective_value_test_base(Config, Prefix, CriticalSection) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {_FileGuid, FileDoc, LinkDoc} = prepare_effective_value_test_tree(Config, binary_to_list(Prefix)),

    % Define callback functions
    Callback = fun
        ([#document{value = #file_meta{name = Name}}, undefined, CalculationInfo]) ->
            {ok, [Name], CalculationInfo};
        ([#document{value = #file_meta{name = Name}}, ParentValue, CalculationInfo]) ->
            {ok, lists:sort([Name | ParentValue]), CalculationInfo}
    end,
    MergeCallback = fun(Value, Acc, CalculationInfo1, _CalculationInfo2) ->
        {ok, lists:sort(Value ++ Acc), CalculationInfo1}
    end,

    Master = self(),
    ProcsNum = 100,
    ExpectedAns = lists:sort([<<"file">>, <<Prefix/binary, "_dir1">>, <<Prefix/binary, "_dir0">>, <<"space_id1">>,
        <<"link">>, <<Prefix/binary, "_dir3">>, <<"space_id1">>]),

    % Execute 100 parallel calls and check their results
    lists:foreach(fun(N) ->
        spawn(fun() ->
            Doc = case N rem 2 of
                0 -> LinkDoc;
                1 -> FileDoc
            end,

            CallAns = ?CALL_CACHE(Worker, get_or_calculate, [Doc, Callback, #{use_referenced_key => true,
                merge_callback => MergeCallback, critical_section_level => CriticalSection}]),
            Master ! {call_ans, CallAns}
        end)
    end, lists:seq(1, ProcsNum)),

    lists:foreach(fun(_) ->
        Ans = receive
            {call_ans, ReceivedAns} -> ReceivedAns
        after
            5000 -> timeout
        end,

        ?assertEqual({ok, ExpectedAns, undefined}, Ans)
    end, lists:seq(1, ProcsNum)).

empty_xattr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t1_file">>,
    Name1 = <<"t1_name1">>,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),

    ?assertEqual({error, ?ENOATTR}, lfm_proxy:get_xattr(Worker, SessId, ?FILE_REF(Guid), Name1)),
    ?assertEqual({ok, []}, lfm_proxy:list_xattr(Worker, SessId, ?FILE_REF(Guid), false, true)).

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
        ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr1)),
        ?assertEqual({ok, Xattr1}, lfm_proxy:get_xattr(Worker, SessId, ?FILE_REF(Guid), Name1)),
        ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), UpdatedXattr1)),
        ?assertEqual({ok, UpdatedXattr1}, lfm_proxy:get_xattr(Worker, SessId, ?FILE_REF(Guid), Name1)),
        ?assertEqual(ok, lfm_proxy:remove_xattr(Worker, SessId, ?FILE_REF(Guid), Name1)),
        ?assertEqual({error, ?ENOATTR}, lfm_proxy:get_xattr(Worker, SessId, ?FILE_REF(Guid), Name1))
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

    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr1)),
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr2)),
    ?assertEqual({ok, [Name1, Name2]}, lfm_proxy:list_xattr(Worker, SessId, ?FILE_REF(Guid), false, true)),
    ?assertEqual(ok, lfm_proxy:remove_xattr(Worker, SessId, ?FILE_REF(Guid), Name1)),
    ?assertEqual({ok, [Name2]}, lfm_proxy:list_xattr(Worker, SessId, ?FILE_REF(Guid), false, true)).

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
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr1, true, false)),
    ?assertEqual({ok, Xattr1}, lfm_proxy:get_xattr(Worker, SessId, ?FILE_REF(Guid), Name1)),

    % fail to replace xattr
    ?assertEqual({error, ?EEXIST}, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr2, true, false)),

    % create second xattr
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), OtherXattr, true, false)),
    ?assertEqual({ok, OtherXattr}, lfm_proxy:get_xattr(Worker, SessId, ?FILE_REF(Guid), OtherName)).

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
    ?assertEqual({error, ?ENODATA}, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr1, false, true)),

    % create first xattr
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr1, false, false)),
    ?assertEqual({ok, Xattr1}, lfm_proxy:get_xattr(Worker, SessId, ?FILE_REF(Guid), Name1)),

    % replace first xattr
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr2, false, true)),
    ?assertEqual({ok, Xattr2}, lfm_proxy:get_xattr(Worker, SessId, ?FILE_REF(Guid), Name1)),

    % fail to create second xattr
    ?assertEqual({error, ?ENODATA}, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), OtherXattr, false, true)).

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
    ?assertEqual({error, ?ENODATA}, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr1, true, true)),

    % create first xattr
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr1, false, false)),
    ?assertEqual({ok, Xattr1}, lfm_proxy:get_xattr(Worker, SessId, ?FILE_REF(Guid), Name1)),

    % fail to set xattr due to create flag
    ?assertEqual({error, ?EEXIST}, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr2, true, true)),

    % fail to create second xattr due to replace flag
    ?assertEqual({error, ?ENODATA}, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), OtherXattr, true, true)).

remove_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t4_file">>,
    Name1 = <<"t4_name1">>,
    Value1 = <<"t4_value1">>,
    Xattr1 = #xattr{name = Name1, value = Value1},
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),
    Uuid = file_id:guid_to_uuid(Guid),

    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr1)),
    ?assertEqual({ok, [Name1]}, lfm_proxy:list_xattr(Worker, SessId, ?FILE_REF(Guid), false, true)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Guid))),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:list_xattr(Worker, SessId, ?FILE_REF(Guid), false, true)),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:get_xattr(Worker, SessId, ?FILE_REF(Guid), Name1)),
    {ok, Guid2} = lfm_proxy:create(Worker, SessId, Path),
    ?assertEqual({ok, []}, lfm_proxy:list_xattr(Worker, SessId, ?FILE_REF(Guid2), false, true)),
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

    ?assertEqual({error, ?EPERM}, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr1)),
    ?assertEqual({ok, []}, lfm_proxy:list_xattr(Worker, SessId, ?FILE_REF(Guid), false, true)).

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
    ?assertEqual(ok, opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid1), json, MetaBlue, [])),
    ?assertEqual(ok, opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid2), json, MetaRed, [])),
    ?assertEqual(ok, opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid3), json, MetaBlue, [])),
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

    ?assertEqual(?ERROR_POSIX(?ENOATTR), opt_file_metadata:get_custom_metadata(Worker, SessId, ?FILE_REF(Guid), json, [], false)).

get_empty_rdf(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t6_file">>,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),

    ?assertEqual(?ERROR_POSIX(?ENOATTR), opt_file_metadata:get_custom_metadata(Worker, SessId, ?FILE_REF(Guid), rdf, [], false)).

has_custom_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Path = <<"/space_name1/t6_file">>,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, Path),

    % json
    ?assertEqual({ok, false}, lfm_proxy:has_custom_metadata(Worker, SessId, ?FILE_REF(Guid))),
    ?assertEqual(ok, opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), json, #{}, [])),
    ?assertEqual({ok, true}, lfm_proxy:has_custom_metadata(Worker, SessId, ?FILE_REF(Guid))),
    ?assertEqual(ok, opt_file_metadata:remove_custom_metadata(Worker, SessId, ?FILE_REF(Guid), json)),

    % rdf
    ?assertEqual({ok, false}, lfm_proxy:has_custom_metadata(Worker, SessId, ?FILE_REF(Guid))),
    ?assertEqual(ok, opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), rdf, <<"<xml>">>, [])),
    ?assertEqual({ok, true}, lfm_proxy:has_custom_metadata(Worker, SessId, ?FILE_REF(Guid))),
    ?assertEqual(ok, opt_file_metadata:remove_custom_metadata(Worker, SessId, ?FILE_REF(Guid), rdf)),

    % xattr
    ?assertEqual({ok, false}, lfm_proxy:has_custom_metadata(Worker, SessId, ?FILE_REF(Guid))),
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), #xattr{name = <<"name">>, value = <<"value">>})),
    ?assertEqual({ok, true}, lfm_proxy:has_custom_metadata(Worker, SessId, ?FILE_REF(Guid))),
    ?assertEqual(ok, lfm_proxy:remove_xattr(Worker, SessId, ?FILE_REF(Guid), <<"name">>)),
    ?assertEqual({ok, false}, lfm_proxy:has_custom_metadata(Worker, SessId, ?FILE_REF(Guid))).

resolve_guid_of_root_should_return_root_guid(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    RootGuid = rpc:call(Worker, fslogic_file_id, user_root_dir_guid, [UserId]),

    ?assertEqual({ok, RootGuid}, lfm_proxy:resolve_guid(Worker, SessId, <<"/">>)).

resolve_guid_of_space_should_return_space_guid(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId, _UserId} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    SpaceDirGuid = rpc:call(Worker, fslogic_file_id, spaceid_to_space_dir_guid, [SpaceId]),

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
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr1)),
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

    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid1), Xattr1)),
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid1), Xattr2)),
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid1), Xattr3)),

    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid2), Xattr1)),
    ?assertEqual(ok, lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid2), Xattr2)),

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
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),

    {ok, ShareId} = opt_shares:create(Worker, SessId, ?FILE_REF(SpaceGuid), <<"szer">>),
    {ok, SpaceAttrs} = ?assertMatch(
        {ok, #file_attr{shares = [ShareId]}},
        lfm_proxy:stat(Worker, SessId, ?FILE_REF(SpaceGuid))
    ),

    lists:foreach(fun(_) ->
        ?assertEqual(ok, rpc:call(Worker, file_meta, make_space_exist, [SpaceId])),
        ?assertMatch({ok, SpaceAttrs}, lfm_proxy:stat(Worker, SessId, ?FILE_REF(SpaceGuid)))
    end, lists:seq(1, 10)).


listing_file_attrs_should_work_properly_in_open_handle_mode(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    User = <<"user1">>,
    [{SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),

    NormalSessId = ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config),

    ClientAccessToken = tokens:confine(
        initializer:create_access_token(User),
        #cv_interface{interface = oneclient}
    ),
    OpenHandleSessId = permissions_test_utils:create_session(
        Worker, User, ClientAccessToken, open_handle
    ),

    {ok, File1Guid} = lfm_proxy:create(Worker, NormalSessId, <<"/", SpaceName/binary, "/file1">>),
    {ok, File2Guid} = lfm_proxy:create(Worker, NormalSessId, <<"/", SpaceName/binary, "/file2">>),
    {ok, File3Guid} = lfm_proxy:create(Worker, NormalSessId, <<"/", SpaceName/binary, "/file3">>),
    {ok, DirGuid} = lfm_proxy:mkdir(Worker, NormalSessId, <<"/", SpaceName/binary, "/dir">>, 8#770),
    {ok, File4Guid} = lfm_proxy:create(Worker, NormalSessId, <<"/", SpaceName/binary, "/dir/file4">>),
    mock_space_get_shares(Worker, []),

    Content = <<"content">>,
    ContentSize = size(Content),
    {ok, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, NormalSessId, ?FILE_REF(File4Guid), write)),
    ?assertMatch({ok, ContentSize}, lfm_proxy:write(Worker, Handle1, 0, Content)),
    ?assertMatch(ok, lfm_proxy:close(Worker, Handle1)),

    % Assert that when listing in normal mode all space files are returned
    ?assertMatch(
        {ok, [{DirGuid, _}, {File1Guid, _}, {File2Guid, _}, {File3Guid, _}]},
        lfm_proxy:get_children(Worker, NormalSessId, ?FILE_REF(SpaceGuid), 0, 100)
    ),

    % Assert that listing in open_handle mode should return nothing as there are no shares with open handle
    ?assertMatch(
        {ok, []},
        lfm_proxy:get_children(Worker, OpenHandleSessId, ?FILE_REF(SpaceGuid), 0, 100)
    ),

    BuildShareRootDirFun = fun(ShareId) ->
        file_id:pack_share_guid(fslogic_file_id:shareid_to_share_root_dir_uuid(ShareId), SpaceId, ShareId)
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
    {ok, _, ListingToken} = ?assertMatch(
        {ok, [
            #file_attr{
                guid = DirShareRootDirGuid, name = DirShareId, mode = 8#005, parent_guid = undefined,
                uid = ?SHARE_UID, gid = ?SHARE_GID, type = ?DIRECTORY_TYPE, size = undefined,
                shares = [], provider_id = <<"unknown">>, owner_id = <<"unknown">>
            },
            #file_attr{
                guid = Share1RootDirGuid, name = Share1Id, mode = 8#005, parent_guid = undefined,
                uid = ?SHARE_UID, gid = ?SHARE_GID, type = ?DIRECTORY_TYPE, size = undefined,
                shares = [], provider_id = <<"unknown">>, owner_id = <<"unknown">>
            },
            #file_attr{
                guid = Share4RootDirGuid, name = Share4Id, mode = 8#005, parent_guid = undefined,
                uid = ?SHARE_UID, gid = ?SHARE_GID, type = ?DIRECTORY_TYPE, size = undefined,
                shares = [], provider_id = <<"unknown">>, owner_id = <<"unknown">>
            },
            #file_attr{
                guid = SpaceShareRootDirGuid, name = SpaceShareId, mode = 8#005, parent_guid = undefined,
                uid = ?SHARE_UID, gid = ?SHARE_GID, type = ?DIRECTORY_TYPE, size = undefined,
                shares = [], provider_id = <<"unknown">>, owner_id = <<"unknown">>
            }
        ], _},
        lfm_proxy:get_children_attrs(Worker, OpenHandleSessId, ?FILE_REF(SpaceGuid),  #{offset => 0, limit => 100, tune_for_large_continuous_listing => false})
    ),
    ?assert(file_listing:is_finished(ListingToken)),

    % Assert listing virtual share root dir returns only share root file
    lists:foreach(fun({ShareRootFileGuid, ShareRootFileName, ShareRootDirGuid}) ->
        ?assertMatch(
            {ok, [{ShareRootFileGuid, ShareRootFileName}]},
            lfm_proxy:get_children(Worker, OpenHandleSessId, ?FILE_REF(ShareRootDirGuid), 0, 100)
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
        lfm_proxy:get_children(Worker, OpenHandleSessId, ?FILE_REF(SpaceShareGuid), 0, 100)
    ),

    % Assert it is possible to operate on file4 using Share4Id (direct share of file4)
    % but is not possible via space/dir share (those are shares created on parents so
    % permissions check must assert traverse ancestors for them too - dir has 8#770 perms
    % and in 'open_handle' mode 'other' bits are checked so permissions will be denied)
    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, OpenHandleSessId, ?FILE_REF(File4ShareGuid), read)),
    ?assertMatch({ok, Content}, lfm_proxy:read(Worker, Handle2, 0, 100)),
    ?assertMatch(ok, lfm_proxy:close(Worker, Handle2)),

    File4DirShareGuid = file_id:guid_to_share_guid(File4Guid, DirShareId),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(Worker, OpenHandleSessId, ?FILE_REF(File4DirShareGuid), read)),

    File4SpaceShareGuid = file_id:guid_to_share_guid(File4Guid, SpaceShareId),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(Worker, OpenHandleSessId, ?FILE_REF(File4SpaceShareGuid), read)),

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
    ?assertMatch(ok, rpc:call(Worker, file_meta, add_share, [
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
init_per_testcase(Case, Config) when Case =:= effective_value_test ;
    Case =:= multiple_references_effective_value_simple_test ;
    Case =:= multiple_references_effective_value_advanced_test ;
    Case =:= deleted_reference_effective_value_test ;
    Case =:= concurent_multiple_references_effective_value_test ;
    Case =:= concurent_multiple_references_effective_value_in_critical_section_test ;
    Case =:= concurent_multiple_references_effective_value_parent_critical_section_test ->
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
end_per_testcase(Case, Config) when Case =:= effective_value_test ;
    Case =:= multiple_references_effective_value_simple_test ;
    Case =:= multiple_references_effective_value_advanced_test ;
    Case =:= deleted_reference_effective_value_test ;
    Case =:= concurent_multiple_references_effective_value_test ;
    Case =:= concurent_multiple_references_effective_value_in_critical_section_test ;
    Case =:= concurent_multiple_references_effective_value_parent_critical_section_test ->
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

prepare_effective_value_test_tree(Config, NamePrefix) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Dir0 = filename:join(["/", SpaceName, NamePrefix ++ "_dir0"]),
    Dir1 = filename:join([Dir0, NamePrefix ++ "_dir1"]),
    Dir2 = filename:join([Dir1, NamePrefix ++ "_dir2"]),
    Dir3 = filename:join(["/", SpaceName, NamePrefix ++ "_dir3"]),
    Link = filename:join(["/", SpaceName, Dir3, "link"]),

    % Create dirs, file and link used during the test
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, Dir0)),
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, Dir1)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, Dir2)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, Dir3)),
    FileGuid = file_ops_test_utils:create_file(Worker, SessId, DirGuid, <<"file">>, <<"xyz">>),
    {ok, #file_attr{guid = LinkGuid}} = ?assertMatch({ok, _}, lfm_proxy:make_link(Worker, SessId, Link, FileGuid)),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    LinkUuid = file_id:guid_to_uuid(LinkGuid),
    {ok, FileDoc} = ?assertMatch({ok, _}, rpc:call(Worker, file_meta, get, [{uuid, FileUuid}])),
    {ok, LinkDoc} = ?assertMatch({ok, _}, rpc:call(Worker, file_meta, get, [{uuid, LinkUuid}])),

    {FileGuid, FileDoc, LinkDoc}.

prepare_effective_value_callbacks() ->
    MainCallback = fun([#document{value = #file_meta{name = Name}}, ParentValue, CalculationInfo]) ->
        {ok, Name, [{Name, ParentValue} | CalculationInfo]}
    end,
    Master = self(),
    MergeCallback = fun(Value, Acc, CalculationInfo, CalculationInfoAcc) ->
        Master ! {merge_callback, {Acc, Value, CalculationInfoAcc ++ CalculationInfo}},
        {ok, {Acc, Value}, CalculationInfoAcc ++ CalculationInfo}
    end,

    {MainCallback, MergeCallback}.

invalidate_effective_value_cache(Worker) ->
    ?assertEqual(ok, ?CALL_CACHE(Worker, invalidate, [])),
    % Sleep to be sure that next operations are perceived as following after invalidation,
    % not parallel to invalidation (millisecond clock is used by effective value)
    timer:sleep(5).

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