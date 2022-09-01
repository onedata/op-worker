%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of generic datasets_structure implemented using datastore links.
%%% @end
%%%-------------------------------------------------------------------
-module(datasets_structure_test_SUITE).
-author("Jakub Kudzia").

-include("onenv_test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


%% exported for CT
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    basic_crud_depth_0/1,
    basic_crud_depth_1/1,
    basic_crud_depth_10/1,
    nested_datasets_depth_0/1,
    nested_datasets_depth_1/1,
    nested_datasets_depth_10/1,
    mark_parent_as_dataset_depth_0/1,
    mark_parent_as_dataset_depth_1/1,
    mark_parent_as_dataset_depth_10/1,
    nested_dirs_visible_on_space_dataset_list/1,
    basic_sort/1,
    list_with_start_index_and_negative_offset/1,
    rename_depth_1/1,
    rename_depth_2/1,
    rename_depth_10/1,
    move_depth_1_dataset_to_another_dataset/1,
    move_depth_2_dataset_to_another_dataset/1,
    move_depth_10_dataset_to_another_dataset/1,
    move_depth_1_dataset_to_normal_dir/1,
    move_depth_2_dataset_to_normal_dir/1,
    move_depth_10_dataset_to_normal_dir/1,
    mixed_test/1,
    move_dataset_with_1_children/1,
    move_dataset_with_10_children/1,
    move_dataset_with_100_children/1,
    move_dataset_with_1000_children/1,
    iterate_over_100_top_empty_datasets_using_offset_and_limit_1/1,
    iterate_over_100_top_empty_datasets_using_offset_and_limit_10/1,
    iterate_over_100_top_empty_datasets_using_offset_and_limit_100/1,
    iterate_over_100_top_empty_datasets_using_offset_and_limit_1000/1,
    iterate_over_100_children_empty_datasets_using_offset_and_limit_1/1,
    iterate_over_100_children_empty_datasets_using_offset_and_limit_10/1,
    iterate_over_100_children_empty_datasets_using_offset_and_limit_100/1,
    iterate_over_100_children_empty_datasets_using_offset_and_limit_1000/1,
    iterate_over_100_top_non_empty_datasets_using_offset_and_limit_1/1,
    iterate_over_100_top_non_empty_datasets_using_offset_and_limit_10/1,
    iterate_over_100_top_non_empty_datasets_using_offset_and_limit_100/1,
    iterate_over_100_top_non_empty_datasets_using_offset_and_limit_1000/1,
    iterate_over_100_children_non_empty_datasets_using_offset_and_limit_1/1,
    iterate_over_100_children_non_empty_datasets_using_offset_and_limit_10/1,
    iterate_over_100_children_non_empty_datasets_using_offset_and_limit_100/1,
    iterate_over_100_children_non_empty_datasets_using_offset_and_limit_1000/1,
    iterate_over_100_top_empty_datasets_using_start_index_and_limit_1/1,
    iterate_over_100_top_empty_datasets_using_start_index_and_limit_10/1,
    iterate_over_100_top_empty_datasets_using_start_index_and_limit_100/1,
    iterate_over_100_top_empty_datasets_using_start_index_and_limit_1000/1,
    iterate_over_100_children_empty_datasets_using_start_index_and_limit_1/1,
    iterate_over_100_children_empty_datasets_using_start_index_and_limit_10/1,
    iterate_over_100_children_empty_datasets_using_start_index_and_limit_100/1,
    iterate_over_100_children_empty_datasets_using_start_index_and_limit_1000/1,
    iterate_over_100_top_non_empty_datasets_using_start_index_and_limit_1/1,
    iterate_over_100_top_non_empty_datasets_using_start_index_and_limit_10/1,
    iterate_over_100_top_non_empty_datasets_using_start_index_and_limit_100/1,
    iterate_over_100_top_non_empty_datasets_using_start_index_and_limit_1000/1,
    iterate_over_100_children_non_empty_datasets_using_start_index_and_limit_1/1,
    iterate_over_100_children_non_empty_datasets_using_start_index_and_limit_10/1,
    iterate_over_100_children_non_empty_datasets_using_start_index_and_limit_100/1,
    iterate_over_100_children_non_empty_datasets_using_start_index_and_limit_1000/1,
    list_children_with_prefix_names_using_start_index/1
]).

all() -> ?ALL([
    basic_crud_depth_0,
    basic_crud_depth_1,
    basic_crud_depth_10,
    nested_datasets_depth_0,
    nested_datasets_depth_1,
    nested_datasets_depth_10,
    mark_parent_as_dataset_depth_0,
    mark_parent_as_dataset_depth_1,
    mark_parent_as_dataset_depth_10,
    nested_dirs_visible_on_space_dataset_list,
    basic_sort,
    list_with_start_index_and_negative_offset,
    rename_depth_1,
    rename_depth_2,
    rename_depth_10,
    move_depth_1_dataset_to_another_dataset,
    move_depth_2_dataset_to_another_dataset,
    move_depth_10_dataset_to_another_dataset,
    move_depth_1_dataset_to_normal_dir,
    move_depth_2_dataset_to_normal_dir,
    move_depth_10_dataset_to_normal_dir,
    mixed_test,
    move_dataset_with_1_children,
    move_dataset_with_10_children,
    move_dataset_with_100_children,
    move_dataset_with_1000_children,
    iterate_over_100_top_empty_datasets_using_offset_and_limit_1,
    iterate_over_100_top_empty_datasets_using_offset_and_limit_10,
    iterate_over_100_top_empty_datasets_using_offset_and_limit_100,
    iterate_over_100_top_empty_datasets_using_offset_and_limit_1000,
    iterate_over_100_children_empty_datasets_using_offset_and_limit_1,
    iterate_over_100_children_empty_datasets_using_offset_and_limit_10,
    iterate_over_100_children_empty_datasets_using_offset_and_limit_100,
    iterate_over_100_children_empty_datasets_using_offset_and_limit_1000,
    iterate_over_100_top_non_empty_datasets_using_offset_and_limit_1,
    iterate_over_100_top_non_empty_datasets_using_offset_and_limit_10,
    iterate_over_100_top_non_empty_datasets_using_offset_and_limit_100,
    iterate_over_100_top_non_empty_datasets_using_offset_and_limit_1000,
    iterate_over_100_children_non_empty_datasets_using_offset_and_limit_1,
    iterate_over_100_children_non_empty_datasets_using_offset_and_limit_10,
    iterate_over_100_children_non_empty_datasets_using_offset_and_limit_100,
    iterate_over_100_children_non_empty_datasets_using_offset_and_limit_1000,
    iterate_over_100_top_empty_datasets_using_start_index_and_limit_1,
    iterate_over_100_top_empty_datasets_using_start_index_and_limit_10,
    iterate_over_100_top_empty_datasets_using_start_index_and_limit_100,
    iterate_over_100_top_empty_datasets_using_start_index_and_limit_1000,
    iterate_over_100_children_empty_datasets_using_start_index_and_limit_1,
    iterate_over_100_children_empty_datasets_using_start_index_and_limit_10,
    iterate_over_100_children_empty_datasets_using_start_index_and_limit_100,
    iterate_over_100_children_empty_datasets_using_start_index_and_limit_1000,
    iterate_over_100_top_non_empty_datasets_using_start_index_and_limit_1,
    iterate_over_100_top_non_empty_datasets_using_start_index_and_limit_10,
    iterate_over_100_top_non_empty_datasets_using_start_index_and_limit_100,
    iterate_over_100_top_non_empty_datasets_using_start_index_and_limit_1000,
    iterate_over_100_children_non_empty_datasets_using_start_index_and_limit_1,
    iterate_over_100_children_non_empty_datasets_using_start_index_and_limit_10,
    iterate_over_100_children_non_empty_datasets_using_start_index_and_limit_100,
    iterate_over_100_children_non_empty_datasets_using_start_index_and_limit_1000,
    list_children_with_prefix_names_using_start_index
]).


-define(ATTEMPTS, 30).
-define(TEST_FOREST_TYPE, <<"TEST_FOREST_TYPE">>).

-define(DATASET_NAME(N), ?NAME(<<"dataset_name">>, N)).
-define(DATASET_NAME, ?RAND_NAME(<<"dataset_name">>)).
-define(DATASET_ID(DatasetPath), lists:last(filepath_utils:split(DatasetPath))).
-define(UUID, ?RAND_NAME(<<"uuid">>)).

-define(RAND_NAME(Prefix), ?NAME(Prefix, rand:uniform(?RAND_RANGE))).
-define(NAME(Prefix, Number), str_utils:join_binary([Prefix, integer_to_binary(Number)], <<"_">>)).
-define(RAND_RANGE, 1000000000).

%%%===================================================================
%%% API functions
%%%===================================================================

basic_crud_depth_0(_Config) ->
    basic_crud_test_base(0).

basic_crud_depth_1(_Config) ->
    basic_crud_test_base(1).

basic_crud_depth_10(_Config) ->
    basic_crud_test_base(10).

nested_datasets_depth_0(_Config) ->
    nested_datasets_test_base(0).

nested_datasets_depth_1(_Config) ->
    nested_datasets_test_base(1).

nested_datasets_depth_10(_Config) ->
    nested_datasets_test_base(10).

mark_parent_as_dataset_depth_0(_Config) ->
    mark_parent_as_dataset_test_base(0).

mark_parent_as_dataset_depth_1(_Config) ->
    mark_parent_as_dataset_test_base(1).

mark_parent_as_dataset_depth_10(_Config) ->
    mark_parent_as_dataset_test_base(10).

nested_dirs_visible_on_space_dataset_list(_Config) ->
    % This test generates nested datasets paths in the following format
    % space
    %     dir0/dir0/.../dir0/dir0 // N times
    %     dir1/dir1/.../dir1      // N - 1 times
    %     ...                     // ...
    %     dirN-2/dirN-2           // 2 times
    %     dirN-1                  // 1 time
    % N = 0 means that no directory is created
    % Then, it creates dataset entry for each generated path and checks whether they are properly listed.
    N = 10,
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    SpaceId = oct_background:get_space_id(space1),
    SpaceUuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),
    SpaceDatasetPath = filename:join(?DIRECTORY_SEPARATOR_BIN, SpaceUuid),

    % create dataset entries
    DatasetsReversed = lists:foldl(fun(I, Acc) ->
        DatasetPath = generate_dataset_path(SpaceDatasetPath, I),
        [{DatasetPath, ?DATASET_ID(DatasetPath)} | Acc]
    end, [], lists:seq(1, N)),

    % create dataset entries for all deepest directories
    DatasetIdsAndNames = lists:map(fun({DatasetPath, DatasetId}) ->
        DatasetName = ?DATASET_NAME,
        ok = add(P1Node, SpaceId, DatasetPath, DatasetName),
        {DatasetId, DatasetName}
    end, DatasetsReversed),
    DatasetIdsAndNamesSorted = sort(DatasetIdsAndNames),

    % check whether datasets are visible on the list of space datasets
    ?assertMatch({ok, DatasetIdsAndNamesSorted, true},
        list_top_datasets_and_skip_indices(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, DatasetIdsAndNamesSorted, true},
        list_top_datasets_and_skip_indices(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS).

basic_sort(_Config) ->
    % This test creates DatasetsCount datasets entries on the top level
    % and checks whether they are correctly sorted and listed using offset
    DatasetsCount = 10,
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    SpaceId = oct_background:get_space_id(space1),
    SpaceUuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),
    SpaceDatasetPath = filename:join(?DIRECTORY_SEPARATOR_BIN, SpaceUuid),

    % create nested directories
    DatasetPathsAndIds = generate_dataset_paths_and_ids(SpaceDatasetPath, 1, DatasetsCount),

    % create dataset entries for all directories
    DatasetIdsAndNames = lists:map(fun({DatasetPath, DatasetId}) ->
        DatasetName = ?DATASET_NAME,
        ok = add(P1Node, SpaceId, DatasetPath, DatasetName),
        {DatasetId, DatasetName}
    end, DatasetPathsAndIds),

    SortedDatasetIdsAndNames = sort(DatasetIdsAndNames),

    % check whether datasets are visible on the list of space datasets
    ?assertMatch({ok, SortedDatasetIdsAndNames, true},
        list_top_datasets_and_skip_indices(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, SortedDatasetIdsAndNames, true},
        list_top_datasets_and_skip_indices(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS),

    % request with offset greater than number of entries should return an empty list
    ?assertMatch({ok, [], true},
        list_top_datasets(P1Node, SpaceId, #{offset => 10, limit => 1})),

    % request with limit lower than 1 should fail
    ?assertMatch(?ERROR_BAD_VALUE_TOO_LOW(limit, 1),
        list_top_datasets(P1Node, SpaceId, #{offset => 1000, limit => 0})),

    ?assertMatch({ok, [], true},
        list_top_datasets(P1Node, SpaceId, #{offset => 10, limit => 1})),
    
    SortedDatasetIdsAndNamesSublist = lists:sublist(SortedDatasetIdsAndNames, 10, 1),
    ?assertMatch({ok, SortedDatasetIdsAndNamesSublist, true},
        list_top_datasets_and_skip_indices(P1Node, SpaceId, #{offset => 9, limit => 1})),

    SortedDatasetIdsAndNamesSublist2 = lists:sublist(SortedDatasetIdsAndNames, 9, 1),
    ?assertMatch({ok, SortedDatasetIdsAndNamesSublist2, false},
        list_top_datasets_and_skip_indices(P1Node, SpaceId, #{offset => 8, limit => 1})).

list_with_start_index_and_negative_offset(_Config) ->
    % This test creates DatasetsCount datasets entries on the top level
    % and checks whether they are correctly sorted and listed using start_index and negative offset
    DatasetsCount = 10,
    [P1Node] = oct_background:get_provider_nodes(krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceUuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),
    SpaceDatasetPath = filename:join(?DIRECTORY_SEPARATOR_BIN, SpaceUuid),

    % create nested directories
    DatasetPathsAndIds = generate_dataset_paths_and_ids(SpaceDatasetPath, 1, DatasetsCount),

    % create dataset entries for all directories
    DatasetIdsAndNames = lists:map(fun({DatasetPath, DatasetId}) ->
        DatasetName = ?DATASET_NAME,
        ok = add(P1Node, SpaceId, DatasetPath, DatasetName),
        {DatasetId, DatasetName}
    end, DatasetPathsAndIds),

    SortedDatasetIdsAndNames = sort(DatasetIdsAndNames),

    % check whether datasets are visible on the list of space datasets
    ?assertMatch({ok, SortedDatasetIdsAndNames, true},
        list_top_datasets_and_skip_indices(P1Node, SpaceId, #{start_index => <<>>, offset => 0, limit => 100})),

    % test requests with negative offset counted from the beginning
    ?assertMatch({ok, SortedDatasetIdsAndNames, true},
        list_top_datasets_and_skip_indices(P1Node, SpaceId, #{start_index => <<>>, offset => -1, limit => 100})),
    ?assertMatch({ok, SortedDatasetIdsAndNames, true},
        list_top_datasets_and_skip_indices(P1Node, SpaceId, #{start_index => <<>>, offset => -10, limit => 100})),

    % request with negative offset, without start_index should fail
    ?assertMatch(?ERROR_BAD_VALUE_TOO_LOW(offset, 0),
        list_top_datasets(P1Node, SpaceId, #{offset => -10, limit => 1})),

    {DatasetId1, _} = hd(SortedDatasetIdsAndNames),
    {DatasetId10, _} = lists:last(SortedDatasetIdsAndNames),

    {DatasetPath1, DatasetId1} = lists:keyfind(DatasetId1, 2, DatasetPathsAndIds),
    {DatasetPath10, DatasetId10} = lists:keyfind(DatasetId10, 2, DatasetPathsAndIds),
    {ok, {_DatasetId1, _DatasetName1, Index1}} = get(P1Node, SpaceId, DatasetPath1),
    {ok, {_DatasetId10, _DatasetName10, Index10}} = get(P1Node, SpaceId, DatasetPath10),

    % request with offset which exceeds length of the list should return an empty list
    ?assertMatch({ok, [], true},
        list_top_datasets(P1Node, SpaceId, #{offset => 1000, start_index => Index1, limit => 1})),

    lists:foreach(fun(Offset) ->
        ExpectedList = lists:sublist(SortedDatasetIdsAndNames, max(1, DatasetsCount + Offset), DatasetsCount),
        ?assertMatch({ok, ExpectedList, true},
            list_top_datasets_and_skip_indices(P1Node, SpaceId, #{offset => Offset, start_index => Index10}))
    end, lists:seq(0, -20, -1)).

rename_depth_1(_Config) ->
    rename_dataset_test_base(1).

rename_depth_2(_Config) ->
    rename_dataset_test_base(2).

rename_depth_10(_Config) ->
    rename_dataset_test_base(10).

move_depth_1_dataset_to_another_dataset(_Config) ->
    move_dataset_test_base(1, dataset).

move_depth_2_dataset_to_another_dataset(_Config) ->
    move_dataset_test_base(2, dataset).

move_depth_10_dataset_to_another_dataset(_Config) ->
    move_dataset_test_base(10, dataset).

move_depth_1_dataset_to_normal_dir(_Config) ->
    move_dataset_test_base(1, normal_dir).

move_depth_2_dataset_to_normal_dir(_Config) ->
    move_dataset_test_base(2, normal_dir).

move_depth_10_dataset_to_normal_dir(_Config) ->
    move_dataset_test_base(10, normal_dir).

mixed_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    SpaceId = oct_background:get_space_id(space1),

    % create datasets in the following structure:
    % SpaceName
    %     dir1: dataset
    %         dir12: dataset
    %     dir2: dataset
    %     dir3: not a dataset
    %         dir34: dataset

    Dir1 = <<"dir1">>,
    Dir12 = <<"dir12">>,
    Dir2 = <<"dir2">>,
    Dir3 = <<"dir3">>,
    Dir34 = <<"dir34">>,

    Dataset1Path = filename:join([?DIRECTORY_SEPARATOR_BIN, SpaceId, ?UUID]),
    Dataset12Path = filename:join([Dataset1Path, ?UUID]),
    Dataset2Path = filename:join([?DIRECTORY_SEPARATOR_BIN, SpaceId, ?UUID]),
    Dataset3Path = filename:join([?DIRECTORY_SEPARATOR_BIN, SpaceId, ?UUID]),
    Dataset34Path = filename:join([Dataset3Path, ?UUID]),

    DatasetPaths = [Dataset1Path, Dataset12Path, Dataset2Path, Dataset3Path, Dataset34Path],
    DatasetNames = [Dir1, Dir12, Dir2, Dir3, Dir34],
    DatasetIds = [?DATASET_ID(DatasetPath) || DatasetPath <- DatasetPaths],
    [DatasetId1, DatasetId12, DatasetId2, DatasetId3, DatasetId34] = DatasetIds,

    % create entries for datasets
    lists:foreach(fun(Dataset = {DatasetPath, DatasetName, _DatasetId}) ->
        % at the beginning, Dir3 is not a dataset
        case DatasetPath =:= Dataset3Path of
            true -> ok;
            false -> ok = add(P1Node, SpaceId, DatasetPath, DatasetName)
        end,
        Dataset
    end, lists:zip3(DatasetPaths, DatasetNames, DatasetIds)),

    % only datasets from the highest effective level should be visible on both providers
    % datasets should be sorted by names

    ?assertMatch({ok, [{DatasetId1, Dir1, _}, {DatasetId2, Dir2, _}, {DatasetId34, Dir34, _}], true},
        list_top_datasets(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{DatasetId1, Dir1, _}, {DatasetId2, Dir2, _}, {DatasetId34, Dir34, _}], true},
        list_top_datasets(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS),

    % dataset Dir12 should be visible in dataset Dir1
    ?assertMatch({ok, [{DatasetId12, Dir12, _}], true},
        list_children_datasets(P1Node, SpaceId, Dataset1Path, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{DatasetId12, Dir12, _}], true},
        list_children_datasets(P2Node, SpaceId, Dataset1Path, #{offset => 0, limit => 100}), ?ATTEMPTS),

    % make Dir3 a dataset too
    add(P1Node, SpaceId, Dataset3Path, Dir3),

    % Dataset34 should no longer be visible on the highest level
    ?assertMatch({ok, [{DatasetId1, Dir1, _}, {DatasetId2, Dir2, _}, {DatasetId3, Dir3, _}], true},
        list_top_datasets(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{DatasetId1, Dir1, _}, {DatasetId2, Dir2, _}, {DatasetId3, Dir3, _}], true},
        list_top_datasets(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS),

    % Dataset34 should be visible inside Dataset3
    ?assertMatch({ok, [{DatasetId34, Dir34, _}], true},
        list_children_datasets(P1Node, SpaceId, Dataset3Path, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{DatasetId34, Dir34, _}], true},
        list_children_datasets(P2Node, SpaceId, Dataset3Path, #{offset => 0, limit => 100}), ?ATTEMPTS).

move_dataset_with_1_children(_Config) ->
    move_dataset_with_many_children_test_base(1).

move_dataset_with_10_children(_Config) ->
    move_dataset_with_many_children_test_base(10).

move_dataset_with_100_children(_Config) ->
    move_dataset_with_many_children_test_base(100).

move_dataset_with_1000_children(_Config) ->
    move_dataset_with_many_children_test_base(1000).

iterate_over_100_top_empty_datasets_using_offset_and_limit_1(_Config) ->
    iterate_over_top_empty_datasets_using_offset_test_base(100, 1).

iterate_over_100_top_empty_datasets_using_offset_and_limit_10(_Config) ->
    iterate_over_top_empty_datasets_using_offset_test_base(100, 10).

iterate_over_100_top_empty_datasets_using_offset_and_limit_100(_Config) ->
    iterate_over_top_empty_datasets_using_offset_test_base(100, 100).

iterate_over_100_top_empty_datasets_using_offset_and_limit_1000(_Config) ->
    iterate_over_top_empty_datasets_using_offset_test_base(100, 1000).

iterate_over_100_children_empty_datasets_using_offset_and_limit_1(_Config) ->
    iterate_over_children_empty_datasets_using_offset_test_base(100, 1).

iterate_over_100_children_empty_datasets_using_offset_and_limit_10(_Config) ->
    iterate_over_children_empty_datasets_using_offset_test_base(100, 10).

iterate_over_100_children_empty_datasets_using_offset_and_limit_100(_Config) ->
    iterate_over_children_empty_datasets_using_offset_test_base(100, 100).

iterate_over_100_children_empty_datasets_using_offset_and_limit_1000(_Config) ->
    iterate_over_children_empty_datasets_using_offset_test_base(100, 1000).

iterate_over_100_top_non_empty_datasets_using_offset_and_limit_1(_Config) ->
    iterate_over_top_non_empty_datasets_using_offset_test_base(100, 1).

iterate_over_100_top_non_empty_datasets_using_offset_and_limit_10(_Config) ->
    iterate_over_top_non_empty_datasets_using_offset_test_base(100, 10).

iterate_over_100_top_non_empty_datasets_using_offset_and_limit_100(_Config) ->
    iterate_over_top_non_empty_datasets_using_offset_test_base(100, 100).

iterate_over_100_top_non_empty_datasets_using_offset_and_limit_1000(_Config) ->
    iterate_over_top_non_empty_datasets_using_offset_test_base(100, 1000).

iterate_over_100_children_non_empty_datasets_using_offset_and_limit_1(_Config) ->
    iterate_over_children_non_empty_datasets_using_offset_test_base(100, 1).

iterate_over_100_children_non_empty_datasets_using_offset_and_limit_10(_Config) ->
    iterate_over_children_non_empty_datasets_using_offset_test_base(100, 10).

iterate_over_100_children_non_empty_datasets_using_offset_and_limit_100(_Config) ->
    iterate_over_children_non_empty_datasets_using_offset_test_base(100, 100).

iterate_over_100_children_non_empty_datasets_using_offset_and_limit_1000(_Config) ->
    iterate_over_children_non_empty_datasets_using_offset_test_base(100, 1000).

iterate_over_100_top_empty_datasets_using_start_index_and_limit_1(_Config) ->
    iterate_over_top_empty_datasets_using_start_index_test_base(100, 1).

iterate_over_100_top_empty_datasets_using_start_index_and_limit_10(_Config) ->
    iterate_over_top_empty_datasets_using_start_index_test_base(100, 10).

iterate_over_100_top_empty_datasets_using_start_index_and_limit_100(_Config) ->
    iterate_over_top_empty_datasets_using_start_index_test_base(100, 100).

iterate_over_100_top_empty_datasets_using_start_index_and_limit_1000(_Config) ->
    iterate_over_top_empty_datasets_using_start_index_test_base(100, 1000).

iterate_over_100_children_empty_datasets_using_start_index_and_limit_1(_Config) ->
    iterate_over_children_empty_datasets_using_start_index_test_base(100, 1).

iterate_over_100_children_empty_datasets_using_start_index_and_limit_10(_Config) ->
    iterate_over_children_empty_datasets_using_start_index_test_base(100, 10).

iterate_over_100_children_empty_datasets_using_start_index_and_limit_100(_Config) ->
    iterate_over_children_empty_datasets_using_start_index_test_base(100, 100).

iterate_over_100_children_empty_datasets_using_start_index_and_limit_1000(_Config) ->
    iterate_over_children_empty_datasets_using_start_index_test_base(100, 1000).

iterate_over_100_top_non_empty_datasets_using_start_index_and_limit_1(_Config) ->
    iterate_over_top_non_empty_datasets_using_start_index_test_base(100, 1).

iterate_over_100_top_non_empty_datasets_using_start_index_and_limit_10(_Config) ->
    iterate_over_top_non_empty_datasets_using_start_index_test_base(100, 10).

iterate_over_100_top_non_empty_datasets_using_start_index_and_limit_100(_Config) ->
    iterate_over_top_non_empty_datasets_using_start_index_test_base(100, 100).

iterate_over_100_top_non_empty_datasets_using_start_index_and_limit_1000(_Config) ->
    iterate_over_top_non_empty_datasets_using_start_index_test_base(100, 1000).

iterate_over_100_children_non_empty_datasets_using_start_index_and_limit_1(_Config) ->
    iterate_over_children_non_empty_datasets_using_start_index_test_base(100, 1).

iterate_over_100_children_non_empty_datasets_using_start_index_and_limit_10(_Config) ->
    iterate_over_children_non_empty_datasets_using_start_index_test_base(100, 10).

iterate_over_100_children_non_empty_datasets_using_start_index_and_limit_100(_Config) ->
    iterate_over_children_non_empty_datasets_using_start_index_test_base(100, 100).

iterate_over_100_children_non_empty_datasets_using_start_index_and_limit_1000(_Config) ->
    iterate_over_children_non_empty_datasets_using_start_index_test_base(100, 1000).

%===================================================================
% Test bases
%===================================================================

basic_crud_test_base(Depth) ->
    % This test generates dataset entries  (dataset paths) associated with
    % directories referenced by paths in format: /space/dir0/dir1/.../dirDepth-1
    % Depth = 0 means that only entry for space directory is used
    % Then, entry for the deepest dataset is created in the dataset structure
    % and test checks CRUD operations on the structure

    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    SpaceId = oct_background:get_space_id(space1),
    SpaceUuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),
    SpaceDatasetPath = filename:join(?DIRECTORY_SEPARATOR_BIN, SpaceUuid),

    % generate nested dataset path
    DatasetPath = generate_dataset_path(SpaceDatasetPath, Depth),
    DatasetId = ?DATASET_ID(DatasetPath),
    DatasetName = ?DATASET_NAME,

    % add dataset entry
    ok = add(P1Node, SpaceId, DatasetPath, DatasetName),

    ?assertMatch({ok, {DatasetId, DatasetName, _}}, get(P1Node, SpaceId, DatasetPath)),
    ?assertMatch({ok, {DatasetId, DatasetName, _}}, get(P2Node, SpaceId, DatasetPath), ?ATTEMPTS),

    % check whether dataset is visible directly in the space (as it's effectively top dataset)
    ?assertMatch({ok, [{DatasetId, DatasetName, _}], true},
        list_top_datasets(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{DatasetId, DatasetName, _}], true},
        list_top_datasets(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS),

    % check whether dataset has no nested datasets
    ?assertMatch({ok, [], true},
        list_children_datasets(P1Node, SpaceId, DatasetPath, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [], true},
        list_children_datasets(P2Node, SpaceId, DatasetPath, #{offset => 0, limit => 100}), ?ATTEMPTS),

    % delete entry for the dataset
    ok = delete(P1Node, SpaceId, DatasetPath),

    % check whether it has disappeared from the list
    ?assertMatch({ok, [], true},
        list_top_datasets(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [], true},
        list_top_datasets(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS).


nested_datasets_test_base(Depth) ->
    % This test generates and creates dataset entries  (dataset paths) associated with
    % directories referenced by paths in format: /space/dir0/dir1/.../dirDepth-1
    % Depth = 0 means that only entry for space directory is used

    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    SpaceId = oct_background:get_space_id(space1),
    SpaceUuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),

    % add entry for space-level dataset
    SpaceDatasetPath = filename:join(?DIRECTORY_SEPARATOR_BIN, SpaceUuid),

    % generate nested dataset paths and ids
    DatasetsReversed = generate_nested_datasets(SpaceDatasetPath, Depth, true),

    lists:foreach(fun({DatasetPath, _DatasetId, DatasetName}) ->
        add(P1Node, SpaceId, DatasetPath, DatasetName)
    end, DatasetsReversed),

    {DeepestDatasetPath, _DeepestDatasetId, _DeepestDatasetName} = hd(DatasetsReversed),

    % check whether the deepest dataset has no nested datasets
    ?assertMatch({ok, [], true},
        list_children_datasets(P1Node, SpaceId, DeepestDatasetPath, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [], true},
        list_children_datasets(P2Node, SpaceId, DeepestDatasetPath, #{offset => 0, limit => 100}), ?ATTEMPTS),

    % check whether only direct child dataset is visible on each level
    {TopDatasetId, TopDatasetName} =
        traverse_bottom_up_and_verify_children_datasets(P1Node, P2Node, SpaceId, DatasetsReversed),

    % check whether top dataset (space dir) is visible on the list of space datasets
    ?assertMatch({ok, [{TopDatasetId, TopDatasetName, _}], true},
        list_top_datasets(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{TopDatasetId, TopDatasetName, _}], true},
        list_top_datasets(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS).


mark_parent_as_dataset_test_base(Depth) ->
    % This test generates dataset entries  (dataset paths) associated with
    % directories referenced by paths in format: /space/dir0/dir1/.../dirDepth-1
    % Depth = 0 means that only entry for space directory is used
    % Then, it iteratively, starting from the bottom, adds each entry to the dataset structure
    % After each step, check is performed to ensure that child dataset is no longer visible on the
    % space level of datasets.

    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    SpaceId = oct_background:get_space_id(space1),
    SpaceUuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),
    SpaceDatasetPath = filename:join(?DIRECTORY_SEPARATOR_BIN, SpaceUuid),

    % generate nested dataset paths and ids
    DatasetsReversed = generate_nested_datasets(SpaceDatasetPath, Depth, true),

    % add entry for the deepest dataset to the datasets structure
    {DeepestDatasetPath, DeepestDatasetId, DeepestDatasetName} = hd(DatasetsReversed),
    ok = add(P1Node, SpaceId, DeepestDatasetPath, DeepestDatasetName),

    % check whether it's on visible the list of space datasets
    ?assertMatch({ok, [{DeepestDatasetId, DeepestDatasetName, _}], true},
        list_top_datasets(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{DeepestDatasetId, DeepestDatasetName, _}], true},
        list_top_datasets(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS),

    % check whether only direct child dataset is visible on each level
    {TopDatasetId, TopDatasetName} = lists:foldl(
        fun({DatasetPath, DatasetId, DatasetName}, {ChildDatasetId, ChildDatasetName}) ->

            % add dataset entry
            ok = add(P1Node, SpaceId, DatasetPath, DatasetName),

            % check whether it's the only visible dataset the list of space datasets
            ?assertMatch({ok, [{DatasetId, DatasetName, _}], true},
                list_top_datasets(P1Node, SpaceId, #{offset => 0, limit => 100})),
            ?assertMatch({ok, [{DatasetId, DatasetName, _}], true},
                list_top_datasets(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS),

            % check whether child dataset is visible inside it
            ?assertMatch({ok, [{ChildDatasetId, ChildDatasetName, _}], true},
                list_children_datasets(P1Node, SpaceId, DatasetPath, #{offset => 0, limit => 100})),
            ?assertMatch({ok, [{ChildDatasetId, ChildDatasetName, _}], true},
                list_children_datasets(P2Node, SpaceId, DatasetPath, #{offset => 0, limit => 100}), ?ATTEMPTS),
            {DatasetId, DatasetName}
        end,
        {DeepestDatasetId, DeepestDatasetName}, tl(DatasetsReversed)
    ),

    % check whether top dataset (space dir) is visible on the list of space datasets
    ?assertMatch({ok, [{TopDatasetId, TopDatasetName, _}], true},
        list_top_datasets(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{TopDatasetId, TopDatasetName, _}], true},
        list_top_datasets(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS).


rename_dataset_test_base(Depth) ->
    % This test generates dataset entries in the format: /space/dir0/dir1/.../dirDepth-1
    % Then, it adds all entries to the dataset structure
    % Finally, it renames the highest dataset and checks whether children datasets are
    % properly listed.

    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    SpaceId = oct_background:get_space_id(space1),
    SpaceUuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),
    SpaceDatasetPath = filename:join(?DIRECTORY_SEPARATOR_BIN, SpaceUuid),
    TopDatasetTargetName = <<"after rename">>,

    DatasetsReversed = generate_nested_datasets(SpaceDatasetPath, Depth, false),

    % create dataset entries
    lists:foreach(fun({DatasetPath, _DatasetId, DatasetName}) ->
        ok = add(P1Node, SpaceId, DatasetPath, DatasetName)
    end, DatasetsReversed),
    Datasets = lists:reverse(DatasetsReversed),
    {TopDatasetPath, TopDatasetId, _TopDatasetName} = hd(Datasets),

    % rename highest dataset
    ok = move(P1Node, SpaceId, TopDatasetPath, TopDatasetPath, TopDatasetTargetName),

    % check whether the highest dataset is visible on the list of space datasets with a new name
    ?assertMatch({ok, [{TopDatasetId, TopDatasetTargetName, _}], true},
        list_top_datasets(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{TopDatasetId, TopDatasetTargetName, _}], true},
        list_top_datasets(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS),

    % check whether only direct child dataset is visible on each level
    traverse_bottom_up_and_verify_children_datasets(P1Node, P2Node, SpaceId, DatasetsReversed).

move_dataset_test_base(Depth, TargetType) ->
    % This test generates nested dataset entries in the format: /space/dir0/dir1/.../dirDepth-1
    % Depth = 0 means that no directory is created
    % Then, it adds entries to the datasets structure
    % It also creates a fake directory /space/target_dir
    % Finally, it moves the highest dataset to target_dir
    % and checks whether children datasets are properly listed.
    % According to flag TargetType, target_dir may be a dataset or a normal directory

    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    SpaceId = oct_background:get_space_id(space1),
    SpaceUuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),
    SpaceDatasetPath = filename:join(?DIRECTORY_SEPARATOR_BIN, SpaceUuid),
    TargetParentUuid = ?UUID,
    TargetParentName = ?DATASET_NAME,
    TopDatasetTargetName = <<"after rename">>,

    DatasetsReversed = generate_nested_datasets(SpaceDatasetPath, Depth, false),
    TargetParentPath = filename:join(SpaceDatasetPath, TargetParentUuid),
    TargetDatasetId = ?DATASET_ID(TargetParentPath),

    % add dataset entries
    lists:foreach(fun({DatasetPath, _DatasetId, DatasetName}) ->
        ok = add(P1Node, SpaceId, DatasetPath, DatasetName)
    end, DatasetsReversed),
    Datasets = lists:reverse(DatasetsReversed),

    case TargetType of
        dataset ->
            add(P1Node, SpaceId, TargetParentPath, TargetParentName);
        normal_dir ->
            ok
    end,

    % move highest dataset
    {TopDatasetPath, TopDatasetId, _TopDatasetName} = hd(Datasets),
    TopDatasetUuid = lists:last(filename:split(TopDatasetPath)),
    TargetDatasetPath =  filename:join(TargetParentPath, TopDatasetUuid),

    move(P1Node, SpaceId, TopDatasetPath, TargetDatasetPath, TopDatasetTargetName),

    case TargetType of
        dataset ->
            % check whether the highest dataset on the list of space datasets is still a target_dir dataset
            ?assertMatch({ok, [{TargetDatasetId, TargetParentName, _}], true},
                list_top_datasets(P1Node, SpaceId, #{offset => 0, limit => 100})),
            ?assertMatch({ok, [{TargetDatasetId, TargetParentName, _}], true},
                list_top_datasets(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS),

            % check whether moved directory is visible in the target dataset
            ?assertMatch({ok, [{TopDatasetId, TopDatasetTargetName, _}], true},
                list_children_datasets(P1Node, SpaceId, TargetParentPath, #{offset => 0, limit => 100})),
            ?assertMatch({ok, [{TopDatasetId, TopDatasetTargetName, _}], true},
                list_children_datasets(P2Node, SpaceId, TargetParentPath, #{offset => 0, limit => 100}), ?ATTEMPTS);
        normal_dir ->
            % check whether the highest dataset is visible on the list of space datasets with a new name
            ?assertMatch({ok, [{TopDatasetId, TopDatasetTargetName, _}], true},
                list_top_datasets(P1Node, SpaceId, #{offset => 0, limit => 100})),
            ?assertMatch({ok, [{TopDatasetId, TopDatasetTargetName, _}], true},
                list_top_datasets(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS)
    end,

    UpdatedDatasetsReversed = lists:map(fun({DatasetPath, DatasetId, DatasetName}) ->
        [?DIRECTORY_SEPARATOR_BIN, _SpaceId | RestTokens] = filename:split(DatasetPath),
        UpdatedDatasetPath = filename:join([TargetParentPath | RestTokens]),
        {UpdatedDatasetPath, DatasetId, DatasetName}
    end, DatasetsReversed),

    traverse_bottom_up_and_verify_children_datasets(P1Node, P2Node, SpaceId, UpdatedDatasetsReversed),

    {UpdatedDeepestDatasetPath, _DeepestDatasetId, _DeepestDatasetName} = hd(UpdatedDatasetsReversed),
    % check whether only direct child dataset is visible on each level
    ?assertMatch({ok, [], true},
        list_children_datasets(P1Node, SpaceId, UpdatedDeepestDatasetPath, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [], true},
        list_children_datasets(P2Node, SpaceId, UpdatedDeepestDatasetPath, #{offset => 0, limit => 100}), ?ATTEMPTS).


move_dataset_with_many_children_test_base(ChildrenCount) ->
    % This test generates one parent dataset entry with ChildrenCount direct children datasets
    % It also creates a fake directory /space/target_dir
    % Finally, it moves the highest dataset to target_dir
    % and checks whether children datasets are properly moved and listed.
    [P1Node] = oct_background:get_provider_nodes(krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceUuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),

    TopDatasetUuid = ?UUID,
    TopDatasetPath = filename:join([?DIRECTORY_SEPARATOR_BIN, SpaceUuid, TopDatasetUuid]),
    TopDatasetId = ?DATASET_ID(TopDatasetPath),
    TopDatasetName = ?DATASET_NAME,
    TopDatasetNewName = <<"after rename">>,

    TargetParentUuid = ?UUID,
    TargetParentDatasetPath = filename:join([?DIRECTORY_SEPARATOR_BIN, SpaceUuid, TargetParentUuid]),
    TargetParentDatasetId = ?DATASET_ID(TargetParentDatasetPath),
    TargetParentName = ?DATASET_NAME,

    Datasets = generate_dataset_paths_and_ids(TopDatasetPath, 1, ChildrenCount),

    ok = add(P1Node, SpaceId, TopDatasetPath, TopDatasetName),
    ok = add(P1Node, SpaceId, TargetParentDatasetPath, TargetParentName),

    % add dataset entries
    ExpectedDatasets = lists:map(fun({DatasetPath, DatasetId}) ->
        DatasetName = ?DATASET_NAME,
        ok = add(P1Node, SpaceId, DatasetPath, DatasetName),
        {DatasetId, DatasetName}
    end, Datasets),

    % move highest dataset
    TargetDatasetPath =  filename:join(TargetParentDatasetPath, TopDatasetUuid),
    move(P1Node, SpaceId, TopDatasetPath, TargetDatasetPath, TopDatasetNewName),

    % check whether the highest dataset on the list of space datasets is still a target_dir dataset
    ?assertMatch({ok, [{TargetParentDatasetId, TargetParentName, _}], _},
        list_top_datasets(P1Node, SpaceId, #{offset => 0, limit => 100})),

    % check whether moved directory is visible in the target dataset
    ?assertMatch({ok, [{TopDatasetId, TopDatasetNewName, _}], _},
        list_children_datasets(P1Node, SpaceId, TargetParentDatasetPath, #{offset => 0, limit => 100})),

    % check whether nested directories are all visible in the moved dataset
    {ok, NestedDatasets, true} = list_children_datasets(P1Node, SpaceId, TargetDatasetPath, #{offset => 0, limit => ChildrenCount}),

    SortedExpectedDatasets = sort(ExpectedDatasets),
    ?assertEqual(skip_indices(NestedDatasets), SortedExpectedDatasets).


iterate_over_top_empty_datasets_using_offset_test_base(ChildrenCount, Limit) ->
    iterate_over_datasets_test_base(ChildrenCount, 1, Limit, top_datasets, offset).

iterate_over_top_non_empty_datasets_using_offset_test_base(ChildrenCount, Limit) ->
    iterate_over_datasets_test_base(ChildrenCount, 3, Limit, top_datasets, offset).

iterate_over_children_empty_datasets_using_offset_test_base(ChildrenCount, Limit) ->
    iterate_over_datasets_test_base(ChildrenCount, 1, Limit, children_datasets, offset).

iterate_over_children_non_empty_datasets_using_offset_test_base(ChildrenCount, Limit) ->
    iterate_over_datasets_test_base(ChildrenCount, 3, Limit, children_datasets, offset).

iterate_over_top_empty_datasets_using_start_index_test_base(ChildrenCount, Limit) ->
    iterate_over_datasets_test_base(ChildrenCount, 1, Limit, top_datasets, start_index).

iterate_over_top_non_empty_datasets_using_start_index_test_base(ChildrenCount, Limit) ->
    iterate_over_datasets_test_base(ChildrenCount, 3, Limit, top_datasets, start_index).

iterate_over_children_empty_datasets_using_start_index_test_base(ChildrenCount, Limit) ->
    iterate_over_datasets_test_base(ChildrenCount, 1, Limit, children_datasets, start_index).

iterate_over_children_non_empty_datasets_using_start_index_test_base(ChildrenCount, Limit) ->
    iterate_over_datasets_test_base(ChildrenCount, 3, Limit, children_datasets, start_index).

iterate_over_datasets_test_base(ChildrenCount, Depth, Limit, ListingType, StartingPoint) ->
    % This test generates and creates ChildrenCount direct children datasets.
    % Each of them has Depth - 1 nested datasets
    % Then the test checks whether dataset entries are correctly listed using StartingPoint.
    % StartingPoint is an enum: [offset, start_index]
    % ListingType is an enum: [top_datasets, children_datasets] which decides whether
    % datasets will be listed as top datasets or as children datasets of dataset attached to space directory
    [P1Node] = oct_background:get_provider_nodes(krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceUuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),
    SpaceDatasetPath = filename:join(?DIRECTORY_SEPARATOR_BIN, SpaceUuid),

    case ListingType of
        top_datasets ->
            ok;
        children_datasets ->
            SpaceDatasetName = ?DATASET_NAME,
            ok = add(P1Node, SpaceId, SpaceDatasetPath, SpaceDatasetName)
    end,

    ExpectedDirectChildrenDatasets = lists:map(fun(_) ->
        DatasetsReversed = generate_nested_datasets(SpaceDatasetPath, Depth, false),
        % generate nested datasets
        lists:foreach(fun({DatasetPath, _DatasetId, DatasetName}) ->
            add(P1Node, SpaceId, DatasetPath, DatasetName)
        end, DatasetsReversed),
        % get top dataset
        {_TopDatasetPath, TopDatasetId, TopDatasetName} = lists:last(DatasetsReversed),
        {TopDatasetId, TopDatasetName}
    end, lists:seq(1, ChildrenCount)),
    SortedExpectedDatasets = sort(ExpectedDirectChildrenDatasets),
    case ListingType of
        top_datasets ->
            check_if_all_top_datasets_listed(P1Node, SpaceId, SortedExpectedDatasets, Limit, StartingPoint);
        children_datasets ->
            check_if_all_datasets_listed(P1Node, SpaceId, SpaceDatasetPath, SortedExpectedDatasets, Limit, StartingPoint)
    end.


list_children_with_prefix_names_using_start_index(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    #object{dataset = #dataset_object{id = DatasetId}} = onenv_file_test_utils:create_and_sync_file_tree(user1, space1, #dir_spec{
        dataset = #dataset_spec{},
        children = [
            #file_spec{name = <<"a">>, dataset = #dataset_spec{}},
            #file_spec{name = <<"a1">>, dataset = #dataset_spec{}},
            #file_spec{name = <<"a11">>, dataset = #dataset_spec{}},
            #file_spec{name = <<"aaa1">>, dataset = #dataset_spec{}}
        ]
    }),
    
    {ok, {[{_, _, Index1}], _}} = ?assertMatch({ok, {[{_, <<"a">>, _}], false}},
        opt_datasets:list_children_datasets(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 1})),
    {ok, {[{_, _, Index2}], _}} = ?assertMatch({ok, {[{_, <<"a1">>, _}], false}},
        opt_datasets:list_children_datasets(P1Node, UserSessIdP1, DatasetId, #{offset => 1, limit => 1, start_index => Index1})),
    {ok, {[{_, _, Index3}], _}} = ?assertMatch({ok, {[{_, <<"a11">>, _}], false}},
        opt_datasets:list_children_datasets(P1Node, UserSessIdP1, DatasetId, #{offset => 1, limit => 1, start_index => Index2})),
    {ok, {[{_, _, Index4}], _}} = ?assertMatch({ok, {[{_, <<"aaa1">>, _}], true}},
        opt_datasets:list_children_datasets(P1Node, UserSessIdP1, DatasetId, #{offset => 1, limit => 1, start_index => Index3})),
    ?assertMatch({ok, {[], true}},
        opt_datasets:list_children_datasets(P1Node, UserSessIdP1, DatasetId, #{offset => 1, limit => 1, start_index => Index4})).


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite([{?LOAD_MODULES, [dir_stats_test_utils]} | Config],
        #onenv_test_config{onenv_scenario = "2op", posthook = fun dir_stats_test_utils:disable_stats_counting_ct_posthook/1}).

end_per_suite(Config) ->
    oct_background:end_per_suite(),
    dir_stats_test_utils:enable_stats_counting(Config).

init_per_testcase(Case, Config)
    when Case =:= move_dataset_with_1_children
    orelse Case =:= move_dataset_with_10_children
    orelse Case =:= move_dataset_with_100_children
    orelse Case =:= move_dataset_with_1000_children
->
    % decrease batch size to ensure that all files will be moved
    % independently of relation of batch size to number of children
    [P1Node] = oct_background:get_provider_nodes(krakow),
    ok = test_utils:set_env(P1Node, op_worker, default_ls_batch_limit, 10),
    init_per_testcase(default, Config);


init_per_testcase(_Case, Config) ->
    % update background config to update sessions
    Config2 = oct_background:update_background_config(Config),
    lfm_proxy:init(Config2).

end_per_testcase(Case, Config)
    when Case =:= move_dataset_with_1_children
    orelse Case =:= move_dataset_with_10_children
    orelse Case =:= move_dataset_with_100_children
    orelse Case =:= move_dataset_with_1000_children
->
    % revert default value
    [P1Node] = oct_background:get_provider_nodes(krakow),
    ok = test_utils:set_env(P1Node, op_worker, default_ls_batch_limit, 5000),
    end_per_testcase(default, Config);

end_per_testcase(_Case, Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    PNodes = oct_background:get_all_providers_nodes(),
    SpaceId = oct_background:get_space_id(space1),
    test_utils:mock_unload(PNodes, [file_meta]),
    lfm_test_utils:clean_space(P1Node, PNodes, SpaceId, ?ATTEMPTS),
    delete_all(P1Node, SpaceId),
    assert_all_dataset_entries_are_deleted(PNodes, SpaceId),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

add(Node, SpaceId, DatasetPath, DatasetName) ->
    ok = rpc:call(Node, datasets_structure, add, [SpaceId, ?TEST_FOREST_TYPE, DatasetPath, DatasetName]).

get(Node, SpaceId, DatasetPath) ->
    rpc:call(Node, datasets_structure, get, [SpaceId, ?TEST_FOREST_TYPE, DatasetPath]).

delete(Node, SpaceId, DatasetPath) ->
    ok = rpc:call(Node, datasets_structure, delete, [SpaceId, ?TEST_FOREST_TYPE, DatasetPath]).

list_top_datasets(Node, SpaceId, Opts) ->
    rpc:call(Node, datasets_structure, list_top_datasets, [SpaceId, ?TEST_FOREST_TYPE, Opts]).

list_top_datasets_and_skip_indices(Node, SpaceId, Opts) ->
    {ok, DatasetEntries, IsLast} = list_top_datasets(Node, SpaceId, Opts),
    {ok, skip_indices(DatasetEntries), IsLast}.

list_children_datasets(Node, SpaceId, DatasetPath, Opts) ->
    rpc:call(Node, datasets_structure, list_children_datasets, [SpaceId, ?TEST_FOREST_TYPE, DatasetPath, Opts]).

move(Node, SpaceId, SourceDatasetPath, TargetDatasetPath, TargetName) ->
    ok = rpc:call(Node, datasets_structure, move,
        [SpaceId, ?TEST_FOREST_TYPE, SourceDatasetPath, TargetDatasetPath, TargetName]).


delete_all(Node, SpaceId) ->
    rpc:call(Node, datasets_structure, delete_all_unsafe, [SpaceId, ?TEST_FOREST_TYPE]).


assert_all_dataset_entries_are_deleted(Nodes, SpaceId) ->
    lists:foreach(fun(N) ->
        ?assertMatch({ok, []}, rpc:call(N, datasets_structure, list_all_unsafe, [SpaceId, ?TEST_FOREST_TYPE]), ?ATTEMPTS)
    end, Nodes).


generate_dataset_paths_and_ids(ParentDatasetPath, Depth, Count) ->
    lists:map(fun(_) ->
        DatasetPath = generate_dataset_path(ParentDatasetPath, Depth),
        {DatasetPath, ?DATASET_ID(DatasetPath)}
    end, lists:seq(1, Count)).


generate_dataset_path(ParentDatasetPath, Depth) ->
    lists:foldl(fun(_Depth, ParentPath) ->
        Uuid = ?UUID,
        filename:join(ParentPath, Uuid)
    end, ParentDatasetPath, lists:seq(0, Depth - 1)).


generate_nested_datasets(RootPath, Depth, GenerateEntryForRoot) ->
    Datasets = lists:foldl(fun(_Depth, Acc = [{ParentDatasetPath, _ParentId, _ParentName} | _]) ->
        Uuid = ?UUID,
        DatasetPath = filename:join(ParentDatasetPath, Uuid),
        [{DatasetPath, ?DATASET_ID(DatasetPath), ?DATASET_NAME} | Acc]
    end, [{RootPath, ?DATASET_ID(RootPath), ?DATASET_NAME}], lists:seq(0, Depth - 1)),

    case GenerateEntryForRoot of
        true -> Datasets;
        false -> lists:droplast(Datasets)
    end.


check_if_all_top_datasets_listed(Node, SpaceId, SortedExpectedDatasets, Limit, StartingPoint) ->
    check_if_all_datasets_listed(Node, SpaceId, undefined, SortedExpectedDatasets, Limit, StartingPoint).


check_if_all_datasets_listed(Node, SpaceId, DatasetPath, SortedExpectedDatasets, Limit, offset) ->
    Opts = #{offset => 0, limit => Limit},
    check_if_all_datasets_listed_helper(Node, SpaceId, DatasetPath, SortedExpectedDatasets, Opts, offset);
check_if_all_datasets_listed(Node, SpaceId, DatasetPath, SortedExpectedDatasets, Limit, start_index) ->
    Opts = #{start_index => <<>>, limit => Limit},
    check_if_all_datasets_listed_helper(Node, SpaceId, DatasetPath, SortedExpectedDatasets, Opts, start_index).


check_if_all_datasets_listed_helper(Node, SpaceId, DatasetPath, SortedExpectedDatasets, Opts, StartingPoint) ->
    {ok, ListedDatasets, AllListed} = case DatasetPath =:= undefined of
        true -> list_top_datasets(Node, SpaceId, Opts);
        false -> list_children_datasets(Node, SpaceId, DatasetPath, Opts)
    end,
    Limit = maps:get(limit, Opts),
    ListedDatasetsWithoutIndices = skip_indices(ListedDatasets),
    ?assertMatch(ListedDatasetsWithoutIndices, lists:sublist(SortedExpectedDatasets, 1, Limit)),
    RestSortedExpectedDatasets = SortedExpectedDatasets -- ListedDatasetsWithoutIndices,
    case {AllListed, length(RestSortedExpectedDatasets) =:= 0} of
        {_, true} ->
            ok;
        {false, false} ->
            NewOpts = update_opts(StartingPoint, Opts, ListedDatasets),
            check_if_all_datasets_listed_helper(Node, SpaceId, DatasetPath, RestSortedExpectedDatasets, NewOpts, StartingPoint)
    end.

update_opts(offset, Opts, ListedDatasets) ->
    maps:update_with(offset, fun(Offset) -> Offset + length(ListedDatasets) end, Opts);
update_opts(start_index, Opts, ListedDatasets) ->
    Opts2 = maps:update(start_index, element(3, lists:last(ListedDatasets)), Opts),
    maps:put(offset, 1, Opts2).

sort(Datasets) ->
    lists:sort(fun({_, N1}, {_, N2}) ->  N1 =< N2 end, Datasets).


-spec traverse_bottom_up_and_verify_children_datasets(node(), node(), od_space:id(),
    [{dataset:path(), dataset:id(), dataset:name()}]) -> {dataset:id(), dataset:name()}.
traverse_bottom_up_and_verify_children_datasets(LocalNode, RemoteNode, SpaceId, DatasetsReversed) ->
    % Datasets should be sorted in reverse order (deepest dataset is in head, top dataset is last)
    {_DeepestDatasetPath, DeepestDatasetId, DeepestDatasetName} = hd(DatasetsReversed),
    lists:foldl(
        fun({DatasetPath, DatasetId, DatasetName}, {ChildDatasetId, ChildDatasetName}) ->
            ?assertMatch({ok, [{ChildDatasetId, ChildDatasetName, _}], true},
                list_children_datasets(LocalNode, SpaceId, DatasetPath, #{offset => 0, limit => 100})),
            ?assertMatch({ok, [{ChildDatasetId, ChildDatasetName, _}], true},
                list_children_datasets(RemoteNode, SpaceId, DatasetPath, #{offset => 0, limit => 100}), ?ATTEMPTS),
            {DatasetId, DatasetName}
        end,
        {DeepestDatasetId, DeepestDatasetName}, tl(DatasetsReversed)).


skip_indices(DatasetEntries) ->
    [{DatasetName, DatasetId} || {DatasetName, DatasetId, _Index} <- DatasetEntries].