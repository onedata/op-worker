%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of dataset_links module.
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_links_test_SUITE).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% exported for CT
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    basic_crud_depth_0/1,
    basic_crud_depth_1/1,
    basic_crud_depth_10/1,
    nested_datasets_depth0/1,
    nested_datasets_depth1/1,
    nested_datasets_depth10/1,
    mark_parent_as_dataset_depth0/1,
    mark_parent_as_dataset_depth1/1,
    mark_parent_as_dataset_depth10/1,
    nested_dirs_visible_on_space_dataset_list/1,
    basic_sort/1,
    rename_depth1/1,
    rename_depth2/1,
    rename_depth10/1,
    move_depth1_dataset_to_another_dataset/1,
    move_depth2_dataset_to_another_dataset/1,
    move_depth10_dataset_to_another_dataset/1,
    move_depth1_dataset_to_normal_dir/1,
    move_depth2_dataset_to_normal_dir/1,
    move_depth10_dataset_to_normal_dir/1,
    mixed_test/1
]).

all() -> ?ALL([
    basic_crud_depth_0,
    basic_crud_depth_1,
    basic_crud_depth_10,
    nested_datasets_depth0,
    nested_datasets_depth1,
    nested_datasets_depth10,
    mark_parent_as_dataset_depth0,
    mark_parent_as_dataset_depth1,
    mark_parent_as_dataset_depth10,
    nested_dirs_visible_on_space_dataset_list,
    basic_sort,
    rename_depth1,
    rename_depth2,
    rename_depth10,
    move_depth1_dataset_to_another_dataset,
    move_depth2_dataset_to_another_dataset,
    move_depth10_dataset_to_another_dataset,
    move_depth1_dataset_to_normal_dir,
    move_depth2_dataset_to_normal_dir,
    move_depth10_dataset_to_normal_dir,
    mixed_test
]).


-define(SEP, <<?DIRECTORY_SEPARATOR>>).

-define(ATTEMPTS, 30).

% TODO test z movem jak zmieni sie guid

%%%===================================================================
%%% API functions
%%%===================================================================

% TODO VFS-7363 testy z konfliktami
% TODO VFS-7363 testy z batchowaniem

basic_crud_depth_0(_Config) ->
    basic_crud_test_base(0).

basic_crud_depth_1(_Config) ->
    basic_crud_test_base(1).

basic_crud_depth_10(_Config) ->
    basic_crud_test_base(10).

nested_datasets_depth0(_Config) ->
    nested_datasets_test_base(0).

nested_datasets_depth1(_Config) ->
    nested_datasets_test_base(1).

nested_datasets_depth10(_Config) ->
    nested_datasets_test_base(10).

mark_parent_as_dataset_depth0(_Config) ->
    mark_parent_as_dataset_test_base(0).

mark_parent_as_dataset_depth1(_Config) ->
    mark_parent_as_dataset_test_base(1).

mark_parent_as_dataset_depth10(_Config) ->
    mark_parent_as_dataset_test_base(10).

nested_dirs_visible_on_space_dataset_list(_Config) ->
    % This test creates nested directories in the following structure
    % space
    %     dir0/dir0/.../dir0/dir0 // N times
    %     dir1/dir1/.../dir1      // N - 1 times
    %     ...                     // ...
    %     dirN-2/dirN-2           // 2 times
    %     dirN-1                  // 1 time
    % N = 0 means that no directory is created
    % Then, it marks each deepest directory in each subdirectory as a
    % dataset and checks whether they are properly listed.
    N = 10,
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),

    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    % create nested directories
    DatasetsReversed = lists:foldl(fun(I, Acc) ->
        DirName = <<"dir_", (integer_to_binary(I))/binary>>,
        DeepestGuid = lists:foldl(fun(_Depth, ParentGuid) ->
            {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ParentGuid, DirName, ?DEFAULT_DIR_PERMS),
            Guid
        end, SpaceGuid, lists:seq(I, 0, -1)),
        [{file_id:guid_to_uuid(DeepestGuid), DirName} | Acc]
    end, [], lists:seq(0, N - 1)),

    % create dataset entries for all deepest directories
    lists:foreach(fun({DatasetUuid, DatasetName}) ->
        ok = add_link(P1Node, SpaceId, DatasetUuid, DatasetName)
    end, DatasetsReversed),
    Datasets = lists:reverse(DatasetsReversed),

    % check whether datasets are visible on the list of space datasets
    ?assertMatch({ok, Datasets, true},
        list_space(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, Datasets, true},
        list_space(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS).

basic_sort(_Config) ->
    % This test creates DirCount directories in the space directory,
    % marks them as datasets and checks whether they are correctly sorted
    DirsCount = 10,
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceName = oct_background:get_space_name(space1),

    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    % create nested directories
    DatasetsReversed = lists:foldl(fun(I, Acc) ->
        DirName = <<"dir_", (integer_to_binary(I))/binary>>,
        {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
        [{file_id:guid_to_uuid(Guid), DirName} | Acc]
    end, [{SpaceGuid, SpaceName}], lists:seq(0, DirsCount - 1)),

    DatasetsReversed2 = lists:droplast(DatasetsReversed),

    % create dataset entries for all directories
    lists:foreach(fun({DatasetUuid, DatasetName}) ->
        ok = add_link(P1Node, SpaceId, DatasetUuid, DatasetName)
    end, DatasetsReversed2),

    Datasets = lists:reverse(DatasetsReversed2),

    % check whether datasets are visible on the list of space datasets
    ?assertMatch({ok, Datasets, true},
        list_space(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, Datasets, true},
        list_space(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS).

rename_depth1(_Config) ->
    rename_dataset_test_base(1).

rename_depth2(_Config) ->
    rename_dataset_test_base(2).

rename_depth10(_Config) ->
    rename_dataset_test_base(10).

move_depth1_dataset_to_another_dataset(_Config) ->
    move_dataset_test_base(1, dataset).

move_depth2_dataset_to_another_dataset(_Config) ->
    move_dataset_test_base(2, dataset).

move_depth10_dataset_to_another_dataset(_Config) ->
    move_dataset_test_base(10, dataset).

move_depth1_dataset_to_normal_dir(_Config) ->
    move_dataset_test_base(1, normal_dir).

move_depth2_dataset_to_normal_dir(_Config) ->
    move_dataset_test_base(2, normal_dir).

move_depth10_dataset_to_normal_dir(_Config) ->
    move_dataset_test_base(10, normal_dir).

mixed_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceName = oct_background:get_space_name(space1),

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
    
    Dir1Path = filename:join([?SEP, SpaceName, Dir1]),
    Dir12Path = filename:join([Dir1Path, Dir12]),
    Dir2Path = filename:join([?SEP, SpaceName, Dir2]),
    Dir3Path = filename:join([?SEP, SpaceName, Dir3]),
    Dir34Path = filename:join([Dir3Path, Dir34]),

    Paths = [Dir1Path, Dir12Path, Dir2Path, Dir3Path, Dir34Path],
    Names = [filename:basename(P) || P <- Paths],
    Uuids = [Uuid1, Uuid12, Uuid2, Uuid3, Uuid34] = lists:map(fun(Path) ->
        {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, Path),
        file_id:guid_to_uuid(Guid)
    end, Paths),

    % create entries for datasets
    lists:foreach(fun({Uuid, DatasetName}) ->
        % at the beginning, Dir3 is not a dataset
        case DatasetName =:= Dir3 of
            true -> ok;
            false -> ok = add_link(P1Node, SpaceId, Uuid, DatasetName)
        end
    end, lists:zip(Uuids, Names)),

    % only datasets from the highest effective level should be visible on both providers
    % datasets should be sorted by names

    ?assertMatch({ok, [{Uuid1, Dir1}, {Uuid2, Dir2}, {Uuid34, Dir34}], true},
        list_space(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{Uuid1, Dir1}, {Uuid2, Dir2}, {Uuid34, Dir34}], true},
        list_space(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS),

    % dataset Dir12 should be visible in dataset Dir1
    ?assertMatch({ok, [{Uuid12, Dir12}], true},
        list(P1Node, SpaceId, Uuid1, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{Uuid12, Dir12}], true},
        list(P2Node, SpaceId, Uuid1, #{offset => 0, limit => 100}), ?ATTEMPTS),

    % make Dir3 a dataset too
    add_link(P1Node, SpaceId, Uuid3, Dir3),

    % Dataset34 should no longer be visible on the highest level
    ?assertMatch({ok, [{Uuid1, Dir1}, {Uuid2, Dir2}, {Uuid3, Dir3}], true},
        list_space(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{Uuid1, Dir1}, {Uuid2, Dir2}, {Uuid3, Dir3}], true},
        list_space(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS),

    % Dataset34 should be visible inside Dataset3
    ?assertMatch({ok, [{Uuid34, Dir34}], true},
        list(P1Node, SpaceId, Uuid3, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{Uuid34, Dir34}], true},
        list(P2Node, SpaceId, Uuid3, #{offset => 0, limit => 100}), ?ATTEMPTS).

%===================================================================
% Test bases
%===================================================================

basic_crud_test_base(Depth) ->
    % This test creates nested directories: /space/dir0/dir1/.../dirDepth-1
    % Depth = 0 means that no directory is created
    % Then, the deepest directory is treated as a dataset
    % and test checks CRUD operations on dataset_links.

    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceName = oct_background:get_space_name(space1),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),

    % create nested directories
    {DatasetUuid, DatasetName} = lists:foldl(fun(Depth, {ParentUuid, _ParentName}) ->
        DirName = <<"dir_", (integer_to_binary(Depth))/binary>>,
        ParentGuid = file_id:pack_guid(ParentUuid, SpaceId),
        {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ParentGuid, DirName, ?DEFAULT_DIR_PERMS),
        {file_id:guid_to_uuid(Guid), DirName}
    end, {SpaceUuid, SpaceName}, lists:seq(0, Depth - 1)),

    % create dataset entry for the deepest directory
    ok = add_link(P1Node, SpaceId, DatasetUuid, DatasetName),

    % check whether dataset is visible directly in the space (as it's effectively "the highest" dataset)
    ?assertMatch({ok, [{DatasetUuid, DatasetName}], true},
        list_space(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{DatasetUuid, DatasetName}], true},
        list_space(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS),

    % check whether dataset has no nested datasets
    ?assertMatch({ok, [], true},
        list(P1Node, SpaceId, DatasetUuid, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [], true},
        list(P2Node, SpaceId, DatasetUuid, #{offset => 0, limit => 100}), ?ATTEMPTS),

    % delete entry for the dataset
    ok = delete_link(P1Node, SpaceId, DatasetUuid),

    % check whether it has disappeared from the list
    ?assertMatch({ok, [], true},
        list_space(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [], true},
        list_space(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS).


nested_datasets_test_base(Depth) ->
    % This test creates nested directories: /space/dir0/dir1/.../dirDepth-1
    % Depth = 0 means that no directory is created
    % Then, for all created directories, dataset entries are created

    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceName = oct_background:get_space_name(space1),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),

    % create nested directories
    DatasetsReversed = lists:foldl(fun(Depth, Acc = [{ParentUuid, _ParentName} | _]) ->
        ParentGuid = file_id:pack_guid(ParentUuid, SpaceId),
        DirName = <<"dir_", (integer_to_binary(Depth))/binary>>,
        {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ParentGuid, DirName, ?DEFAULT_DIR_PERMS),
        [{file_id:guid_to_uuid(Guid), DirName} | Acc]
    end, [{SpaceUuid, SpaceName}], lists:seq(0, Depth - 1)),

    % create dataset entries for all directories
    lists:foreach(fun({DatasetUuid, DatasetName}) ->
        ok = add_link(P1Node, SpaceId, DatasetUuid, DatasetName)
    end, DatasetsReversed),

    {DeepestDatasetUuid, DeepestDatasetName} = hd(DatasetsReversed),

    % check whether the deepest dataset has no nested datasets
    ?assertMatch({ok, [], true},
        list(P1Node, SpaceId, DeepestDatasetUuid, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [], true},
        list(P2Node, SpaceId, DeepestDatasetUuid, #{offset => 0, limit => 100}), ?ATTEMPTS),


    % check whether only direct child dataset is visible on each level
    {HighestDatasetUuid, HighestDatasetName} = lists:foldl(
        fun({DatasetUuid, DatasetName}, {ChildDatasetUuid, ChildDatasetName}) ->
            ?assertMatch({ok, [{ChildDatasetUuid, ChildDatasetName}], true},
                list(P1Node, SpaceId, DatasetUuid, #{offset => 0, limit => 100})),
            ?assertMatch({ok, [{ChildDatasetUuid, ChildDatasetName}], true},
                list(P2Node, SpaceId, DatasetUuid, #{offset => 0, limit => 100}), ?ATTEMPTS),
            {DatasetUuid, DatasetName}
        end, 
        {DeepestDatasetUuid, DeepestDatasetName}, tl(DatasetsReversed)
    ),

    % check whether the highest dataset (space dir) is visible on the list of space datasets
    ?assertMatch({ok, [{HighestDatasetUuid, HighestDatasetName}], true},
        list_space(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{HighestDatasetUuid, HighestDatasetName}], true},
        list_space(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS).


mark_parent_as_dataset_test_base(Depth) ->
    % This test creates nested directories: /space/dir0/dir1/dir2/.../dirDepth-1
    % Depth = 0 means that no directory is created
    % Then, it iteratively marks each directory as dataset.
    % After each step, check is performed to ensure that child dataset is no longer visible on the
    % space level of datasets.

    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceName = oct_background:get_space_name(space1),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),

    % create nested directories
    DirsReversed = lists:foldl(fun(Depth, Acc = [{ParentUuid, _ParentName} | _]) ->
        DirName = <<"dir_", (integer_to_binary(Depth))/binary>>,
        ParentGuid = file_id:pack_guid(ParentUuid, SpaceId),
        {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ParentGuid, DirName, ?DEFAULT_DIR_PERMS),
        [{file_id:guid_to_uuid(Guid), DirName} | Acc]
    end, [{SpaceUuid, SpaceName}], lists:seq(0, Depth - 1)),

    % mark deepest dir as dataset
    {DeepestDatasetUuid, DeepestDatasetName} = hd(DirsReversed),
    ok = add_link(P1Node, SpaceId, DeepestDatasetUuid, DeepestDatasetName),

    % check whether it's on visible the list of space datasets
    ?assertMatch({ok, [{DeepestDatasetUuid, DeepestDatasetName}], true},
        list_space(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{DeepestDatasetUuid, DeepestDatasetName}], true},
        list_space(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS),


    % check whether only direct child dataset is visible on each level
    {HighestDatasetUuid, HighestDatasetName} = lists:foldl(
        fun({DirUuid, DirName}, {ChildDatasetUuid, ChildDatasetName}) ->

            % mark dir as dataset
            ok = add_link(P1Node, SpaceId, DirUuid, DirName),

            % check whether it's the only visible dataset the list of space datasets
            ?assertMatch({ok, [{DirUuid, DirName}], true},
                list_space(P1Node, SpaceId, #{offset => 0, limit => 100})),
            ?assertMatch({ok, [{DirUuid, DirName}], true},
                list_space(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS),

            % check whether child dataset is visible inside it
            ?assertMatch({ok, [{ChildDatasetUuid, ChildDatasetName}], true},
                list(P1Node, SpaceId, DirUuid, #{offset => 0, limit => 100})),
            ?assertMatch({ok, [{ChildDatasetUuid, ChildDatasetName}], true},
                list(P2Node, SpaceId, DirUuid, #{offset => 0, limit => 100}), ?ATTEMPTS),
            {DirUuid, DirName}
        end,
        {DeepestDatasetUuid, DeepestDatasetName}, tl(DirsReversed)
    ),

    % check whether the highest dataset (space dir) is visible on the list of space datasets
    ?assertMatch({ok, [{HighestDatasetUuid, HighestDatasetName}], true},
        list_space(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{HighestDatasetUuid, HighestDatasetName}], true},
        list_space(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS).


rename_dataset_test_base(Depth) ->
    % This test creates nested directories: /space/dir0/dir1/.../dirDepth-1
    % Depth = 0 means that no directory is created
    % Then, it marks all directories (except from space dir) as datasets.
    % Finally, it renames the highest dataset and checks whether children datasets are
    % properly listed.

    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceName = oct_background:get_space_name(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    HighestDatasetNewName = <<"after rename">>,

    % create nested directories
    DatasetsReversed = lists:foldl(fun(Depth, Acc = [{ParentUuid, _ParentName} | _]) ->
        ParentGuid = file_id:pack_guid(ParentUuid, SpaceId),
        DirName = <<"dir_", (integer_to_binary(Depth))/binary>>,
        {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ParentGuid, DirName, ?DEFAULT_DIR_PERMS),
        [{file_id:guid_to_uuid(Guid), DirName} | Acc]
    end, [{SpaceUuid, SpaceName}], lists:seq(0, Depth - 1)),

    % drop space
    DatasetsReversed2 = lists:droplast(DatasetsReversed),

    % create dataset entries for all directories
    lists:foreach(fun({DatasetUuid, DatasetName}) ->
        ok = add_link(P1Node, SpaceId, DatasetUuid, DatasetName)
    end, DatasetsReversed2),

    Datasets = lists:reverse(DatasetsReversed2),

    % rename highest dataset
    {HighestDatasetUuid, _HighestDatasetName} = hd(Datasets),
    HighestDatasetGuid = file_id:pack_guid(HighestDatasetUuid, SpaceId),
    mock_dataset(P1Node, HighestDatasetUuid),
    {ok, _} = lfm_proxy:mv(P1Node, UserSessIdP1, {guid, HighestDatasetGuid}, {guid, SpaceGuid}, HighestDatasetNewName),

    % check whether the highest dataset is visible on the list of space datasets with a new name
    ?assertMatch({ok, [{HighestDatasetUuid, HighestDatasetNewName}], true},
        list_space(P1Node, SpaceId, #{offset => 0, limit => 100})),
    ?assertMatch({ok, [{HighestDatasetUuid, HighestDatasetNewName}], true},
        list_space(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS),

    {DeepestDatasetUuid, DeepestDatasetName} = hd(DatasetsReversed2),

    % check whether only direct child dataset is visible on each level
    lists:foldl(
        fun({DatasetUuid, DatasetName}, {ChildDatasetUuid, ChildDatasetName}) ->
            ?assertMatch({ok, [{ChildDatasetUuid, ChildDatasetName}], true},
                list(P1Node, SpaceId, DatasetUuid, #{offset => 0, limit => 100})),
            ?assertMatch({ok, [{ChildDatasetUuid, ChildDatasetName}], true},
                list(P2Node, SpaceId, DatasetUuid, #{offset => 0, limit => 100}), ?ATTEMPTS),
            {DatasetUuid, DatasetName}
        end,
        {DeepestDatasetUuid, DeepestDatasetName}, tl(DatasetsReversed2)
    ).

move_dataset_test_base(Depth, TargetType) ->
    % This test creates nested directories: /space/dir0/dir1/.../dirDepth-1
    % Depth = 0 means that no directory is created
    % Then, it marks all directories (except from space dir) as datasets.
    % It also creates a directory /space/target_dir
    % Finally, it moves renames the highest dataset to target_dir
    % and checks whether children datasets are properly listed.
    % According to flag TargetType, target_dir may be a dataset or a normal directory

    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceName = oct_background:get_space_name(space1),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    TargetName = <<"target_dir">>,
    HighestDatasetNewName = <<"after rename">>,

    % create nested directories
    DatasetsReversed = lists:foldl(fun(Depth, Acc = [{ParentUuid, _ParentName} | _]) ->
        ParentGuid = file_id:pack_guid(ParentUuid, SpaceId),
        DirName = <<"dir_", (integer_to_binary(Depth))/binary>>,
        {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ParentGuid, DirName, ?DEFAULT_DIR_PERMS),
        [{file_id:guid_to_uuid(Guid), DirName} | Acc]
    end, [{SpaceUuid, SpaceName}], lists:seq(0, Depth - 1)),

    {ok, TargetGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, TargetName, ?DEFAULT_DIR_PERMS),


    % drop space
    DatasetsReversed2 = lists:droplast(DatasetsReversed),

    % create dataset entries for all directories
    lists:foreach(fun({DatasetUuid, DatasetName}) ->
        ok = add_link(P1Node, SpaceId, DatasetUuid, DatasetName)
    end, DatasetsReversed2),
    Datasets = lists:reverse(DatasetsReversed2),

    TargetUuid = file_id:guid_to_uuid(TargetGuid),
    case TargetType of
        dataset ->
            add_link(P1Node, SpaceId, TargetUuid, TargetName);
        normal_dir ->
            ok
    end,

    % move highest dataset
    {HighestDatasetUuid, _HighestDatasetName} = hd(Datasets),
    HighestDatasetGuid = file_id:pack_guid(HighestDatasetUuid, SpaceId),

    mock_dataset(P1Node, HighestDatasetUuid),
    {ok, _} = lfm_proxy:mv(P1Node, UserSessIdP1, {guid, HighestDatasetGuid}, {guid, TargetGuid}, HighestDatasetNewName),

    case TargetType of
        dataset ->
            % check whether the highest dataset on the list of space datasets is still a target_dir dataset
            ?assertMatch({ok, [{TargetUuid, TargetName}], true},
                list_space(P1Node, SpaceId, #{offset => 0, limit => 100})),
            ?assertMatch({ok, [{TargetUuid, TargetName}], true},
                list_space(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS),

            % check whether moved directory is visible in the target dataset
            ?assertMatch({ok, [{HighestDatasetUuid, HighestDatasetNewName}], true},
                list(P1Node, SpaceId, TargetUuid, #{offset => 0, limit => 100})),
            ?assertMatch({ok, [{HighestDatasetUuid, HighestDatasetNewName}], true},
                list(P2Node, SpaceId, TargetUuid, #{offset => 0, limit => 100}), ?ATTEMPTS);
        normal_dir ->
            % check whether the highest dataset is visible on the list of space datasets with a new name
            ?assertMatch({ok, [{HighestDatasetUuid, HighestDatasetNewName}], true},
                list_space(P1Node, SpaceId, #{offset => 0, limit => 100})),
            ?assertMatch({ok, [{HighestDatasetUuid, HighestDatasetNewName}], true},
                list_space(P2Node, SpaceId, #{offset => 0, limit => 100}), ?ATTEMPTS)
    end,
    {DeepestDatasetUuid, DeepestDatasetName} = hd(DatasetsReversed2),

    % check whether only direct child dataset is visible on each level
    lists:foldl(
        fun({DatasetUuid, DatasetName}, {ChildDatasetUuid, ChildDatasetName}) ->
            ?assertMatch({ok, [{ChildDatasetUuid, ChildDatasetName}], true},
                list(P1Node, SpaceId, DatasetUuid, #{offset => 0, limit => 100})),
            ?assertMatch({ok, [{ChildDatasetUuid, ChildDatasetName}], true},
                list(P2Node, SpaceId, DatasetUuid, #{offset => 0, limit => 100}), ?ATTEMPTS),
            {DatasetUuid, DatasetName}
        end,
        {DeepestDatasetUuid, DeepestDatasetName}, tl(DatasetsReversed2)
    ).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    ssl:start(),
    hackney:start(),
    oct_background:init_per_suite(Config, #onenv_test_config{onenv_scenario = "2op"}).

end_per_suite(_Config) ->
    hackney:stop(),
    ssl:stop().

init_per_testcase(_Case, Config) ->
    % update background config to update sessions
    Config2 = oct_background:update_background_config(Config),
    lfm_proxy:init(Config2).

end_per_testcase(_Case, Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    PNodes = oct_background:get_all_providers_nodes(),
    SpaceId = oct_background:get_space_id(space1),
    test_utils:mock_unload(PNodes, [file_meta]),
    lfm_test_utils:clean_space(P1Node, PNodes, SpaceId, ?ATTEMPTS),
    cleanup_dataset_links(P1Node, SpaceId),
    assert_all_dataset_links_are_deleted(PNodes, SpaceId),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

add_link(Node, SpaceId, DatasetId, FileUuid, DatasetName) ->
    rpc:call(Node, dataset_links, add, [SpaceId, DatasetId, FileUuid, DatasetName]).

list_space(Node, SpaceId, Opts) ->
    rpc:call(Node, dataset_links, list_space, [SpaceId, Opts]).

list(Node, DatasetId, Opts) ->
    rpc:call(Node, dataset_links, list, [DatasetId, Opts]).

delete_link(Node, SpaceId, DatasetUuid) ->
    rpc:call(Node, dataset_links, delete, [SpaceId, DatasetUuid]).

mock_dataset(Node, DatasetUuid) ->
    ok = test_utils:mock_new(Node, file_meta),
    ok = test_utils:mock_expect(Node, file_meta, is_dataset, fun(Doc = #document{key = Uuid}) ->
        case Uuid =:= DatasetUuid of
            true -> true;
            false -> meck:passthrough([Doc])
        end
    end).

cleanup_dataset_links(Node, SpaceId) ->
    rpc:call(Node, dataset_links, delete_all_unsafe, [SpaceId]).

assert_all_dataset_links_are_deleted(Nodes, SpaceId) ->
    lists:foreach(fun(N) ->
        ?assertMatch({ok, []}, rpc:call(N, dataset_links, list_all_unsafe, [SpaceId]), ?ATTEMPTS)
    end, Nodes).