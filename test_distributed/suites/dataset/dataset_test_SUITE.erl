%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of datasets mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_test_SUITE).
-author("Jakub Kudzia").

-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/file_details.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% exported for CT
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    establish_dataset_attached_to_space_dir/1,
    establish_dataset_attached_to_dir/1,
    establish_dataset_attached_to_file/1,
    detach_and_reattach_dataset/1,
    remove_attached_dataset/1,
    remove_detached_dataset/1,
    remove_file_should_detach_dataset/1,
    reattach_if_root_file_is_deleted_should_fail/1,
    remove_detached_dataset_if_root_file_has_already_been_deleted/1,
    establish_dataset_on_not_existing_file_should_fail/1,
    establish_2nd_dataset_on_file_should_fail/1,
    establish_nested_datasets_structure/1,
    establish_nested_datasets_structure_end_detach_all/1,
    all_files_in_dataset_should_have_ancestor_dataset_membership/1,
    rename_file_should_rename_attached_dataset/1,
    rename_file_should_not_rename_detached_dataset/1,
    move_file_should_move_attached_dataset/1,
    move_file_should_not_move_detached_dataset/1,
    reattach_to_moved_root_file/1,
    establish_datasets_with_the_same_names/1
]).

all() -> ?ALL([
    establish_dataset_attached_to_space_dir,
    establish_dataset_attached_to_dir,
    establish_dataset_attached_to_file,
    detach_and_reattach_dataset,
    remove_attached_dataset,
    remove_detached_dataset,
    remove_file_should_detach_dataset,
    reattach_if_root_file_is_deleted_should_fail,
    remove_detached_dataset_if_root_file_has_already_been_deleted,
    establish_dataset_on_not_existing_file_should_fail,
    establish_2nd_dataset_on_file_should_fail,
    establish_nested_datasets_structure,
    establish_nested_datasets_structure_end_detach_all,
    all_files_in_dataset_should_have_ancestor_dataset_membership,
    rename_file_should_rename_attached_dataset,
    rename_file_should_not_rename_detached_dataset,
    move_file_should_move_attached_dataset,
    move_file_should_not_move_detached_dataset,
    reattach_to_moved_root_file,
    establish_datasets_with_the_same_names
]).


-define(SEP, <<?DIRECTORY_SEPARATOR>>).

-define(ATTEMPTS, 30).

-define(FILE_NAME, <<"file_", (?RAND_NAME)/binary>>).
-define(DIR_NAME, <<"dir_", (?RAND_NAME)/binary>>).
-define(RAND_NAME,
    <<(str_utils:to_binary(?FUNCTION))/binary, "_", (integer_to_binary(rand:uniform(?RAND_RANGE)))/binary>>).
-define(RAND_RANGE, 1000000000).


-define(assertNoDataset(Node, SessionId, DatasetId),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_dataset_info(P1Node, UserSessIdP1, DatasetId), ?ATTEMPTS)
).

-define(assertAttachedDataset(Node, SessionId, DatasetId, Guid, ExpectedParentDatasetId),
    assert_attached_dataset(Node, SessionId, DatasetId, Guid, ExpectedParentDatasetId)
).

-define(assertDetachedDataset(Node, SessionId, DatasetId, ExpectedRootFileGuid, ExpectedParentDatasetId,
    ExpectedRootFilePath, ExpectedRootFileType),

    assert_detached_dataset(Node, SessionId, DatasetId, ExpectedRootFileGuid, ExpectedParentDatasetId,
        ExpectedRootFilePath, ExpectedRootFileType)
).

-define(assertDatasetMembership(Node, SessionId, Guid, ExpectedMembership),
    ?assertMatch({ok, #file_details{
        eff_dataset_membership = ExpectedMembership
    }}, lfm_proxy:get_details(Node, SessionId, {guid, Guid}), ?ATTEMPTS)
).

-define(assertFileEffDatasetSummary(Node, SessionId, Guid, ExpectedDirectDataset, ExpectedAncestorDatasets),
    ?assertMatch({ok, #file_eff_dataset_summary{
        direct_dataset = ExpectedDirectDataset,
        eff_ancestor_datasets = ExpectedAncestorDatasets
    }}, lfm_proxy:get_file_eff_dataset_summary(Node, SessionId, {guid, Guid}), ?ATTEMPTS)
).

-define(assertNoTopDatasets(Node, SessionId, SpaceId, State),
    ?assertMatch({ok, [], true},
        lfm_proxy:list_top_datasets(Node, SessionId, SpaceId, State, #{offset => 0, limit => 100}), ?ATTEMPTS)
).

%%%===================================================================
%%% API functions
%%%===================================================================

establish_dataset_attached_to_space_dir(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, SpaceGuid})),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, SpaceGuid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, SpaceGuid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, SpaceGuid, DatasetId, []).

establish_dataset_attached_to_dir(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME,
    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []).

establish_dataset_attached_to_file(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []).

detach_and_reattach_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, ?DIR_NAME, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),

    % detach dataset
    lfm_proxy:detach_dataset(P1Node, UserSessIdP1, DatasetId),

    {ok, Path} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, Path, ?DIRECTORY_TYPE),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?NONE_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),

    % reattach dataset
    ?assertMatch(ok, lfm_proxy:reattach_dataset(P1Node, UserSessIdP1, DatasetId)),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []).

remove_attached_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME,
    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),

    ok = lfm_proxy:remove_dataset(P1Node, UserSessIdP1, DatasetId),

    ?assertNoDataset(P1Node, UserSessIdP1, DatasetId),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?NONE_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, undefined, []),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, detached).


remove_detached_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME,
    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, Path} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),

    ok = lfm_proxy:detach_dataset(P1Node, UserSessIdP1, DatasetId),
    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, Path, ?DIRECTORY_TYPE),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?NONE_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),

    ok = lfm_proxy:remove_dataset(P1Node, UserSessIdP1, DatasetId),

    ?assertNoDataset(P1Node, UserSessIdP1, DatasetId),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?NONE_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, undefined, []),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, detached).

remove_file_should_detach_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME,
    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, Path} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),

    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, {guid, Guid}),

    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, Path, ?DIRECTORY_TYPE),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_details(P1Node, UserSessIdP1, {guid, Guid})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_file_eff_dataset_summary(P1Node, UserSessIdP1, {guid, Guid})),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached).

reattach_if_root_file_is_deleted_should_fail(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME,
    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, Path} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),

    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, {guid, Guid}),

    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, Path, ?DIRECTORY_TYPE),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_details(P1Node, UserSessIdP1, {guid, Guid})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_file_eff_dataset_summary(P1Node, UserSessIdP1, {guid, Guid})),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),

    % reattaching dataset which root file has been remove should fail
    ?assertMatch({error, ?ENOENT}, lfm_proxy:reattach_dataset(P1Node, UserSessIdP1, DatasetId)).


remove_detached_dataset_if_root_file_has_already_been_deleted(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME,
    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, Path} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),

    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, {guid, Guid}),

    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, Path, ?DIRECTORY_TYPE),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_details(P1Node, UserSessIdP1, {guid, Guid})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_file_eff_dataset_summary(P1Node, UserSessIdP1, {guid, Guid})),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),

    % reattaching dataset which root file has been remove should fail
    ?assertMatch({error, ?ENOENT}, lfm_proxy:reattach_dataset(P1Node, UserSessIdP1, DatasetId)),

    % removing dataset should succeed
    ok = lfm_proxy:remove_dataset(P1Node, UserSessIdP1, DatasetId),

    ?assertNoDataset(P1Node, UserSessIdP1, DatasetId),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, detached).


establish_dataset_on_not_existing_file_should_fail(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, {guid, Guid}),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})).

establish_2nd_dataset_on_file_should_fail(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),
    ?assertMatch({error, ?EEXIST}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})).

establish_nested_datasets_structure(_Config) ->
    Depth = 10,
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceName = oct_background:get_space_name(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirNamePrefix = ?DIR_NAME,

    {ok, SpaceDatasetId} = lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, SpaceGuid}),

    GuidsAndDatasetsReversed = lists:foldl(fun(N, AccIn = [{ParentGuid, _, _} | _]) ->
        DirName = str_utils:join_binary([DirNamePrefix, integer_to_binary(N)]),
        {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ParentGuid, DirName, ?DEFAULT_DIR_PERMS),
        {ok, DatasetId} = lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid}),
        [{Guid, DirName, DatasetId} | AccIn]
    end, [{SpaceGuid, SpaceName, SpaceDatasetId}], lists:seq(1, Depth)),

    GuidsAndDatasets = lists:reverse(GuidsAndDatasetsReversed),

    ?assertMatch({ok, [{SpaceDatasetId, SpaceName}], true},
        lfm_proxy:list_top_datasets(P1Node, UserSessIdP1, SpaceId, attached, #{offset => 0, limit => 100})),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, detached),

    lists:foldl(fun({ChildGuid, _ChildName, ChildDatasetId}, {Guid, DatasetId, ExpParentDatasetIds}) ->
        ExpParentDatasetId = case ExpParentDatasetIds =:= [] of
            true -> undefined;
            false -> hd(ExpParentDatasetIds)
        end,
        ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, ExpParentDatasetId),
        ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
        ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, ExpParentDatasetIds),
        {ChildGuid, ChildDatasetId, [DatasetId | ExpParentDatasetIds]}
    end, {SpaceGuid, SpaceDatasetId, []} , tl(GuidsAndDatasets)).

establish_nested_datasets_structure_end_detach_all(_Config) ->
    Depth = 10,
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceName = oct_background:get_space_name(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirNamePrefix = ?DIR_NAME,

    {ok, SpaceDatasetId} = lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, SpaceGuid}),
    ok = lfm_proxy:detach_dataset(P1Node, UserSessIdP1, SpaceDatasetId),

    GuidsAndDatasetsReversed = lists:foldl(fun(N, AccIn = [{ParentGuid, _, _} | _]) ->
        DirName = str_utils:join_binary([DirNamePrefix, integer_to_binary(N)]),
        {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ParentGuid, DirName, ?DEFAULT_DIR_PERMS),
        {ok, DatasetId} = lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid}),
        ok = lfm_proxy:detach_dataset(P1Node, UserSessIdP1, DatasetId),
        [{Guid, DirName, DatasetId} | AccIn]
    end, [{SpaceGuid, SpaceName, SpaceDatasetId}], lists:seq(1, Depth)),

    GuidsAndDatasets = lists:reverse(GuidsAndDatasetsReversed),

    ?assertMatch({ok, [{SpaceDatasetId, SpaceName}], true},
        lfm_proxy:list_top_datasets(P1Node, UserSessIdP1, SpaceId, detached, #{offset => 0, limit => 100})),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),

    lists:foldl(fun({ChildGuid, _ChildName, ChildDatasetId}, {Guid, DatasetId, ExpParentDatasetId}) ->
        {ok, Path} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
        ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, ExpParentDatasetId, Path, ?DIRECTORY_TYPE),
        ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?NONE_DATASET_MEMBERSHIP),
        ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),
        {ChildGuid, ChildDatasetId, DatasetId}
    end, {SpaceGuid, SpaceDatasetId, undefined} , tl(GuidsAndDatasets)).

all_files_in_dataset_should_have_ancestor_dataset_membership(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    {ok, SpaceDatasetId} = lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, SpaceGuid}),
    {DirGuids, FileGuids} = lfm_test_utils:create_files_tree(P1Node, UserSessIdP1, [{10, 10}, {10, 10}], SpaceGuid),

    ?assertAttachedDataset(P1Node, UserSessIdP1, SpaceDatasetId, SpaceGuid, undefined),
    ?assertMatch({ok, [], true},
        lfm_proxy:list_nested_datasets(P1Node, UserSessIdP1, SpaceDatasetId, #{offset => 0, limit => 100})),

    lists:foreach(fun(Guid) ->
        ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?ANCESTOR_DATASET_MEMBERSHIP),
        ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, undefined, [SpaceDatasetId])
    end, DirGuids ++ FileGuids).


rename_file_should_rename_attached_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME,
    NewDirName = ?DIR_NAME,

    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),

    {ok, _} = lfm_proxy:mv(P1Node, UserSessIdP1, {guid, Guid}, {guid, SpaceGuid}, NewDirName),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []).

rename_file_should_not_rename_detached_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME,
    NewDirName = ?DIR_NAME,

    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})),
    {ok, SourcePatch} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),

    ok = lfm_proxy:detach_dataset(P1Node, UserSessIdP1, DatasetId),
    {ok, _} = lfm_proxy:mv(P1Node, UserSessIdP1, {guid, Guid}, {guid, SpaceGuid}, NewDirName),

    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, SourcePatch, ?DIRECTORY_TYPE),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?NONE_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []).


move_file_should_move_attached_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME,
    TargetParentName = ?DIR_NAME,
    NewDirName = ?DIR_NAME,

    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),

    {ok, TargetParentGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, TargetParentName, ?DEFAULT_DIR_PERMS),
    {ok, _} = lfm_proxy:mv(P1Node, UserSessIdP1, {guid, Guid}, {guid, TargetParentGuid}, NewDirName),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []).

move_file_should_not_move_detached_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME,
    TargetParentName = ?DIR_NAME,
    NewDirName = ?DIR_NAME,

    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})),
    {ok, SourcePatch} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),

    {ok, TargetParentGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, TargetParentName, ?DEFAULT_DIR_PERMS),
    ok = lfm_proxy:detach_dataset(P1Node, UserSessIdP1, DatasetId),
    {ok, _} = lfm_proxy:mv(P1Node, UserSessIdP1, {guid, Guid}, {guid, TargetParentGuid}, NewDirName),

    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, SourcePatch, ?DIRECTORY_TYPE),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?NONE_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []).

reattach_to_moved_root_file(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME,
    TargetParentName = ?DIR_NAME,
    NewDirName = ?DIR_NAME,

    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})),
    {ok, SourcePatch} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),

    {ok, TargetParentGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, TargetParentName, ?DEFAULT_DIR_PERMS),
    ok = lfm_proxy:detach_dataset(P1Node, UserSessIdP1, DatasetId),
    {ok, _} = lfm_proxy:mv(P1Node, UserSessIdP1, {guid, Guid}, {guid, TargetParentGuid}, NewDirName),

    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, SourcePatch, ?DIRECTORY_TYPE),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?NONE_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),

    ok = lfm_proxy:reattach_dataset(P1Node, UserSessIdP1, DatasetId),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_DATASET_MEMBERSHIP),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, []),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, detached).

establish_datasets_with_the_same_names(_Config) ->
    Count = 10,
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME,

    GuidsAndDatasetIds = lists:map(fun(I) ->
        % create I nested directories
        Name = <<"dir_", (integer_to_binary(I))/binary>>,
        ParentGuid = lists:foldl(fun(_Depth, AncestorGuid) ->
            {ok, NextAncestorGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, AncestorGuid, Name, ?DEFAULT_DIR_PERMS),
            NextAncestorGuid
        end, SpaceGuid, lists:seq(1, I - 1)),

        % create directory with name DirName on which dataset will be established
        {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ParentGuid, DirName, ?DEFAULT_DIR_PERMS),
        {ok, DatasetId} = lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid}),
        {Guid, DatasetId}
    end, lists:seq(1, Count)),

    {_Guids, DatasetIds} = lists:unzip(GuidsAndDatasetIds),
    ExpectedDatasets = [{DatasetId, DirName} || DatasetId <- DatasetIds],

    {ok, Datasets, true} =
        lfm_proxy:list_top_datasets(P1Node, UserSessIdP1, SpaceId, attached, #{offset => 0, limit => 100}),
    ?assertEqual(lists:sort(ExpectedDatasets), lists:sort(Datasets)),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, detached).


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
    cleanup_attached_datasets(P1Node, SpaceId),
    cleanup_detached_datasets(P1Node, SpaceId),
    lfm_test_utils:clean_space(P1Node, PNodes, SpaceId, ?ATTEMPTS),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

cleanup_attached_datasets(Node, SpaceId) ->
    cleanup_and_verify_datasets(Node, SpaceId, <<"ATTACHED">>).

cleanup_detached_datasets(Node, SpaceId) ->
    cleanup_and_verify_datasets(Node, SpaceId, <<"DETACHED">>).

cleanup_and_verify_datasets(Node, SpaceId, ForestType) ->
    cleanup_datasets(Node, SpaceId, ForestType),
    assert_all_dataset_entries_are_deleted(SpaceId, ForestType).

cleanup_datasets(Node, SpaceId, ForestType) ->
    {ok, Datasets} = rpc:call(Node, datasets_structure, list_all_unsafe, [SpaceId, ForestType]),
    lists:foreach(fun({_DatasetPath, {DatasetId, _DatasetName}}) ->
        ok = rpc:call(Node, dataset_api, remove, [DatasetId])
    end, Datasets).

assert_all_dataset_entries_are_deleted(SpaceId, ForestType) ->
    lists:foreach(fun(N) ->
        ?assertMatch({ok, []}, rpc:call(N, datasets_structure, list_all_unsafe, [SpaceId, ForestType]), ?ATTEMPTS)
    end, oct_background:get_all_providers_nodes()).

assert_attached_dataset(Node, SessionId, DatasetId, ExpectedRootFileGuid, ExpectedParentDatasetId) ->
    {ok, #file_attr{type = ExpectedRootFileType}} = lfm_proxy:stat(Node, SessionId, {guid, ExpectedRootFileGuid}),
    {ok, ExpectedRootFilePath} = lfm_proxy:get_file_path(Node, SessionId, ExpectedRootFileGuid),
    assert_dataset(Node, SessionId, DatasetId, ExpectedRootFileGuid, ExpectedParentDatasetId, ExpectedRootFilePath,
        ExpectedRootFileType, ?ATTACHED_DATASET).


assert_detached_dataset(Node, SessionId, DatasetId, ExpectedRootFileGuid, ExpectedParentDatasetId,
    ExpectedRootFilePath, ExpectedRootFileType
) ->
    assert_dataset(Node, SessionId, DatasetId, ExpectedRootFileGuid, ExpectedParentDatasetId, ExpectedRootFilePath,
        ExpectedRootFileType, ?DETACHED_DATASET).


assert_dataset(Node, SessionId, DatasetId, ExpectedRootFileGuid, ExpectedParentDatasetId, ExpectedRootFilePath,
    ExpectedRootFileType, ExpectedState
) ->
    % check dataset info
    ?assertMatch({ok, #dataset_info{
        id = DatasetId,
        state = ExpectedState,
        guid = ExpectedRootFileGuid,
        path = ExpectedRootFilePath,
        type = ExpectedRootFileType,
        parent = ExpectedParentDatasetId
    }}, lfm_proxy:get_dataset_info(Node, SessionId, DatasetId), ?ATTEMPTS),

    % check dataset structure entry
    Name = filename:basename(ExpectedRootFilePath),
    case ExpectedParentDatasetId =/= undefined of
        true ->
            % check whether dataset is visible on parent dataset's list
            ?assertMatch({ok, [{DatasetId, Name}], true},
                lfm_proxy:list_nested_datasets(Node, SessionId, ExpectedParentDatasetId, #{offset => 0, limit => 100}), ?ATTEMPTS);
        false ->
            % check whether dataset is visible on space top dataset list
            SpaceId = file_id:guid_to_space_id(ExpectedRootFileGuid),
            ?assertMatch({ok, [{DatasetId, Name}], true},
                lfm_proxy:list_top_datasets(Node, SessionId, SpaceId, ExpectedState, #{offset => 0, limit => 100}), ?ATTEMPTS)
    end.