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
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/file_details.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/privileges.hrl").
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
    establish_dataset_attached_to_hardlink/1,
    establish_dataset_attached_to_file_symlink/1,
    establish_dataset_attached_to_dir_symlink/1,
    detach_and_reattach_dataset/1,
    remove_attached_dataset/1,
    remove_detached_dataset/1,
    remove_file_should_detach_dataset/1,
    remove_hardlink_should_detach_dataset/1,
    remove_file_symlink_should_detach_dataset/1,
    remove_dir_symlink_should_detach_dataset/1,
    remove_file_pointed_by_hardlink/1,
    remove_file_pointed_by_symlink/1,
    remove_dir_pointed_by_symlink/1,
    reattach_if_root_file_is_deleted_should_fail/1,
    reattach_if_hardlink_is_deleted_should_fail/1,
    reattach_if_file_symlink_is_deleted_should_fail/1,
    reattach_if_dir_symlink_is_deleted_should_fail/1,
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
    establish_datasets_with_the_same_names/1,
    establish_nested_datasets_filetree_structure_with_hardlinks/1
]).

all() -> ?ALL([
    establish_dataset_attached_to_space_dir,
    establish_dataset_attached_to_dir,
    establish_dataset_attached_to_file,
    establish_dataset_attached_to_hardlink,
    establish_dataset_attached_to_file_symlink,
    establish_dataset_attached_to_dir_symlink,
    detach_and_reattach_dataset,
    remove_attached_dataset,
    remove_detached_dataset,
    remove_file_should_detach_dataset,
    remove_hardlink_should_detach_dataset,
    remove_file_symlink_should_detach_dataset,
    remove_dir_symlink_should_detach_dataset,
    remove_file_pointed_by_hardlink,
    remove_file_pointed_by_symlink,
    remove_dir_pointed_by_symlink,
    reattach_if_root_file_is_deleted_should_fail,
    reattach_if_hardlink_is_deleted_should_fail,
    reattach_if_file_symlink_is_deleted_should_fail,
    reattach_if_dir_symlink_is_deleted_should_fail,
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
    establish_datasets_with_the_same_names,
    establish_nested_datasets_filetree_structure_with_hardlinks
]).

-define(ATTEMPTS, 30).

-define(FILE_NAME(), <<"file_", (?RAND_NAME())/binary>>).
-define(DIR_NAME(), <<"dir_", (?RAND_NAME())/binary>>).
-define(RAND_NAME(),
    <<(str_utils:to_binary(?FUNCTION))/binary, "_", (integer_to_binary(rand:uniform(?RAND_RANGE)))/binary>>).
-define(RAND_RANGE, 1000000000).


-define(assertNoDataset(Node, SessionId, DatasetId),
    ?assertMatch({error, ?ENOENT}, opt_datasets:get_info(P1Node, UserSessIdP1, DatasetId), ?ATTEMPTS)
).

-define(assertAttachedDataset(Node, SessionId, DatasetId, Guid, ExpectedParentDatasetId, ExpectedProtectionFlags),
    assert_attached_dataset(Node, SessionId, DatasetId, Guid, ExpectedParentDatasetId, ExpectedProtectionFlags, ExpectedProtectionFlags)
).
-define(assertAttachedDataset(Node, SessionId, DatasetId, Guid, ExpectedParentDatasetId, ExpectedProtectionFlags, ExpectedEffProtectionFlags),
    assert_attached_dataset(Node, SessionId, DatasetId, Guid, ExpectedParentDatasetId, ExpectedProtectionFlags, ExpectedEffProtectionFlags)
).

-define(assertDetachedDataset(Node, SessionId, DatasetId, ExpectedRootFileGuid, ExpectedParentDatasetId,
    ExpectedRootFilePath, ExpectedRootFileType, ExpectedRootFileDeleted, ExpectedProtectionFlags),

    assert_detached_dataset(Node, SessionId, DatasetId, ExpectedRootFileGuid, ExpectedParentDatasetId,
        ExpectedRootFilePath, ExpectedRootFileType, ExpectedRootFileDeleted, ExpectedProtectionFlags)
).

-define(assertDatasetMembership(Node, SessionId, Guid, ExpectedMembership, ExpectedProtectionFlags),
    ?assertMatch({ok, #file_details{
        eff_dataset_membership = ExpectedMembership,
        eff_protection_flags = ExpectedProtectionFlags
    }}, lfm_proxy:get_details(Node, SessionId, ?FILE_REF(Guid)), ?ATTEMPTS)
).

-define(assertFileEffDatasetSummary(Node, SessionId, Guid, ExpectedDirectDataset, ExpectedAncestorDatasets, ExpectedProtectionFlags),
    ?assertMatch({ok, #file_eff_dataset_summary{
        direct_dataset = ExpectedDirectDataset,
        eff_ancestor_datasets = ExpectedAncestorDatasets,
        eff_protection_flags = ExpectedProtectionFlags
    }}, opt_datasets:get_file_eff_summary(Node, SessionId, ?FILE_REF(Guid)), ?ATTEMPTS)
).

-define(assertNoTopDatasets(Node, SessionId, SpaceId, State),
    ?assertMatch({ok, {[], true}},
        opt_datasets:list_top_datasets(Node, SessionId, SpaceId, State, #{offset => 0, limit => 100}), ?ATTEMPTS)
).

-define(assertTopDataset(Node, SessionId, SpaceId, DatasetId, Name, State), begin
    Fun = fun() ->
        {ok, {DatasetList, true}} = opt_datasets:list_top_datasets(Node, SessionId, SpaceId, State, #{offset => 0, limit => 100}),
        lists:filter(
            fun ({D, _, _}) when D == DatasetId -> true; 
                (_) -> false 
            end, DatasetList
        )
    end,
    ?assertMatch([{DatasetId, Name, _}], Fun(), ?ATTEMPTS)
 end).

-define(assertFileEffDatasetSummaryAndMembership(Node, SessionId, Guid, ExpectedDirectDataset, ExpectedAncestorDatasets, ExpectedMembership, ExpectedProtectionFlags), begin
    ?assertDatasetMembership(Node, SessionId, Guid, ExpectedMembership, ExpectedProtectionFlags),
    ?assertFileEffDatasetSummary(Node, SessionId, Guid, ExpectedDirectDataset, ExpectedAncestorDatasets, ExpectedProtectionFlags)
end).

-define(RAND_PROTECTION_FLAGS(), begin
    case rand:uniform(4) of
        1 -> ?no_flags_mask;
        2 -> ?METADATA_PROTECTION;
        3 -> ?DATA_PROTECTION;
        4 -> ?set_flags(?METADATA_PROTECTION, ?DATA_PROTECTION)
    end
end).

-define(ALL_PROTECTION, ?set_flags(?METADATA_PROTECTION, ?DATA_PROTECTION)).

%%%===================================================================
%%% API functions
%%%===================================================================

establish_dataset_attached_to_space_dir(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    ProtectionFlags = ?RAND_PROTECTION_FLAGS(),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(SpaceGuid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, SpaceGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, SpaceGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, SpaceGuid, DatasetId, [], ProtectionFlags).

establish_dataset_attached_to_dir(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME(),
    ProtectionFlags = ?RAND_PROTECTION_FLAGS(),
    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags).

establish_dataset_attached_to_file(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME(),
    ProtectionFlags = ?RAND_PROTECTION_FLAGS(),
    {ok, Guid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags).


establish_dataset_attached_to_hardlink(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME(),
    ProtectionFlags = ?RAND_PROTECTION_FLAGS(),
    {ok, FileGuid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    HardLinkName = <<FileName/binary, <<"_hard_link">>/binary>>,
    LinkPath = filename:join(["/", oct_background:get_space_name(space1), HardLinkName]),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_link(P1Node, UserSessIdP1, LinkPath, FileGuid),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, LinkGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, LinkGuid, DatasetId, [], ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, FileGuid, undefined, [], ProtectionFlags).


establish_dataset_attached_to_file_symlink(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME(),
    ProtectionFlags = ?RAND_PROTECTION_FLAGS(),
    {ok, FileGuid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    SymLinkName = <<FileName/binary, <<"_sym_link">>/binary>>,
    LinkPath = filename:join(["/", oct_background:get_space_name(space1), SymLinkName]),
    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(oct_background:get_space_id(space1)),
    LinkTarget = filename:join([SpaceIdPrefix, FileName]),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_symlink(P1Node, UserSessIdP1, LinkPath, LinkTarget),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, LinkGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, LinkGuid, DatasetId, [], ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, FileGuid, undefined, [], ?no_flags_mask).


establish_dataset_attached_to_dir_symlink(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME(),
    ProtectionFlags = ?RAND_PROTECTION_FLAGS(),
    {ok, DirGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    SymLinkName = <<DirName/binary, <<"_sym_link">>/binary>>,
    LinkPath = filename:join(["/", oct_background:get_space_name(space1), SymLinkName]),
    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(oct_background:get_space_id(space1)),
    LinkTarget = filename:join([SpaceIdPrefix, DirName]),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_symlink(P1Node, UserSessIdP1, LinkPath, LinkTarget),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, LinkGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, LinkGuid, DatasetId, [], ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, DirGuid, undefined, [], ?no_flags_mask).


detach_and_reattach_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    ProtectionFlags = ?RAND_PROTECTION_FLAGS(),
    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, ?DIR_NAME(), ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags)),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags),

    % detach dataset
    detach(P1Node, UserSessIdP1, DatasetId),

    {ok, Path} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, Path, ?DIRECTORY_TYPE, false, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?NONE_MEMBERSHIP, ?no_flags_mask),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ?no_flags_mask),

    % reattach dataset
    ?assertMatch(ok, reattach(P1Node, UserSessIdP1, DatasetId)),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags).

remove_attached_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    User2SessIdP1 = oct_background:get_user_session_id(user2, krakow),
    UserId2 = oct_background:get_user_id(user2),
    SpaceId = oct_background:get_space_id(space1),
    % assign user2 privilege to manage datasets
    ozw_test_rpc:space_set_user_privileges(SpaceId, UserId2, [?SPACE_MANAGE_DATASETS | privileges:space_member()]),

    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    ParentDirName = ?DIR_NAME(),
    DirName = ?DIR_NAME(),
    ProtectionFlags = ?RAND_PROTECTION_FLAGS(),
    {ok, ParentGuid} = lfm_proxy:mkdir(P1Node, User2SessIdP1, SpaceGuid, ParentDirName, ?DEFAULT_DIR_PERMS),
    {ok, Guid} = lfm_proxy:mkdir(P1Node, User2SessIdP1, ParentGuid, DirName, ?DEFAULT_DIR_PERMS),

    {ok, DatasetId} = ?assertMatch({ok, _},
        opt_datasets:establish(P1Node, User2SessIdP1, ?FILE_REF(Guid), ProtectionFlags), ?ATTEMPTS),

    ?assertAttachedDataset(P1Node, User2SessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, User2SessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, User2SessIdP1, Guid, DatasetId, [], ProtectionFlags),

    % traverse permission to file is required to remove attached dataset
    ok = lfm_proxy:set_perms(P1Node, UserSessIdP1, ?FILE_REF(ParentGuid), 8#444),
    % user2 should not be able to remove the dataset
    ?assertMatch({error, ?EACCES}, opt_datasets:remove(P1Node, User2SessIdP1, DatasetId)),

    % revert permissions
    ok = lfm_proxy:set_perms(P1Node, UserSessIdP1, ?FILE_REF(ParentGuid), ?DEFAULT_DIR_PERMS),
    % now user2 should be able to remove the dataset
    ?assertMatch(ok, opt_datasets:remove(P1Node, User2SessIdP1, DatasetId)),

    ?assertNoDataset(P1Node, User2SessIdP1, DatasetId),
    ?assertDatasetMembership(P1Node, User2SessIdP1, Guid, ?NONE_MEMBERSHIP, ?no_flags_mask),
    ?assertFileEffDatasetSummary(P1Node, User2SessIdP1, Guid, undefined, [], ?no_flags_mask),
    ?assertNoTopDatasets(P1Node, User2SessIdP1, SpaceId, attached),
    ?assertNoTopDatasets(P1Node, User2SessIdP1, SpaceId, detached).


remove_detached_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    User2SessIdP1 = oct_background:get_user_session_id(user2, krakow),
    UserId2 = oct_background:get_user_id(user2),
    % assign user2 privilege to manage datasets
    SpaceId = oct_background:get_space_id(space1),
    ozw_test_rpc:space_set_user_privileges(SpaceId, UserId2, [?SPACE_MANAGE_DATASETS | privileges:space_member()]),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME(),
    ParentDirName = ?DIR_NAME(),
    ProtectionFlags = ?RAND_PROTECTION_FLAGS(),
    {ok, ParentGuid} = lfm_proxy:mkdir(P1Node, User2SessIdP1, SpaceGuid, ParentDirName, ?DEFAULT_DIR_PERMS),
    {ok, Guid} = lfm_proxy:mkdir(P1Node, User2SessIdP1, ParentGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, Path} = lfm_proxy:get_file_path(P1Node, User2SessIdP1, Guid),
    {ok, DatasetId} = ?assertMatch({ok, _},
        opt_datasets:establish(P1Node, User2SessIdP1, ?FILE_REF(Guid), ProtectionFlags), ?ATTEMPTS),
    ?assertAttachedDataset(P1Node, User2SessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, User2SessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, User2SessIdP1, Guid, DatasetId, [], ProtectionFlags),

    ok = detach(P1Node, User2SessIdP1, DatasetId),
    ?assertDetachedDataset(P1Node, User2SessIdP1, DatasetId, Guid, undefined, Path, ?DIRECTORY_TYPE, false, ProtectionFlags),
    ?assertDatasetMembership(P1Node, User2SessIdP1, Guid, ?NONE_MEMBERSHIP, ?no_flags_mask),
    ?assertFileEffDatasetSummary(P1Node, User2SessIdP1, Guid, DatasetId, [], ?no_flags_mask),

    % traverse permission to file is NOT required to remove detached dataset
    ok = lfm_proxy:set_perms(P1Node, UserSessIdP1, ?FILE_REF(ParentGuid), 8#444),
    % user2 should be able to remove the dataset
    ?assertMatch(ok, opt_datasets:remove(P1Node, User2SessIdP1, DatasetId)),

    ?assertNoDataset(P1Node, User2SessIdP1, DatasetId),

    % revert perms to perform operations on file
    ok = lfm_proxy:set_perms(P1Node, UserSessIdP1, ?FILE_REF(ParentGuid), ?DEFAULT_DIR_PERMS),

    ?assertDatasetMembership(P1Node, User2SessIdP1, Guid, ?NONE_MEMBERSHIP, ?no_flags_mask),
    ?assertFileEffDatasetSummary(P1Node, User2SessIdP1, Guid, undefined, [], ?no_flags_mask),
    ?assertNoTopDatasets(P1Node, User2SessIdP1, SpaceId, attached),
    ?assertNoTopDatasets(P1Node, User2SessIdP1, SpaceId, detached).

remove_file_should_detach_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME(),
    ProtectionFlags = ?METADATA_PROTECTION,
    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, Path} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags)),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags),

    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, ?FILE_REF(Guid)),

    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, Path, ?DIRECTORY_TYPE, true, ProtectionFlags),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_details(P1Node, UserSessIdP1, ?FILE_REF(Guid))),
    ?assertMatch({error, ?ENOENT}, opt_datasets:get_file_eff_summary(P1Node, UserSessIdP1, ?FILE_REF(Guid))),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),
    ?assertTopDataset(P1Node, UserSessIdP1, SpaceId, DatasetId, DirName, detached).


remove_hardlink_should_detach_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME(),
    ProtectionFlags = ?METADATA_PROTECTION,
    {ok, FileGuid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    HardLinkName = <<FileName/binary, <<"_hard_link">>/binary>>,
    LinkPath = filename:join(["/", oct_background:get_space_name(space1), HardLinkName]),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_link(P1Node, UserSessIdP1, LinkPath, FileGuid),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, LinkGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, LinkGuid, DatasetId, [], ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, FileGuid, undefined, [], ProtectionFlags),
    % delete hardlink, dataset should be detached
    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid)),
    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, LinkPath, ?REGULAR_FILE_TYPE, true, ProtectionFlags),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_details(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid))),
    ?assertMatch({error, ?ENOENT}, opt_datasets:get_file_eff_summary(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid))),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),
    ?assertTopDataset(P1Node, UserSessIdP1, SpaceId, DatasetId, HardLinkName, detached).


remove_file_symlink_should_detach_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME(),
    ProtectionFlags = ?METADATA_PROTECTION,
    {ok, FileGuid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    SymLinkName = <<FileName/binary, <<"_sym_link">>/binary>>,
    LinkPath = filename:join(["/", oct_background:get_space_name(space1), SymLinkName]),
    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(oct_background:get_space_id(space1)),
    LinkTarget = filename:join([SpaceIdPrefix, FileName]),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_symlink(P1Node, UserSessIdP1, LinkPath, LinkTarget),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, LinkGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, LinkGuid, DatasetId, [], ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, FileGuid, undefined, [], ?no_flags_mask),
    % delete symlink, dataset should be detached
    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid)),
    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, LinkPath, ?SYMLINK_TYPE, true, ProtectionFlags),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_details(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid))),
    ?assertMatch({error, ?ENOENT}, opt_datasets:get_file_eff_summary(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid))),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),
    ?assertTopDataset(P1Node, UserSessIdP1, SpaceId, DatasetId, SymLinkName, detached).


remove_dir_symlink_should_detach_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME(),
    ProtectionFlags = ?METADATA_PROTECTION,
    {ok, DirGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    SymLinkName = <<DirName/binary, <<"_sym_link">>/binary>>,
    LinkPath = filename:join(["/", oct_background:get_space_name(space1), SymLinkName]),
    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(oct_background:get_space_id(space1)),
    LinkTarget = filename:join([SpaceIdPrefix, DirName]),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_symlink(P1Node, UserSessIdP1, LinkPath, LinkTarget),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, LinkGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, LinkGuid, DatasetId, [], ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, DirGuid, undefined, [], ?no_flags_mask),
    % delete symlink, dataset should be detached
    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid)),
    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, LinkPath, ?SYMLINK_TYPE, true, ProtectionFlags),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_details(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid))),
    ?assertMatch({error, ?ENOENT}, opt_datasets:get_file_eff_summary(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid))),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),
    ?assertTopDataset(P1Node, UserSessIdP1, SpaceId, DatasetId, SymLinkName, detached).


remove_file_pointed_by_hardlink(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME(),
    ProtectionFlags = ?METADATA_PROTECTION,
    {ok, FileGuid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    HardLinkName = <<FileName/binary, <<"_hard_link">>/binary>>,
    LinkPath = filename:join(["/", oct_background:get_space_name(space1), HardLinkName]),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_link(P1Node, UserSessIdP1, LinkPath, FileGuid),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, LinkGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, LinkGuid, DatasetId, [], ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, FileGuid, undefined, [], ProtectionFlags),
    % delete file, dataset should stay attached
    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, ?FILE_REF(FileGuid)),
    ?assertMatch({error, ?ENOENT}, opt_datasets:get_file_eff_summary(P1Node, UserSessIdP1, ?FILE_REF(FileGuid))),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, LinkGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, LinkGuid, DatasetId, [], ProtectionFlags).


remove_file_pointed_by_symlink(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME(),
    ProtectionFlags = ?METADATA_PROTECTION,
    {ok, FileGuid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    SymLinkName = <<FileName/binary, <<"_sym_link">>/binary>>,
    LinkPath = filename:join(["/", oct_background:get_space_name(space1), SymLinkName]),
    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(oct_background:get_space_id(space1)),
    LinkTarget = filename:join([SpaceIdPrefix, FileName]),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_symlink(P1Node, UserSessIdP1, LinkPath, LinkTarget),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, LinkGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, LinkGuid, DatasetId, [], ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, FileGuid, undefined, [], ?no_flags_mask),
    % delete file, dataset should stay attached
    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, ?FILE_REF(FileGuid)),
    ?assertMatch({error, ?ENOENT}, opt_datasets:get_file_eff_summary(P1Node, UserSessIdP1, ?FILE_REF(FileGuid))),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, LinkGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, LinkGuid, DatasetId, [], ProtectionFlags).


remove_dir_pointed_by_symlink(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME(),
    ProtectionFlags = ?METADATA_PROTECTION,
    {ok, DirGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    SymLinkName = <<DirName/binary, <<"_sym_link">>/binary>>,
    LinkPath = filename:join(["/", oct_background:get_space_name(space1), SymLinkName]),
    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(oct_background:get_space_id(space1)),
    LinkTarget = filename:join([SpaceIdPrefix, DirName]),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_symlink(P1Node, UserSessIdP1, LinkPath, LinkTarget),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, LinkGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, LinkGuid, DatasetId, [], ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, DirGuid, undefined, [], ?no_flags_mask),
    % delete directory, dataset should stay attached
    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, ?FILE_REF(DirGuid)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, LinkGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, LinkGuid, DatasetId, [], ProtectionFlags).


reattach_if_root_file_is_deleted_should_fail(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME(),
    ProtectionFlags = ?METADATA_PROTECTION,
    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, Path} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags)),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags),

    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, ?FILE_REF(Guid)),

    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, Path, ?DIRECTORY_TYPE, true, ProtectionFlags),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_details(P1Node, UserSessIdP1, ?FILE_REF(Guid))),
    ?assertMatch({error, ?ENOENT}, opt_datasets:get_file_eff_summary(P1Node, UserSessIdP1, ?FILE_REF(Guid))),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),

    % reattaching dataset which root file has been remove should fail
    ?assertMatch({error, ?ENOENT}, reattach(P1Node, UserSessIdP1, DatasetId)).


reattach_if_hardlink_is_deleted_should_fail(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME(),
    ProtectionFlags = ?METADATA_PROTECTION,
    {ok, FileGuid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    HardLinkName = <<FileName/binary, <<"_hard_link">>/binary>>,
    LinkPath = filename:join(["/", oct_background:get_space_name(space1), HardLinkName]),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_link(P1Node, UserSessIdP1, LinkPath, FileGuid),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, LinkGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, LinkGuid, DatasetId, [], ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, FileGuid, undefined, [], ProtectionFlags),

    % delete hardlink
    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid)),

    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, LinkPath, ?REGULAR_FILE_TYPE, true, ProtectionFlags),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_details(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid))),
    ?assertMatch({error, ?ENOENT}, opt_datasets:get_file_eff_summary(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid))),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),

    % reattaching dataset which root file hardlink has been remove should fail
    ?assertMatch({error, ?ENOENT}, reattach(P1Node, UserSessIdP1, DatasetId)).


reattach_if_file_symlink_is_deleted_should_fail(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME(),
    ProtectionFlags = ?METADATA_PROTECTION,
    {ok, FileGuid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    SymLinkName = <<FileName/binary, <<"_sym_link">>/binary>>,
    LinkPath = filename:join(["/", oct_background:get_space_name(space1), SymLinkName]),
    LinkTarget = filename:join(["/", oct_background:get_space_name(space1), FileName]),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_symlink(P1Node, UserSessIdP1, LinkPath, LinkTarget),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, LinkGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, LinkGuid, DatasetId, [], ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, FileGuid, undefined, [], ?no_flags_mask),

    % delete hardlink
    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid)),

    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, LinkPath, ?SYMLINK_TYPE, true, ProtectionFlags),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_details(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid))),
    ?assertMatch({error, ?ENOENT}, opt_datasets:get_file_eff_summary(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid))),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),

    % reattaching dataset which root file symlink has been remove should fail
    ?assertMatch({error, ?ENOENT}, reattach(P1Node, UserSessIdP1, DatasetId)).


reattach_if_dir_symlink_is_deleted_should_fail(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME(),
    ProtectionFlags = ?METADATA_PROTECTION,
    {ok, DirGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    SymLinkName = <<DirName/binary, <<"_sym_link">>/binary>>,
    LinkPath = filename:join(["/", oct_background:get_space_name(space1), SymLinkName]),
    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(oct_background:get_space_id(space1)),
    LinkTarget = filename:join([SpaceIdPrefix, DirName]),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_symlink(P1Node, UserSessIdP1, LinkPath, LinkTarget),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, LinkGuid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, LinkGuid, DatasetId, [], ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, DirGuid, undefined, [], ?no_flags_mask),

    % delete hardlink
    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid)),
    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, LinkGuid, undefined, LinkPath, ?SYMLINK_TYPE, true, ProtectionFlags),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_details(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid))),
    ?assertMatch({error, ?ENOENT}, opt_datasets:get_file_eff_summary(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid))),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),

    % reattaching dataset which root dir symlink has been remove should fail
    ?assertMatch({error, ?ENOENT}, reattach(P1Node, UserSessIdP1, DatasetId)).

remove_detached_dataset_if_root_file_has_already_been_deleted(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME(),
    ProtectionFlags = ?METADATA_PROTECTION,
    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, Path} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags)),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags),

    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, ?FILE_REF(Guid)),

    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, Path, ?DIRECTORY_TYPE, true, ProtectionFlags),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_details(P1Node, UserSessIdP1, ?FILE_REF(Guid))),
    ?assertMatch({error, ?ENOENT}, opt_datasets:get_file_eff_summary(P1Node, UserSessIdP1, ?FILE_REF(Guid))),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),

    % reattaching dataset which root file has been remove should fail
    ?assertMatch({error, ?ENOENT}, reattach(P1Node, UserSessIdP1, DatasetId)),

    % removing dataset should succeed
    ok = opt_datasets:remove(P1Node, UserSessIdP1, DatasetId),

    ?assertNoDataset(P1Node, UserSessIdP1, DatasetId),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, detached).


establish_dataset_on_not_existing_file_should_fail(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME(),
    {ok, Guid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, ?FILE_REF(Guid)),
    ?assertMatch({error, ?ENOENT}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?RAND_PROTECTION_FLAGS())).

establish_2nd_dataset_on_file_should_fail(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME(),
    ProtectionFlags = ?RAND_PROTECTION_FLAGS(),
    {ok, Guid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags),
    ?assertMatch({error, ?EEXIST}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags)).

establish_nested_datasets_structure(_Config) ->
    Depth = 10,
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceName = oct_background:get_space_name(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirNamePrefix = ?DIR_NAME(),
    ProtectionFlags = ?METADATA_PROTECTION,
    {ok, SpaceDatasetId} = opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(SpaceGuid), ProtectionFlags),

    GuidsAndDatasetsReversed = lists:foldl(fun(N, AccIn = [{ParentGuid, _, _} | _]) ->
        DirName = str_utils:join_binary([DirNamePrefix, integer_to_binary(N)]),
        {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ParentGuid, DirName, ?DEFAULT_DIR_PERMS),
        {ok, DatasetId} = opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags),
        [{Guid, DirName, DatasetId} | AccIn]
    end, [{SpaceGuid, SpaceName, SpaceDatasetId}], lists:seq(1, Depth)),

    GuidsAndDatasets = lists:reverse(GuidsAndDatasetsReversed),

    ?assertMatch({ok, {[{SpaceDatasetId, SpaceName, _}], true}},
        opt_datasets:list_top_datasets(P1Node, UserSessIdP1, SpaceId, attached, #{offset => 0, limit => 100})),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, detached),

    lists:foldl(fun({ChildGuid, _ChildName, ChildDatasetId}, {Guid, DatasetId, ExpParentDatasetIds}) ->
        {ExpParentDatasetId, ExpMembership} = case ExpParentDatasetIds =:= [] of
            true -> {undefined, ?DIRECT_MEMBERSHIP};
            false -> {hd(ExpParentDatasetIds), ?DIRECT_AND_ANCESTOR_MEMBERSHIP}
        end,
        ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, ExpParentDatasetId, ProtectionFlags),
        ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ExpMembership, ProtectionFlags),
        ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, ExpParentDatasetIds, ProtectionFlags),
        {ChildGuid, ChildDatasetId, [DatasetId | ExpParentDatasetIds]}
    end, {SpaceGuid, SpaceDatasetId, []}, tl(GuidsAndDatasets)).

establish_nested_datasets_structure_end_detach_all(_Config) ->
    Depth = 10,
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceName = oct_background:get_space_name(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirNamePrefix = ?DIR_NAME(),
    ProtectionFlags = ?RAND_PROTECTION_FLAGS(),
    {ok, SpaceDatasetId} = opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(SpaceGuid), ProtectionFlags),
    ok = detach(P1Node, UserSessIdP1, SpaceDatasetId),

    GuidsAndDatasetsReversed = lists:foldl(fun(N, AccIn = [{ParentGuid, _, _} | _]) ->
        DirName = str_utils:join_binary([DirNamePrefix, integer_to_binary(N)]),
        {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ParentGuid, DirName, ?DEFAULT_DIR_PERMS),
        {ok, DatasetId} = opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags),
        ok = detach(P1Node, UserSessIdP1, DatasetId),
        [{Guid, DirName, DatasetId} | AccIn]
    end, [{SpaceGuid, SpaceName, SpaceDatasetId}], lists:seq(1, Depth)),

    GuidsAndDatasets = lists:reverse(GuidsAndDatasetsReversed),

    ?assertMatch({ok, {[{SpaceDatasetId, SpaceName, _}], true}},
        opt_datasets:list_top_datasets(P1Node, UserSessIdP1, SpaceId, detached, #{offset => 0, limit => 100})),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, attached),

    lists:foldl(fun({ChildGuid, _ChildName, ChildDatasetId}, {Guid, DatasetId, ExpParentDatasetId}) ->
        {ok, Path} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
        ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, ExpParentDatasetId, Path, ?DIRECTORY_TYPE, false, ProtectionFlags),
        ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?NONE_MEMBERSHIP, ?no_flags_mask),
        ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ?no_flags_mask),
        {ChildGuid, ChildDatasetId, DatasetId}
    end, {SpaceGuid, SpaceDatasetId, undefined}, tl(GuidsAndDatasets)).

all_files_in_dataset_should_have_ancestor_dataset_membership(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    ProtectionFlags = ?METADATA_PROTECTION,
    {ok, SpaceDatasetId} = opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(SpaceGuid), ProtectionFlags),
    {DirGuids, FileGuids} = lfm_test_utils:create_files_tree(P1Node, UserSessIdP1, [{10, 10}, {10, 10}], SpaceGuid),

    ?assertAttachedDataset(P1Node, UserSessIdP1, SpaceDatasetId, SpaceGuid, undefined, ProtectionFlags),
    ?assertMatch({ok, {[], true}},
        opt_datasets:list_children_datasets(P1Node, UserSessIdP1, SpaceDatasetId, #{offset => 0, limit => 100})),

    lists:foreach(fun(Guid) ->
        ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?ANCESTOR_MEMBERSHIP, ProtectionFlags),
        ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, undefined, [SpaceDatasetId], ProtectionFlags)
    end, DirGuids ++ FileGuids).


rename_file_should_rename_attached_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME(),
    NewDirName = ?DIR_NAME(),
    ProtectionFlags = ?METADATA_PROTECTION,

    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags),

    {ok, _} = lfm_proxy:mv(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?FILE_REF(SpaceGuid), NewDirName),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags).

rename_file_should_not_rename_detached_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME(),
    NewDirName = ?DIR_NAME(),
    ProtectionFlags = ?RAND_PROTECTION_FLAGS(),

    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags)),
    {ok, SourcePatch} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags),

    ok = detach(P1Node, UserSessIdP1, DatasetId),
    {ok, _} = lfm_proxy:mv(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?FILE_REF(SpaceGuid), NewDirName),

    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, SourcePatch, ?DIRECTORY_TYPE, false, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?NONE_MEMBERSHIP, ?no_flags_mask),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ?no_flags_mask).


move_file_should_move_attached_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME(),
    TargetParentName = ?DIR_NAME(),
    NewDirName = ?DIR_NAME(),
    ProtectionFlags = ?METADATA_PROTECTION,

    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags)),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags),

    {ok, TargetParentGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, TargetParentName, ?DEFAULT_DIR_PERMS),
    {ok, _} = lfm_proxy:mv(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?FILE_REF(TargetParentGuid), NewDirName),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags).


move_file_should_not_move_detached_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME(),
    TargetParentName = ?DIR_NAME(),
    NewDirName = ?DIR_NAME(),
    ProtectionFlags = ?RAND_PROTECTION_FLAGS(),

    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags)),
    {ok, SourcePatch} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags),

    {ok, TargetParentGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, TargetParentName, ?DEFAULT_DIR_PERMS),
    ok = detach(P1Node, UserSessIdP1, DatasetId),
    {ok, _} = lfm_proxy:mv(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?FILE_REF(TargetParentGuid), NewDirName),

    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, SourcePatch, ?DIRECTORY_TYPE, false, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?NONE_MEMBERSHIP, ?no_flags_mask),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ?no_flags_mask).


reattach_to_moved_root_file(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME(),
    TargetParentName = ?DIR_NAME(),
    NewDirName = ?DIR_NAME(),
    ProtectionFlags = ?RAND_PROTECTION_FLAGS(),

    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags)),
    {ok, SourcePatch} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, Guid),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags),

    {ok, TargetParentGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, TargetParentName, ?DEFAULT_DIR_PERMS),
    ok = detach(P1Node, UserSessIdP1, DatasetId),
    {ok, _} = lfm_proxy:mv(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?FILE_REF(TargetParentGuid), NewDirName),

    ?assertDetachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, SourcePatch, ?DIRECTORY_TYPE, false, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?NONE_MEMBERSHIP, ?no_flags_mask),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ?no_flags_mask),

    ok = reattach(P1Node, UserSessIdP1, DatasetId),
    ?assertAttachedDataset(P1Node, UserSessIdP1, DatasetId, Guid, undefined, ProtectionFlags),
    ?assertDatasetMembership(P1Node, UserSessIdP1, Guid, ?DIRECT_MEMBERSHIP, ProtectionFlags),
    ?assertFileEffDatasetSummary(P1Node, UserSessIdP1, Guid, DatasetId, [], ProtectionFlags),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, detached).


establish_datasets_with_the_same_names(_Config) ->
    Count = 10,
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME(),
    ProtectionFlags = ?RAND_PROTECTION_FLAGS(),

    GuidsAndDatasetIds = lists:map(fun(I) ->
        % create I nested directories
        Name = <<"dir_", (integer_to_binary(I))/binary>>,
        ParentGuid = lists:foldl(fun(_Depth, AncestorGuid) ->
            {ok, NextAncestorGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, AncestorGuid, Name, ?DEFAULT_DIR_PERMS),
            NextAncestorGuid
        end, SpaceGuid, lists:seq(1, I - 1)),

        % create directory with name DirName on which dataset will be established
        {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ParentGuid, DirName, ?DEFAULT_DIR_PERMS),
        {ok, DatasetId} = opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Guid), ProtectionFlags),
        {Guid, DatasetId}
    end, lists:seq(1, Count)),

    {_Guids, DatasetIds} = lists:unzip(GuidsAndDatasetIds),
    ExpectedDatasets = [{DatasetId, DirName} || DatasetId <- DatasetIds],

    {ok, {Datasets, true}} =
        opt_datasets:list_top_datasets(P1Node, UserSessIdP1, SpaceId, attached, #{offset => 0, limit => 100}),
    DatasetsWithoutIndices = [{DN, DI} || {DN, DI, _} <- Datasets],
    ?assertEqual(lists:sort(ExpectedDatasets), lists:sort(DatasetsWithoutIndices)),
    ?assertNoTopDatasets(P1Node, UserSessIdP1, SpaceId, detached).


establish_nested_datasets_filetree_structure_with_hardlinks(_Config) ->
    %%===================================================================
    %%   This testcase creates the following file-tree:
    %%
    %%            SpaceDir
    %%                |
    %%               Dir1
    %%               /   \
    %%         Link1/D   Dir2/D
    %%                   /   \
    %%                Link2   Dir3/D
    %%                        /   \
    %%                   Link3/D  Dir4/D
    %%                            / | \
    %%                           /  |  \
    %%                          /   |   \
    %%                     Link4/D DirA5 DirB5
    %%                              |       |
    %%                            Link5    File/D
    %%
    %%   Note, that all links are hardlinks, and they
    %%   point to File placed in DirB5 directory.
    %%   Notation _/D indicates established dataset.
    %%===================================================================

    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    SpaceName = oct_background:get_space_name(space1),

    % create 5-level dir tree
    DirLvl1Name = ?DIR_NAME(),
    DirLvl2Name = ?DIR_NAME(),
    DirLvl3Name = ?DIR_NAME(),
    DirLvl4Name = ?DIR_NAME(),
    DirALvl5Name = ?DIR_NAME(),
    DirBLvl5Name = ?DIR_NAME(),
    {ok, DirLvl1Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirLvl1Name, ?DEFAULT_DIR_PERMS),
    {ok, DirLvl2Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, DirLvl1Guid, DirLvl2Name, ?DEFAULT_DIR_PERMS),
    {ok, DirLvl3Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, DirLvl2Guid, DirLvl3Name, ?DEFAULT_DIR_PERMS),
    {ok, DirLvl4Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, DirLvl3Guid, DirLvl4Name, ?DEFAULT_DIR_PERMS),
    {ok, _DirALvl5Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, DirLvl4Guid, DirALvl5Name, ?DEFAULT_DIR_PERMS),
    {ok, DirBLvl5Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, DirLvl4Guid, DirBLvl5Name, ?DEFAULT_DIR_PERMS),

    % create file in the last level directory
    FileName = ?FILE_NAME(),
    {ok, FileGuid} = lfm_proxy:create(P1Node, UserSessIdP1, DirBLvl5Guid, FileName, ?DEFAULT_DIR_PERMS),

    % create 5 hardlinks to file, place them in each dir level
    Link1Path = filename:join(["/", SpaceName, DirLvl1Name, ?FILE_NAME()]),
    {ok, #file_attr{guid = Link1Guid}} = lfm_proxy:make_link(P1Node, UserSessIdP1, Link1Path, FileGuid),
    Link2Path = filename:join(["/", SpaceName, DirLvl1Name, DirLvl2Name, ?FILE_NAME()]),
    {ok, #file_attr{guid = _Link2Guid}} = lfm_proxy:make_link(P1Node, UserSessIdP1, Link2Path, FileGuid),
    Link3Path = filename:join(["/", SpaceName, DirLvl1Name, DirLvl2Name, DirLvl3Name, ?FILE_NAME()]),
    {ok, #file_attr{guid = Link3Guid}} = lfm_proxy:make_link(P1Node, UserSessIdP1, Link3Path, FileGuid),
    Link4Path = filename:join(["/", SpaceName, DirLvl1Name, DirLvl2Name, DirLvl3Name, DirLvl4Name, ?FILE_NAME()]),
    {ok, #file_attr{guid = Link4Guid}} = lfm_proxy:make_link(P1Node, UserSessIdP1, Link4Path, FileGuid),
    Link5Path = filename:join(["/", SpaceName, DirLvl1Name, DirLvl2Name, DirLvl3Name, DirLvl4Name, DirALvl5Name, ?FILE_NAME()]),
    {ok, #file_attr{guid = _Link5Guid}} = lfm_proxy:make_link(P1Node, UserSessIdP1, Link5Path, FileGuid),

    % establish dataset on file
    {ok, FileDatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(FileGuid), ?no_flags_mask)),

    % establish datasets on choosen dirs
    {ok, DirLvl2DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(DirLvl2Guid), ?no_flags_mask)),
    {ok, DirLvl3DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(DirLvl3Guid), ?METADATA_PROTECTION)),
    {ok, DirLvl4DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(DirLvl4Guid), ?DATA_PROTECTION)),

    % establish datasets on choosen hardlinks
    {ok, Link1DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Link1Guid), ?no_flags_mask)),
    {ok, Link3DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Link3Guid), ?METADATA_PROTECTION)),
    {ok, Link4DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(P1Node, UserSessIdP1, ?FILE_REF(Link4Guid), ?DATA_PROTECTION)),

    %%===================================================================
    %%   Protection flags should be set as shown below:
    %%
    %%    Dataset| Flags |     EffFlags
    %%    -------+-------+--------------------
    %%    Dir2/D |  ---  |        ---
    %%    Dir3/D |   M   |         M
    %%    Dir4/D |   D   |        M+D
    %%    Link1/D|  ---  |        ---
    %%    Link3/D|   M   |         M
    %%    Link4/D|   D   |        M+D
    %%    File/D |  ---  |        M+D
    %%
    %%    File   | EffFlagsSinglePath | FinalEffFlags
    %%    -------+--------------------+--------------
    %%    Dir2/D |        ---         |      ---
    %%    Dir3/D |         M          |       M
    %%    Dir4/D |        M+D         |      M+D
    %%    DirA5  |        M+D         |      M+D
    %%    DirB5  |        M+D         |      M+D
    %%    Link1/D|        ---         |      M+D
    %%    Link2  |        ---         |      M+D
    %%    Link3/D|         M          |      M+D
    %%    Link4/D|        M+D         |      M+D
    %%    Link5  |        M+D         |      M+D
    %%    File/D |        M+D         |      M+D
    %%===================================================================

    % verify file dataset ancestors and protection flags
    ?assertAttachedDataset(P1Node, UserSessIdP1, FileDatasetId, FileGuid, DirLvl4DatasetId, ?no_flags_mask, ?ALL_PROTECTION),
    ?assertFileEffDatasetSummaryAndMembership(P1Node, UserSessIdP1, FileGuid, FileDatasetId, [DirLvl4DatasetId, DirLvl3DatasetId, DirLvl2DatasetId], ?DIRECT_AND_ANCESTOR_MEMBERSHIP, ?ALL_PROTECTION),

    % verify dirs dataset ancestors and protection flags
    ?assertAttachedDataset(P1Node, UserSessIdP1, DirLvl4DatasetId, DirLvl4Guid, DirLvl3DatasetId, ?DATA_PROTECTION, ?ALL_PROTECTION),
    ?assertFileEffDatasetSummaryAndMembership(P1Node, UserSessIdP1, DirLvl4Guid, DirLvl4DatasetId, [DirLvl3DatasetId, DirLvl2DatasetId], ?DIRECT_AND_ANCESTOR_MEMBERSHIP, ?ALL_PROTECTION),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DirLvl3DatasetId, DirLvl3Guid, DirLvl2DatasetId, ?METADATA_PROTECTION, ?METADATA_PROTECTION),
    ?assertFileEffDatasetSummaryAndMembership(P1Node, UserSessIdP1, DirLvl3Guid, DirLvl3DatasetId, [DirLvl2DatasetId], ?DIRECT_AND_ANCESTOR_MEMBERSHIP, ?METADATA_PROTECTION),

    ?assertAttachedDataset(P1Node, UserSessIdP1, DirLvl2DatasetId, DirLvl2Guid, undefined, ?no_flags_mask, ?no_flags_mask),
    ?assertFileEffDatasetSummaryAndMembership(P1Node, UserSessIdP1, DirLvl2Guid, DirLvl2DatasetId, [], ?DIRECT_MEMBERSHIP, ?no_flags_mask),

    % verify links dataset ancestors and protection flags
    ?assertAttachedDataset(P1Node, UserSessIdP1, Link4DatasetId, Link4Guid, DirLvl4DatasetId, ?DATA_PROTECTION, ?ALL_PROTECTION),
    ?assertFileEffDatasetSummaryAndMembership(P1Node, UserSessIdP1, Link4Guid, Link4DatasetId, [DirLvl4DatasetId, DirLvl3DatasetId, DirLvl2DatasetId], ?DIRECT_AND_ANCESTOR_MEMBERSHIP, ?ALL_PROTECTION),

    ?assertAttachedDataset(P1Node, UserSessIdP1, Link3DatasetId, Link3Guid, DirLvl3DatasetId, ?METADATA_PROTECTION, ?METADATA_PROTECTION),
    ?assertFileEffDatasetSummaryAndMembership(P1Node, UserSessIdP1, Link3Guid, Link3DatasetId, [DirLvl3DatasetId, DirLvl2DatasetId], ?DIRECT_AND_ANCESTOR_MEMBERSHIP, ?ALL_PROTECTION),

    ?assertAttachedDataset(P1Node, UserSessIdP1, Link1DatasetId, Link1Guid, undefined, ?no_flags_mask, ?no_flags_mask),
    ?assertFileEffDatasetSummaryAndMembership(P1Node, UserSessIdP1, Link1Guid, Link1DatasetId, [], ?DIRECT_MEMBERSHIP, ?ALL_PROTECTION).


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{onenv_scenario = "2op"}).

end_per_suite(_Config) ->
    oct_background:end_per_suite().

init_per_testcase(_Case, Config) ->
    % update background config to update sessions
    Config2 = oct_background:update_background_config(Config),
    lfm_proxy:init(Config2).

end_per_testcase(_Case, Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    PNodes = oct_background:get_all_providers_nodes(),
    SpaceId = oct_background:get_space_id(space1),
    onenv_dataset_test_utils:cleanup_all_datasets(krakow, space1),
    lfm_test_utils:clean_space(P1Node, PNodes, SpaceId, ?ATTEMPTS),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

detach(Node, SessionId, DatasetId) ->
    opt_datasets:update(Node, SessionId, DatasetId, ?DETACHED_DATASET, ?no_flags_mask, ?no_flags_mask).

reattach(Node, SessionId, DatasetId) ->
    opt_datasets:update(Node, SessionId, DatasetId, ?ATTACHED_DATASET, ?no_flags_mask, ?no_flags_mask).

assert_attached_dataset(Node, SessionId, DatasetId, ExpectedRootFileGuid, ExpectedParentDatasetId,
    ExpectedProtectionFlags, ExpectedEffProtectionFlags
) ->
    {ok, #file_attr{type = ExpectedRootFileType}} = lfm_proxy:stat(Node, SessionId, ?FILE_REF(ExpectedRootFileGuid)),
    {ok, ExpectedRootFilePath} = lfm_proxy:get_file_path(Node, SessionId, ExpectedRootFileGuid),
    assert_dataset(Node, SessionId, DatasetId, ExpectedRootFileGuid, ExpectedParentDatasetId, ExpectedRootFilePath,
        ExpectedRootFileType, false, ?ATTACHED_DATASET, ExpectedProtectionFlags, ExpectedEffProtectionFlags).


assert_detached_dataset(Node, SessionId, DatasetId, ExpectedRootFileGuid, ExpectedParentDatasetId,
    ExpectedRootFilePath, ExpectedRootFileType, ExpectedRootFileDeleted, ExpectedProtectionFlags
) ->
    assert_dataset(Node, SessionId, DatasetId, ExpectedRootFileGuid, ExpectedParentDatasetId, ExpectedRootFilePath,
        ExpectedRootFileType, ExpectedRootFileDeleted, ?DETACHED_DATASET, ExpectedProtectionFlags, ?no_flags_mask).


assert_dataset(Node, SessionId, DatasetId, ExpectedRootFileGuid, ExpectedParentDatasetId, ExpectedRootFilePath,
    ExpectedRootFileType, ExpectedRootFileDeleted, ExpectedState, ExpectedProtectionFlags, ExpectedEffProtectionFlags
) ->
    % check dataset info
    ?assertMatch({ok, #dataset_info{
        id = DatasetId,
        state = ExpectedState,
        root_file_guid = ExpectedRootFileGuid,
        root_file_path = ExpectedRootFilePath,
        root_file_type = ExpectedRootFileType,
        root_file_deleted = ExpectedRootFileDeleted,
        parent = ExpectedParentDatasetId,
        protection_flags = ExpectedProtectionFlags,
        eff_protection_flags = ExpectedEffProtectionFlags
    }}, opt_datasets:get_info(Node, SessionId, DatasetId), ?ATTEMPTS),

    % check dataset structure entry
    Name = filename:basename(ExpectedRootFilePath),
    case ExpectedParentDatasetId =/= undefined of
        true ->
            % check whether dataset is visible on parent dataset's list
            {ok, {DatasetsList, true}} = ?assertMatch({ok, _, true},
                opt_datasets:list_children_datasets(Node, SessionId, ExpectedParentDatasetId, #{offset => 0, limit => 100}), ?ATTEMPTS);

        false ->
            % check whether dataset is visible on space top dataset list
            SpaceId = file_id:guid_to_space_id(ExpectedRootFileGuid),
            {ok, {DatasetsList, true}} = ?assertMatch({ok, _, true},
                opt_datasets:list_top_datasets(Node, SessionId, SpaceId, ExpectedState, #{offset => 0, limit => 100}), ?ATTEMPTS)
    end,
    ?assert(lists:any(fun(Dataset) ->
        {ExpDatasetId, ExpName, _} = Dataset,
        case {ExpDatasetId, ExpName} of
            {DatasetId, Name} -> true;
            _ -> false
        end
    end, DatasetsList)).
