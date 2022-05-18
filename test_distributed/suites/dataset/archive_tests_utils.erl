%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for archive tests.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_tests_utils).
-author("Jakub Kudzia").


-include("onenv_test_utils.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/dataset/archive.hrl").
-include("modules/dataset/archivisation_tree.hrl").
-include("modules/dataset/bagit.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/test/assertions.hrl").

-export([create_archive_dir/5]).
-export([
    assert_archive_dir_structure_is_correct/7, assert_archive_state/3, 
    assert_archive_is_preserved/8, assert_incremental_archive_links/3,
    assert_copied/6
]).
-export([mock_archive_verification/0, wait_for_archive_verification_traverse/2, start_verification_traverse/2]).


-define(SPACE, space_krk_par_p).
-define(USER1, user1).
-define(LISTED_CHILDREN_LIMIT, 1000).


%===================================================================
% API
%===================================================================

create_archive_dir(Node, ArchiveId, DatasetId, SpaceId, UserId) ->
    rpc:call(Node, archivisation_tree, create_archive_dir, [ArchiveId, DatasetId, SpaceId, UserId]).


assert_archive_dir_structure_is_correct(Node, SessionId, SpaceId, DatasetId, ArchiveId, UserId, Attempts) ->
    assert_archives_root_dir_exists(Node, SessionId, SpaceId, Attempts),
    assert_dataset_archives_dir_exists(Node, SessionId, SpaceId, DatasetId, Attempts),
    assert_archive_dir_exists(Node, SessionId, SpaceId, DatasetId, ArchiveId, UserId, Attempts).


assert_archive_state(ArchiveId, ExpectedState, Attempts) ->
    lists:foreach(fun(Provider) ->
        Node = oct_background:get_random_provider_node(Provider),
        SessionId = oct_background:get_user_session_id(?USER1, Provider),
        ?assertMatch({ok, #archive_info{state = ExpectedState}}, opt_archives:get_info(Node, SessionId, ArchiveId), Attempts)
    end, oct_background:get_space_supporting_providers(?SPACE)).


assert_archive_is_preserved(_Node, _SessionId, undefined, _DatasetId, _DatasetRootFileGuid, _FileCount, _ExpSize, _Attempts) ->
    ok;
assert_archive_is_preserved(Node, SessionId, ArchiveId, DatasetId, DatasetRootFileGuid, FileCount, ExpSize, Attempts) ->
    {ok, #archive_info{
        related_aip_id = RelatedAip,
        related_dip_id = RelatedDip
    }} = ?assertMatch({ok, #archive_info{
        state = ?ARCHIVE_PRESERVED,
        stats = #archive_stats{
            files_archived = FileCount,
            files_failed = 0,
            bytes_archived = ExpSize
        }
    }}, get_archive_info_without_config(Node, SessionId, ArchiveId), Attempts),
    
    {ok, #archive_info{config = #archive_config{layout = ArchiveLayout, follow_symlinks = FollowSymlinks}}} =
        opt_archives:get_info(Node, SessionId, ArchiveId),
    
    GetDatasetArchives = fun() ->
        case opt_archives:list(Node, SessionId, DatasetId, #{offset => 0, limit => 10000}) of
            {ok, {ArchiveIdsAndIndices, _}} ->
                [AID || {_, AID} <- ArchiveIdsAndIndices];
            _ ->
                error
        end
    end,
    case RelatedAip of
        undefined ->
            ?assertEqual(true, lists:member(ArchiveId, GetDatasetArchives()), Attempts);
        _ ->
            % DIP archives are not on the dataset list but check that they have enforced plain layout
            ?assertEqual(?ARCHIVE_PLAIN_LAYOUT, ArchiveLayout)
    end,
    
    assert_structure(Node, SessionId, ArchiveId, DatasetRootFileGuid, ArchiveLayout, FollowSymlinks, Attempts),
    assert_layout_custom_features(Node, SessionId, ArchiveId, ArchiveLayout),
    assert_archive_is_preserved(Node, SessionId, RelatedDip, DatasetId, DatasetRootFileGuid, FileCount, ExpSize, Attempts).


assert_incremental_archive_links(BaseArchiveId, ArchiveId, ModifiedFiles) ->
    lists:foreach(fun(Provider) ->
        Node = oct_background:get_random_provider_node(Provider),
        SessionId = oct_background:get_user_session_id(?USER1, Provider),
        {ok, ArchiveDataDirGuid} = rpc:call(Node, archive, get_data_dir_guid, [ArchiveId]),
        assert_incremental_archive_links(Node, SessionId, BaseArchiveId, ArchiveDataDirGuid, ModifiedFiles)
    end, oct_background:get_space_supporting_providers(?SPACE)).


mock_archive_verification() ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_new(Nodes, archive_verification_traverse, [passthrough]),
    Pid = self(),
    test_utils:mock_expect(Nodes, archive_verification_traverse, block_archive_modification,
        fun(ArchiveDoc) ->
            {ok, ArchiveId} = archive:get_id(ArchiveDoc),
            Pid ! {archive_verification_mock, ArchiveId, self()},
            receive {continue, ArchiveId} ->
                meck:passthrough([ArchiveDoc])
            end
        end).


wait_for_archive_verification_traverse(ArchiveId, Attempts) ->
    receive {archive_verification_mock, ArchiveId, Pid} ->
        {ok, Pid}
    after timer:seconds(Attempts) ->
        {error, archive_creation_not_finished}
    end.


start_verification_traverse(Pid, ArchiveId) ->
    Pid ! {continue, ArchiveId}.


assert_copied(Node, SessionId, SourceGuid, TargetGuid, FollowSymlinks, Attempts) ->
    assert_attrs_copied(Node, SessionId, SourceGuid, TargetGuid, Attempts),
    assert_metadata_copied(Node, SessionId, SourceGuid, TargetGuid),
    {ok, SourceAttr} = lfm_proxy:stat(Node, SessionId, ?FILE_REF(SourceGuid)),
    case SourceAttr#file_attr.type of
        ?DIRECTORY_TYPE ->
            assert_children_copied(Node, SessionId, SourceGuid, TargetGuid, FollowSymlinks, Attempts),
            assert_json_metadata_copied(Node, SessionId, SourceGuid, TargetGuid, Attempts);
        ?REGULAR_FILE_TYPE ->
            assert_content_copied(Node, SessionId, SourceGuid, TargetGuid, Attempts),
            assert_json_metadata_copied(Node, SessionId, SourceGuid, TargetGuid, Attempts);
        ?SYMLINK_TYPE ->
            ShouldFollowSymlink = case FollowSymlinks of
                true -> true;
                false -> false;
                nested_archive_only ->
                    {ok, G} = lfm_proxy:resolve_symlink(Node, SessionId, ?FILE_REF(SourceAttr#file_attr.guid)),
                    {ok, Path} = lfm_proxy:get_file_path(Node, SessionId, G),
                    case archivisation_tree:extract_archive_id(Path) of
                        {ok, ArchiveId} ->
                            {ok, ParentGuid} = lfm_proxy:get_parent(Node, SessionId, #file_ref{guid = G}),
                            case opw_test_rpc:call(Node, archive, get_data_dir_guid, [ArchiveId]) of
                                {ok, ParentGuid} -> true;
                                _ -> false
                            end;
                        _ ->
                            false
                    end
            end,
            case ShouldFollowSymlink of
                true ->
                    {ok, LinkTargetGuid} = lfm_proxy:resolve_symlink(Node, SessionId, ?FILE_REF(SourceAttr#file_attr.guid)),
                    {ok, LinkTargetAttr} = lfm_proxy:stat(Node, SessionId, ?FILE_REF(LinkTargetGuid)),
                    case LinkTargetAttr#file_attr.type of
                        ?REGULAR_FILE_TYPE ->
                            assert_content_copied(Node, SessionId, LinkTargetGuid, TargetGuid, Attempts),
                            assert_json_metadata_copied(Node, SessionId, LinkTargetGuid, TargetGuid, Attempts);
                        ?DIRECTORY_TYPE ->
                            assert_children_copied(Node, SessionId, LinkTargetGuid, TargetGuid, FollowSymlinks, Attempts),
                            assert_json_metadata_copied(Node, SessionId, LinkTargetGuid, TargetGuid, Attempts)
                    end;
                false ->
                    assert_symlink_values_copied(Node, SessionId, SourceGuid, TargetGuid, Attempts)
            end
    end.


%===================================================================
% Internal functions
%===================================================================

assert_archives_root_dir_exists(Node, SessionId, SpaceId, Attempts) ->
    ArchivesRootUuid = ?ARCHIVES_ROOT_DIR_UUID(SpaceId),
    ArchivesRootGuid = file_id:pack_guid(ArchivesRootUuid, SpaceId),
    ArchivesRootDirName = ?ARCHIVES_ROOT_DIR_NAME,
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    ?assertMatch({ok, #file_attr{
        guid = ArchivesRootGuid,
        name = ArchivesRootDirName,
        mode = ?ARCHIVES_ROOT_DIR_PERMS,
        owner_id = ?SPACE_OWNER_ID(SpaceId),
        parent_guid = SpaceGuid
    }}, lfm_proxy:stat(Node, SessionId, ?FILE_REF(ArchivesRootGuid)), Attempts).


assert_dataset_archives_dir_exists(Node, SessionId, SpaceId, DatasetId, Attempts) ->
    ArchivesRootUuid = ?ARCHIVES_ROOT_DIR_UUID(SpaceId),
    ArchivesRootGuid = file_id:pack_guid(ArchivesRootUuid, SpaceId),
    DatasetArchivesDirUuid = ?DATASET_ARCHIVES_DIR_UUID(DatasetId),
    DatasetArchivesDirGuid = file_id:pack_guid(DatasetArchivesDirUuid, SpaceId),

    ?assertMatch({ok, #file_attr{
        guid = DatasetArchivesDirGuid,
        name = DatasetArchivesDirUuid,
        mode = ?DEFAULT_DIR_PERMS,
        owner_id = ?SPACE_OWNER_ID(SpaceId),
        parent_guid = ArchivesRootGuid
    }}, lfm_proxy:stat(Node, SessionId, ?FILE_REF(DatasetArchivesDirGuid)), Attempts).


assert_archive_dir_exists(Node, SessionId, SpaceId, DatasetId, ArchiveId, UserId, Attempts) ->
    ArchiveDirUuid = ?ARCHIVE_DIR_UUID(ArchiveId),
    ArchiveDirGuid = file_id:pack_guid(ArchiveDirUuid, SpaceId),
    DatasetArchivesDirUuid = ?DATASET_ARCHIVES_DIR_UUID(DatasetId),
    DatasetArchivesDirGuid = file_id:pack_guid(DatasetArchivesDirUuid, SpaceId),

    ?assertMatch({ok, #file_attr{
        guid = ArchiveDirGuid,
        name = ArchiveDirUuid,
        mode = ?DEFAULT_DIR_PERMS,
        owner_id = UserId,
        parent_guid = DatasetArchivesDirGuid
    }}, lfm_proxy:stat(Node, SessionId, ?FILE_REF(ArchiveDirGuid)), Attempts).


assert_structure(Node, SessionId, ArchiveId, DatasetRootFileGuid, ?ARCHIVE_PLAIN_LAYOUT, FollowSymlinks, Attempts) ->
    ArchiveRootDirUuid = ?ARCHIVE_DIR_UUID(ArchiveId),
    ArchiveRootDirGuid = file_id:pack_guid(ArchiveRootDirUuid, oct_background:get_space_id(?SPACE)),
    {ok, [{TargetGuid, _} | _]} = 
        ?assertMatch({ok, [_ | _]}, lfm_proxy:get_children(Node, SessionId, ?FILE_REF(ArchiveRootDirGuid), 0, ?LISTED_CHILDREN_LIMIT), Attempts),
    assert_copied(Node, SessionId, DatasetRootFileGuid, TargetGuid, FollowSymlinks, Attempts);
assert_structure(Node, SessionId, ArchiveId, DatasetRootFileGuid, ?ARCHIVE_BAGIT_LAYOUT, FollowSymlinks, Attempts) ->
    ArchiveRootDirUuid = ?ARCHIVE_DIR_UUID(ArchiveId),
    ArchiveRootDirGuid = file_id:pack_guid(ArchiveRootDirUuid, oct_background:get_space_id(?SPACE)),
    {ok, ArchiveRootDirPath} = lfm_proxy:get_file_path(Node, SessionId, ArchiveRootDirGuid),
    ArchiveDataDirPath = filename:join([ArchiveRootDirPath, <<"data">>]),
    {ok, #file_attr{guid = ArchiveDataDirGuid}} = lfm_proxy:stat(Node, SessionId, {path, ArchiveDataDirPath}),
    {ok, [{TargetGuid, _} | _]} = 
        ?assertMatch({ok, [_ | _]}, lfm_proxy:get_children(Node, SessionId, ?FILE_REF(ArchiveDataDirGuid), 0, ?LISTED_CHILDREN_LIMIT), Attempts),
    assert_copied(Node, SessionId, DatasetRootFileGuid, TargetGuid, FollowSymlinks, Attempts).


assert_attrs_copied(Node, SessionId, SourceGuid, TargetGuid, Attempts) ->
    Stat = fun(Guid) ->
        lfm_proxy:stat(Node, SessionId, ?FILE_REF(Guid))
    end,
    {ok, SourceAttr} = Stat(SourceGuid),

    ?assertEqual(true, try
        {ok, TargetAttr} = ?assertMatch({ok, #file_attr{}}, Stat(TargetGuid), Attempts),
        case {SourceAttr#file_attr.type, TargetAttr#file_attr.type} of
            {?SYMLINK_TYPE, _} ->
                ?assertEqual(SourceAttr#file_attr.name, TargetAttr#file_attr.name),
                true;
            {_, ?SYMLINK_TYPE} ->
                ?assertEqual(SourceAttr#file_attr.name, TargetAttr#file_attr.name),
                {ok, LinkTargetGuid} = lfm_proxy:resolve_symlink(Node, SessionId, ?FILE_REF(TargetAttr#file_attr.guid)),
                assert_attrs_copied(Node, SessionId, SourceGuid, LinkTargetGuid, Attempts),
                true;
            {_, _} ->
                ?assertEqual(SourceAttr#file_attr.name, TargetAttr#file_attr.name),
                ?assertEqual(SourceAttr#file_attr.mode, TargetAttr#file_attr.mode),
                ?assertEqual(SourceAttr#file_attr.type, TargetAttr#file_attr.type),
                ?assertEqual(SourceAttr#file_attr.size, TargetAttr#file_attr.size),
                true
        end
    catch
        _:_ ->
            false
    end, Attempts).


assert_metadata_copied(Node, SessionId, SourceGuid, TargetGuid) ->
    GetXattrs = fun(Guid) ->
        lfm_proxy:list_xattr(Node, SessionId, ?FILE_REF(Guid), false, false)
    end,
    ?assertEqual(GetXattrs(SourceGuid), GetXattrs(TargetGuid)).


assert_children_copied(Node, SessionId, SourceGuid, TargetGuid, FollowSymlinks, Attempts) ->
    ListOpts = #{tune_for_large_continuous_listing => false, offset => 0, limit => ?LISTED_CHILDREN_LIMIT},
    assert_children_copied(Node, SessionId, SourceGuid, TargetGuid, ListOpts, FollowSymlinks, Attempts).

assert_children_copied(Node, SessionId, SourceGuid, TargetGuid, ListOpts = #{offset := Offset}, FollowSymlinks, Attempts) ->
    {ok, SourceChildren, SourceListingToken} =
        lfm_proxy:get_children(Node, SessionId, ?FILE_REF(SourceGuid), ListOpts),
    GetTargetChildrenFun = fun() ->
        {ok, TargetChildren, TargetListingToken} = ?assertMatch({ok, _, _},
            lfm_proxy:get_children(Node, SessionId, ?FILE_REF(TargetGuid), ListOpts), Attempts),
        ?assertEqual(file_listing:is_finished(SourceListingToken), file_listing:is_finished(TargetListingToken)),
        TargetChildren
    end,
    SourceNames = [N || {_, N} <- SourceChildren],
    ?assertEqual(SourceNames, [N || {_, N} <- GetTargetChildrenFun()], Attempts),
    TargetChildren = GetTargetChildrenFun(),
    lists:foreach(fun({{SourceChildGuid, _}, {TargetChildGuid, _}}) ->
        assert_copied(Node, SessionId, SourceChildGuid, TargetChildGuid, FollowSymlinks, Attempts)
    end, lists:zip(SourceChildren, TargetChildren)),

    case file_listing:is_finished(SourceListingToken) of
        true ->
            ok;
        false ->
            assert_children_copied(Node, SessionId, SourceGuid, TargetGuid, ListOpts#{offset => Offset + length(SourceChildren)}, FollowSymlinks, Attempts)
    end.


assert_content_copied(Node, SessionId, SourceGuid, TargetGuid, Attempts) ->
    {ok, SourceHandle} = lfm_proxy:open(Node, SessionId, ?FILE_REF(SourceGuid), read),
    {ok, TargetHandle} = ?assertMatch({ok, _},
        lfm_proxy:open(Node, SessionId, ?FILE_REF(TargetGuid), read), Attempts),
    assert_content_copied_internal(Node, SourceHandle, TargetHandle, 0, Attempts),
    
    lfm_proxy:close(Node, SourceHandle),
    lfm_proxy:close(Node, TargetHandle),
    TargetGuid2 = resolve_if_symlink(Node, SessionId, TargetGuid),
    assert_file_is_flushed_from_buffer(Node, SessionId, SourceGuid, TargetGuid2, Attempts).

assert_content_copied_internal(Node, SourceHandle, TargetHandle, Offset, Attempts) ->
    BytesToRead = 10000, 
    {ok, SourceContent} = lfm_proxy:check_size_and_read(Node, SourceHandle, Offset, BytesToRead),
    ?assertEqual({ok, SourceContent},
        lfm_proxy:check_size_and_read(Node, TargetHandle, Offset, BytesToRead), Attempts),
    case SourceContent of
        <<>> -> ok;
        _ -> assert_content_copied_internal(Node, SourceHandle, TargetHandle, Offset + BytesToRead, Attempts)
    end.


assert_json_metadata_copied(Node, SessionId, SourceGuid, TargetGuid, Attempts) ->
    case lfm_proxy:get_metadata(Node, SessionId, ?FILE_REF(SourceGuid), json, [], false) of
        {ok, SourceJson} ->
            ?assertEqual({ok, SourceJson},
                lfm_proxy:get_metadata(Node, SessionId, ?FILE_REF(TargetGuid), json, [], false), Attempts);
        {error, ?ENODATA} ->
            ok
    end.


assert_file_is_flushed_from_buffer(Node, SessionId, SourceGuid, TargetGuid, Attempts) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    {ok, #file_attr{size = SourceSize}} = lfm_proxy:stat(Node, SessionId, ?FILE_REF(SourceGuid)),
    TargetSDHandle = sd_test_utils:new_handle(Node, SpaceId, get_storage_file_id(Node, TargetGuid)),
    GetStorageSize = fun(SDHandle) ->
        case sd_test_utils:stat(Node, SDHandle) of
            {ok, #statbuf{st_size = SourceSize}} -> SourceSize;
            _ -> error
        end
    end,
    ?assertEqual(SourceSize, GetStorageSize(TargetSDHandle), Attempts).


assert_symlink_values_copied(Node, SessionId, SourceGuid, TargetGuid, Attempts) ->
    ReadSymlink = fun(Guid) ->
        {ok, Path} = ?assertMatch({ok, _}, lfm_proxy:read_symlink(Node, SessionId, ?FILE_REF(Guid))), Attempts,
        Path
    end,
    SpaceId = file_id:guid_to_space_id(SourceGuid),
    NormalizePath = fun(SymlinkPath) ->
        [_ | Rest] = filename:split(SymlinkPath),
        filename:join([<<"/">>, SpaceId | Rest])
    end,
    SourcePath = NormalizePath(ReadSymlink(SourceGuid)),
    TargetPath = NormalizePath(ReadSymlink(TargetGuid)),
    
    case {archivisation_tree:is_in_archive(SourcePath), archivisation_tree:is_in_archive(TargetPath)} of
        {false, false} ->
            ?assertEqual(SourcePath, TargetPath);
        {true, false} ->
            assert_internal_symlinks_target_paths(SourcePath, TargetPath),
            assert_internal_symlinks_validity(Node, SessionId, SourceGuid, TargetGuid);
        {false, true} ->
            assert_internal_symlinks_target_paths(TargetPath, SourcePath),
            assert_internal_symlinks_validity(Node, SessionId, SourceGuid, TargetGuid)
    end.


assert_internal_symlinks_target_paths(PathInArchive, OtherPath) ->
    [_Sep, _SpaceId, _ArchiveRoot, _DatasetDir, _ArchiveDir | PathTokens] = filename:split(PathInArchive),
    FinalArchivePathTokens = case PathTokens of
        [?BAGIT_DATA_DIR_NAME | Rest] -> Rest;
        _ -> PathTokens
    end,
    ?assertEqual(true, lists:suffix(FinalArchivePathTokens, filename:split(OtherPath))).


assert_internal_symlinks_validity(Node, SessionId, SourceSymGuid, TargetSymGuid) ->
    ?assertMatch({ok, _}, lfm_proxy:resolve_symlink(Node, SessionId, #file_ref{guid = SourceSymGuid})),
    ?assertMatch({ok, _}, lfm_proxy:resolve_symlink(Node, SessionId, #file_ref{guid = TargetSymGuid})).


assert_layout_custom_features(_Node, _SessionId, _ArchiveId, ?ARCHIVE_PLAIN_LAYOUT) ->
    ok;
assert_layout_custom_features(Node, SessionId, ArchiveId, ?ARCHIVE_BAGIT_LAYOUT) ->
    ArchiveRootDirUuid = ?ARCHIVE_DIR_UUID(ArchiveId),
    ArchiveRootDirGuid = file_id:pack_guid(ArchiveRootDirUuid, oct_background:get_space_id(?SPACE)),
    bagit_test_utils:validate_all_files_checksums(Node, SessionId, ArchiveRootDirGuid),
    bagit_test_utils:validate_all_files_json_metadata(Node, SessionId, ArchiveRootDirGuid).


get_storage_file_id(Node, Guid) ->
    FileCtx = rpc:call(Node, file_ctx, new_by_guid, [Guid]),
    {StorageFileId, _} = rpc:call(Node, file_ctx, get_storage_file_id, [FileCtx]),
    StorageFileId.


resolve_if_symlink(Node, SessionId, Guid) ->
    {ok, #file_attr{type = Type}} = lfm_proxy:stat(Node, SessionId, ?FILE_REF(Guid)),
    case Type =:= ?SYMLINK_TYPE of
        true ->
            {ok, LinkTargetGuid} = lfm_proxy:resolve_symlink(Node, SessionId, ?FILE_REF(Guid)),
            LinkTargetGuid;
        false ->
            Guid
    end.


get_archive_info_without_config(Node, SessionId, ArchiveId) ->
    case opt_archives:get_info(Node, SessionId, ArchiveId) of
        {ok, ArchiveInfo} ->
            {ok, ArchiveInfo#archive_info{config = undefined}};
        Other ->
            Other
    end.


assert_incremental_archive_links(Node, SessionId, BaseArchiveId, Guid, ModifiedFiles) ->
    case lfm_proxy:stat(Node, SessionId, #file_ref{guid = Guid}) of
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE, name = FileName}} ->
            case lists:member(FileName, ModifiedFiles) of
                true -> ?assertNotEqual({ok, BaseArchiveId}, extract_base_archive_id(Node, SessionId, Guid));
                false -> ?assertEqual({ok, BaseArchiveId}, extract_base_archive_id(Node, SessionId, Guid))
            end;
        {ok, #file_attr{type = ?SYMLINK_TYPE}} ->
            ok;
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} ->
            {ok, Children} = lfm_proxy:get_children(Node, SessionId, ?FILE_REF(Guid), 0, ?LISTED_CHILDREN_LIMIT),
            lists:foreach(fun({ChildGuid, _}) ->
                assert_incremental_archive_links(Node, SessionId, BaseArchiveId, ChildGuid, ModifiedFiles)
            end, Children)
    end.


extract_base_archive_id(Node, SessionId, Guid) ->
    {ok, Path} = lfm_proxy:get_file_path(Node, SessionId, ensure_referenced_guid(Guid)),
    archivisation_tree:extract_archive_id(Path).


ensure_referenced_guid(Guid) ->
    {Uuid, SpaceId} = file_id:unpack_guid(Guid),
    ReferencedUuid = fslogic_uuid:ensure_referenced_uuid(Uuid),
    file_id:pack_guid(ReferencedUuid, SpaceId).
