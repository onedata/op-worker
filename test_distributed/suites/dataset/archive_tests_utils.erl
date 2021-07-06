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
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/test/assertions.hrl").

-export([create_archive_dir/5]).
-export([
    assert_archive_dir_structure_is_correct/6, assert_archive_state/2, 
    assert_archive_is_preserved/7, assert_incremental_archive_links/3
]).


-define(ATTEMPTS, 60).

-define(SPACE, space_krk_par_p).
-define(USER1, user1).
-define(LISTED_CHILDREN_LIMIT, 1000).


%===================================================================
% API
%===================================================================

create_archive_dir(Node, ArchiveId, DatasetId, SpaceId, UserId) ->
    rpc:call(Node, archivisation_tree, create_archive_dir, [ArchiveId, DatasetId, SpaceId, UserId]).


assert_archive_dir_structure_is_correct(Node, SessionId, SpaceId, DatasetId, ArchiveId, UserId) ->
    assert_archives_root_dir_exists(Node, SessionId, SpaceId),
    assert_dataset_archives_dir_exists(Node, SessionId, SpaceId, DatasetId),
    assert_archive_dir_exists(Node, SessionId, SpaceId, DatasetId, ArchiveId, UserId).


assert_archive_state(ArchiveId, ExpectedState) ->
    lists:foreach(fun(Provider) ->
        Node = oct_background:get_random_provider_node(Provider),
        SessionId = oct_background:get_user_session_id(?USER1, Provider),
        ?assertMatch({ok, #archive_info{state = ExpectedState}}, lfm_proxy:get_archive_info(Node, SessionId, ArchiveId), ?ATTEMPTS)
    end, oct_background:get_space_supporting_providers(?SPACE)).


assert_archive_is_preserved(Node, SessionId, ArchiveId, DatasetId, DatasetRootFileGuid, FileCount, ExpSize) ->
    ?assertMatch({ok, #archive_info{
        state = ?ARCHIVE_PRESERVED,
        stats = #archive_stats{
            files_archived = FileCount,
            files_failed = 0,
            bytes_archived = ExpSize
        }
    }}, get_archive_info_without_config(Node, SessionId, ArchiveId), ?ATTEMPTS),
    
    {ok, #archive_info{config = #archive_config{layout = ArchiveLayout}}} =
        lfm_proxy:get_archive_info(Node, SessionId, ArchiveId),
    
    GetDatasetArchives = fun() ->
        case lfm_proxy:list_archives(Node, SessionId, DatasetId, #{offset => 0, limit => 10000}) of
            {ok, ArchiveIdsAndIndices, _} ->
                [AID || {_, AID} <- ArchiveIdsAndIndices];
            _ ->
                error
        end
    end,
    ?assertEqual(true, lists:member(ArchiveId, GetDatasetArchives()), ?ATTEMPTS),
    
    assert_structure(Node, SessionId, ArchiveId, DatasetRootFileGuid, ArchiveLayout),
    assert_layout_custom_features(Node, SessionId, ArchiveId, ArchiveLayout).


assert_incremental_archive_links(BaseArchiveId, ArchiveId, ModifiedFiles) ->
    lists:foreach(fun(Provider) ->
        Node = oct_background:get_random_provider_node(Provider),
        SessionId = oct_background:get_user_session_id(?USER1, Provider),
        {ok, ArchiveDataDirGuid} = rpc:call(Node, archive, get_data_dir_guid, [ArchiveId]),
        assert_incremental_archive_links(Node, SessionId, BaseArchiveId, ArchiveDataDirGuid, ModifiedFiles)
    end, oct_background:get_space_supporting_providers(?SPACE)).


%===================================================================
% Internal functions
%===================================================================

assert_archives_root_dir_exists(Node, SessionId, SpaceId) ->
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
    }}, lfm_proxy:stat(Node, SessionId, ?FILE_REF(ArchivesRootGuid)), ?ATTEMPTS).


assert_dataset_archives_dir_exists(Node, SessionId, SpaceId, DatasetId) ->
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
    }}, lfm_proxy:stat(Node, SessionId, ?FILE_REF(DatasetArchivesDirGuid)), ?ATTEMPTS).


assert_archive_dir_exists(Node, SessionId, SpaceId, DatasetId, ArchiveId, UserId) ->
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
    }}, lfm_proxy:stat(Node, SessionId, ?FILE_REF(ArchiveDirGuid)), ?ATTEMPTS).


assert_structure(Node, SessionId, ArchiveId, DatasetRootFileGuid, ?ARCHIVE_PLAIN_LAYOUT) ->
    ArchiveRootDirUuid = ?ARCHIVE_DIR_UUID(ArchiveId),
    ArchiveRootDirGuid = file_id:pack_guid(ArchiveRootDirUuid, oct_background:get_space_id(?SPACE)),
    {ok, [{TargetGuid, _}, _]} = lfm_proxy:get_children(Node, SessionId, ?FILE_REF(ArchiveRootDirGuid), 0, ?LISTED_CHILDREN_LIMIT),
    assert_copied(Node, SessionId, DatasetRootFileGuid, TargetGuid);
assert_structure(Node, SessionId, ArchiveId, DatasetRootFileGuid, ?ARCHIVE_BAGIT_LAYOUT) ->
    ArchiveRootDirUuid = ?ARCHIVE_DIR_UUID(ArchiveId),
    ArchiveRootDirGuid = file_id:pack_guid(ArchiveRootDirUuid, oct_background:get_space_id(?SPACE)),
    {ok, ArchiveRootDirPath} = lfm_proxy:get_file_path(Node, SessionId, ArchiveRootDirGuid),
    ArchiveDataDirPath = filename:join([ArchiveRootDirPath, <<"data">>]),
    {ok, #file_attr{guid = ArchiveDataDirGuid}} = lfm_proxy:stat(Node, SessionId, {path, ArchiveDataDirPath}),
    {ok, [{TargetGuid, _}, _]} = lfm_proxy:get_children(Node, SessionId, ?FILE_REF(ArchiveDataDirGuid), 0, ?LISTED_CHILDREN_LIMIT),
    assert_copied(Node, SessionId, DatasetRootFileGuid, TargetGuid).


assert_copied(Node, SessionId, SourceGuid, TargetGuid) ->
    assert_attrs_copied(Node, SessionId, SourceGuid, TargetGuid),
    assert_metadata_copied(Node, SessionId, SourceGuid, TargetGuid),
    {ok, SourceAttr} = lfm_proxy:stat(Node, SessionId, ?FILE_REF(SourceGuid)),
    case SourceAttr#file_attr.type of
        ?DIRECTORY_TYPE ->
            assert_children_copied(Node, SessionId, SourceGuid, TargetGuid);
        ?REGULAR_FILE_TYPE ->
            assert_content_copied(Node, SessionId, SourceGuid, TargetGuid),
            assert_json_metadata_copied(Node, SessionId, SourceGuid, TargetGuid);
        ?SYMLINK_TYPE ->
            assert_symlink_values_copied(Node, SessionId, SourceGuid, TargetGuid)
    end.


assert_attrs_copied(Node, SessionId, SourceGuid, TargetGuid) ->
    Stat = fun(Guid) ->
        lfm_proxy:stat(Node, SessionId, ?FILE_REF(Guid))
    end,
    {ok, SourceAttr} = Stat(SourceGuid),

    ?assertEqual(true, try
        {ok, TargetAttr} = ?assertMatch({ok, #file_attr{}}, Stat(TargetGuid), ?ATTEMPTS),
        case SourceAttr#file_attr.type /= ?SYMLINK_TYPE andalso TargetAttr#file_attr.type == ?SYMLINK_TYPE of
            true ->
                ?assertEqual(SourceAttr#file_attr.name, TargetAttr#file_attr.name),
                {ok, LinkTargetGuid} = lfm_proxy:resolve_symlink(Node, SessionId, ?FILE_REF(TargetAttr#file_attr.guid)),
                assert_attrs_copied(Node, SessionId, SourceGuid, LinkTargetGuid),
                true;
            false ->
                ?assertEqual(SourceAttr#file_attr.name, TargetAttr#file_attr.name),
                ?assertEqual(SourceAttr#file_attr.mode, TargetAttr#file_attr.mode),
                ?assertEqual(SourceAttr#file_attr.type, TargetAttr#file_attr.type),
                ?assertEqual(SourceAttr#file_attr.size, TargetAttr#file_attr.size),
                true
        end
    catch
        _:_ ->
            false
    end, ?ATTEMPTS).


assert_metadata_copied(Node, SessionId, SourceGuid, TargetGuid) ->
    GetXattrs = fun(Guid) ->
        lfm_proxy:list_xattr(Node, SessionId, ?FILE_REF(Guid), false, false)
    end,
    ?assertEqual(GetXattrs(SourceGuid), GetXattrs(TargetGuid)).


assert_children_copied(Node, SessionId, SourceGuid, TargetGuid) ->
    assert_children_copied(Node, SessionId, SourceGuid, TargetGuid, #{offset => 0, size => ?LISTED_CHILDREN_LIMIT}).

assert_children_copied(Node, SessionId, SourceGuid, TargetGuid, ListOpts = #{offset := Offset}) ->
    {ok, SourceChildren, #{is_last := SourceIsLast}} =
        lfm_proxy:get_children(Node, SessionId, ?FILE_REF(SourceGuid), ListOpts),
    {ok, TargetChildren, _} = ?assertMatch({ok, _, #{is_last := SourceIsLast}},
        lfm_proxy:get_children(Node, SessionId, ?FILE_REF(TargetGuid), ListOpts), ?ATTEMPTS),
    SourceNames = [N || {_, N} <- SourceChildren],
    TargetNames = [N || {_, N} <- TargetChildren],
    ?assertEqual(SourceNames, TargetNames),
    lists:foreach(fun({{SourceChildGuid, _}, {TargetChildGuid, _}}) ->
        assert_copied(Node, SessionId, SourceChildGuid, TargetChildGuid)
    end, lists:zip(SourceChildren, TargetChildren)),

    case SourceIsLast of
        true ->
            ok;
        false ->
            assert_children_copied(Node, SessionId, SourceGuid, TargetGuid, ListOpts#{offset => Offset + length(SourceChildren)})
    end.


assert_content_copied(Node, SessionId, SourceGuid, TargetGuid) ->
    {ok, SourceHandle} = lfm_proxy:open(Node, SessionId, ?FILE_REF(SourceGuid), read),
    {ok, TargetHandle} = ?assertMatch({ok, _},
        lfm_proxy:open(Node, SessionId, ?FILE_REF(TargetGuid), read), ?ATTEMPTS),
    assert_content_copied_internal(Node, SourceHandle, TargetHandle, 0),
    
    lfm_proxy:close(Node, SourceHandle),
    lfm_proxy:close(Node, TargetHandle),
    TargetGuid2 = resolve_if_symlink(Node, SessionId, TargetGuid),
    assert_file_is_flushed_from_buffer(Node, SessionId, SourceGuid, TargetGuid2).

assert_content_copied_internal(Node, SourceHandle, TargetHandle, Offset) ->
    BytesToRead = 10000, 
    {ok, SourceContent} = lfm_proxy:read(Node, SourceHandle, Offset, BytesToRead),
    ?assertEqual({ok, SourceContent},
        lfm_proxy:read(Node, TargetHandle, Offset, BytesToRead), ?ATTEMPTS),
    case SourceContent of
        <<>> -> ok;
        _ -> assert_content_copied_internal(Node, SourceHandle, TargetHandle, Offset + BytesToRead)
    end.


assert_json_metadata_copied(Node, SessionId, SourceGuid, TargetGuid) ->
    case lfm_proxy:get_metadata(Node, SessionId, ?FILE_REF(SourceGuid), json, [], false) of
        {ok, SourceJson} ->
            ?assertEqual({ok, SourceJson},
                lfm_proxy:get_metadata(Node, SessionId, ?FILE_REF(TargetGuid), json, [], false), ?ATTEMPTS);
        {error, ?ENODATA} ->
            ok
    end.


assert_file_is_flushed_from_buffer(Node, SessionId, SourceGuid, TargetGuid) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    {ok, #file_attr{size = SourceSize}} = lfm_proxy:stat(Node, SessionId, ?FILE_REF(SourceGuid)),
    TargetSDHandle = sd_test_utils:new_handle(Node, SpaceId, get_storage_file_id(Node, TargetGuid)),
    GetStorageSize = fun(SDHandle) ->
        case sd_test_utils:stat(Node, SDHandle) of
            {ok, #statbuf{st_size = SourceSize}} -> SourceSize;
            _ -> error
        end
    end,
    ?assertEqual(SourceSize, GetStorageSize(TargetSDHandle), ?ATTEMPTS).


assert_symlink_values_copied(Node, SessionId, SourceGuid, TargetGuid) ->
    ReadSymlink = fun(Guid) ->
        lfm_proxy:read_symlink(Node, SessionId, ?FILE_REF(Guid))
    end,
    ?assertEqual(ReadSymlink(SourceGuid), ReadSymlink(TargetGuid), ?ATTEMPTS).


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
    case lfm_proxy:get_archive_info(Node, SessionId, ArchiveId) of
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
