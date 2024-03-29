%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This modules is used to create archive with plain layout.
%%%
%%% If archive is created with create_nested_archives=true,
%%% archives for nested datasets are also created and symlinks to these
%%% nested archives are created in parent archives.
%%% If create_nested_archives=false, files are simply copied.
%%%
%%%-------------------------------------------------------------------
%%% Example
%%%-------------------------------------------------------------------
%%% Following file structure
%%%
%%% Dir1(DS1)
%%% |--- f.txt (DS2)
%%% |--- f2.txt (DS3)
%%% |--- f3.txt
%%% |--- Dir1.1(DS4)
%%%      |--- hello.txt
%%%
%%% will have the following archive structure, in case of create_nested_archives=true:
%%%
%%% .__onedata_archive
%%% |--- dataset_DS1
%%% |    |--- archive_123
%%% |         |--- Dir1
%%% |              |---  f.txt  (SL -> dataset_DS2/archive_1234/f.txt)
%%% |              |---  f2.txt (SL -> dataset_DS3/archive_1235/f2.txt)
%%% |              |---  f3.txt
%%% |              |---  Dir1.1 (SL -> dataset_DS4/archive_1236/Dir1.1)
%%% |
%%% |--- dataset_DS2
%%% |    |--- archive_1234
%%% |         |--- f.txt
%%% |
%%% |--- dataset_DS3
%%% |    |--- archive_1235
%%% |         |--- f2.txt
%%% |
%%% |--- dataset_DS4
%%%      |--- archive_1236
%%%           |--- Dir1.1
%%%                |--- hello.txt
%%%
%%% If create_nested_archives=false, the structure will be as follows:
%%%
%%% .__onedata_archive
%%% |--- dataset_DS1
%%%      |--- archive_123
%%%           |--- Dir1
%%%                |--- f.txt
%%%                |--- f2.txt
%%%                |--- f3.txt
%%%                |--- Dir1.1
%%%                      |--- hello.txt
%%% @end
%%%-------------------------------------------------------------------
-module(plain_archive).
-author("Jakub Kudzia").

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/fslogic/file_attr.hrl").

%% API
-export([archive_regular_file/7, archive_symlink/4]).

-define(FILE_READONLY_STORAGE_PERMS, 8#440).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec archive_regular_file(archive:doc(), file_ctx:ctx(), file_ctx:ctx(),
    archive:doc() | undefined, file_meta:path(), user_ctx:ctx(), file_copy:options()) ->
    {ok, file_ctx:ctx()} | no_return().
archive_regular_file(
    _ArchiveDoc, FileCtx, TargetParentCtx, undefined, ResolvedFilePath, UserCtx, CopyOpts
) ->
    copy_file_to_archive(FileCtx, TargetParentCtx, ResolvedFilePath, UserCtx, CopyOpts);
archive_regular_file(
    ArchiveDoc, FileCtx, TargetParentCtx, BaseArchiveDoc, ResolvedFilePath, UserCtx, CopyOpts
) ->
    {ok, DatasetRootParentPath} = archive:get_dataset_root_parent_path(ArchiveDoc, UserCtx),
    RelativeFilePath = filepath_utils:relative(DatasetRootParentPath, ResolvedFilePath),
    
    case archive:find_file(BaseArchiveDoc, RelativeFilePath, UserCtx) of
        {ok, BaseArchiveFileCtx} ->
            case incremental_archive:has_file_changed(BaseArchiveFileCtx, FileCtx, UserCtx) of
                true ->
                    copy_file_to_archive(FileCtx, TargetParentCtx, ResolvedFilePath, UserCtx, CopyOpts);
                false ->
                    make_hardlink_to_file_in_base_archive(
                        FileCtx, TargetParentCtx, BaseArchiveFileCtx, UserCtx)
            end;
        ?ERROR_NOT_FOUND ->
            copy_file_to_archive(FileCtx, TargetParentCtx, ResolvedFilePath, UserCtx, CopyOpts)
    end.


-spec archive_symlink(file_ctx:ctx(), file_ctx:ctx(), archive:doc(), user_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | no_return().
archive_symlink(FileCtx, TargetParentCtx, ArchiveDoc, UserCtx) ->
    {ok, DatasetId} = archive:get_dataset_id(ArchiveDoc),
    {ok, SpaceId} = archive:get_space_id(ArchiveDoc),
    {ok, ArchiveDataGuid} = archive:get_data_dir_guid(ArchiveDoc),
    {ok, SymlinkPath} = lfm:read_symlink(user_ctx:get_session_id(UserCtx),
        ?FILE_REF(file_ctx:get_logical_guid_const(FileCtx))),
    {DatasetLogicalPath, _DatasetFileCtx} = file_ctx:get_logical_path(
        file_ctx:new_by_uuid(DatasetId, SpaceId), UserCtx),
    {ArchiveDataLogicalPath, _ArchiveFileCtx} = file_ctx:get_logical_path(
        file_ctx:new_by_guid(ArchiveDataGuid), UserCtx),
    [_Sep, SpaceName | DatasetPathTokens] = filename:split(DatasetLogicalPath),
    [_, _SpaceName | ArchivePathTokens] = filename:split(ArchiveDataLogicalPath),
    [SpaceIdPrefix | SymlinkPathTokens] = filename:split(SymlinkPath),
    FinalSymlinkValue = case lists:prefix(DatasetPathTokens, SymlinkPathTokens) of
        true ->
            RelativePathTokens = SymlinkPathTokens -- DatasetPathTokens,
            DatasetName = case DatasetPathTokens of
                [] -> SpaceName; % dataset established on space dir
                _ -> lists:last(DatasetPathTokens)
            end,
            filename:join([SpaceIdPrefix] ++ ArchivePathTokens ++ [DatasetName] ++ RelativePathTokens);
        _ ->
            SymlinkPath
    end,
    {TargetName, _} = file_ctx:get_aliased_name(FileCtx, undefined),
    {ok, #file_attr{guid = Guid}} = lfm:make_symlink(
        user_ctx:get_session_id(UserCtx), ?FILE_REF(file_ctx:get_logical_guid_const(TargetParentCtx)),
        TargetName, FinalSymlinkValue),
    {ok, file_ctx:new_by_guid(Guid)}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec copy_file_to_archive(file_ctx:ctx(), file_ctx:ctx(), file_meta:path(), user_ctx:ctx(), file_copy:options()) -> 
    {ok, file_ctx:ctx()}.
copy_file_to_archive(FileCtx, TargetParentCtx, ResolvedFilePath, UserCtx, CopyOpts) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    FileName = filename:basename(ResolvedFilePath),
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    TargetParentGuid = file_ctx:get_logical_guid_const(TargetParentCtx),

    {ok, CopyGuid, _} = file_copy:copy(SessionId, FileGuid, TargetParentGuid, FileName, CopyOpts),

    CopyCtx = file_ctx:new_by_guid(CopyGuid),
    ok = archivisation_checksum:file_calculate_and_save(CopyCtx, UserCtx),

    {FileSize, CopyCtx2} = file_ctx:get_local_storage_file_size(CopyCtx),
    {SDHandle, CopyCtx3} = storage_driver:new_handle(SessionId, CopyCtx2),
    ok = storage_driver:flushbuffer(SDHandle, FileSize),
    {ok, _CopyCtx4} = sd_utils:chmod(UserCtx, CopyCtx3, ?FILE_READONLY_STORAGE_PERMS).


-spec make_hardlink_to_file_in_base_archive(file_ctx:ctx(), file_ctx:ctx(), file_ctx:ctx(), 
    user_ctx:ctx()) -> {ok, file_ctx:ctx()}.
make_hardlink_to_file_in_base_archive(FileCtx, TargetParentCtx, BaseArchiveFileCtx, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),

    {Name, _} = file_ctx:get_aliased_name(FileCtx, UserCtx),
    TargetGuid = file_ctx:get_logical_guid_const(BaseArchiveFileCtx),
    TargetParentGuid = file_ctx:get_logical_guid_const(TargetParentCtx),

    {ok, #file_attr{guid = LinkGuid}} =
        lfm:make_link(SessionId, ?FILE_REF(TargetGuid), ?FILE_REF(TargetParentGuid), Name),

    {ok, file_ctx:new_by_guid(LinkGuid)}.
