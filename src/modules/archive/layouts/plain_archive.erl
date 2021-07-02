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
-export([archive_file/5]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec archive_file(archive:doc(), file_ctx:ctx(), file_ctx:ctx(), archive:doc() | undefined,
    user_ctx:ctx()) -> {ok, file_ctx:ctx()} | {error, term()}.
archive_file(ArchiveDoc, FileCtx, TargetParentCtx, BaseArchiveDoc, UserCtx) ->
    try
        archive_file_insecure(ArchiveDoc, FileCtx, TargetParentCtx, BaseArchiveDoc, UserCtx)
    catch
        Class:Reason ->
            Guid = file_ctx:get_logical_guid_const(FileCtx),
            ?error_stacktrace("Unexpected error ~p:~p occured during archivisation of file ~s.", [Class, Reason, Guid]),
            {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec archive_file_insecure(archive:doc(), file_ctx:ctx(), file_ctx:ctx(), archive:doc() | undefined, user_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | error.
archive_file_insecure(_ArchiveDoc, FileCtx, TargetParentCtx, undefined, UserCtx) ->
    copy_file_to_archive(FileCtx, TargetParentCtx, UserCtx);
archive_file_insecure(ArchiveDoc, FileCtx, TargetParentCtx, BaseArchiveDoc, UserCtx) ->
    {ok, DatasetRootFileGuid} = archive:get_dataset_root_file_guid(ArchiveDoc),
    DatasetRootFileCtx = file_ctx:new_by_guid(DatasetRootFileGuid),
    {DatasetRootLogicalPath, _DatasetRootFileCtx2} = file_ctx:get_logical_path(DatasetRootFileCtx, UserCtx),
    {_, DatasetRootParentPath} = filepath_utils:basename_and_parent_dir(DatasetRootLogicalPath),
    {FileLogicalPath, FileCtx2} = file_ctx:get_logical_path(FileCtx, UserCtx),

    RelativeFilePath = filepath_utils:relative(DatasetRootParentPath, FileLogicalPath),

    case archive:find_file(BaseArchiveDoc, RelativeFilePath, UserCtx) of
        {ok, BaseArchiveFileCtx} ->
            case incremental_archive:has_file_changed(BaseArchiveFileCtx, FileCtx2, UserCtx) of
                true ->
                    copy_file_to_archive(FileCtx2, TargetParentCtx, UserCtx);
                false ->
                    make_hardlink_to_file_in_base_archive(FileCtx2, TargetParentCtx, BaseArchiveFileCtx, UserCtx)
            end;
        ?ERROR_NOT_FOUND ->
            copy_file_to_archive(FileCtx2, TargetParentCtx, UserCtx)
    end.


-spec copy_file_to_archive(file_ctx:ctx(), file_ctx:ctx(), user_ctx:ctx()) -> {ok, file_ctx:ctx()}.
copy_file_to_archive(FileCtx, TargetParentCtx, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    {FileName, FileCtx2} = file_ctx:get_aliased_name(FileCtx, UserCtx),
    FileGuid = file_ctx:get_logical_guid_const(FileCtx2),
    TargetParentGuid = file_ctx:get_logical_guid_const(TargetParentCtx),

    {ok, CopyGuid, _} = file_copy:copy(SessionId, FileGuid, TargetParentGuid, FileName, false),

    CopyCtx = file_ctx:new_by_guid(CopyGuid),
    ok = archivisation_checksum:calculate_and_save(CopyCtx, UserCtx),

    {FileSize, CopyCtx2} = file_ctx:get_local_storage_file_size(CopyCtx),
    {SDHandle, CopyCtx3} = storage_driver:new_handle(SessionId, CopyCtx2),
    ok = storage_driver:flushbuffer(SDHandle, FileSize),
    {ok, CopyCtx3}.


-spec make_hardlink_to_file_in_base_archive(file_ctx:ctx(), file_ctx:ctx(), file_ctx:ctx(), user_ctx:ctx()) ->
    {ok, file_ctx:ctx()}.
make_hardlink_to_file_in_base_archive(FileCtx, TargetParentCtx, BaseArchiveFileCtx, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),

    {Name, _} = file_ctx:get_aliased_name(FileCtx, UserCtx),
    TargetGuid = file_ctx:get_logical_guid_const(BaseArchiveFileCtx),
    TargetParentGuid = file_ctx:get_logical_guid_const(TargetParentCtx),

    {ok, #file_attr{guid = LinkGuid}} =
        lfm:make_link(SessionId, ?FILE_REF(TargetGuid), ?FILE_REF(TargetParentGuid), Name),

    {ok, file_ctx:new_by_guid(LinkGuid)}.