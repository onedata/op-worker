%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO UPDATE !!!!
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_engine).
% rename to storage_sync_engine
-author("Jakub Kudzia").

-include("modules/storage_sync/storage_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_sufix.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include("global_definitions.hrl").
-include("proto/oneprovider/provider_messages.hrl").

-type result() :: imported | updated | processed | failed.

-export_type([result/0]).

%% API
-export([process_file/2]).

% exported for mocking in CT tests
-export([import_file_unsafe/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Processes storage file associated with StorageFileCtx.
%% It returns the tuple {Result, FileCtx, StorageFileCtx} where:
%%    * Result is one of:
%%      ** imported - which means that file was imported to the Onedata file-system
%%      ** updated - which means that file had already existed in the Onedata file system
%%                   and that update of its attributes on storage was detected and reflected
%%                   in the Onedata file-system attributes
%%      ** processed - which means that file has been successfully processed but
%%                     no changes were introduced to the Onedata file system.
%%                     This might have happened if file hadn't been changed since last scan or if
%%                     it was being deleted or if it was being replicated in the very moment of scan.
%%                     and that it hasn't changed on storage since last scan
%%      ** failed - which means that an error occurred during processing the file
%%    * FileCtx is a:
%%      ** file_ctx:ctx() - the file in the Onedata file system which is associated
%%         with the file referenced by StorageFileCtx.
%%         This happens when the Result is one of imported/updated/processed (no changes).
%%      ** undefined - if the Result was one of processed/failed
%%    * StorageFileCtx which is updated StorageFileCtx passed to the function.
%% @end
%%--------------------------------------------------------------------
-spec process_file(storage_file_ctx:ctx(), storage_sync_traverse:info()) ->
    {result(), file_ctx:ctx() | undefined, storage_file_ctx:ctx()}.
process_file(StorageFileCtx, Info = #{space_dir_path := SpaceStorageFileId}) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    {#statbuf{st_mode = Mode}, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    FileName = storage_file_ctx:get_file_name_const(StorageFileCtx2),
    FileType = file_meta:type(Mode),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    SpaceCtx = file_ctx:new_by_guid(SpaceGuid),
    Info2 =  #{parent_ctx := ParentCtx2} = case
        storage_file_ctx:get_storage_file_id_const(StorageFileCtx2) =:= SpaceStorageFileId
    of
        true ->
            Info;
        false ->
            find_direct_parent_and_ensure_all_parents_exist(StorageFileCtx2, Info#{parent_ctx => SpaceCtx})
    end,

    case file_ctx:is_root_dir_const(ParentCtx2) of
        true ->
            sync_if_file_is_not_being_replicated(StorageFileCtx2, SpaceCtx, ?DIRECTORY_TYPE, Info2);
        false ->
            % check whether FileName of processed file is in the form
            % FileName = <<FileBaseName, ?CONFLICTING_STORAGE_FILE_SUFFIX_SEPARATOR, FileUuid>>
            % such files are created by the system if it's impossible to create file FileBaseName on storage
            % i.e. when file with the same name was deleted but there is still an open handle to it
            {HasSuffix, FileUuid, FileBaseName} = case is_suffixed(FileName) of
                {true, StorageUuid, StorageBaseName} -> {true, StorageUuid, StorageBaseName};
                false -> {false, undefined, FileName}
            end,

            case link_utils:try_to_resolve_child_link(FileBaseName, ParentCtx2) of
                {error, not_found} ->
                    % Link from Parent to FileBaseName is missing.
                    % We must check deletion_link to ensure that file may be synced.
                    % Deletion links are removed if and only if file was successfully deleted from storage.
                    case link_utils:try_to_resolve_child_deletion_link(FileName, ParentCtx2) of
                        {error, not_found} when HasSuffix =:= false ->
                            % Do not call storage_file_ctx:stat as stat result may be cached
                            % in #storage_file_ctx.
                            % We must ensure whether file is still on storage at the very moment
                            % to avoid stat/delete race.
                            % Race could happen if previous stat was performed before file was
                            % deleted from the system and if links (and file) were deleted
                            % before we checked the links.
                            SFMHandle = storage_file_ctx:get_handle_const(StorageFileCtx2),
                            case storage_file_manager:stat(SFMHandle) of
                                {ok, _} ->
                                    import_file(StorageFileCtx2,  undefined,  Info2);
                                _ ->
                                    {processed, undefined, StorageFileCtx2}
                            end;
                        % It's impossible that deletion link is not found and HasSuffix == true,
                        % which is proved below:
                        %
                        % Assumptions:
                        %  - file with name FileName is processed
                        %  - link from Parent to FileBaseName is not found
                        %  - deletion link from Parent to Filename is not found
                        % Thesis:
                        % - FileName == FileBaseName (FileName has no suffix)
                        %
                        % Proof by contradiction
                        % Let's assume, contradictory to thesis, that:
                        %  - file with name FileName is processed
                        %  - link from Parent to FileBaseName is not found
                        %  - deletion link from Parent to Filename is not found
                        %  - FileName != FileBaseName (FileName has suffix)
                        % File is created on storage with suffix if and only if
                        % file with FileBaseName exists on storage.
                        % It's impossible that such file exists on storage if link from parent
                        % to FileBaseName is not found and if deletion link from parent to FileName
                        % is not found either.
                        % Here we have contradiction which proves the thesis.
                        {ok, _FileUuid} ->
                            % deletion_link exists, it means that deletion of the file has been scheduled
                            % we may ignore this file
                            {processed, undefined, StorageFileCtx2}
                    end;
                {ok, ResolvedUuid} ->
                    FileUuid2 = utils:ensure_defined(FileUuid, undefined, ResolvedUuid),
                    case link_utils:try_to_resolve_child_deletion_link(FileName, ParentCtx2) of
                        {error, not_found} ->
                            FileGuid = file_id:pack_guid(FileUuid2, SpaceId),
                            FileCtx = file_ctx:new_by_guid(FileGuid),
                            sync_if_file_is_not_being_replicated(StorageFileCtx2, FileCtx, FileType, Info2);
                        {ok, _} ->
                            {processed, undefined, StorageFileCtx2}
                    end
            end
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec find_direct_parent_and_ensure_all_parents_exist(storage_file_ctx:ctx(), storage_sync_traverse:info()) ->
    storage_sync_traverse:info().
find_direct_parent_and_ensure_all_parents_exist(StorageFileCtx, Info = #{parent_ctx := ParentCtx}) ->
    % ParentCtx may not be associated with direct parent of the file.
    % This is caused by the fact that on object storages, file structure is flat
    % and all files are "direct" children of the space directory.
    {ParentStorageFileId, ParentCtx2} = file_ctx:get_storage_file_id(ParentCtx),
    ParentStorageFileIdTokens = fslogic_path:split(ParentStorageFileId),
    % Path to the direct parent of the child can be acquired from the file's path.
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    DirectParentStorageFileId = filename:dirname(StorageFileId),
    DirectParentStorageFileIdTokens = fslogic_path:split(DirectParentStorageFileId),
    % compare tokens of both parents' paths
    MissingParentTokens = DirectParentStorageFileIdTokens -- ParentStorageFileIdTokens,
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    ParentStorageFileCtx = storage_file_ctx:new(ParentStorageFileId, SpaceId, StorageId),
    Info2 = Info#{parent_ctx => ParentCtx2},
    Info3 = ensure_parents_exist(ParentStorageFileCtx, Info2, MissingParentTokens),
    Info3.


-spec ensure_parents_exist(storage_file_ctx:ctx(), storage_sync_traverse:info(), [helpers:file_id()]) ->
    storage_sync_traverse:info().
ensure_parents_exist(_ParentStorageFileCtx, Info, []) ->
    Info;
ensure_parents_exist(ParentStorageFileCtx, Info = #{parent_ctx := ParentCtx}, [MissingParentName | Rest]) ->
    MissingParentStorageCtx = storage_file_ctx:get_child_ctx_const(ParentStorageFileCtx, MissingParentName),
    case get_child_safe(ParentCtx, MissingParentName) of
        {ok, MissingParentCtx} ->
            ensure_parents_exist(MissingParentStorageCtx, Info#{parent_ctx => MissingParentCtx}, Rest);
        {error, ?ENOENT} ->
            ParentUuid = file_ctx:get_uuid_const(ParentCtx),
            % todo wyjasnic dlaczego jest ta sekcja krytyczna
            MissingParentCtx2 = critical_section:run({create_missing_parent, ParentUuid, MissingParentName},
                fun() ->
                    case get_child_safe(ParentCtx, MissingParentName) of
                        {ok, MissingParentCtx} ->
                            MissingParentCtx;
                        {error, ?ENOENT} ->
                            {imported, MissingParentCtx, _StorageFileCtx} =
                                import_file(MissingParentStorageCtx, undefined, Info),
                            MissingParentCtx
                    end
                end
            ),
            ensure_parents_exist(MissingParentStorageCtx, Info#{parent_ctx => MissingParentCtx2}, Rest)
    end.

-spec get_child_safe(file_ctx:ctx(), file_meta:name()) -> {ok, file_ctx:ctx()} | {error, term()}.
get_child_safe(FileCtx, ChildName) ->
    try
        {ChildCtx, _} = file_ctx:get_child(FileCtx, ChildName, user_ctx:new(?ROOT_SESS_ID)),
        {ok, ChildCtx}
    catch
        throw:?ENOENT ->
            {error, ?ENOENT}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function synchronizes the file.
%% For regular files, it checks whether the file is being replicated and
%% synchronizes it ONLY if it isn't.
%% Directories are always processed.
%% @end
%%-------------------------------------------------------------------
-spec sync_if_file_is_not_being_replicated(storage_file_ctx:ctx(), file_ctx:ctx(),
    file_meta:type(), storage_sync_traverse:info()) ->
    {result(), file_ctx:ctx() | undefined, storage_file_ctx:ctx()}.
sync_if_file_is_not_being_replicated(StorageFileCtx, FileCtx, ?DIRECTORY_TYPE, Info) ->
    maybe_sync_file_with_existing_metadata(StorageFileCtx, FileCtx, Info);
sync_if_file_is_not_being_replicated(StorageFileCtx, FileCtx, ?REGULAR_FILE_TYPE, Info) ->
    % Get only two blocks - it is enough to verify if file can be imported
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    case fslogic_location_cache:get_location(
        file_location:id(FileUuid, oneprovider:get_id()), FileUuid, {blocks_num, 2}) of
        {ok, #document{
            value = #file_location{
                file_id = FileId,
                size = Size
        }} = FL} ->
            FileIdCheck = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            case FileId =:= FileIdCheck of
                true ->
                    case fslogic_location_cache:get_blocks(FL, #{count => 2}) of
                        [#file_block{offset = 0, size = Size}] ->
                            maybe_sync_file_with_existing_metadata(StorageFileCtx, FileCtx, Info);
                        [] when Size =:= 0 ->
                            maybe_sync_file_with_existing_metadata(StorageFileCtx, FileCtx, Info);
                        _ ->
                            {processed, FileCtx, StorageFileCtx}
                    end;
                _ ->
                    import_file(StorageFileCtx,  FileUuid,  Info)
            end;
        {error, not_found} ->
            import_file(StorageFileCtx,  FileUuid,  Info);
        _Other ->
            {processed, FileCtx, StorageFileCtx}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if file (which metadata exists in onedata) is fully imported
%% (.i.e. for regular files checks if its file_location exists).
%% @end
%%--------------------------------------------------------------------
-spec maybe_sync_file_with_existing_metadata(storage_file_ctx:ctx(), file_ctx:ctx(),
    storage_sync_traverse:info()) -> {result(), file_ctx:ctx(), storage_file_ctx:ctx()}.
maybe_sync_file_with_existing_metadata(StorageFileCtx, FileCtx, Info) ->
    #fuse_response{
        status = #status{code = StatusCode},
        fuse_response = FileAttr
    } = get_attr(FileCtx),
    case StatusCode of
        ?OK ->
            Result = handle_already_imported_file(StorageFileCtx, FileAttr, FileCtx, Info),
            {Result, FileCtx ,StorageFileCtx};
        ErrorCode when
            ErrorCode =:= ?ENOENT;
            ErrorCode =:= ?EAGAIN
        ->
            % TODO VFS-5273
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            import_file(StorageFileCtx, FileUuid, Info)
    end.

-spec import_file(storage_file_ctx:ctx(), file_meta:uuid() | undefined, storage_sync_traverse:info()) ->
    {result(), file_ctx:ctx(), storage_file_ctx:ctx()} | {error, term()}.
import_file(StorageFileCtx, FileUuid, Info = #{parent_ctx := ParentCtx}) ->
    try
        storage_sync_engine:import_file_unsafe(StorageFileCtx, FileUuid, Info)
    catch
        Error:Reason ->
            FileName = storage_file_ctx:get_file_name_const(StorageFileCtx),
            SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
            StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
            ?error_stacktrace("importing file ~p on storage ~p in space ~p failed due to ~w:~w",
                [FileName, StorageId, SpaceId, Error, Reason]),
            storage_sync_deletion:delete_imported_file(FileName, ParentCtx),
            {error, Reason}
    end.

-spec import_file_unsafe(storage_file_ctx:ctx(), file_meta:uuid() | undefined, storage_sync_traverse:info()) ->
    {result(), file_ctx:ctx(), storage_file_ctx:ctx()}.
import_file_unsafe(StorageFileCtx, FileUuid, Info = #{parent_ctx := ParentCtx}) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    FileName = storage_file_ctx:get_file_name_const(StorageFileCtx),
    {StatBuf, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    #statbuf{
        st_mode = Mode,
        st_atime = ATime,
        st_ctime = CTime,
        st_mtime = MTime,
        st_size = FSize
    } = StatBuf,
    {OwnerId, StorageFileCtx3} = get_owner_id(StorageFileCtx2, StorageId),
    {GroupId, StorageFileCtx4} = get_group_owner_id(StorageFileCtx3, SpaceId, StorageId),
    {FileUuid2, CreateLinks} = case FileUuid =:= undefined of
        true -> {datastore_utils:gen_key(), true};
        false -> {FileUuid, false}
    end,
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx4),
    ParentUuid = file_ctx:get_uuid_const(ParentCtx),
    case file_meta:type(Mode) of
        ?REGULAR_FILE_TYPE ->
            StatTimestamp = storage_file_ctx:get_stat_timestamp_const(StorageFileCtx4),
            storage_sync_info:update_mtime(StorageFileId, SpaceId, MTime, StatTimestamp),
            ok = location_and_link_utils:create_imported_file_location(
                SpaceId, StorageId, FileUuid2, StorageFileId, FSize, OwnerId);
        _ ->
            {ok, _} = dir_location:mark_dir_created_on_storage(FileUuid2, SpaceId)
    end,

    {ok, FileUuid2} = create_file_meta(FileUuid2, FileName, Mode, OwnerId, GroupId, ParentUuid,
        SpaceId, CreateLinks),
    {ok, _} = create_times(FileUuid2, MTime, ATime, CTime, SpaceId),
    SyncAcl = maps:get(sync_acl, Info, false),
    FileGuid = file_id:pack_guid(FileUuid2, SpaceId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    case SyncAcl of
        true -> ok = import_nfs4_acl(FileCtx, StorageFileCtx4);
        _ -> ok
    end,
    {CanonicalPath, _} = file_ctx:get_canonical_path(FileCtx),
    ?debug("Import storage file ~p", [{StorageFileId, CanonicalPath, FileUuid2}]),
    storage_sync_logger:log_import(StorageFileId, SpaceId),
    {imported, FileCtx, StorageFileCtx4}.


%%--------------------------------------------------------------------
%% @doc
%% Updates mode, times, size, file_location and ACLs of already imported file.
%% @end
%%--------------------------------------------------------------------
-spec handle_already_imported_file(storage_file_ctx:ctx(), #file_attr{}, file_ctx:ctx(),
    storage_sync_traverse:info()) -> result().
handle_already_imported_file(StorageFileCtx, FileAttr, FileCtx, Info) ->
    SyncAcl = maps:get(sync_acl, Info, false),
    try
        {#statbuf{st_mode = Mode}, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
        maybe_update_attrs(FileAttr, FileCtx, StorageFileCtx2, Mode, SyncAcl)
    catch
        Error:Reason ->
            FileName = storage_file_ctx:get_file_name_const(StorageFileCtx),
            SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
            ?error_stacktrace(
                "storage_sync_traverse:handle_already_imported file for file ~p in space ~p"
                " failed due to ~w:~w",
                [FileName, SpaceId, Error, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates mode, times and size of already imported file.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_attrs(#file_attr{}, file_ctx:ctx(), storage_file_ctx:ctx(),
    file_meta:mode(), boolean()) -> result().
maybe_update_attrs(FileAttr, FileCtx, StorageFileCtx, Mode, SyncAcl) ->
    {FileStat, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    Results = [
        maybe_update_file_location(FileStat, FileCtx, file_meta:type(Mode), StorageFileCtx2),
        maybe_update_mode(FileAttr, FileStat, FileCtx),
        maybe_update_times(FileAttr, FileStat, FileCtx),
        maybe_update_owner(FileAttr, StorageFileCtx2, FileCtx),
        maybe_update_nfs4_acl(StorageFileCtx2, FileCtx, SyncAcl)
    ],
    case lists:member(updated, Results) of
        true ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            {StorageFileId, _FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
            storage_sync_logger:log_update(StorageFileId, SpaceId),
            updated;
        false ->
            processed
    end.

-spec get_attr(file_ctx:ctx()) -> fslogic_worker:fuse_response().
get_attr(FileCtx) ->
    try
        attr_req:get_file_attr_insecure(user_ctx:new(?ROOT_SESS_ID), FileCtx)
    catch
        _:Error ->
            #fuse_response{status = fslogic_errors:gen_status_message(Error)}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's size if it has changed since last import.
%% @end
%%--------------------------------------------------------------------
maybe_update_file_location(#statbuf{}, _FileCtx, ?DIRECTORY_TYPE, _StorageFileCtx) ->
    not_updated;
maybe_update_file_location(#statbuf{st_mtime = StMtime, st_size = StSize},
    FileCtx, ?REGULAR_FILE_TYPE, StorageFileCtx
) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageSyncInfo = case storage_sync_info:get(StorageFileId, SpaceId) of
        {error, not_found} -> undefined;
        {ok, SSI} -> SSI
    end,
    {Size, FileCtx2} = file_ctx:get_local_storage_file_size(FileCtx),
    {{_, _, MTime}, FileCtx4} = file_ctx:get_times(FileCtx2),
    FileUuid = file_ctx:get_uuid_const(FileCtx4),
    SpaceId = file_ctx:get_space_id_const(FileCtx4),
    NewLastStat = storage_file_ctx:get_stat_timestamp_const(StorageFileCtx),
    LocationId = file_location:local_id(FileUuid),
    {ok, #document{
        value = #file_location{
            last_replication_timestamp = LastReplicationTimestamp
    }}} = fslogic_location_cache:get_location(LocationId, FileUuid),

    Result2 = case {LastReplicationTimestamp, StorageSyncInfo} of
        %todo VFS-4847 refactor this case, use when wherever possible
        {undefined, undefined} when MTime < StMtime ->
            % file created locally and modified on storage
            location_and_link_utils:update_imported_file_location(FileCtx4, StSize),
            updated;

        {undefined, undefined} ->
            % file created locally and not modified on storage
            not_updated;

        {undefined, #document{value = #storage_sync_info{
            mtime = LastMtime,
            last_stat = LastStat
        }}} when LastMtime =:= StMtime
            andalso Size =:= StSize
            andalso LastStat > StMtime
        ->
            % file not replicated and already handled because LastStat > StMtime
            not_updated;

        {undefined, #document{value = #storage_sync_info{}}} ->
            case (MTime < StMtime) or (Size =/= StSize) of
                true ->
                    location_and_link_utils:update_imported_file_location(FileCtx4, StSize),
                    updated;
                false ->
                    not_updated
            end;

        {_, undefined} ->
            case LastReplicationTimestamp < StMtime of
                true ->
                    % file was modified after replication and has never been synced
                    case (MTime < StMtime) of
                        true ->
                            % file was modified on storage
                            location_and_link_utils:update_imported_file_location(FileCtx4, StSize),
                            updated;
                        false ->
                            % file was modified via onedata
                            not_updated
                    end;
                false ->
                    % file was replicated
                    not_updated
            end;

        {_, #document{value = #storage_sync_info{
            mtime = LastMtime,
            last_stat = LastStat
        }}} when LastMtime =:= StMtime
            andalso Size =:= StSize
            andalso LastStat > StMtime
        ->
            % file replicated and already handled because LastStat > StMtime
            not_updated;

        {_, #document{value = #storage_sync_info{}}} ->
            case LastReplicationTimestamp < StMtime of
                true ->
                    % file was modified after replication
                    case (MTime < StMtime) of
                        true ->
                            %there was modified on storage
                            location_and_link_utils:update_imported_file_location(FileCtx4, StSize),
                            updated;
                        false ->
                            % file was modified via onedata
                            not_updated
                    end;
                false ->
                    % file was replicated
                    not_updated
            end
    end,
    {StorageFileId, _} = file_ctx:get_storage_file_id(FileCtx4),
    storage_sync_info:update_mtime(StorageFileId, SpaceId, StMtime, NewLastStat),
    Result2.

-spec maybe_update_mode(#file_attr{}, helpers:stat(), file_ctx:ctx()) -> updated | not_updated.
maybe_update_mode(#file_attr{mode = OldMode}, #statbuf{st_mode = Mode}, FileCtx) ->
    case file_ctx:is_space_dir_const(FileCtx) of
        false ->
            case Mode band 8#1777 of
                OldMode ->
                    not_updated;
                NewMode ->
                    update_mode(FileCtx, NewMode),
                    updated
            end;
        _ ->
            not_updated
    end.

-spec update_mode(file_ctx:ctx(), file_meta:mode()) ->
    ok | fslogic_worker:fuse_response().
update_mode(FileCtx, NewMode) ->
    case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            ok;
        _ ->
            ok = attr_req:chmod_attrs_only_insecure(FileCtx, NewMode)
    end.

-spec maybe_update_times(#file_attr{}, helpers:stat(), file_ctx:ctx()) -> updated | not_updated.
maybe_update_times(#file_attr{atime = ATime, mtime = MTime, ctime = CTime},
    #statbuf{st_atime = StorageATime, st_mtime = StorageMTime, st_ctime = StorageCTime},
    _FileCtx
) when
    ATime >= StorageATime andalso
    MTime >= StorageMTime andalso
    CTime >= StorageCTime
->
    not_updated;
maybe_update_times(_FileAttr,
    #statbuf{st_atime = StorageATime, st_mtime = StorageMTime, st_ctime = StorageCTime},
    FileCtx
) ->
    ok = fslogic_times:update_times_and_emit(FileCtx,
        fun(T = #times{
            atime = ATime,
            mtime = MTime,
            ctime = CTime
        }) ->
            {ok, T#times{
                atime = max(StorageATime, ATime),
                mtime = max(StorageMTime, MTime),
                ctime = max(StorageCTime, CTime)
            }}
        end),
    updated.

-spec maybe_update_owner(#file_attr{}, storage_file_ctx:ctx(), file_ctx:ctx()) ->
    updated | not_updated.
maybe_update_owner(#file_attr{owner_id = OldOwnerId}, StorageFileCtx, FileCtx) ->
    case file_ctx:is_space_dir_const(FileCtx) of
        true -> not_updated;
        _ ->
            StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
            case get_owner_id(StorageFileCtx, StorageId) of
                {OldOwnerId, _StorageFileCtx2} ->
                    not_updated;
                {NewOwnerId, _StorageFileCtx2} ->
                    FileUuid = file_ctx:get_uuid_const(FileCtx),
                    {ok, _} = file_meta:update(FileUuid, fun(FileMeta = #file_meta{}) ->
                        {ok, FileMeta#file_meta{owner = NewOwnerId}}
                    end),
                    updated
            end
    end.

-spec create_file_meta(file_meta:uuid() | undefined, file_meta:name(),
    file_meta:mode(), od_user:id(), undefined | od_group:id(), file_meta:uuid(),
    od_space:id(), boolean()) -> {ok, file_meta:uuid()} | {error, term()}.
% TODO VFS-5273 - Maybe delete CreateLinks argument;
create_file_meta(FileUuid, FileName, Mode, OwnerId, GroupId, ParentUuid, SpaceId, CreateLinks) ->
    FileMetaDoc = file_meta:new_doc(FileUuid, FileName, file_meta:type(Mode),
        Mode band 8#1777, OwnerId, GroupId, ParentUuid, SpaceId),
    case CreateLinks of
        true ->
            case file_meta:create({uuid, ParentUuid}, FileMetaDoc) of
                {error, already_exists} ->
                    % there was race with creating file by lfm
                    % file will be imported with suffix
                    FileName2 = ?IMPORTED_CONFLICTING_FILE_NAME(FileName),
                    FileMetaDoc2 = file_meta:new_doc(FileUuid, FileName2, file_meta:type(Mode),
                        Mode band 8#1777, OwnerId, GroupId, ParentUuid, SpaceId),
                    file_meta:create({uuid, ParentUuid}, FileMetaDoc2);
                Other ->
                    Other
            end;
        _ ->
            file_meta:save(FileMetaDoc, false)
    end.

-spec create_times(file_meta:uuid(), times:time(), times:time(), times:time(), od_space:id()) ->
    {ok, datastore:key()}.
create_times(FileUuid, MTime, ATime, CTime, SpaceId) ->
    times:save(#document{
        key = FileUuid,
        value = #times{
            mtime = MTime,
            atime = ATime,
            ctime = CTime
        },
        scope = SpaceId}
    ).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns owner id of given file, acquired from reverse LUMA.
%% @end
%%-------------------------------------------------------------------
-spec get_owner_id(storage_file_ctx:ctx(), storage:id()) -> {od_user:id(), storage_file_ctx:ctx()}.
get_owner_id(StorageFileCtx, StorageId) ->
    {StatBuf, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    #statbuf{st_uid = Uid} = StatBuf,
    {ok, OwnerId} = reverse_luma:get_user_id(Uid, StorageId),
    {OwnerId, StorageFileCtx2}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns group owner id of given file, acquired from reverse LUMA.
%% @end
%%-------------------------------------------------------------------
-spec get_group_owner_id(storage_file_ctx:ctx(), od_space:id(), storage:id()) ->
    {od_group:id() | undefined, storage_file_ctx:ctx()}.
get_group_owner_id(StorageFileCtx, SpaceId, StorageId) ->
    {StatBuf, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    #statbuf{st_gid = Gid} = StatBuf,
    try
        {ok, GroupId} = reverse_luma:get_group_id(Gid, SpaceId, StorageId),
        {GroupId, StorageFileCtx2}
    catch
        _:Reason ->
            ?error_stacktrace("Resolving group with Gid ~p failed due to ~p", [Gid, Reason]),
            {undefined, StorageFileCtx2}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Import file's nfs4 ACL.
%% @end
%%-------------------------------------------------------------------
-spec import_nfs4_acl(file_ctx:ctx(), storage_file_ctx:ctx()) -> ok.
import_nfs4_acl(FileCtx, StorageFileCtx) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            ok;
        false ->
            try
                {ACLBin, _} = storage_file_ctx:get_nfs4_acl(StorageFileCtx),
                {ok, NormalizedACL} = nfs4_acl:decode_and_normalize(ACLBin, SpaceId, StorageId),
                {SanitizedAcl, FileCtx2} = sanitize_acl(NormalizedACL, FileCtx),
                #provider_response{status = #status{code = ?OK}} =
                    acl_req:set_acl(UserCtx, FileCtx2, SanitizedAcl),
                ok
            catch
                throw:?ENOTSUP ->
                    ok;
                throw:?ENOENT ->
                    ok;
                throw:?ENODATA ->
                    ok
            end
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's nfs4 ACL if it has CHANGED.
%% @end
%%-------------------------------------------------------------------
-spec maybe_update_nfs4_acl(storage_file_ctx:ctx(), file_ctx:ctx(),
    SyncAcl :: boolean()) -> updated | not_updated.
maybe_update_nfs4_acl(_StorageFileCtx, _FileCtx, false) ->
    not_updated;
maybe_update_nfs4_acl(StorageFileCtx, FileCtx, true) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            not_updated;
        false ->
            SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
            StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
            #provider_response{provider_response = ACL} = acl_req:get_acl(UserCtx, FileCtx),
            try
                {ACLBin, _} = storage_file_ctx:get_nfs4_acl(StorageFileCtx),
                {ok, NormalizedNewACL} = nfs4_acl:decode_and_normalize(ACLBin, SpaceId, StorageId),
                {SanitizedAcl, FileCtx2} = sanitize_acl(NormalizedNewACL, FileCtx),
                case #acl{value = SanitizedAcl} of
                    ACL ->
                        not_updated;
                    _ ->
                        #provider_response{status = #status{code = ?OK}} =
                            acl_req:set_acl(UserCtx, FileCtx2, SanitizedAcl),
                        updated
                end
            catch
                throw:?ENOTSUP ->
                    not_updated;
                throw:?ENOENT ->
                    not_updated;
                throw:?ENODATA ->
                    ok
            end
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Filters given acl leaving only `allow` and `deny` aces. Also disables
%% unknown/unsupported flags (aceflags) and operations (acemask).
%% @end
%%-------------------------------------------------------------------
-spec sanitize_acl(acl:acl(), file_ctx:ctx()) -> {acl:acl(), file_ctx:ctx()}.
sanitize_acl(Acl, FileCtx) ->
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
    AllPerms = case IsDir of
        true -> ?all_container_perms_mask;
        false -> ?all_object_perms_mask
    end,

    SanitizedAcl = lists:filtermap(fun(#access_control_entity{
        acetype = Type,
        aceflags = Flags,
        acemask = Mask
    } = Ace) ->
        case lists:member(Type, [?allow_mask, ?deny_mask]) of
            true ->
                {true, Ace#access_control_entity{
                    aceflags = Flags band ?identifier_group_mask,
                    acemask = Mask band AllPerms
                }};
            false ->
                false
        end
    end, Acl),

    {SanitizedAcl, FileCtx2}.

-spec is_suffixed(file_meta:name()) -> {true, file_meta:uuid(), file_meta:name()} | false.
is_suffixed(FileName) ->
    Tokens = binary:split(FileName, ?CONFLICTING_STORAGE_FILE_SUFFIX_SEPARATOR, [global]),
    case lists:reverse(Tokens) of
        [FileName] ->
            false;
        [FileUuid | Tokens2] ->
            FileName2 = list_to_binary(lists:reverse(Tokens2)),
            {true, FileUuid, FileName2}
    end.