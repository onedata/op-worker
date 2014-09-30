%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: FSLogic rename request implementation.
%% @end
%% ===================================================================
-module(fslogic_req_rename_impl).
-author("Rafal Slota").

-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include_lib("ctool/include/logging.hrl").

%% Types for internal use only.
-type operation_info() :: #{transfer_type => none | move | link, source_fileid => storage_file_id(), source_path => path(),
                            target_path => path(), file_doc => file_doc(), storage => storage_doc() | undefined,
                            target_fileid => storage_file_id() | undefined}.

-type operation_status_pos() :: {ok, operation_info()}.
-type operation_status_neg() :: {error, operation_info()}.
-type operation_status_any() :: operation_status_pos() | operation_status_neg().


%% API
-export([rename_file_trivial/3, rename_file_interspace/4, rename_file_interprovider/4]).
-export([common_assertions/3]).


%% ====================================================================
%% API functions
%% ====================================================================


%% common_assertions/3
%% ====================================================================
%% @doc Common assertions for rename operation. Shall be used prior to every rename operation
%%      that is handled as 'internal' (not inter-provider).
%%      This function returns bunch of useful common data or fails with exception.
%% @end
-spec common_assertions(UserDoc :: user_doc(), SourceFilePath :: path(), TargetFilePath :: path()) ->
    {ok, OldFile :: file_doc(), OldDoc :: file_doc(), NewParentUUID :: uuid()} | no_return().
%% ====================================================================
common_assertions(UserDoc, SourceFilePath, TargetFilePath) ->
    NewDir = fslogic_path:strip_path_leaf(TargetFilePath),

    {ok, #db_document{record = #file{} = OldFile} = OldDoc} = fslogic_objects:get_file(SourceFilePath),
    {ok, #db_document{uuid = NewParentUUID} = NewParentDoc} = fslogic_objects:get_file(NewDir),

    ok = fslogic_perms:check_file_perms(SourceFilePath, UserDoc, OldDoc, delete),
    ok = fslogic_perms:check_file_perms(NewDir, UserDoc, NewParentDoc, write),

    {ok, OldFile, OldDoc, NewParentUUID}.


%% rename_file_trivial/3
%% ====================================================================
%% @doc One of rename operation's implementation. Trivial rename is the fastest one
%%      and also the safest one since the whole operation is fully atomic.
%%
%%      Precondition: Both SourceFilePath and TargetFilePath are pointing to the same space (have common prefix like /spaces/some_name).
%% @end
-spec rename_file_trivial(SourceFilePath :: path(), TargetFilePath :: path(),
    {OldFile :: file_info(), OldFileDoc :: file_doc(), NewParentUUID :: uuid()}) ->
    ok | no_return().
%% ====================================================================
rename_file_trivial(SourceFilePath, TargetFilePath, {OldFile, OldFileDoc, NewParentUUID}) ->
    ?debug("rename_file_trivial ~p ~p", [SourceFilePath, TargetFilePath]),

    RenamedFileInit =
        OldFile#file{parent = NewParentUUID, name = fslogic_path:basename(TargetFilePath)},

    RenamedFile = fslogic_meta:update_meta_attr(RenamedFileInit, ctime, vcn_utils:time()),
    Renamed = OldFileDoc#db_document{record = RenamedFile},

    {ok, _} = fslogic_objects:save_file(Renamed),

    CTime = vcn_utils:time(),
    fslogic_meta:update_parent_ctime(fslogic_path:get_user_file_name(SourceFilePath), CTime),
    fslogic_meta:update_parent_ctime(fslogic_path:get_user_file_name(TargetFilePath), CTime),

    ok.


%% rename_file_interspace/4
%% ====================================================================
%% @doc One of rename operation's implementation. Inter-Space rename is quite fast
%%      since meta-tree move is atomic. Only regular files are handled one by one, so in case of failure
%%      only those can end up not renamed.
%%
%%      Precondition: Both SourceFilePath and TargetFilePath are pointing spaces that are supported by this provider.
%% @end
-spec rename_file_interspace(UserDoc :: user_doc(), SourceFilePath :: path(), TargetFilePath :: path(),
    {OldFile :: file_info(), OldFileDoc :: file_doc(), NewParentUUID :: uuid()}) ->
    ok | no_return().
%% ====================================================================
rename_file_interspace(UserDoc, SourceFilePath, TargetFilePath, {_, _, NewParentUUID}) ->
    ?debug("rename_file_interspace ~p ~p", [SourceFilePath, TargetFilePath]),

    SourceFileTokens = filename:split(SourceFilePath),
    TargetFileTokens = filename:split(TargetFilePath),
    {ok, #space_info{} = SourceSpaceInfo} = fslogic_utils:get_space_info_for_path(SourceFilePath),
    {ok, #space_info{} = TargetSpaceInfo} = fslogic_utils:get_space_info_for_path(TargetFilePath),

    %% [{SourcePath, TargetPath}]
    AllRegularFiles = fslogic_utils:path_walk(SourceFilePath, [],
        fun(ElemPath, ElemType, AccIn) ->
            case ElemType of
                ?REG_TYPE_PROT ->
                    ElemTokens = filename:split(ElemPath),
                    ElemTargetTokens = TargetFileTokens ++ lists:sublist(ElemTokens, length(SourceFileTokens) + 1, length(ElemTokens)),
                    [{ElemPath, fslogic_path:absolute_join(ElemTargetTokens)} | AccIn];
                _ ->
                    AccIn
            end
        end),

    StorageMoveResult = lists:map(
        fun({SourceFile, TargetFile}) ->
            case rename_on_storage(UserDoc, TargetSpaceInfo, SourceFile, TargetFile) of
                {ok, #{target_fileid := NewFileID, source_path := SourceSubFilePath, file_doc := SubFileDoc} = OPInfo} ->
                    case update_moved_file(SourceSubFilePath, SubFileDoc, NewFileID, 3) of
                        ok ->
                            {ok, OPInfo};
                        {error, _} ->
                            {error, OPInfo}
                    end;
                {error, Resaon} -> {error, Resaon}
            end
        end, AllRegularFiles),

    {GoodRes, BadRes} =
        lists:partition(
            fun(Status) ->
                case Status of
                    {ok, _} -> true;
                    {error, _} -> false
                end
            end, StorageMoveResult),

    ok = fslogic_utils:run_as_root(fun() -> storage_rollback(SourceSpaceInfo, BadRes) end),

    try
        {ok, #db_document{record = #file{} = OldFile} = OldFileDoc} = fslogic_objects:get_file(SourceFilePath),
        ok = rename_file_trivial(SourceFilePath, TargetFilePath, {OldFile, OldFileDoc, NewParentUUID}),
        catch storage_cleanup(GoodRes) %% We don't really care whether cleanup fails or not.
    catch
        _:Reason ->
            ?error_stacktrace("Unable to move file tree due to: ~p. Initializing rollback...", [Reason]),
            fslogic_utils:run_as_root(
                fun() ->
                    storage_rollback(SourceSpaceInfo, GoodRes ++ BadRes),
                    db_rollback(GoodRes ++ BadRes)
                end),
            throw({?VEREMOTEIO, Reason})
    end,

    case BadRes of
        [] -> ok;
        _  ->
            reconstuct_tree_for_zombie_files(BadRes),
            throw({?VEREMOTEIO, {files_not_moved, BadRes}})
    end.


%% rename_file_interprovider/4
%% ====================================================================
%% @doc One of rename operation's implementation. Inter-Provider rename is the most general one.
%%      Each and every file is handled separately. In case of failure, operation will be aborted (exact behaviour is not specified)
%%      and all files that weren't transferred successfully will be available at source location while successful ones
%%      will be available at target location.
%%      Since there are not preconditions for this function call, it can be used instead any other rename_file_* implementation
%%      although there can be significant negative performance and data consistency impact.
%%
%%      For best performance, this function should be used by provider that supports source space.
%%
%%      Precondition: none.
%% @end
-spec rename_file_interprovider(UserDoc :: user_doc(), FileType :: file_type_protocol(), SourceFilePath :: path(), TargetFilePath :: path()) ->
    ok | no_return().
%% ====================================================================
rename_file_interprovider(UserDoc, ?DIR_TYPE_PROT, SourceFilePath, TargetFilePath) ->
    ?debug("rename_file_interprovider DIR ~p ~p", [SourceFilePath, TargetFilePath]),

    ok = logical_files_manager:mkdir(TargetFilePath),

    Self = self(),
    DN_CTX = fslogic_context:get_user_dn(),
    {AT_CTX1, AT_CTX2} = fslogic_context:get_gr_auth(),

    PIDs = lists:map(
        fun(#dir_entry{name = FileName, type = FileType}) ->
%%             Uncomment for parallel renaming
%%             spawn_monitor(
%%                 fun() ->
            fslogic_context:set_user_dn(DN_CTX),
            fslogic_context:set_gr_auth(AT_CTX1, AT_CTX2),
            NewSourceFilePath = filename:join(SourceFilePath, FileName),
            NewTargetFilePath = filename:join(TargetFilePath, FileName),

            rename_file_interprovider(UserDoc, FileType, NewSourceFilePath, NewTargetFilePath),
            Self ! {self(), ok}
%%                 end)
        end, fslogic_utils:list_dir(SourceFilePath)),

    Res = lists:map(
        fun({Pid, _}) ->
            receive
                {Pid, ok} -> ok;
                {'DOWN', _MonitorRef, process, Pid, Info} ->
                    {error, Info}
            end
        end, PIDs),

    Errors = lists:filter(fun(Elem) -> Elem =/= ok end, Res),
    [] = Errors,

    ok = logical_files_manager:rmdir(SourceFilePath);
rename_file_interprovider(_UserDoc, ?LNK_TYPE_PROT, SourceFilePath, TargetFilePath) ->
    ?debug("rename_file_interprovider LNK ~p ~p", [SourceFilePath, TargetFilePath]),

    {ok, LinkValue} = logical_files_manager:read_link(SourceFilePath),
    ok = logical_files_manager:create_symlink(LinkValue, TargetFilePath),
    ok = logical_files_manager:rmlink(SourceFilePath);
rename_file_interprovider(_UserDoc, ?REG_TYPE_PROT, SourceFilePath, TargetFilePath) ->
    ?debug("rename_file_interprovider REG ~p ~p", [SourceFilePath, TargetFilePath]),

    ok = logical_files_manager:create(TargetFilePath),
    ok = transfer_data(SourceFilePath, TargetFilePath),
    ok = logical_files_manager:delete(SourceFilePath).


%% ====================================================================
%% Internal functions
%% ====================================================================


%% transfer_data/2
%% ====================================================================
%% @doc Copies data from SourceFilePath to TargetFilePath. Target will be overridden.
%%      Both files have to be existing valid regular files.
%% @end
-spec transfer_data(SourceFilePath :: path(), TargetFilePath :: path()) ->
    ok | {error, {write | read, ValidBytes :: non_neg_integer(), Reason :: any()}}.
%% ====================================================================
transfer_data(SourceFilePath, TargetFilePath) ->
    {ok, BlockSize} = oneprovider_node_app:get_env(provider_proxy_block_size),
    transfer_data4(SourceFilePath, TargetFilePath, 0, BlockSize).
transfer_data4(SourceFilePath, TargetFilePath, Offset, Size) ->
    case logical_files_manager:read(SourceFilePath, Offset, Size) of
        {ok, Data} ->
            DataSize = size(Data),
            case logical_files_manager:write(TargetFilePath, Offset, Data) of
                {_, _} = WriteError ->
                    {error, {write, Offset, WriteError}};
                BytesWritten0 when is_integer(BytesWritten0), DataSize < Size, DataSize =:= BytesWritten0 ->
                    ok;
                BytesWritten1 when BytesWritten1 > 0 ->
                    transfer_data4(SourceFilePath, TargetFilePath, Offset + BytesWritten1, Size);
                _ ->
                    {error, {write, Offset, no_bytes_written}}
            end;
        {error, Reason} ->
            {error, {read, Offset, Reason}};
        ReasonCompat ->
            {error, {read, Offset, ReasonCompat}}
    end.


%% reconstuct_tree_for_zombie_files/1
%% ====================================================================
%% @doc For each operation_status_neg() runs {@link reconstuct_tree_for_zombie_file/2}. Any argument that does not give
%%      required information is skipped.
%% @end
-spec reconstuct_tree_for_zombie_files([OPStatus :: operation_status_neg() | any()]) ->
    [{error, Reason :: any()}].
%% ====================================================================
reconstuct_tree_for_zombie_files([]) ->
    [];
reconstuct_tree_for_zombie_files([{error, #{file_doc := FileDoc, source_path := SourceFilePath}} | T]) ->
    try reconstuct_tree_for_zombie_file(SourceFilePath, FileDoc) of
        ok -> reconstuct_tree_for_zombie_files(T)
    catch
        _:Reason ->
            ?error_stacktrace("[ ZOMBIE FILE ] Unable to reconstruct file tree ~p for file ~p die to: ~p", [SourceFilePath, FileDoc, Reason]),
            [{error, Reason} | reconstuct_tree_for_zombie_files(T)]
    end;
reconstuct_tree_for_zombie_files([_ | T]) ->
    reconstuct_tree_for_zombie_files(T).


%% reconstuct_tree_for_zombie_file/2
%% ====================================================================
%% @doc For given (already moved) file_doc(), tries to revert this file document to original location.
%%      This operation may require to recreate some directories in source location.
%% @end
-spec reconstuct_tree_for_zombie_file(SourceFilePath :: path(), FileDoc :: file_doc()) -> ok | no_return().
%% ====================================================================
reconstuct_tree_for_zombie_file(SourceFilePath, #db_document{record = #file{type = ?REG_TYPE} = File} = FileDoc) ->
    SourceParentPath = fslogic_path:strip_path_leaf(SourceFilePath),
    [?SPACES_BASE_DIR_NAME, SpaceDir | SourceParentTokens] = fslogic_path:split(SourceParentPath),
    lists:foldl(
        fun(NextDirName, CurrTokens) ->
            NextTokens = CurrTokens ++ [NextDirName],
            logical_files_manager:mkdir(filename:join(NextTokens)),
            NextTokens
        end, [?SPACES_BASE_DIR_NAME, SpaceDir], SourceParentTokens),

    {ok, #db_document{uuid = ParentUUID}} = fslogic_objects:get_file(SourceParentPath),
    {ok, _} = fslogic_objects:save_file(FileDoc#db_document{force_update = true, record = File#file{parent = ParentUUID}});
reconstuct_tree_for_zombie_file(_SourceFilePath, #db_document{record = #file{}}) ->
    ok.


%% storage_cleanup/1
%% ====================================================================
%% @doc For each operation_status_pos() cleanups storage (i.e. removes unneeded hard-links).
%% @end
-spec storage_cleanup([operation_status_pos()]) -> [{error, {Reason :: any(), OPInfo :: operation_info()}}].
%% ====================================================================
storage_cleanup([]) ->
    [];
storage_cleanup([{ok, #{transfer_type := move}} | T]) ->
    storage_cleanup(T);
storage_cleanup([{ok, #{transfer_type := link, source_fileid := FileID, storage := Storage} = OPInfo} | T]) ->
    SHInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
    case storage_files_manager:delete(SHInfo, FileID) of
        ok -> storage_cleanup(T);
        {error, Reason} ->
            ?error("Unable to delete unused file ~p on storage ~p due to: ~p", [FileID, Storage, Reason]),
            [{error, {Reason, OPInfo}} | storage_cleanup(T)]
    end.


%% db_rollback/1
%% ====================================================================
%% @doc For each operation_status_any() reverts changes in DB if there were any.
%% @end
-spec db_rollback([OPStatus :: operation_status_any()]) -> [ok | {error, Reason :: any()}].
%% ====================================================================
db_rollback([]) ->
    [];
db_rollback([{_, #{source_fileid := FileID, source_path := SourceFilePath, file_doc := FileDoc}} | T]) ->
    [update_moved_file(SourceFilePath, FileDoc, FileID, 3) | db_rollback(T)];
db_rollback([_ | T]) ->
    db_rollback(T).


%% update_moved_file/4
%% ====================================================================
%% @doc Updates given file_doc() with new storage_file_id().
%% @end
-spec update_moved_file(SourceFilePath :: path(), FileDoc :: file_doc(), NewFileId :: storage_file_id(), RetryCount :: non_neg_integer()) ->
    ok | {error, Reason :: any()}.
%% ====================================================================
update_moved_file(SourceFilePath, #db_document{record = #file{location = Location} = File, uuid = FileUUID} = FileDoc, NewFileID, RetryCount) ->
    NewFile = File#file{location = Location#file_location{file_id = NewFileID}},

    case fslogic_objects:save_file(FileDoc#db_document{record = NewFile}) of
        {ok, _} -> ok;
        {error, Reason} when RetryCount > 0 ->
            ?error("Unable to save updated file document for file ~p due to: ~p", [FileUUID, Reason]),
            case fslogic_objects:get_file(SourceFilePath) of
                {ok, NewFileDoc} ->
                    update_moved_file(SourceFilePath, NewFileDoc, NewFileID, RetryCount - 1);
                {error, GetError} ->
                    ?error("Unable to refresh file document for file ~p due to: ~p", [FileUUID, GetError]),
                    update_moved_file(SourceFilePath, FileDoc, NewFileID, RetryCount - 1)
            end;
        {error, Reason1} ->
            ?error("Unable to update storage fileId for file ~p due to: ~p", [FileUUID, Reason1]),
            {error, {FileUUID, Reason1}}
    end.


%% storage_rollback/2
%% ====================================================================
%% @doc For each operation_status_any() reverts the operation on storage.
%% @end
-spec storage_rollback(SourceSpaceInfo :: space_info(), [OPStatus :: operation_status_any()]) -> ok.
%% ====================================================================
storage_rollback(_, []) ->
    ok;
storage_rollback(#space_info{} = SourceSpaceInfo,
    [{_, #{transfer_type := TransferType, source_fileid := FileID,
           target_fileid := NewFileID,    storage       := Storage}} | T]) when is_atom(TransferType) ->
    SHInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),

    case TransferType of
        none -> ok;
        link -> storage_files_manager:delete(SHInfo, NewFileID);
        move ->
            storage_files_manager:mv(SHInfo, NewFileID, FileID),
            storage_files_manager:chown(SHInfo, FileID, -1, fslogic_spaces:map_to_grp_owner(SourceSpaceInfo))
    end,

    storage_rollback(SourceSpaceInfo, T);
storage_rollback(#space_info{} = SourceSpaceInfo, [{_, _} | T]) ->
    storage_rollback(SourceSpaceInfo, T).


%% rename_on_storage/4
%% ====================================================================
%% @doc Makes given file accessible on target space (storage level). If only possible (supported) this is done
%%      using hard links in order to simulate semi-atomicity of the whole rename operation. In this case old hard link is NOT
%%      removed. If storage does not support hard links, file is moved which means that no cleanup is needed afterwards.
%%      This function returns operation_status_any() which gives just enough information to find out what went wrong and what
%%      kind of cleanup / recovery is required.
%%
%%      operation_info() field semantics:
%%          transfer_type => 'none' (file wasn't moved at all), 'link' (file was hard-linked) or 'move' (file was moved)
%%          source_fileid => initial storage fileId
%%          source_path and source_path => logical paths to source and target file location
%%          storage => if this field doesn't exist, operation failed before storage information was selected
%%          target_fileid => storage target (destination) fileId. If this field doesn't exist, operation failed before the fileId was generated
%%      If both 'storage' and 'target_fileid' are set, 'transfer_type' is not 'none' and operation has failed,
%%      file was moved/linked but is not viable to use. Normally rollback is required in that case although if file was linked, old - source_fileid can still be used.
%% @end
-spec rename_on_storage(UserDoc :: user_doc(), TargetSpaceInfo :: space_info(), SourceFilePath :: path(), TargetFilePath :: path()) ->
    OPStatus :: operation_status_any() | {error, Reason :: any()}.
%% ====================================================================
rename_on_storage(UserDoc, TargetSpaceInfo, SourceFilePath, TargetFilePath) ->
    try
        {ok, #db_document{record = File} = FileDoc} = fslogic_objects:get_file(SourceFilePath),
        StorageID   = File#file.location#file_location.storage_id,
        FileID      = File#file.location#file_location.file_id,

        OPInfo0 = #{transfer_type => none, source_fileid => FileID,
            source_path => SourceFilePath, source_path => TargetFilePath, file_doc => FileDoc},

        Storage = %% Storage info for the file
        case dao_lib:apply(dao_vfs, get_storage, [{uuid, StorageID}], 1) of
            {ok, #db_document{record = #storage_info{} = S}} -> S;
            {error, MReason} ->
                ?error("Cannot fetch storage (ID: ~p) information for file ~p. Reason: ~p", [StorageID, SourceFilePath, MReason]),
                throw(OPInfo0)
        end,

        OPInfo1 = OPInfo0#{storage => Storage},

        SHInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage), %% Storage helper for cluster
        NewFileID = fslogic_storage:get_new_file_id(TargetSpaceInfo, TargetFilePath, UserDoc, SHInfo, fslogic_context:get_protocol_version()),
        {ok, #st_stat{st_uid = OwnerUID}} = storage_files_manager:getattr(SHInfo, FileID),

        OPInfo2 = OPInfo1#{target_fileid => NewFileID},

        TransferType =
            case fslogic_utils:run_as_root(fun() -> storage_files_manager:link(SHInfo, FileID, NewFileID) end) of
                ok -> link;
                {error, ErrCode} ->
                    ?warning("Cannot move file using hard links '~p' -> '~p' due to: ~p", [FileID, NewFileID, ErrCode]),
                    case fslogic_utils:run_as_root(fun() -> storage_files_manager:mv(SHInfo, FileID, NewFileID) end) of
                        ok -> move;
                        {error, ErrCode1} ->
                            ?error("Cannot move file '~p' -> '~p' due to: ~p", [FileID, NewFileID, ErrCode1]),
                            throw(OPInfo2);
                        ErrCode1Compat ->
                            ?error("Cannot move file '~p' -> '~p' due to: ~p", [FileID, NewFileID, ErrCode1Compat]),
                            throw(OPInfo2)
                    end
            end,

        OPInfo3 = OPInfo2#{transfer_type := TransferType},

        %% Change group owner
        case fslogic_utils:run_as_root(fun() -> storage_files_manager:chown(SHInfo, NewFileID, OwnerUID, fslogic_spaces:map_to_grp_owner(TargetSpaceInfo)) end) of
            ok -> ok;
            MReason1 ->
                ?error("Cannot change group owner for file (ID: ~p) to ~p due to: ~p.", [NewFileID, fslogic_spaces:map_to_grp_owner(TargetSpaceInfo), MReason1]),
                throw(OPInfo3)
        end,

        {ok, OPInfo3}
    catch
        throw:Reason ->
            {error, Reason};
        _:Reason1 ->
            ?error_stacktrace("Unknown error while renaming file on storage: ~p", [Reason1]),
            {error, Reason1}
    end.