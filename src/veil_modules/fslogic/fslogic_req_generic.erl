%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: FSLogic generic request handlers.
%% @end
%% ===================================================================
-module(fslogic_req_generic).
-author("Rafal Slota").

-include("veil_modules/dao/dao.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([update_times/4, change_file_owner/3, change_file_group/3, change_file_perms/2, get_file_attr/1, delete_file/1, rename_file/2, get_statfs/0]).

%% ====================================================================
%% API functions
%% ====================================================================


%% update_times/4
%% ====================================================================
%% @doc Updates file's access times.
%% @end
-spec update_times(FullFileName :: string(), ATime :: non_neg_integer(),
    MTime :: non_neg_integer(), CTime :: non_neg_integer()) ->
    #atom{} | no_return().
%% ====================================================================
update_times(FullFileName, ATime, MTime, CTime) ->
    ?debug("update_times(FullFileName: ~p, ATime: ~p, MTime: ~p, CTime: ~p)", [FullFileName, ATime, MTime, CTime]),
    case FullFileName of
        [?PATH_SEPARATOR] ->
            ?warning("Trying to update times for root directory. FuseID: ~p. Aborting.", [fslogic_context:get_fuse_id()]),
            throw(invalid_updatetimes_request);
        _ -> ok
    end,

    {ok, #veil_document{record = #file{} = File} = FileDoc} = fslogic_objects:get_file(FullFileName),

    File1 = fslogic_meta:update_meta_attr(File, times, {ATime, MTime, CTime}),

    Status = string:equal(File1#file.meta_doc, File#file.meta_doc),
    if
        Status -> #atom{value = ?VOK};
        true ->
            {ok, _} = fslogic_objects:save_file(FileDoc#veil_document{record = File1})
    end.


%% change_file_owner/3
%% ====================================================================
%% @doc Changes file's owner.
%% @end
-spec change_file_owner(FullFileName :: string(), NewUID :: non_neg_integer(), NewUName :: string()) ->
    #atom{} | no_return().
%% ====================================================================
change_file_owner(FullFileName, NewUID, NewUName) ->
    ?debug("change_file_owner(FullFileName: ~p, NewUID: ~p, NewUName: ~p)", [FullFileName, NewUID, NewUName]),

    {ok, #veil_document{record = #file{} = File} = FileDoc} = fslogic_objects:get_file(FullFileName),
    {ok, UserDoc} = fslogic_objects:get_user(),

    ok = fslogic_perms:check_file_perms(FullFileName, UserDoc, FileDoc, root),

    NewFile =
        case user_logic:get_user({login, NewUName}) of
            {ok, #veil_document{record = #user{}, uuid = UID}} ->
                File#file{uid = UID};
            {error, user_not_found} ->
                ?warning("chown: cannot find user with name ~p. lTrying UID (~p) lookup...", [NewUName, NewUID]),
                case dao_lib:apply(dao_users, get_user, [{uuid, integer_to_list(NewUID)}], fslogic_context:get_protocol_version()) of
                    {ok, #veil_document{record = #user{}, uuid = UID1}} ->
                        File#file{uid = UID1};
                    {error, {not_found, missing}} ->
                        ?warning("chown: cannot find user with uid ~p", [NewUID]),
                        throw(?VEINVAL);
                    {error, Reason1} ->
                        ?error("chown: cannot find user with uid ~p due to error: ~p", [NewUID, Reason1]),
                        throw(?VEREMOTEIO)
                end;
            {error, Reason1} ->
                ?error("chown: cannot find user with uid ~p due to error: ~p", [NewUID, Reason1]),
                throw(?VEREMOTEIO)
        end,
    NewFile1 = fslogic_meta:update_meta_attr(NewFile, ctime, vcn_utils:time()),

    {ok, _} = fslogic_objects:save_file(FileDoc#veil_document{record = NewFile1}),

    #atom{value = ?VOK}.


%% change_file_group/3
%% ====================================================================
%% @doc Changes file's group owner.
%%      Operation currently not supported.
%% @end
-spec change_file_group(FullFileName :: string(), NewGID :: non_neg_integer(), NewGName :: string()) ->
    #atom{} | no_return().
%% ====================================================================
change_file_group(_FullFileName, _GID, _GName) ->
    ?debug("change_file_group(FullFileName: ~p, GID: ~p, GName: ~p)", [_FullFileName, _GID, _GName]),
    #atom{value = ?VENOTSUP}.


%% change_file_perms/2
%% ====================================================================
%% @doc Changes file permissions.
%% @end
-spec change_file_perms(FullFileName :: string(), Perms :: non_neg_integer()) ->
    #atom{} | no_return().
%% ====================================================================
change_file_perms(FullFileName, Perms) ->
    ?debug("change_file_perms(FullFileName: ~p, Perms: ~p)", [FullFileName, Perms]),
    {ok, UserDoc} = fslogic_objects:get_user(),
    {ok, #veil_document{record = #file{perms = ActualPerms, location = #file_location{storage_id = StorageId, file_id = FileId}} = File} = FileDoc} =
        fslogic_objects:get_file(FullFileName),

    ok = fslogic_perms:check_file_perms(FullFileName, UserDoc, FileDoc, owner),

    NewFile = fslogic_meta:update_meta_attr(File, ctime, vcn_utils:time()),
    NewFile1 = FileDoc#veil_document{record = NewFile#file{perms = Perms}},
    {ok, _} = fslogic_objects:save_file(NewFile1),

    case (ActualPerms == Perms orelse StorageId == []) of
        true -> ok;
        false ->
            {ok, #veil_document{record = Storage}} = fslogic_objects:get_storage({uuid, StorageId}),
            {SH, File_id} = fslogic_utils:get_sh_and_id(?CLUSTER_FUSE_ID, Storage, FileId),
            storage_files_manager:chmod(SH, File_id, Perms)
    end,

    #atom{value = ?VOK}.


%% get_file_attr/2
%% ====================================================================
%% @doc Gets file's attributes.
%% @end
-spec get_file_attr(FullFileName :: string()) ->
    #fileattr{} | no_return().
%% ====================================================================
get_file_attr(FileDoc = #veil_document{record = #file{}}) ->
    #veil_document{record = #file{} = File, uuid = FileUUID} = FileDoc,
    Type = fslogic_file:normalize_file_type(protocol, File#file.type),
    Size = fslogic_file:get_real_file_size(File),

    fslogic_file:update_file_size(File, Size),

    %% Get owner
    {UName, UID} = fslogic_file:get_file_owner(File),

    {ok, FilePath} = logical_files_manager:get_file_full_name_by_uuid(FileUUID),
    {ok, #space_info{name = SpaceName} = SpaceInfo} = fslogic_utils:get_space_info_for_path(FilePath),

    %% Get attributes
    {CTime, MTime, ATime, _SizeFromDB} =
        case dao_lib:apply(dao_vfs, get_file_meta, [File#file.meta_doc], 1) of
            {ok, #veil_document{record = FMeta}} ->
                {FMeta#file_meta.ctime, FMeta#file_meta.mtime, FMeta#file_meta.atime, FMeta#file_meta.size};
            {error, Error} ->
                ?warning("Cannot fetch file_meta for file (uuid ~p) due to error: ~p", [FileUUID, Error]),
                {0, 0, 0, 0}
        end,

    %% Get file links
    Links = case Type of
                "DIR" ->
                    case dao_lib:apply(dao_vfs, count_subdirs, [{uuid, FileUUID}], fslogic_context:get_protocol_version()) of
                         {ok, Sum} -> Sum + 2;
                         _Other ->
                             ?error("Error: can not get number of links for file: ~s", [File]),
                             0
                     end;
                _ -> 1
            end,

    #fileattr{answer = ?VOK, mode = File#file.perms, atime = ATime, ctime = CTime, mtime = MTime,
        type = Type, size = Size, uname = UName, gname = unicode:characters_to_list(SpaceName), uid = UID, gid = fslogic_spaces:map_to_grp_owner(SpaceInfo), links = Links};
get_file_attr(FullFileName) ->
    ?debug("get_file_attr(FullFileName: ~p)", [FullFileName]),
    case fslogic_objects:get_file(FullFileName) of
        {ok, FileDoc} ->            %% Throw VENOENT in order not to trigger error-log
            get_file_attr(FileDoc); %% which would be unnecessary since get_file_attr is also used to check
        {error, file_not_found} ->  %% if the file exists
            throw(?VENOENT)
    end.


%% delete_file/1
%% ====================================================================
%% @doc Deletes file.
%% @end
-spec delete_file(FullFileName :: string()) ->
    #atom{} | no_return().
%% ====================================================================
delete_file(FullFileName) ->
    ?debug("delete_file(FullFileName: ~p)", [FullFileName]),
    {ok, FileDoc} = fslogic_objects:get_file(FullFileName),
    {ok, UserDoc} = fslogic_objects:get_user(),

    ok = fslogic_perms:check_file_perms(FullFileName, UserDoc, FileDoc, delete),

    FileDesc = FileDoc#veil_document.record,
    {ok, ChildrenTmpAns} =
        case FileDesc#file.type of
            ?DIR_TYPE ->
                dao_lib:apply(dao_vfs, list_dir, [FullFileName, 1, 0], fslogic_context:get_protocol_version());
            _OtherType -> {ok, []}
        end,

    case length(ChildrenTmpAns) of
        0 ->
            ok = dao_lib:apply(dao_vfs, remove_file, [FullFileName], fslogic_context:get_protocol_version()),

            fslogic_meta:update_parent_ctime(fslogic_path:get_user_file_name(FullFileName), vcn_utils:time()),
            #atom{value = ?VOK};
        _Other ->
            ?error("Error: can not remove directory (it's not empty): ~s", [FullFileName]),
            #atom{value = ?VENOTEMPTY}
    end.


%% get_statfs/0
%% ====================================================================
%% @doc Gets file system statistics.
%% @end
-spec get_statfs() ->
    #statfsinfo{} | no_return().
%% ====================================================================
get_statfs() ->
    ?debug("get_statfs()"),
    {ok, UserDoc} = fslogic_objects:get_user(),
    Quota =
        case user_logic:get_quota(UserDoc) of
            {ok, QuotaRes} -> QuotaRes;
            {error, Reason} ->
                throw({?VEREMOTEIO, {failed_to_get_quota, Reason}})
        end,

    case user_logic:get_files_size(UserDoc#veil_document.uuid, fslogic_context:get_protocol_version()) of
        {ok, Size} when Size>Quota#quota.size ->
            %% df -h cannot handle situation when files_size is greater than quota_size
            #statfsinfo{answer = ?VOK, quota_size = Quota#quota.size, files_size = Quota#quota.size};
        {ok, Size} ->
            #statfsinfo{answer = ?VOK, quota_size = Quota#quota.size, files_size = Size};
        _ ->
            #statfsinfo{answer = ?VEREMOTEIO, quota_size = -1, files_size = -1}
    end.


%% rename_file/2
%% ====================================================================
%% @doc Renames file.
%% @end
-spec rename_file(FullFileName :: string(), FullTargetFileName :: string()) ->
    #atom{} | no_return().
%% ====================================================================
rename_file(FullFileName, FullTargetFileName) ->
    ?debug("rename_file(FullFileName: ~p, FullTargetFileName: ~p)", [FullFileName, FullTargetFileName]),
    {ok, #veil_document{record = #user{access_token = AccessToken, global_id = GRUID}} = UserDoc} = fslogic_objects:get_user(),

    %% Check source path
    case filename:split(FullFileName) of
        [_InvalidBaseDir] ->
            ?info("Attempt to move 'spaces' directory. Query: ~p", [FullFileName]),
            throw(?VEACCES);
        [?SPACES_BASE_DIR_NAME, _InvalidSource] ->
            ?info("Attempt to move the space directory. Query: ~p", [FullFileName]),
            throw(?VEACCES);
        _ -> ok
    end,

    %% Check target path
    case filename:split(FullFileName) of
        [_InvalidBaseDir1] ->
            ?info("Attempt to move onto 'spaces' directory. Query: ~p", [FullFileName]),
            throw(?VEACCES);
        [?SPACES_BASE_DIR_NAME, _InvalidSource1] ->
            ?info("Attempt to move onto the space directory. Query: ~p", [FullFileName]),
            throw(?VEACCES);
        _ -> ok
    end,

    {ok, #space_info{space_id = SourceSpaceId, providers = SourceSpaceProviders}} = fslogic_utils:get_space_info_for_path(FullFileName),
    {ok, #space_info{space_id = TargetSpaceId, providers = TargetSpaceProviders}} = fslogic_utils:get_space_info_for_path(FullTargetFileName),

    SelfGRPID = cluster_manager_lib:get_provider_id(),

    {ok, #fileattributes{} = SourceAttrs} = logical_files_manager:getfileattr(FullFileName),

    %% Check if destination file exists
    case logical_files_manager:getfileattr(FullTargetFileName) of
        {logical_file_system_error, ?VENOENT} ->
            ok;
        {ok, #fileattributes{}} ->
            ?warning("Destination file already exists: ~p", [FullTargetFileName]),
             throw(?VEEXIST)
    end,

    NewDirTokens = filename:split(fslogic_path:strip_path_leaf(FullTargetFileName)),
    SourceTokens = filename:split(FullFileName),
    SourceFileType = SourceAttrs#fileattributes.type,

    case (SourceFileType =:= ?DIR_TYPE_PROT) and (lists:prefix(SourceTokens, NewDirTokens) == 1) of
        true ->
            ?warning("Moving dir ~p to its child: ~p", [FullFileName, SourceTokens]),
            throw(?VEREMOTEIO);
        false -> ok
    end,


    %% Check if operation is trivial, inter-space or inter-provider
    case SourceSpaceId =:= TargetSpaceId of
        true -> %% Trivial
            {ok, OldFile, OldFileDoc, NewParentUUID} = rename_file_common_local_assertions(UserDoc, FullFileName, FullTargetFileName),
            ok = rename_file_trivial(UserDoc, SourceFileType, FullFileName, FullTargetFileName, {OldFile, OldFileDoc, NewParentUUID});
        false -> %% Not trivial
            NotCommonProviders = SourceSpaceProviders -- TargetSpaceProviders,
            CommonProviders = SourceSpaceProviders -- NotCommonProviders,

            case lists:member(SelfGRPID, CommonProviders) of
                true -> %% Inter-Space
                    {ok, OldFile, OldFileDoc, NewParentUUID} = rename_file_common_local_assertions(UserDoc, FullFileName, FullTargetFileName),
                    ok = rename_file_interspace(UserDoc, SourceFileType, FullFileName, FullTargetFileName, {OldFile, OldFileDoc, NewParentUUID});
                false when is_binary(AccessToken) -> %% Inter-Provider
                    ok = rename_file_interprovider(UserDoc, SourceFileType, FullFileName, FullTargetFileName);
                _ ->
                    ?error("Unable to handle rename request due to insufficient local permissions of user (GRUID) ~p", [GRUID]),
                    throw(?VECOMM)
            end
    end,

    #atom{value = ?VOK}.




%% ====================================================================
%% Internal functions
%% ====================================================================

rename_file_trivial(_UserDoc, _FileType, SourceFilePath, TargetFilePath, {OldFile, OldFileDoc, NewParentUUID}) ->
    ?debug("rename_file_trivial ~p ~p", [SourceFilePath, TargetFilePath]),

    RenamedFileInit =
        OldFile#file{parent = NewParentUUID, name = fslogic_path:basename(TargetFilePath)},

    RenamedFile = fslogic_meta:update_meta_attr(RenamedFileInit, ctime, vcn_utils:time()),
    Renamed = OldFileDoc#veil_document{record = RenamedFile},

    {ok, _} = fslogic_objects:save_file(Renamed),

    CTime = vcn_utils:time(),
    fslogic_meta:update_parent_ctime(fslogic_path:get_user_file_name(SourceFilePath), CTime),
    fslogic_meta:update_parent_ctime(fslogic_path:get_user_file_name(TargetFilePath), CTime),

    ok.

rename_file_interspace(UserDoc, SourceAttrs, SourceFilePath, TargetFilePath, {OldFile, OldFileDoc, NewParentUUID}) ->
    ?debug("rename_file_interspace ~p ~p ~p", [SourceAttrs, SourceFilePath, TargetFilePath]),

    SourceFileTokens = filename:split(SourceFilePath),
    TargetFileTokens = filename:split(TargetFilePath),
    {ok, #space_info{} = SourceSpaceInfo} = fslogic_utils:get_space_info_for_path(SourceFilePath),
    {ok, #space_info{} = TargetSpaceInfo} = fslogic_utils:get_space_info_for_path(TargetFilePath),

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
                {ok, {_TransferType, _FileID, NewFileID, SourceSubFilePath, SubFileDoc, _Storage} = OPInfo} ->
                    case update_moved_file(SourceSubFilePath, SubFileDoc, NewFileID, 3) of
                        ok -> {ok, OPInfo};
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

    ok = fslogic_utils:run_as_root(fun() -> rename_storage_rollback(SourceSpaceInfo, BadRes) end),

    try
        ok = rename_file_trivial(UserDoc, SourceAttrs, SourceFilePath, TargetFilePath, {OldFile, OldFileDoc, NewParentUUID}),
        rename_storage_cleanup(GoodRes)
    catch
        _:Reason ->
            ?error_stacktrace("Unable to move file tree due to: ~p. Initializing rollback...", [Reason]),
            fslogic_utils:run_as_root(
                fun() ->
                    rename_storage_rollback(SourceSpaceInfo, GoodRes ++ BadRes),
                    rename_db_rollback(GoodRes ++ BadRes)
                end),
            throw({?VEREMOTEIO, Reason})
    end,

    ok.


rename_storage_cleanup([]) ->
    ok;
rename_storage_cleanup([{ok, {move, _FileID, _NewFileID, _SourceFilePath, _FileDoc, _Storage}} | T]) ->
    rename_storage_cleanup(T);
rename_storage_cleanup([{ok, {link, FileID, _NewFileID, _SourceFilePath, _FileDoc, Storage}} | T]) ->
    SHInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
    case storage_files_manager:delete(SHInfo, FileID) of
        ok -> ok;
        {error, Reason} ->
            ?error("Unable to delete unused file ~p on storage ~p due to: ~p", [FileID, Storage, Reason]),
            ok
    end,
    rename_storage_cleanup(T).


rename_db_rollback([]) ->
    [];
rename_db_rollback([{_, {_TransferType, FileID, _NewFileID, SourceFilePath, FileDoc, _Storage}} | T]) ->
    [update_moved_file(SourceFilePath, FileDoc, FileID, 3) | rename_db_rollback(T)];
rename_db_rollback([_ | T]) ->
    rename_db_rollback(T).

update_moved_file(SourceFilePath, #veil_document{record = #file{location = Location} = File, uuid = FileUUID} = FileDoc, NewFileID, RetryCount) ->
    NewFile = File#file{location = Location#file_location{file_id = NewFileID}},

    case fslogic_objects:save_file(FileDoc#veil_document{record = NewFile}) of
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

rename_storage_rollback(_, []) ->
    ok;
rename_storage_rollback(#space_info{} = SourceSpaceInfo, [{_, {TransferType, FileID, NewFileID, _SourceFilePath, _FileDoc, Storage}} | T]) when is_atom(TransferType) ->
    SHInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),

    case TransferType of
        link -> storage_files_manager:delete(SHInfo, NewFileID);
        move ->
            storage_files_manager:mv(SHInfo, NewFileID, FileID),
            storage_files_manager:chown(SHInfo, FileID, -1, fslogic_spaces:map_to_grp_owner(SourceSpaceInfo))
    end,

    rename_storage_rollback(SourceSpaceInfo, T);
rename_storage_rollback(#space_info{} = SourceSpaceInfo, [{error, _} | T]) ->
    rename_storage_rollback(SourceSpaceInfo, T).



rename_on_storage(UserDoc, TargetSpaceInfo, SourceFilePath, TargetFilePath) ->
    try
        {ok, #veil_document{record = File} = FileDoc} = fslogic_objects:get_file(SourceFilePath),
        StorageID   = File#file.location#file_location.storage_id,
        FileID      = File#file.location#file_location.file_id,
        Storage = %% Storage info for the file
        case dao_lib:apply(dao_vfs, get_storage, [{uuid, StorageID}], 1) of
            {ok, #veil_document{record = #storage_info{} = S}} -> S;
            {error, MReason} ->
                ?error("Cannot fetch storage (ID: ~p) information for file ~p. Reason: ~p", [StorageID, SourceFilePath, MReason]),
                throw({storage_not_selected, MReason})
        end,

        SHInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage), %% Storage helper for cluster
        NewFileID = fslogic_storage:get_new_file_id(TargetSpaceInfo, TargetFilePath, UserDoc, SHInfo, fslogic_context:get_protocol_version()),

        TransferType =
            case storage_files_manager:link(SHInfo, FileID, NewFileID) of
                ok -> link;
                {error, ErrCode} ->
                    ?warning("Cannot move file using hard links '~p' -> '~p' due to: ~p", [FileID, NewFileID, ErrCode]),
                    case storage_files_manager:mv(SHInfo, FileID, NewFileID) of
                        ok -> move;
                        {error, ErrCode1} ->
                            ?error("Cannot move file '~p' -> '~p' due to: ~p", [FileID, NewFileID, ErrCode1]),
                            throw({file_not_moved, {{link_error, ErrCode}, {move_error, ErrCode1}}})
                    end
            end,

        OPInfo = {TransferType, FileID, NewFileID, SourceFilePath, FileDoc, Storage},

        %% Change group owner
        case fslogic_utils:run_as_root(fun() -> storage_files_manager:chown(SHInfo, NewFileID, -1, fslogic_spaces:map_to_grp_owner(TargetSpaceInfo)) end) of
            ok -> ok;
            MReason1 ->
                ?error("Cannot change group owner for file (ID: ~p) to ~p due to: ~p.", [FileID, fslogic_spaces:map_to_grp_owner(TargetSpaceInfo), MReason1]),
                throw(OPInfo)
        end,

        {ok, OPInfo}
    catch
        _:Reason ->
            {error, Reason}
    end.


rename_file_interprovider(UserDoc, ?DIR_TYPE_PROT, SourceFilePath, TargetFilePath) ->
    ?debug("rename_file_interprovider DIR ~p ~p", [SourceFilePath, TargetFilePath]),

    ok = logical_files_manager:mkdir(TargetFilePath),

    Self = self(),
    DN_CTX = fslogic_context:get_user_dn(),
    {AT_CTX1, AT_CTX2} = fslogic_context:get_access_token(),

    PIDs = lists:map(
        fun(#dir_entry{name = FileName, type = FileType}) ->
%%             spawn_monitor(
%%                 fun() ->
                    fslogic_context:set_user_dn(DN_CTX),
                    fslogic_context:set_access_token(AT_CTX1, AT_CTX2),
                    NewSourceFilePath = filename:join(SourceFilePath, FileName),
                    NewTargetFilePath = filename:join(TargetFilePath, FileName),
                    %% {ok, NewSourceAttrs} = logical_files_manager:getfileattr(NewSourceFilePath),
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

    ok = logical_files_manager:rmdir(SourceFilePath),

    ok;
rename_file_interprovider(_UserDoc, ?LNK_TYPE_PROT, SourceFilePath, TargetFilePath) ->
    ?debug("rename_file_interprovider LNK ~p ~p", [SourceFilePath, TargetFilePath]),

    {ok, LinkValue} = logical_files_manager:read_link(SourceFilePath),
    ok = logical_files_manager:create_link(LinkValue, TargetFilePath),
    ok = logical_files_manager:rmlink(SourceFilePath),

    ok;
rename_file_interprovider(_UserDoc, ?REG_TYPE_PROT, SourceFilePath, TargetFilePath) ->
    ?debug("rename_file_interprovider REG ~p ~p", [SourceFilePath, TargetFilePath]),

    ok = logical_files_manager:create(TargetFilePath),
    ok = transfer_data(SourceFilePath, TargetFilePath),
    ok = logical_files_manager:delete(SourceFilePath),

    ok.


transfer_data(SourceFilePath, TargetFilePath) ->
    transfer_data(SourceFilePath, TargetFilePath, 0, 1024 * 1024).

transfer_data(SourceFilePath, TargetFilePath, Offset, Size) ->
    {ok, Data} = logical_files_manager:read(SourceFilePath, Offset, Size),
    BytesWritten = logical_files_manager:write(TargetFilePath, Offset, Data),
    case size(Data) < Size andalso size(Data) =:= BytesWritten of
        true -> ok;
        false when BytesWritten > 0 ->
            transfer_data(SourceFilePath, TargetFilePath, Offset + BytesWritten, Size);
        _ ->
            {error, write_failed}
    end.


rename_file_common_local_assertions(UserDoc, SourceFilePath, TargetFilePath) ->
    OldDir = fslogic_path:strip_path_leaf(SourceFilePath),
    NewDir = fslogic_path:strip_path_leaf(TargetFilePath),

    {ok, #veil_document{record = #file{} = OldFile} = OldDoc} = fslogic_objects:get_file(SourceFilePath),
    {ok, #veil_document{uuid = NewParentUUID} = NewParentDoc} = fslogic_objects:get_file(NewDir),
    {ok, #veil_document{record = #file{}} = OldParentDoc} = fslogic_objects:get_file(OldDir),

    ok = fslogic_perms:check_file_perms(SourceFilePath, UserDoc, OldDoc, delete),
    ok = fslogic_perms:check_file_perms(NewDir, UserDoc, NewParentDoc, write),

    {ok, OldFile, OldDoc, NewParentUUID}.