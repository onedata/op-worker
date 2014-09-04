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


%% rename_file/2
%% ====================================================================
%% @doc Renames file.
%% @end
-spec rename_file(FullFileName :: string(), FullTargetFileName :: string()) ->
    #atom{} | no_return().
%% ====================================================================
rename_file(FullFileName, FullTargetFileName) ->
    ?debug("rename_file(FullFileName: ~p, FullTargetFileName: ~p)", [FullFileName, FullTargetFileName]),
    {ok, #veil_document{record = #user{access_token = AccessToken}} = UserDoc} = fslogic_objects:get_user(),

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



    case SourceSpaceId =:= TargetSpaceId of
        true ->
            ok = rename_file_trivial(UserDoc, SourceFileType, FullFileName, FullTargetFileName);
        false ->
            NotCommonProviders = SourceSpaceProviders -- TargetSpaceProviders,
            CommonProviders = SourceSpaceProviders -- NotCommonProviders,

            case lists:member(SelfGRPID, CommonProviders) of
                true ->
                    ok = rename_file_interspace(UserDoc, SourceFileType, FullFileName, FullTargetFileName);
                false when is_binary(AccessToken) ->
                    ok = rename_file_interprovider(UserDoc, SourceFileType, FullFileName, FullTargetFileName);
                    %% ok = logical_files_manager:force_remove(FullFileName);
                _ ->
                    throw(?VECOMM)
            end
    end,

    #atom{value = ?VOK}.

rename_file_trivial(UserDoc, _FileType, SourceFilePath, TargetFilePath) ->
    ?info("rename_file_trivial ~p ~p", [SourceFilePath, TargetFilePath]),
    {ok, OldFile, OldFileDoc, NewParentUUID} = rename_file_common_local_assertions(UserDoc, SourceFilePath, TargetFilePath),

    RenamedFileInit =
        OldFile#file{parent = NewParentUUID, name = fslogic_path:basename(TargetFilePath)},

    RenamedFile = fslogic_meta:update_meta_attr(RenamedFileInit, ctime, vcn_utils:time()),
    Renamed = OldFileDoc#veil_document{record = RenamedFile},

    {ok, _} = fslogic_objects:save_file(Renamed),

    CTime = vcn_utils:time(),
    fslogic_meta:update_parent_ctime(fslogic_path:get_user_file_name(SourceFilePath), CTime),
    fslogic_meta:update_parent_ctime(fslogic_path:get_user_file_name(TargetFilePath), CTime),

    ok.

rename_file_interspace(UserDoc, SourceAttrs, SourceFilePath, TargetFilePath) ->
    ?info("rename_file_interspace ~p ~p ~p", [SourceAttrs, SourceFilePath, TargetFilePath]),
    {ok, _, _, _} = rename_file_common_local_assertions(UserDoc, SourceFilePath, TargetFilePath),

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


    ?info("==========> ALL REG: ~p", [AllRegularFiles]),
    StorageMoveResult = lists:map(
        fun({SourceFile, TargetFile}) ->
            move_on_storage(UserDoc, TargetSpaceInfo, SourceFile, TargetFile)
        end, AllRegularFiles),

    ?info("==========> MOVE REG: ~p", [StorageMoveResult]),

    {GoodRes, BadRes} =
        lists:partition(
            fun(Status) ->
                case Status of
                    {ok, _} -> true;
                    {error, _} -> false
                end
            end, StorageMoveResult),

    case BadRes of
        [] ->
            try
                Errors = lists:filter(fun(UpdateRes) -> UpdateRes =/= ok end, update_moved_files(GoodRes)),
                InvalidUUIDs = lists:map(fun({error, {UUID, _}}) -> UUID end, Errors),
                {BadRes1, GoodRes1} = lists:partition(fun({ok, {_, _, _, _, #veil_document{uuid = UUID}, _}}) -> lists:member(UUID, InvalidUUIDs) end, GoodRes),
                ok = rename_file_trivial(UserDoc, SourceAttrs, SourceFilePath, TargetFilePath),
                ok = storage_cleanup(GoodRes1)
                %% @todo: BadRes1 ???
            catch
                _:Reason ->
                    %% @todo: log
                    fslogic_utils:run_as_root(fun() -> storage_rollback(SourceSpaceInfo, GoodRes ++ BadRes) end),
                    throw(?VEREMOTEIO)
            end;
        _ ->
            fslogic_utils:run_as_root(fun() -> storage_rollback(SourceSpaceInfo, GoodRes ++ BadRes) end),
            %% @todo: log
            throw(?VEREMOTEIO)
    end,

    ok.


storage_cleanup([]) ->
    ok;
storage_cleanup([{ok, {move, _FileID, _NewFileID, _SourceFilePath, _FileDoc, _Storage}} | T]) ->
    storage_cleanup(T);
storage_cleanup([{ok, {link, FileID, _NewFileID, _SourceFilePath, _FileDoc, Storage}} | T]) ->
    SHInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
    case storage_files_manager:delete(SHInfo, FileID) of
        ok -> ok;
        {error, Reason} ->
            %% @todo: log
            ok
    end,
    storage_cleanup(T).


update_moved_files([]) ->
    [];
update_moved_files([{ok, {_TransferType, _FileID, NewFileID, SourceFilePath, FileDoc, _Storage}} | T]) ->
    [update_moved_file(SourceFilePath, FileDoc, NewFileID, 3) | update_moved_files(T)].

update_moved_file(SourceFilePath, #veil_document{record = #file{location = Location} = File, uuid = FileUUID} = FileDoc, NewFileID, RetryCount) ->
    NewFile = File#file{location = Location#filelocation{file_id = NewFileID}},

    case fslogic_objects:save_file(FileDoc#veil_document{record = NewFile}) of
        {ok, _} -> ok;
        {error, Reason} when RetryCount > 0 ->
            %% @todo: log
            case fslogic_objects:get_file(SourceFilePath) of
                {ok, NewFileDoc} ->
                    update_moved_file(SourceFilePath, NewFileDoc, NewFileID, RetryCount - 1);
                {error, GetError} ->
                    %% @todo: log
                    update_moved_file(SourceFilePath, FileDoc, NewFileID, RetryCount - 1)
            end;
        {error, Reason1} ->
            %% @todo: log
            {error, {FileUUID, Reason1}}
    end.

storage_rollback(_, []) ->
    ok;
storage_rollback(#space_info{} = SourceSpaceInfo, [{_, {TransferType, FileID, NewFileID, _SourceFilePath, _FileDoc, Storage}} | T]) when is_atom(TransferType) ->
    SHInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),

    case TransferType of
        link -> storage_files_manager:delete(SHInfo, NewFileID);
        move ->
            storage_files_manager:mv(SHInfo, NewFileID, FileID),
            storage_files_manager:chown(SHInfo, FileID, -1, fslogic_spaces:map_to_grp_owner(SourceSpaceInfo))
    end,

    storage_rollback(SourceSpaceInfo, T);
storage_rollback(#space_info{} = SourceSpaceInfo, [{error, _} | T]) ->
    storage_rollback(SourceSpaceInfo, T).



move_on_storage(UserDoc, TargetSpaceInfo, SourceFilePath, TargetFilePath) ->
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
        case storage_files_manager:chown(SHInfo, NewFileID, -1, fslogic_spaces:map_to_grp_owner(TargetSpaceInfo)) of
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
    ?info("rename_file_interprovider DIR ~p ~p", [SourceFilePath, TargetFilePath]),

    ok = logical_files_manager:mkdir(TargetFilePath),

    lists:foreach(
        fun(#dir_entry{name = FileName, type = FileType}) ->
            NewSourceFilePath = filename:join(SourceFilePath, FileName),
            NewTargetFilePath = filename:join(TargetFilePath, FileName),
            %% {ok, NewSourceAttrs} = logical_files_manager:getfileattr(NewSourceFilePath),
            rename_file_interprovider(UserDoc, FileType, NewSourceFilePath, NewTargetFilePath)
        end, fslogic_utils:list_dir(SourceFilePath)),

    ok = logical_files_manager:rmdir(SourceFilePath),

    ok;
rename_file_interprovider(_UserDoc, ?LNK_TYPE_PROT, SourceFilePath, TargetFilePath) ->
    ?info("rename_file_interprovider LNK ~p ~p", [SourceFilePath, TargetFilePath]),

    {ok, LinkValue} = logical_files_manager:read_link(SourceFilePath),
    ok = logical_files_manager:create_link(LinkValue, TargetFilePath),
    ok = logical_files_manager:rmlink(SourceFilePath),

    ok;
rename_file_interprovider(_UserDoc, ?REG_TYPE_PROT, SourceFilePath, TargetFilePath) ->
    ?info("rename_file_interprovider REG ~p ~p", [SourceFilePath, TargetFilePath]),

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


%%     {ok, #veil_document{record = #file{} = OldFile} = OldDoc} = fslogic_objects:get_file(FullFileName),
%%
%%     ok = fslogic_perms:check_file_perms(FullFileName, UserDoc, OldDoc, delete),
%%
%%
%%     {ok, #veil_document{uuid = NewParent} = NewParentDoc} = fslogic_objects:get_file(NewDir),
%%
%%     OldDir = fslogic_path:strip_path_leaf(FullFileName),
%%     {ok, OldParentDoc} = fslogic_objects:get_file(OldDir),
%%
%%     ok = fslogic_perms:check_file_perms(NewDir, UserDoc, NewParentDoc, write),
%%
%%     {ok, TargetSpaceInfo} = fslogic_utils:get_space_info_for_path(FullNewFileName),
%%
%%     MoveOnStorage =
%%         fun(#file{type = ?REG_TYPE}) -> %% Returns new file record with updated file_id field or throws excpetion
%%             %% Get storage info
%%             StorageID   = OldFile#file.location#file_location.storage_id,
%%             FileID      = OldFile#file.location#file_location.file_id,
%%             Storage = %% Storage info for the file
%%             case dao_lib:apply(dao_vfs, get_storage, [{uuid, StorageID}], 1) of
%%                 {ok, #veil_document{record = #storage_info{} = S}} -> S;
%%                 {error, MReason} ->
%%                     ?error("Cannot fetch storage (ID: ~p) information for file ~p. Reason: ~p", [StorageID, FullFileName, MReason]),
%%                     throw(?VEREMOTEIO)
%%             end,
%%             SHInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage), %% Storage helper for cluster
%%             NewFileID = fslogic_storage:get_new_file_id(TargetSpaceInfo, FullNewFileName, UserDoc, SHInfo, fslogic_context:get_protocol_version()),
%%
%%             %% Change group owner if needed
%%             case storage_files_manager:chown(SHInfo, FileID, -1, fslogic_spaces:map_to_grp_owner(TargetSpaceInfo)) of
%%                 ok -> ok;
%%                 MReason1 ->
%%                     ?error("Cannot change group owner for file (ID: ~p) to ~p due to: ~p.", [FileID, fslogic_spaces:map_to_grp_owner(TargetSpaceInfo), MReason1]),
%%                     throw(?VEREMOTEIO)
%%             end,
%%
%%             %% Move file to new location on storage
%%             ActionR = storage_files_manager:mv(SHInfo, FileID, NewFileID),
%%             _NewFile =
%%                 case ActionR of
%%                     ok -> OldFile#file{location = OldFile#file.location#file_location{file_id = NewFileID}};
%%                     MReason0 ->
%%                         ?error("Cannot move file (from ID ~p, to ID: ~p) on storage due to: ~p", [FileID, NewFileID, MReason0]),
%%                         throw(?VEREMOTEIO)
%%                 end;
%%             (_) -> ok %% Dont move non-regular files
%%         end, %% end fun()
%%
%%     %% Check if we need to move file on storage and do it when we do need it
%%     NewFile =
%%         case {string:tokens(fslogic_path:get_user_file_name(FullFileName), "/"), string:tokens(fslogic_path:get_user_file_name(FullNewFileName), "/")} of
%%             {_, [?SPACES_BASE_DIR_NAME, _InvalidTarget]} -> %% Moving into ?GROUPS_BASE_DIR_NAME dir is not allowed
%%                 ?info("Attempt to move file to base group directory. Query: ~p", [stub]),
%%                 throw(?VEACCES);
%%             {[?SPACES_BASE_DIR_NAME, _InvalidSource], _} -> %% Moving from ?GROUPS_BASE_DIR_NAME dir is not allowed
%%                 ?info("Attemt to move base group directory. Query: ~p", [stub]),
%%                 throw(?VEACCES);
%%
%%             {[?SPACES_BASE_DIR_NAME, X | _FromF0], [?SPACES_BASE_DIR_NAME, X | _ToF0]} -> %% Local (group dir) move, no storage actions are required
%%                 OldFile;
%%
%%             {[?SPACES_BASE_DIR_NAME, _FromGrp0 | _FromF0], [?SPACES_BASE_DIR_NAME, _ToGrp0 | _ToF0]} -> %% From group X to Y
%%                 MoveOnStorage(OldFile);
%%             {[?SPACES_BASE_DIR_NAME, _FromGrp1 | _FromF1], _} ->
%%                 %% From group X user dir
%%                 MoveOnStorage(OldFile);
%%             {_, [?SPACES_BASE_DIR_NAME, _ToGrp2 | _ToF2]} ->
%%                 %% From user dir to group X
%%                 MoveOnStorage(OldFile);
%%
%%             {_, _} -> %% Local (user dir) move, no storage actions are required
%%                 OldFile
%%         end,
%%
%%     RenamedFileInit =
%%         NewFile#file{parent = NewParent, name = fslogic_path:basename(FullNewFileName)},
%%
%%     RenamedFile = fslogic_meta:update_meta_attr(RenamedFileInit, ctime, vcn_utils:time()),
%%     Renamed = OldDoc#veil_document{record = RenamedFile},
%%
%%     {ok, _} = fslogic_objects:save_file(Renamed),
%%
%%     CTime = vcn_utils:time(),
%%     fslogic_meta:update_parent_ctime(fslogic_path:get_user_file_name(FullNewFileName), CTime),
%%     fslogic_meta:update_parent_ctime(fslogic_path:get_user_file_name(FullFileName), CTime),
%%     #atom{value = ?VOK}.


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

%% ====================================================================
%% Internal functions
%% ====================================================================
