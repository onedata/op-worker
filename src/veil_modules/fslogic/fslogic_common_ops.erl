%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Write me !
%% @end
%% ===================================================================
-module(fslogic_common_ops).
-author("Rafal Slota").

-include("registered_names.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("files_common.hrl").
-include("fuse_messages_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("veil_modules/dao/dao_users.hrl").
-include("veil_modules/dao/dao_types.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include("logging.hrl").


%% API
-export([update_times/4, change_file_owner/3, change_file_group/3, change_file_perms/2, get_file_attr/1, delete_file/1, rename_file/2, get_link/1, get_statfs/0]).

%% ====================================================================
%% API functions
%% ====================================================================

update_times(FullFileName, ATime, MTime, CTime) ->
    ?debug("update_times(FullFileName: ~p, ATime: ~p, MTime: ~p, CTime: ~p)", [FullFileName, ATime, MTime, CTime]),
    case FullFileName of
        [?PATH_SEPARATOR] ->
            lager:warning("Trying to update times for root directory. FuseID: ~p. Aborting.", [fslogic_context:get_fuse_id()]),
            throw(invalid_updatetimes_request);
        _ -> ok
    end,
    {ok, #veil_document{record = #file{} = File} = FileDoc} = fslogic_objects:get_file(FullFileName),

    File1 = fslogic_utils:update_meta_attr(File, times, {ATime, MTime}),
    File2 = fslogic_utils:update_meta_attr(File1, ctime, CTime),

    Status = string:equal(File2#file.meta_doc, File#file.meta_doc),
    if
        Status -> #atom{value = ?VOK};
        true ->
            dao_lib:apply(dao_vfs, save_file, [FileDoc#veil_document{record = File2}], fslogic_context:get_protocol_version()),
            #atom{value = ?VOK}
    end.

change_file_owner(FullFileName, NewUID, NewUName) ->
    ?debug("change_file_owner(FullFileName: ~p, NewUID: ~p, NewUName: ~p)", [FullFileName, NewUID, NewUName]),

    #veil_document{record = #file{} = File} = FileDoc = fslogic_objects:get_file(FullFileName),
    {UserDocStatus, UserDoc} = fslogic_objects:get_user(),

    case fslogic_utils:check_file_perms(FullFileName, UserDocStatus, UserDoc, FileDoc, root) of
        {ok, true} -> ok;
        {ok, true} ->
            lager:warning("Changing file's owner without permissions: ~p", [FullFileName]),
            throw(?VEPERM);
        {PermsStat, PermsOK} ->
            lager:warning("Cannot check permissions of file. Reason: ~p:~p", [PermsStat, PermsOK]),
            throw(?VEREMOTEIO)
    end,

    NewFile =
        case user_logic:get_user({login, NewUName}) of
            {ok, #veil_document{record = #user{}, uuid = UID}} ->
                File#file{uid = UID};
            {error, user_not_found} ->
                lager:warning("chown: cannot find user with name ~p. lTrying UID (~p) lookup...", [NewUName, NewUID]),
                case dao_lib:apply(dao_users, get_user, [{uuid, integer_to_list(NewUID)}], fslogic_context:get_protocol_version()) of
                    {ok, #veil_document{record = #user{}, uuid = UID1}} ->
                        File#file{uid = UID1};
                    {error, {not_found, missing}} ->
                        lager:warning("chown: cannot find user with uid ~p", [NewUID]),
                        throw(?VEINVAL);
                    {error, Reason1} ->
                        lager:error("chown: cannot find user with uid ~p due to error: ~p", [NewUID, Reason1]),
                        throw(?VEREMOTEIO)
                end;
            {error, Reason1} ->
                lager:error("chown: cannot find user with uid ~p due to error: ~p", [NewUID, Reason1]),
                throw(?VEREMOTEIO)
        end,

    NewFile1 = fslogic_utils:update_meta_attr(NewFile, ctime, fslogic_utils:time()),
    case dao_lib:apply(dao_vfs, save_file, [FileDoc#veil_document{record = NewFile1}], fslogic_context:get_protocol_version()) of
        {ok, _} -> #atom{value = ?VOK};
        Other1 ->
            lager:error("fslogic could not save file ~p due to: ~p", [FullFileName, Other1]),
            #atom{value = ?VEREMOTEIO}
    end.

change_file_group(FullFileName, GID, GName) ->
    #atom{value = ?VENOTSUP}.

change_file_perms(FullFileName, Perms) ->
    {UserDocStatus, UserDoc} = fslogic_objects:get_user(),
    case fslogic_objects:get_file(FullFileName) of
        {ok, #veil_document{record = #file{} = File} = Doc} ->
            {PermsStat, PermsOK} = fslogic_utils:check_file_perms(FullFileName, UserDocStatus, UserDoc, Doc),
            case PermsStat of
                ok ->
                    case PermsOK of
                        true ->
                            File1 = fslogic_utils:update_meta_attr(File, ctime, fslogic_utils:time()),
                            NewFile = Doc#veil_document{record = File1#file{perms = Perms}},
                            case dao_lib:apply(dao_vfs, save_file, [NewFile], fslogic_context:get_protocol_version()) of
                                {ok, _} -> #atom{value = ?VOK};
                                Other1 ->
                                    lager:error("fslogic could not save file ~p due to: ~p", [FullFileName, Other1]),
                                    #atom{value = ?VEREMOTEIO}
                            end;
                        false ->
                            lager:warning("Changing file's perms file without permissions: ~p", [FullFileName]),
                            #atom{value = ?VEPERM}
                    end;
                _ ->
                    lager:warning("Cannot check permissions of file. Reason: ~p:~p", [PermsStat, PermsOK]),
                    #atom{value = ?VEREMOTEIO}
            end;
        {error, file_not_found} -> #atom{value = ?VENOENT};
        Other ->
            lager:error("fslogic could not get file ~p due to: ~p", [FullFileName, Other]),
            #atom{value = ?VEREMOTEIO}
    end.

get_file_attr(FileDoc = #veil_document{record = #file{}}) ->
    #veil_document{record = #file{} = File, uuid = FileUUID} = FileDoc,
    {Type, Size} =
        case File#file.type of
            ?DIR_TYPE -> {"DIR", 0};
            ?REG_TYPE ->
                FileLoc = File#file.location,
                S =
                    case dao_lib:apply(dao_vfs, get_storage, [{uuid, FileLoc#file_location.storage_id}], fslogic_context:get_protocol_version()) of
                        {ok, #veil_document{record = Storage}} ->
                            {SH, File_id} = fslogic_utils:get_sh_and_id(?CLUSTER_FUSE_ID, Storage, FileLoc#file_location.file_id),
                            case veilhelpers:exec(getattr, SH, [File_id]) of
                                {0, #st_stat{st_size = ST_Size} = _Stat} ->
                                    fslogic_utils:update_meta_attr(File, size, ST_Size),
                                    ST_Size;
                                {Errno, _} ->
                                    lager:error("Cannot fetch attributes for file: ~p, errno: ~p", [FileUUID, Errno]),
                                    0
                            end;
                        Other ->
                            lager:error("Cannot fetch storage: ~p for file: ~p, reason: ~p", [FileLoc#file_location.storage_id, FileUUID, Other]),
                            0
                    end,
                {"REG", S};
            ?LNK_TYPE -> {"LNK", 0};
            _ -> "UNK"
        end,

    %% Get owner
    {UName, UID} =
        case dao_lib:apply(dao_users, get_user, [{uuid, File#file.uid}], fslogic_context:get_protocol_version()) of
            {ok, #veil_document{record = #user{}} = UserDoc} ->
                {user_logic:get_login(UserDoc), list_to_integer(UserDoc#veil_document.uuid)};
            {error, UError} ->
                lager:error("Owner of file ~p not found due to error: ~p", [FileUUID, UError]),
                {"", -1}
        end,
    GName = %% Get group name
    case File#file.gids of
        [GNam | _] -> GNam; %% Select first one as main group
        [] -> UName
    end,

    %% Get attributes
    {CTime, MTime, ATime, _SizeFromDB} =
        case dao_lib:apply(dao_vfs, get_file_meta, [File#file.meta_doc], 1) of
            {ok, #veil_document{record = FMeta}} ->
                {FMeta#file_meta.ctime, FMeta#file_meta.mtime, FMeta#file_meta.atime, FMeta#file_meta.size};
            {error, Error} ->
                lager:warning("Cannot fetch file_meta for file (uuid ~p) due to error: ~p", [FileUUID, Error]),
                {0, 0, 0, 0}
        end,

    %% Get file links
    Links = case Type of
                "DIR" -> case dao_lib:apply(dao_vfs, count_subdirs, [{uuid, FileUUID}], fslogic_context:get_protocol_version()) of
                             {ok, Sum} -> Sum + 2;
                             _Other ->
                                 lager:error([{mod, ?MODULE}], "Error: can not get number of links for file: ~s", [File]),
                                 -1
                         end;
                "REG" -> 1;
                _ -> -1
            end,

    #fileattr{answer = ?VOK, mode = File#file.perms, atime = ATime, ctime = CTime, mtime = MTime, type = Type, size = Size, uname = UName, gname = GName, uid = UID, gid = UID, links = Links};
get_file_attr(FullFileName) ->
    ?debug("FileAttr for ~p", [FullFileName]),
    {ok, FileDoc} = fslogic_objects:get_file(FullFileName),
    get_file_attr(FileDoc).


delete_file(FullFileName) ->
    {FindStatus, FindTmpAns} = fslogic_objects:get_file(FullFileName),
    {UserDocStatus, UserDoc} = fslogic_objects:get_user(),
    case FindStatus of
        ok ->
            {PermsStat, PermsOK} = fslogic_utils:check_file_perms(FullFileName, UserDocStatus, UserDoc, FindTmpAns),
            case PermsStat of
                ok ->
                    case PermsOK of
                        true ->
                            FileDesc = FindTmpAns#veil_document.record,
                            {ChildrenStatus, ChildrenTmpAns} = case FileDesc#file.type of
                                                                   ?DIR_TYPE ->
                                                                       dao_lib:apply(dao_vfs, list_dir, [FullFileName, 10, 0], fslogic_context:get_protocol_version());
                                                                   _OtherType -> {ok, []}
                                                               end,

                            case ChildrenStatus of
                                ok ->
                                    case length(ChildrenTmpAns) of
                                        0 ->
                                            Status = dao_lib:apply(dao_vfs, remove_file, [FullFileName], fslogic_context:get_protocol_version()),
                                            case Status of
                                                ok ->
                                                    fslogic_utils:update_parent_ctime(fslogic_utils:get_user_file_name(FullFileName), fslogic_utils:time()),
                                                    #atom{value = ?VOK};
                                                _BadStatus ->
                                                    lager:error([{mod, ?MODULE}], "Error: can not remove file: ~s", [FullFileName]),
                                                    #atom{value = ?VEREMOTEIO}
                                            end;
                                        _Other ->
                                            lager:error([{mod, ?MODULE}], "Error: can not remove file (it has children): ~s", [FullFileName]),
                                            #atom{value = ?VENOTEMPTY}
                                    end;
                                _Other2 ->
                                    lager:error([{mod, ?MODULE}], "Error: can not remove file (can not check children): ~s", [FullFileName]),
                                    #atom{value = ?VEREMOTEIO}
                            end;
                        false ->
                            lager:warning("Deleting file without permissions: ~p", [FullFileName]),
                            #atom{value = ?VEPERM}
                    end;
                _ ->
                    lager:warning("Cannot check permissions of file. Reason: ~p:~p", [PermsStat, PermsOK]),
                    #atom{value = ?VEREMOTEIO}
            end;
        _FindError ->
            lager:error([{mod, ?MODULE}], "Error: can not remove file (can not check file type): ~s", [FullFileName]),
            #atom{value = ?VEREMOTEIO}
    end.


rename_file(FullFileName, FullNewFileName) ->
    {UserDocStatus, UserDoc} = fslogic_objects:get_user(),
    case fslogic_objects:get_file(FullFileName) of
        {ok, #veil_document{record = #file{} = OldFile} = OldDoc} ->
            {PermsStat, PermsOK} = fslogic_utils:check_file_perms(FullFileName, UserDocStatus, UserDoc, OldDoc),
            case PermsStat of
                ok ->
                    case PermsOK of
                        true ->
                            case fslogic_objects:get_file(FullNewFileName) of
                                {error, file_not_found} ->
                                    NewDir = fslogic_utils:strip_path_leaf(FullNewFileName),
                                    case (OldFile#file.type =:= ?DIR_TYPE) and (string:str(NewDir, FullFileName) == 1) of
                                        true ->
                                            lager:warning("Moving dir ~p to its child: ~p", [FullFileName, NewDir]),
                                            #atom{value = ?VEREMOTEIO};
                                        false ->
                                            case fslogic_objects:get_file(NewDir) of
                                                {ok, #veil_document{uuid = NewParent} = NewParentDoc} ->
                                                    OldDir = fslogic_utils:strip_path_leaf(FullFileName),
                                                    case fslogic_objects:get_file(OldDir) of
                                                        {ok, OldParentDoc} ->
                                                            {PermsStat1, PermsOK1} = fslogic_utils:check_file_perms(NewDir, UserDocStatus, UserDoc, NewParentDoc, write),
                                                            {PermsStat2, PermsOK2} = fslogic_utils:check_file_perms(OldDir, UserDocStatus, UserDoc, OldParentDoc, write),
                                                            case {PermsStat1, PermsStat2} of
                                                                {ok, ok} ->
                                                                    case {PermsOK1, PermsOK2} of
                                                                        {true, true} ->
                                                                            MoveOnStorage =
                                                                                fun(#file{type = ?REG_TYPE}) -> %% Returns new file record with updated file_id field or throws excpetion
                                                                                    %%                                         case {UserDocStatus, RootStatus} of %% Assert user doc && user root get status
                                                                                    %%                                             {ok, ok} -> ok;
                                                                                    %%                                             _ ->
                                                                                    %%                                                 ?error("Cannot fetch user doc or user root for file ~p.", [File]),
                                                                                    %%                                                 ?debug("get_user_doc response: ~p", [{UserDocStatus, UserDoc}]),
                                                                                    %%                                                 ?debug("get_user_root response: ~p", [{RootStatus, Root}]),
                                                                                    %%                                                 throw(gen_error_message(renamefile, ?VEREMOTEIO))
                                                                                    %%                                         end,

                                                                                    %% Get storage info
                                                                                    StorageID   = OldFile#file.location#file_location.storage_id,
                                                                                    FileID      = OldFile#file.location#file_location.file_id,
                                                                                    Storage = %% Storage info for the file
                                                                                    case dao_lib:apply(dao_vfs, get_storage, [{uuid, StorageID}], 1) of
                                                                                        {ok, #veil_document{record = #storage_info{} = S}} -> S;
                                                                                        {error, MReason} ->
                                                                                            ?error("Cannot fetch storage (ID: ~p) information for file ~p. Reason: ~p", [StorageID, FullFileName, MReason]),
                                                                                            throw(?VEREMOTEIO)
                                                                                    end,
                                                                                    SHInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage), %% Storage helper for cluster
                                                                                    NewFileID = fslogic_utils:get_new_file_id(FullNewFileName, UserDoc, SHInfo, fslogic_context:get_protocol_version()),

                                                                                    %% Change group owner if needed
                                                                                    case fslogic_utils:get_group_owner(FullNewFileName) of
                                                                                        [] -> ok; %% Dont change group owner
                                                                                        [NewGroup | _] -> %% We are moving file to group folder -> change owner
                                                                                            case storage_files_manager:chown(SHInfo, FileID, "", NewGroup) of
                                                                                                ok -> ok;
                                                                                                MReason1 ->
                                                                                                    ?error("Cannot change group owner for file (ID: ~p) to ~p due to: ~p.", [FileID, NewGroup, MReason1]),
                                                                                                    throw(?VEREMOTEIO)
                                                                                            end
                                                                                    end,

                                                                                    %% Move file to new location on storage
                                                                                    ActionR = storage_files_manager:mv(SHInfo, FileID, NewFileID),
                                                                                    _NewFile =
                                                                                        case ActionR of
                                                                                            ok -> OldFile#file{location = OldFile#file.location#file_location{file_id = NewFileID}};
                                                                                            MReason0 ->
                                                                                                ?error("Cannot move file (from ID ~p, to ID: ~p) on storage due to: ~p", [FileID, NewFileID, MReason0]),
                                                                                                throw(?VEREMOTEIO)
                                                                                        end;
                                                                                    (_) -> ok %% Dont move non-regular files
                                                                                end, %% end fun()

                                                                            %% Check if we need to move file on storage and do it when we do need it
                                                                            NewFile =
                                                                                case {string:tokens(fslogic_utils:get_user_file_name(FullFileName), "/"), string:tokens(fslogic_utils:get_user_file_name(FullNewFileName), "/")} of
                                                                                    {_, [?GROUPS_BASE_DIR_NAME, _InvalidTarget]} -> %% Moving into ?GROUPS_BASE_DIR_NAME dir is not allowed
                                                                                        ?info("Attemt to move file to base group directory. Query: ~p", [stub]),
                                                                                        throw(?VEPERM);
                                                                                    {[?GROUPS_BASE_DIR_NAME, _InvalidSource], _} -> %% Moving from ?GROUPS_BASE_DIR_NAME dir is not allowed
                                                                                        ?info("Attemt to move base group directory. Query: ~p", [stub]),
                                                                                        throw(?VEPERM);

                                                                                    {[?GROUPS_BASE_DIR_NAME, X | _FromF0], [?GROUPS_BASE_DIR_NAME, X | _ToF0]} -> %% Local (group dir) move, no storage actions are required
                                                                                        OldFile;

                                                                                    {[?GROUPS_BASE_DIR_NAME, _FromGrp0 | _FromF0], [?GROUPS_BASE_DIR_NAME, _ToGrp0 | _ToF0]} -> %% From group X to Y
                                                                                        MoveOnStorage(OldFile);
                                                                                    {[?GROUPS_BASE_DIR_NAME, _FromGrp1 | _FromF1], _} ->
                                                                                        %% From group X user dir
                                                                                        MoveOnStorage(OldFile);
                                                                                    {_, [?GROUPS_BASE_DIR_NAME, _ToGrp2 | _ToF2]} ->
                                                                                        %% From user dir to group X
                                                                                        MoveOnStorage(OldFile);

                                                                                    {_, _} -> %% Local (user dir) move, no storage actions are required
                                                                                        OldFile
                                                                                end,

                                                                            RenamedFileInit =
                                                                                case fslogic_utils:get_group_owner(FullNewFileName) of %% Do we need to update group owner?
                                                                                    [] -> NewFile#file{parent = NewParent, name = fslogic_utils:basename(FullNewFileName)}; %% Dont change group owner
                                                                                    [_NewGroup | _] = GIDs -> %% We are moving file to group folder -> change owner
                                                                                        NewFile#file{parent = NewParent, name = fslogic_utils:basename(FullNewFileName), gids = GIDs}
                                                                                end,
                                                                            RenamedFile = fslogic_utils:update_meta_attr(RenamedFileInit, ctime, fslogic_utils:time()),
                                                                            Renamed = OldDoc#veil_document{record = RenamedFile},
                                                                            case dao_lib:apply(dao_vfs, save_file, [Renamed], fslogic_context:get_protocol_version()) of
                                                                                {ok, _} ->
                                                                                    CTime = fslogic_utils:time(),
                                                                                    fslogic_utils:update_parent_ctime(fslogic_utils:get_user_file_name(FullNewFileName), CTime),
                                                                                    fslogic_utils:update_parent_ctime(fslogic_utils:get_user_file_name(FullFileName), CTime),
                                                                                    #atom{value = ?VOK};
                                                                                Other ->
                                                                                    lager:warning("Cannot save file document. Reason: ~p", [Other]),
                                                                                    #atom{value = ?VEREMOTEIO}
                                                                            end;
                                                                        _ ->
                                                                            lager:warning("Moving of file without permissions: ~p", [FullFileName]),
                                                                            #atom{value = ?VEPERM}
                                                                    end;
                                                                _ ->
                                                                    lager:warning("Cannot move file. Reason: ~p:~p", [{PermsStat1, PermsStat2}, {PermsOK1, PermsOK2}]),
                                                                    #atom{value = ?VEREMOTEIO}
                                                            end;
                                                        _ -> #atom{value = ?VEREMOTEIO}
                                                    end;
                                                {error, file_not_found} ->
                                                    lager:warning("Cannot find destination dir: ~p", [NewDir]),
                                                    #atom{value = ?VENOENT};
                                                _ -> #atom{value = ?VEREMOTEIO}
                                            end
                                    end;
                                {ok, #veil_document{}} ->
                                    lager:warning("Destination file already exists: ~p", [FullFileName]),
                                    #atom{value = ?VEEXIST};
                                _ -> #atom{value = ?VEREMOTEIO}
                            end;
                        false ->
                            lager:warning("Renaming file without permissions: ~p", [FullFileName]),
                            #atom{value = ?VEPERM}
                    end;
                _ ->
                    lager:warning("Cannot check permissions of file. Reason: ~p:~p", [PermsStat, PermsOK]),
                    #atom{value = ?VEREMOTEIO}
            end;
        {error, file_not_found} ->
            lager:warning("Cannot find source file: ~p", [FullFileName]),
            #atom{value = ?VENOENT};
        _ -> #atom{value = ?VEREMOTEIO}
    end.

get_link(FullFileName) ->
    case fslogic_objects:get_file(FullFileName) of
        {ok, #veil_document{record = #file{ref_file = Target}}} ->
            #linkinfo{file_logic_name = Target};
        {error, file_not_found} ->
            lager:error("Link ~p does not exist.", [FullFileName]),
            #linkinfo{answer = ?VENOENT, file_logic_name = ""};
        {error, Reason} ->
            lager:error("Cannot read link ~p due to error: ~p", [FullFileName, Reason]),
            #linkinfo{answer = ?VEREMOTEIO, file_logic_name = ""}
    end.

get_statfs() ->
    case fslogic_objects:get_user() of
        {ok, UserDoc} ->
            case user_logic:get_quota(UserDoc) of
                {ok, Quota} ->
                    case user_logic:get_files_size(UserDoc#veil_document.uuid, fslogic_context:get_protocol_version()) of
                        {ok, Size} when Size>Quota#quota.size ->
                            %% df -h cannot handle situation when files_size is greater than quota_size
                            #statfsinfo{answer = ?VOK, quota_size = Quota#quota.size, files_size = Quota#quota.size};
                        {ok, Size} ->
                            #statfsinfo{answer = ?VOK, quota_size = Quota#quota.size, files_size = Size};
                        _ ->
                            #statfsinfo{answer = ?VEREMOTEIO, quota_size = -1, files_size = -1}
                    end;
                _ ->
                    #statfsinfo{answer = ?VEREMOTEIO, quota_size = -1, files_size = -1}
            end;
        _ ->
            #statfsinfo{answer = ?VEREMOTEIO, quota_size = -1, files_size = -1}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
