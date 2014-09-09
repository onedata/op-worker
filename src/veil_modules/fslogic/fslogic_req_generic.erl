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
    {CTime, MTime, ATime, _SizeFromDB, UserMetadata} =
        case dao_lib:apply(dao_vfs, get_file_meta, [File#file.meta_doc], 1) of
            {ok, #veil_document{record = FMeta}} ->
                {FMeta#file_meta.ctime, FMeta#file_meta.mtime, FMeta#file_meta.atime, FMeta#file_meta.size,
                    FMeta#file_meta.user_metadata};
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
        type = Type, size = Size, uname = UName, gname = unicode:characters_to_list(SpaceName), uid = UID,
        gid = fslogic_spaces:map_to_grp_owner(SpaceInfo), links = Links, user_metadata = UserMetadata};
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
-spec rename_file(FullFileName :: string(), FullNewFileName :: string()) ->
    #atom{} | no_return().
%% ====================================================================
rename_file(FullFileName, FullNewFileName) ->
    ?debug("rename_file(FullFileName: ~p, FullNewFileName: ~p)", [FullFileName, FullNewFileName]),
    {ok, UserDoc} = fslogic_objects:get_user(),
    {ok, #veil_document{record = #file{} = OldFile} = OldDoc} = fslogic_objects:get_file(FullFileName),

    ok = fslogic_perms:check_file_perms(FullFileName, UserDoc, OldDoc, delete),

    %% Check if destination file exists
    case fslogic_objects:get_file(FullNewFileName) of
        {ok, #veil_document{}} ->
            ?warning("Destination file already exists: ~p", [FullFileName]),
            throw(?VEEXIST);
        {error, file_not_found} ->
            ok
    end,

    NewDir = fslogic_path:strip_path_leaf(FullNewFileName),

    case (OldFile#file.type =:= ?DIR_TYPE) and (string:str(NewDir, FullFileName) == 1) of
        true ->
            ?warning("Moving dir ~p to its child: ~p", [FullFileName, NewDir]),
            throw(?VEREMOTEIO);
        false -> ok
    end,

    {ok, #veil_document{uuid = NewParent} = NewParentDoc} = fslogic_objects:get_file(NewDir),

    OldDir = fslogic_path:strip_path_leaf(FullFileName),
    {ok, OldParentDoc} = fslogic_objects:get_file(OldDir),

    ok = fslogic_perms:check_file_perms(NewDir, UserDoc, NewParentDoc, write),

    {ok, TargetSpaceInfo} = fslogic_utils:get_space_info_for_path(FullNewFileName),

    MoveOnStorage =
        fun(#file{type = ?REG_TYPE}) -> %% Returns new file record with updated file_id field or throws excpetion
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
            NewFileID = fslogic_storage:get_new_file_id(TargetSpaceInfo, FullNewFileName, UserDoc, SHInfo, fslogic_context:get_protocol_version()),

            %% Change group owner if needed
            case storage_files_manager:chown(SHInfo, FileID, -1, fslogic_spaces:map_to_grp_owner(TargetSpaceInfo)) of
                ok -> ok;
                MReason1 ->
                    ?error("Cannot change group owner for file (ID: ~p) to ~p due to: ~p.", [FileID, fslogic_spaces:map_to_grp_owner(TargetSpaceInfo), MReason1]),
                    throw(?VEREMOTEIO)
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
        case {string:tokens(fslogic_path:get_user_file_name(FullFileName), "/"), string:tokens(fslogic_path:get_user_file_name(FullNewFileName), "/")} of
            {_, [?SPACES_BASE_DIR_NAME, _InvalidTarget]} -> %% Moving into ?GROUPS_BASE_DIR_NAME dir is not allowed
                ?info("Attempt to move file to base group directory. Query: ~p", [stub]),
                throw(?VEACCES);
            {[?SPACES_BASE_DIR_NAME, _InvalidSource], _} -> %% Moving from ?GROUPS_BASE_DIR_NAME dir is not allowed
                ?info("Attemt to move base group directory. Query: ~p", [stub]),
                throw(?VEACCES);

            {[?SPACES_BASE_DIR_NAME, X | _FromF0], [?SPACES_BASE_DIR_NAME, X | _ToF0]} -> %% Local (group dir) move, no storage actions are required
                OldFile;

            {[?SPACES_BASE_DIR_NAME, _FromGrp0 | _FromF0], [?SPACES_BASE_DIR_NAME, _ToGrp0 | _ToF0]} -> %% From group X to Y
                MoveOnStorage(OldFile);
            {[?SPACES_BASE_DIR_NAME, _FromGrp1 | _FromF1], _} ->
                %% From group X user dir
                MoveOnStorage(OldFile);
            {_, [?SPACES_BASE_DIR_NAME, _ToGrp2 | _ToF2]} ->
                %% From user dir to group X
                MoveOnStorage(OldFile);

            {_, _} -> %% Local (user dir) move, no storage actions are required
                OldFile
        end,

    RenamedFileInit =
        NewFile#file{parent = NewParent, name = fslogic_path:basename(FullNewFileName)},

    RenamedFile = fslogic_meta:update_meta_attr(RenamedFileInit, ctime, vcn_utils:time()),
    Renamed = OldDoc#veil_document{record = RenamedFile},

    {ok, _} = fslogic_objects:save_file(Renamed),

    CTime = vcn_utils:time(),
    fslogic_meta:update_parent_ctime(fslogic_path:get_user_file_name(FullNewFileName), CTime),
    fslogic_meta:update_parent_ctime(fslogic_path:get_user_file_name(FullFileName), CTime),
    #atom{value = ?VOK}.


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
