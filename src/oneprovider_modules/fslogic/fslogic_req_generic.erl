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

-include("oneprovider_modules/dao/dao.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include_lib("ctool/include/logging.hrl").
-include("registered_names.hrl").
-include("oneprovider_modules/fslogic/fslogic_available_blocks.hrl").
-include("oneprovider_modules/fslogic/ranges_struct.hrl").
-include("oneprovider_modules/rtransfer/rtransfer.hrl").
-include("oneprovider_modules/fslogic/ranges_struct.hrl").


%% API
-export([update_times/4, change_file_owner/2, change_file_group/3, change_file_perms/2, check_file_perms/2, get_file_attr/1, get_xattr/2, set_xattr/4,
    remove_xattr/2, list_xattr/1, get_acl/1, set_acl/2, delete_file/1, rename_file/2, get_statfs/0, get_file_block_map/1, attr_unsubscribe/1]).

%% ====================================================================
%% API functions
%% ====================================================================

attr_unsubscribe(FileUUID) ->
    dao_lib:apply(dao_vfs, remove_attr_watcher, [FileUUID, fslogic_context:get_fuse_id()], fslogic_context:get_protocol_version()),
    #atom{value = ?VOK}.


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

    {ok, #db_document{record = #file{} = File} = FileDoc} = fslogic_objects:get_file(FullFileName),

    File1 = fslogic_meta:update_meta_attr(File, times, {ATime, MTime, CTime}),

    Status = string:equal(File1#file.meta_doc, File#file.meta_doc),
    if
        Status -> #atom{value = ?VOK};
        true ->
            {ok, _} = fslogic_objects:save_file(FileDoc#db_document{record = File1})
    end.


%% change_file_owner/2
%% ====================================================================
%% @doc Changes file's owner.
%% @end
-spec change_file_owner(FullFileName :: string(), NewUID :: non_neg_integer()) ->
    #atom{} | no_return().
%% ====================================================================
change_file_owner(FullFileName, NewUID) ->
    ?debug("change_file_owner(FullFileName: ~p, NewUID: ~p)", [FullFileName, NewUID]),

    {ok, #db_document{record = #file{} = File} = FileDoc} = fslogic_objects:get_file(FullFileName),
    {ok, UserDoc} = fslogic_objects:get_user(),

    ok = fslogic_perms:check_file_perms(FullFileName, UserDoc, FileDoc, root),

    NewFile = case dao_lib:apply(dao_users, get_user, [{uuid, integer_to_list(NewUID)}], fslogic_context:get_protocol_version()) of
                  {ok, #db_document{record = #user{}, uuid = UID1}} ->
                      File#file{uid = UID1};
                  {error, {not_found, missing}} ->
                      ?warning("chown: cannot find user with uid ~p", [NewUID]),
                      throw(?VEINVAL);
                  {error, Reason1} ->
                      ?error("chown: cannot find user with uid ~p due to error: ~p", [NewUID, Reason1]),
                      throw(?VEREMOTEIO)
              end,
    NewFile1 = fslogic_meta:update_meta_attr(NewFile, ctime, utils:time()),

    {ok, _} = fslogic_objects:save_file(FileDoc#db_document{record = NewFile1}),

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
    {ok, #db_document{record = #file{perms = ActualPerms, type = Type} = File} = FileDoc} =
        fslogic_objects:get_file(FullFileName),

    ok = fslogic_perms:check_file_perms(FullFileName, UserDoc, FileDoc, owner),

    NewFile = fslogic_meta:update_meta_attr(File, ctime, utils:time()),
    NewFile1 = FileDoc#db_document{record = NewFile#file{perms = Perms}},
    {ok, _} = fslogic_objects:save_file(NewFile1),

    case (ActualPerms == Perms orelse Type =/= ?REG_TYPE) of
        true -> ok;
        false ->
            #file_location{storage_uuid = StorageId, storage_file_id = FileId} = fslogic_file:get_file_local_location(FileDoc),
            {ok, #db_document{record = Storage}} = fslogic_objects:get_storage({uuid, StorageId}),
            {SH, File_id} = fslogic_utils:get_sh_and_id(?CLUSTER_FUSE_ID, Storage, FileId),
            storage_files_manager:chmod(SH, File_id, Perms)
    end,
    set_acl(FullFileName, []),

    #atom{value = ?VOK}.


%% check_file_perms/2
%% ====================================================================
%% @doc Checks for rights to read/write/delete/rdwr etc.
%% @end
-spec check_file_perms(FullFileName :: string(), Type :: root | owner | delete | read | write | execute | rdwr | '') ->
    #atom{} | no_return().
%% ====================================================================
check_file_perms(FullFileName, Type) ->
    {ok, FileDoc} = fslogic_objects:get_file(FullFileName),
    {ok, UserDoc} = fslogic_objects:get_user(),
    ok = fslogic_perms:check_file_perms(FullFileName, UserDoc, FileDoc, list_to_existing_atom(Type)),
    #atom{value = ?VOK}.


%% get_file_attr/2
%% ====================================================================
%% @doc Gets file's attributes.
%% @end
-spec get_file_attr(FullFileName :: string()) ->
    #fileattr{} | no_return().
%% ====================================================================
get_file_attr(FileDoc = #db_document{uuid = FileId, record = #file{}}) ->
    #db_document{record = #file{} = File, uuid = FileUUID} = FileDoc,
    Type = fslogic_file:normalize_file_type(protocol, File#file.type),

    %% Get owner
    {UName, VCUID, _RSUID} = fslogic_file:get_file_owner(File),

%%     catch fslogic_file:fix_storage_owner(FileDoc), %todo we often check attrs of non existing files, operation on storage can be time consuming

    {ok, FilePath} = logical_files_manager:get_file_full_name_by_uuid(FileUUID),
    {ok, #space_info{name = SpaceName} = SpaceInfo} = fslogic_utils:get_space_info_for_path(FilePath),

    %% Get attributes
    {CTime, MTime, ATime, Size, HasAcl} =
        case dao_lib:apply(dao_vfs, get_file_meta, [File#file.meta_doc], 1) of
            {ok, #db_document{record = FMeta}} ->
                SizeFromMap = case Type of
                           ?DIR_TYPE_PROT -> 0;
                           _ ->
                               {ok, {_Stamp, FileSize}} = fslogic_available_blocks:call({get_file_size, FileId}),
                               FileSize
                       end,
                {FMeta#file_meta.ctime, FMeta#file_meta.mtime, FMeta#file_meta.atime, SizeFromMap, FMeta#file_meta.acl =/= []};
            {error, Error} ->
                ?warning("Cannot fetch file_meta for file (uuid ~p) due to error: ~p", [FileUUID, Error]),
                {0, 0, 0, 0, 0}
        end,

    %% Get file links
    Links = case Type of
                ?DIR_TYPE_PROT ->
                    case dao_lib:apply(dao_vfs, count_subdirs, [{uuid, FileUUID}], fslogic_context:get_protocol_version()) of
                        {ok, Sum} -> Sum + 2;
                        _Other ->
                            ?error("Error: can not get number of links for file: ~p", [File]),
                            0
                    end;
                _ -> 1
            end,

    FuseId = fslogic_context:get_fuse_id(),
    ProtocolVersion = fslogic_context:get_protocol_version(),

    LocalFuseId =
        case fslogic_context:is_global_fuse_id(FuseId) of
            true ->
                {_RemoteProviderId, RemoteFuseId} = fslogic_context:read_global_fuse_id(FuseId),
                utils:ensure_list(RemoteFuseId);
            false ->
                utils:ensure_list(FuseId)
        end,

    case LocalFuseId of
        ?CLUSTER_FUSE_ID -> ignore;
        _ ->
            spawn(fun() ->
                dao_lib:apply(dao_vfs, remove_attr_watcher, [FileUUID, FuseId], ProtocolVersion),
                dao_lib:apply(dao_vfs, save_attr_watcher,
                    [#file_attr_watcher{
                        fuse_id = utils:ensure_list(FuseId),
                        file = FileUUID, create_time = utils:time(),
                        validity_time = 5 * 60}], ProtocolVersion)
            end)
    end,

    #fileattr{uuid = utils:ensure_list(FileUUID), answer = ?VOK, mode = File#file.perms, atime = ATime, ctime = CTime, mtime = MTime,
        type = Type, size = Size, uname = UName, gname = unicode:characters_to_list(SpaceName), uid = VCUID,
        gid = fslogic_spaces:map_to_grp_owner(SpaceInfo), links = Links, has_acl = HasAcl};
get_file_attr(FullFileName) ->
    ?debug("get_file_attr(FullFileName: ~p)", [FullFileName]),
    case fslogic_objects:get_file(FullFileName) of
        {ok, FileDoc} ->            %% Throw VENOENT in order not to trigger error-log
            get_file_attr(FileDoc); %% which would be unnecessary since get_file_attr is also used to check
        {error, file_not_found} ->  %% if the file exists
            throw(?VENOENT)
    end.

%% get_xattr/2
%% ====================================================================
%% @doc Gets file's extended attribute by name.
%% @end
-spec get_xattr(FullFileName :: string(), Name :: binary()) ->
    #xattr{} | no_return().
%% ====================================================================
get_xattr(FullFileName, Name) ->
    {ok, #db_document{record = #file{meta_doc = MetaUuid}}} = fslogic_objects:get_file(FullFileName),
    {ok, #db_document{record = #file_meta{xattrs = XAttrs}}} = dao_lib:apply(dao_vfs, get_file_meta, [MetaUuid], fslogic_context:get_protocol_version()),
    Value = case proplists:get_value(Name,XAttrs) of
        undefined -> throw(?VENOATTR);
        Val -> Val
    end,
    #xattr{answer = ?VOK, name = Name, value = Value}.

%% set_xattr/4
%% ====================================================================
%% @doc Sets file's extended attribute as {Name, Value}.
%% @end
-spec set_xattr(FullFileName :: string(), Name :: binary(), Value :: binary(), Flags :: integer()) ->
    #atom{} | no_return().
%% ====================================================================
set_xattr(FullFileName, Name, Value, _Flags) ->
    {ok, #db_document{record = FileDoc}} = fslogic_objects:get_file(FullFileName),
    #file{} = fslogic_meta:update_meta_attr(FileDoc, xattr_set, {Name,Value}, true),
    #atom{value = ?VOK}.

%% remove_xattr/2
%% ====================================================================
%% @doc Removes file's extended attribute with given Name.
%% @end
-spec remove_xattr(FullFileName :: string(), Name :: binary()) ->
    #atom{} | no_return().
%% ====================================================================
remove_xattr(FullFileName, Name) ->
    {ok, #db_document{record = FileDoc}} = fslogic_objects:get_file(FullFileName),
    #file{} = fslogic_meta:update_meta_attr(FileDoc, xattr_remove, Name, true),
    #atom{value = ?VOK}.

%% list_xattr/1
%% ====================================================================
%% @doc Gets file's extended attribute list.
%% @end
-spec list_xattr(FullFileName :: string()) ->
    #xattrlist{} | no_return().
%% ====================================================================
list_xattr(FullFileName) ->
    {ok, #db_document{record = #file{meta_doc = MetaUuid}}} = fslogic_objects:get_file(FullFileName),
    {ok, #db_document{record = #file_meta{xattrs = XAttrs}}} = dao_lib:apply(dao_vfs, get_file_meta, [MetaUuid], fslogic_context:get_protocol_version()),
    #xattrlist{answer = ?VOK, attrs = [#xattrlist_xattrentry{name = Name, value = Value} || {Name,Value} <- XAttrs]}.

%% get_acl/1
%% ====================================================================
%% @doc Gets file's access control list.
%% @end
-spec get_acl(FullFileName :: string()) ->
    #acl{} | no_return().
%% ====================================================================
get_acl(FullFileName) ->
    {ok, FileDoc = #db_document{record = #file{meta_doc = MetaUuid}}} = fslogic_objects:get_file(FullFileName),
    {ok, #db_document{record = #file_meta{acl = Acl}}} = dao_lib:apply(dao_vfs, get_file_meta, [MetaUuid], fslogic_context:get_protocol_version()),
    VirtualAcl =
        case Acl of
            [] -> fslogic_acl:get_virtual_acl(FullFileName, FileDoc);
            _ -> Acl
        end,
    #acl{answer = ?VOK, entities = VirtualAcl}.

%% set_acl/1
%% ====================================================================
%% @doc Sets file's access control list.
%% @end
-spec set_acl(FullFileName :: string(),Entities :: [#accesscontrolentity{}]) ->
    #atom{} | no_return().
%% ====================================================================
set_acl(FullFileName, Entities) ->
    true = lists:all(fun(X) -> is_record(X, accesscontrolentity) end, Entities),
    {ok, #db_document{record = #file{type = Type} = File} = FileDoc} = fslogic_objects:get_file(FullFileName),
    case Entities of
        [] -> ok;
        _ -> #atom{value = ?VOK} = fslogic_req_generic:change_file_perms(FullFileName, 0)
    end,
    #file{} = fslogic_meta:update_meta_attr(File, acl, Entities, true),

    % invalidate file permission cache
    case Type of
        ?REG_TYPE ->
            FileLoc = fslogic_file:get_file_local_location(FileDoc),
            {ok, #db_document{record = Storage}} = fslogic_objects:get_storage({uuid, FileLoc#file_location.storage_uuid}),
            {_SH, StorageFileName} = fslogic_utils:get_sh_and_id(?CLUSTER_FUSE_ID, Storage, FileLoc#file_location.storage_file_id),
            gen_server:call(?Dispatcher_Name, {fslogic, fslogic_context:get_protocol_version(), {invalidate_cache, StorageFileName}}, ?CACHE_REQUEST_TIMEOUT);
        _ -> ok
    end,

    #atom{value = ?VOK}.

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

    FileDesc = FileDoc#db_document.record,
    {ok, ChildrenTmpAns} =
        case FileDesc#file.type of
            ?DIR_TYPE ->
                dao_lib:apply(dao_vfs, list_dir, [FullFileName, 1, 0], fslogic_context:get_protocol_version());
            _OtherType -> {ok, []}
        end,

    case length(ChildrenTmpAns) of
        0 ->
            ok = dao_lib:apply(dao_vfs, remove_file, [FullFileName], fslogic_context:get_protocol_version()),

            fslogic_meta:update_parent_ctime(fslogic_path:get_user_file_name(FullFileName), utils:time()),
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

    case user_logic:get_files_size(UserDoc#db_document.uuid, fslogic_context:get_protocol_version()) of
        {ok, Size} when Size > Quota#quota.size ->
            %% df -h cannot handle situation when files_size is greater than quota_size
            #statfsinfo{answer = ?VOK, quota_size = Quota#quota.size, files_size = Quota#quota.size};
        {ok, Size} ->
            #statfsinfo{answer = ?VOK, quota_size = Quota#quota.size, files_size = Size};
        _ ->
            #statfsinfo{answer = ?VEREMOTEIO, quota_size = -1, files_size = -1}
    end.

%% get_file_block_map/1
%% ====================================================================
%% @doc Gets list of available_blocks for each provider supporting space.
%% @end
-spec get_file_block_map(FileId :: string()) -> #fileblockmap{} | no_return().
%% ====================================================================
get_file_block_map(FullFileName) ->
    ?debug("get_file_block_map(FullFileName: ~p)", [FullFileName]),
    {ok, #db_document{uuid = FileId}} = fslogic_objects:get_file(FullFileName),
    {ok, Docs} = fslogic_available_blocks:call({list_all_available_blocks, FileId}),
    ProtobufList = lists:map(
        fun(#db_document{record = #available_blocks{provider_id = Id, file_parts = Blocks}}) ->
            RawBlocks = ranges_struct:strip_timestamps(Blocks),
            ProtobufRanges = [#fileblockmap_blockmapentity_blockrange{from = From, to = To} || #range{from = From, to = To} <- RawBlocks],
            #fileblockmap_blockmapentity{provider_id = Id, ranges = ProtobufRanges}
        end, Docs),
    #fileblockmap{block_map = ProtobufList}.


%% rename_file/2
%% ====================================================================
%% @doc Renames file.
%% @end
-spec rename_file(FullFileName :: string(), FullTargetFileName :: string()) ->
    #atom{} | no_return().
%% ====================================================================
rename_file(FullFileName, FullTargetFileName) ->
    ?debug("rename_file(FullFileName: ~p, FullTargetFileName: ~p)", [FullFileName, FullTargetFileName]),
    {ok, #db_document{record = #user{access_token = AccessToken, global_id = GRUID}} = UserDoc} = fslogic_objects:get_user(),

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

    case (SourceFileType =:= ?DIR_TYPE_PROT) and lists:prefix(SourceTokens, NewDirTokens) of
        true ->
            ?warning("Moving dir ~p to its child: ~p", [FullFileName, SourceTokens]),
            throw(?VEREMOTEIO);
        false -> ok
    end,


    %% Check if operation is trivial, inter-space or inter-provider
    case SourceSpaceId =:= TargetSpaceId of
        true -> %% Trivial
            {ok, OldFile, OldFileDoc, NewParentUUID} = fslogic_req_rename_impl:common_assertions(UserDoc, FullFileName, FullTargetFileName),
            ok = fslogic_req_rename_impl:rename_file_trivial(FullFileName, FullTargetFileName, {OldFile, OldFileDoc, NewParentUUID});
        false -> %% Not trivial
            SourceSpaceProvidersSet = ordsets:from_list(SourceSpaceProviders),
            TargetSpaceProvidersSet = ordsets:from_list(TargetSpaceProviders),
            CommonProvidersSet = ordsets:intersection(SourceSpaceProvidersSet, TargetSpaceProvidersSet),

            case ordsets:is_element(SelfGRPID, CommonProvidersSet) of
                true -> %% Inter-Space
                    {ok, OldFile, OldFileDoc, NewParentUUID} = fslogic_req_rename_impl:common_assertions(UserDoc, FullFileName, FullTargetFileName),
                    ok = fslogic_req_rename_impl:rename_file_interspace(UserDoc, FullFileName, FullTargetFileName, {OldFile, OldFileDoc, NewParentUUID});
                false when is_binary(AccessToken) -> %% Inter-Provider
                    ok = fslogic_req_rename_impl:rename_file_interprovider(UserDoc, SourceFileType, FullFileName, FullTargetFileName);
                _ ->
                    ?error("Unable to handle rename request due to insufficient local permissions of user (GRUID) ~p", [GRUID]),
                    throw(?VECOMM)
            end
    end,

    #atom{value = ?VOK}.

%% ====================================================================
%% Internal functions
%% ====================================================================
