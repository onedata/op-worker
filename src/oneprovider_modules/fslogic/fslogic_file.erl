%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides convenience methods that helps with managing #file{} record.
%%       It also provides some abstract getters/setters for some #file{} record in case further changes.
%% @end
%% ===================================================================
-module(fslogic_file).
-author("Rafal Slota").

-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("files_common.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([normalize_file_type/2, get_file_local_location_doc/1]).
-export([update_file_size/1, update_file_size/2, get_real_file_size_and_uid/1, get_file_owner/1, get_file_local_location/1, fix_storage_owner/1]).
-export([ensure_file_location_exists/2]).

%% ====================================================================
%% API functions
%% ====================================================================


%% fix_storage_owner/1
%% ====================================================================
%% @doc Fixes storage file (user) owner for given file.
-spec fix_storage_owner(File :: file_doc() | file()) -> ok | {error, Reason :: any()}.
%% ====================================================================
fix_storage_owner(#db_document{record = #file{type = ?REG_TYPE} = File, uuid = FileUUID} = FileDoc) ->
    {_UName, _VCUID, RSUID} = fslogic_file:get_file_owner(File),
    {_Size, SUID} = fslogic_file:get_real_file_size_and_uid(FileDoc),

    case SUID =:= RSUID of
        true -> ok;
        false ->
            ?info("SUID missmatch on file ~p (~p vs correct ~p) - fixing", [FileUUID, SUID, RSUID]),
            FileLoc = fslogic_file:get_file_local_location(FileDoc),
            {ok, #db_document{record = Storage}} = fslogic_objects:get_storage({uuid, FileLoc#file_location.storage_uuid}),
            {SH, File_id} = fslogic_utils:get_sh_and_id(?CLUSTER_FUSE_ID, Storage, FileLoc#file_location.storage_file_id),
            case storage_files_manager:chown(SH, File_id, RSUID, -1) of
                ok -> ok;
                SReason ->
                    ?error("Could not fix SUID of file ~p due to: ~p", [FileUUID, SReason]),
                    {error, SReason}
            end
    end;
fix_storage_owner(#db_document{record = #file{}}) ->
    ok;
fix_storage_owner(FileQuery) ->
    {ok, #db_document{record = #file{}} = FileDoc} = fslogic_objects:get_file(FileQuery),
    fix_storage_owner(FileDoc).


%% get_file_owner/1
%% ====================================================================
%% @doc Fetches owner's username, provider's UID and current correct storage UID for given file.
%%      Returns {"", -1} on error.
-spec get_file_owner(File :: file_doc() | file_info() | file()) ->
    {Login :: string(), VCUID :: integer(), SUID :: integer()} |
    {[], -1, -1}.
%% ====================================================================
get_file_owner(#file{} = File) ->
    case user_logic:get_user({uuid, File#file.uid}) of
        {ok, #db_document{record = #user{}} = UserDoc} ->
            {{_, Login}, SUID} = user_logic:get_login_with_uid(UserDoc),

            %% Translate GRUID to integer that shall be be used as storage's UID
            <<GID0:16/big-unsigned-integer-unit:8>> = crypto:hash(md5, utils:ensure_binary(File#file.uid)),
            {ok, LowestGID} = oneprovider_node_app:get_env(lowest_generated_storage_gid),
            VCUID = LowestGID + GID0 rem 1000000,
            {Login, VCUID, SUID};
        {error, UError} ->
            ?error("Owner of file ~p not found due to error: ~p", [File, UError]),
            {"", -1, -1}
    end;
get_file_owner(FilePath) ->
    {ok, #db_document{record = #file{} = File}} = fslogic_objects:get_file(FilePath),
    get_file_owner(File).


%% get_file_local_location/1
%% ====================================================================
%% @doc Fetches local #file_location{} from #file{} record.
%%      #file_location{} shall never be accessed directly since this could be subject to change.
-spec get_file_local_location(File :: file_doc() | file_info()) -> #db_document{record :: #file_location{}}.
%% ====================================================================
get_file_local_location(#db_document{uuid = FileId, record = #file{}}) ->
    get_file_local_location(FileId);
get_file_local_location(FileId) when is_list(FileId) ->
    #db_document{record = Location} = get_file_local_location_doc(FileId),
    Location.
get_file_local_location_doc(#db_document{uuid = FileId, record = #file{}}) ->
    get_file_local_location_doc(FileId);
get_file_local_location_doc(FileId) when is_list(FileId) ->
    {ok, [Location | _Locations]} = dao_lib:apply(dao_vfs, get_file_locations, [FileId], fslogic_context:get_protocol_version()),
    Location.


%% get_real_file_size_and_uid/1
%% ====================================================================
%% @doc Fetches real file size and uid from underlying storage. Returns {0, -1} for non-regular file.
%%      Also errors are silently dropped (return value {0, -1}).
-spec get_real_file_size_and_uid(File :: file() | file_doc() | file_info()) -> FileSize :: non_neg_integer().
%% ====================================================================
get_real_file_size_and_uid(#db_document{uuid = FileId, record = #file{type = ?REG_TYPE} = File}) ->
    FileLoc = get_file_local_location(FileId),
    {ok, #db_document{record = Storage}} = fslogic_objects:get_storage({uuid, FileLoc#file_location.storage_uuid}),

    {SH, File_id} = fslogic_utils:get_sh_and_id(?CLUSTER_FUSE_ID, Storage, FileLoc#file_location.storage_file_id),
    case helpers:exec(getattr, SH, [File_id]) of
        {0, #st_stat{st_size = ST_Size, st_uid = SUID} = _Stat} ->
            {ST_Size, SUID};
        {Errno, _} ->
            ?error("Cannot fetch attributes for file: ~p, errno: ~p", [File, Errno]),
            {0, -1}
    end;
get_real_file_size_and_uid(#db_document{record = #file{}}) ->
    {0, -1};
get_real_file_size_and_uid(Path) ->
    {ok, #db_document{record = #file{}} = FileDoc} = fslogic_objects:get_file(Path),
    get_real_file_size_and_uid(FileDoc).


%% update_file_size/1
%% ====================================================================
%% @doc Updates file size based on it's real size on underlying storage. Whether this call is asynchronous or not depends on
%%      fslogic_meta:update_meta_attr implementation. <br/>
%%      Does nothing if given file_info() corresponds to non-regular file.
-spec update_file_size(FileDoc :: file_doc()) ->
    UpdatedFile :: file_info().
%% ====================================================================
update_file_size(#db_document{record = #file{type = ?REG_TYPE} = File} = FileDoc) ->
    {Size, _} = get_real_file_size_and_uid(FileDoc),
    update_file_size(File, Size);
update_file_size(#db_document{record = #file{} = File}) ->
    File.


%% update_file_size/2
%% ====================================================================
%% @doc Sets file size to given value. Whether this call is asynchronous or not depends on
%%      fslogic_meta:update_meta_attr implementation.
-spec update_file_size(File :: file_info(), Size :: non_neg_integer()) ->
    UpdatedFile :: file_info().
%% ====================================================================
update_file_size(#file{} = File, Size) when Size >= 0 ->
    fslogic_meta:update_meta_attr(File, size, Size).



%% normalize_file_type/2
%% ====================================================================
%% @doc Translates given file type into internal or protocol representation
%%      (types file_type() and file_type_protocol() respectively) <br/>
%%      This method can and should be used in order to ensure that given file_type
%%      has requested format.
-spec normalize_file_type(protocol | internal, file_type() | file_type_protocol()) -> file_type() | file_type_protocol().
%% ====================================================================
normalize_file_type(protocol, ?DIR_TYPE) ->
    ?DIR_TYPE_PROT;
normalize_file_type(protocol, ?REG_TYPE) ->
    ?REG_TYPE_PROT;
normalize_file_type(protocol, ?LNK_TYPE) ->
    ?LNK_TYPE_PROT;
normalize_file_type(protocol, ?DIR_TYPE_PROT) ->
    ?DIR_TYPE_PROT;
normalize_file_type(protocol, ?REG_TYPE_PROT) ->
    ?REG_TYPE_PROT;
normalize_file_type(protocol, ?LNK_TYPE_PROT) ->
    ?LNK_TYPE_PROT;
normalize_file_type(protocol, Type) ->
    ?error("Unknown file type: ~p", [Type]),
    throw({unknown_file_type, Type});
normalize_file_type(internal, ?DIR_TYPE_PROT) ->
    ?DIR_TYPE;
normalize_file_type(internal, ?REG_TYPE_PROT) ->
    ?REG_TYPE;
normalize_file_type(internal, ?LNK_TYPE_PROT) ->
    ?LNK_TYPE;
normalize_file_type(internal, ?DIR_TYPE) ->
    ?DIR_TYPE;
normalize_file_type(internal, ?REG_TYPE) ->
    ?REG_TYPE;
normalize_file_type(internal, ?LNK_TYPE) ->
    ?LNK_TYPE;
normalize_file_type(internal, Type) ->
    ?error("Unknown file type: ~p", [Type]),
    throw({unknown_file_type, Type}).

ensure_file_location_exists(FullFileName, FileDoc) ->
    FileId = FileDoc#db_document.uuid,
    case dao_vfs:list_file_locations(FileId) of
        {ok, []} -> create_file_location_for_remote_file(FullFileName, FileId);
        _ -> ok
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

create_file_location_for_remote_file(FullFileName, FileUuid) ->
    {ok, #space_info{space_id = SpaceId} = SpaceInfo} = fslogic_utils:get_space_info_for_path(FullFileName),

    {ok, UserDoc} = fslogic_objects:get_user(),
    FileBaseName = fslogic_path:get_user_file_name(FullFileName, UserDoc),

    {ok, StorageList} = dao_lib:apply(dao_vfs, list_storage, [], fslogic_context:get_protocol_version()),
    #db_document{uuid = UUID, record = #storage_info{} = Storage} = fslogic_storage:select_storage(fslogic_context:get_fuse_id(), StorageList),
    SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
    FileId = fslogic_storage:get_new_file_id(SpaceInfo, FileBaseName, UserDoc, SHI, fslogic_context:get_protocol_version()),

    FileLocation = #file_location{file_id = FileUuid, storage_uuid = UUID, storage_file_id = FileId},
    {ok, _LocationId} = dao_lib:apply(dao_vfs, save_file_location, [FileLocation], fslogic_context:get_protocol_version()),

    {ok, _} = fslogic_objects:save_file_descriptor(fslogic_context:get_protocol_version(), FileUuid, fslogic_context:get_fuse_id(), ?LOCATION_VALIDITY),
%%     _FuseFileBlocks = [#filelocation_blockavailability{offset = 0, size = ?FILE_BLOCK_SIZE_INF}],
%%     FileBlock = #file_block{file_location_id = LocationId, offset = 0, size = ?FILE_BLOCK_SIZE_INF},
%%     {ok, _} = dao_lib:apply(dao_vfs, save_file_block, [FileBlock], fslogic_context:get_protocol_version()),

    {SH, StorageFileId} = fslogic_utils:get_sh_and_id(fslogic_context:get_fuse_id(), Storage, FileId, SpaceId, false),
    #storage_helper_info{name = SHName, init_args = SHArgs} = SH,

    Storage_helper_info = #storage_helper_info{name = SHName, init_args = SHArgs},
    ok = storage_files_manager:create(Storage_helper_info, StorageFileId).
