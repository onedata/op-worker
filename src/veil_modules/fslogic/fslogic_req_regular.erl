%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: FSLogic request handlers for regular files.
%% @end
%% ===================================================================
-module(fslogic_req_regular).
-author("Rafal Slota").

-include("veil_modules/dao/dao.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao_types.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_file_location/1, get_new_file_location/2, create_file_ack/1, file_not_used/1, renew_file_location/1]).

%% ====================================================================
%% API functions
%% ====================================================================


%% get_file_location/1
%% ====================================================================
%% @doc Gets file location (implicit file open operation).
%% @end
-spec get_file_location(FullFileName :: string() | file_doc()) ->
    #filelocation{} | no_return().
%% ====================================================================
get_file_location(FileDoc = #veil_document{record = #file{}}) ->
    Validity = ?LOCATION_VALIDITY,
    case FileDoc#veil_document.record#file.type of
        ?REG_TYPE -> ok;
        UnSuppType ->
            ?error("Unsupported operation: get_file_location for file [UUID ~p] with type: ~p", [FileDoc#veil_document.uuid, UnSuppType]),
            throw(?VENOTSUP)
    end,

    {ok, _} = fslogic_objects:save_file_descriptor(fslogic_context:get_protocol_version(), FileDoc#veil_document.uuid, fslogic_context:get_fuse_id(), Validity),

    FileDesc = FileDoc#veil_document.record,
    FileLoc = fslogic_file:get_file_local_location(FileDesc),

    {ok, #veil_document{record = Storage}} = fslogic_objects:get_storage({uuid, FileLoc#file_location.storage_id}),

    {SH, File_id} = fslogic_utils:get_sh_and_id(fslogic_context:get_fuse_id(), Storage, FileLoc#file_location.file_id),
    #filelocation{storage_id = Storage#storage_info.id, file_id = File_id, validity = Validity,
        storage_helper_name = SH#storage_helper_info.name, storage_helper_args = SH#storage_helper_info.init_args};
get_file_location(FullFileName) ->
    ?debug("get_file_location(FullFileName ~p)", [FullFileName]),
    {ok, FileDoc} = fslogic_objects:get_file(FullFileName),
    get_file_location(FileDoc).


%% get_new_file_location/2
%% ====================================================================
%% @doc Gets new file location (implicit mknod operation).
%% @end
-spec get_new_file_location(FullFileName :: string(), Mode :: non_neg_integer()) ->
    #filelocation{} | no_return().
%% ====================================================================
get_new_file_location(FullFileName, Mode) ->
    ?debug("get_new_file_location(FullFileName ~p, Mode: ~p)", [FullFileName, Mode]),

    NewFileName = fslogic_path:basename(FullFileName),
    ParentFileName = fslogic_path:strip_path_leaf(FullFileName),
    {ok, #veil_document{record = #file{}} = ParentDoc} = fslogic_objects:get_file(ParentFileName),

    {ok, UserDoc} = fslogic_objects:get_user(),
    FileBaseName = fslogic_path:get_user_file_name(FullFileName, UserDoc),

    ok = fslogic_perms:check_file_perms(FileBaseName, UserDoc, ParentDoc, write),

    {ok, StorageList} = dao_lib:apply(dao_vfs, list_storage, [], fslogic_context:get_protocol_version()),
    #veil_document{uuid = UUID, record = #storage_info{} = Storage} = fslogic_storage:select_storage(fslogic_context:get_fuse_id(), StorageList),
    SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
    FileId = fslogic_storage:get_new_file_id(FileBaseName, UserDoc, SHI, fslogic_context:get_protocol_version()),
    FileLocation = #file_location{storage_id = UUID, file_id = FileId},

    {ok, UserID} = fslogic_context:get_user_id(),

    CTime = vcn_utils:time(),

    Groups = fslogic_utils:get_group_owner(FileBaseName), %% Get owner group name based on file access path

    FileRecordInit = #file{type = ?REG_TYPE, name = NewFileName, uid = UserID, gids = Groups, parent = ParentDoc#veil_document.uuid, perms = Mode, location = FileLocation, created = false},
    %% Async *times update
    FileRecord = fslogic_meta:update_meta_attr(FileRecordInit, times, {CTime, CTime, CTime}),

    Validity = ?LOCATION_VALIDITY,
    FCreateStatus = dao_lib:apply(dao_vfs, save_new_file, [FullFileName, FileRecord], fslogic_context:get_protocol_version()),

    case FCreateStatus of
        {ok, {waiting_file, ExistingWFile}} ->
            ExistingWFileUUID = ExistingWFile#veil_document.uuid,
            fslogic_meta:update_parent_ctime(FileBaseName, CTime),
            {ok, _} = fslogic_objects:save_file_descriptor(fslogic_context:get_protocol_version(), ExistingWFileUUID, fslogic_context:get_fuse_id(), Validity),

            ExistingWFileRecord = ExistingWFile#veil_document.record,
            ExistingWFileLocation= ExistingWFileRecord#file.location,

            {ok, #veil_document{record = ExistingWFileStorage}} = fslogic_objects:get_storage({uuid, ExistingWFileLocation#file_location.storage_id}),
            {SH, File_id2} = fslogic_utils:get_sh_and_id(fslogic_context:get_fuse_id(), ExistingWFileStorage, ExistingWFileLocation#file_location.file_id),
            #storage_helper_info{name = ExistingWFileStorageSHName, init_args = ExistingWFileStorageSHArgs} = SH,
            #filelocation{storage_id = Storage#storage_info.id, file_id = File_id2, validity = Validity, storage_helper_name = ExistingWFileStorageSHName, storage_helper_args = ExistingWFileStorageSHArgs};
        {ok, FileUUID} ->
            fslogic_meta:update_parent_ctime(FileBaseName, CTime),
            {ok, _} = fslogic_objects:save_file_descriptor(fslogic_context:get_protocol_version(), FileUUID, fslogic_context:get_fuse_id(), Validity),

            {SH, File_id2} = fslogic_utils:get_sh_and_id(fslogic_context:get_fuse_id(), Storage, FileId),
            #storage_helper_info{name = SHName, init_args = SHArgs} = SH,
            #filelocation{storage_id = Storage#storage_info.id, file_id = File_id2, validity = Validity, storage_helper_name = SHName, storage_helper_args = SHArgs}
    end.


%% create_file_ack/1
%% ====================================================================
%% @doc ACK file creation on storage.
%% @end
-spec create_file_ack(FullFileName :: string()) ->
    #atom{} | no_return().
%% ====================================================================
create_file_ack(FullFileName) ->
    ?debug("create_file_ack(FullFileName ~p)", [FullFileName]),

    case fslogic_objects:get_waiting_file(FullFileName) of
        {ok, #veil_document{record = #file{} = OldFile} = OldDoc} ->
            ChangedFile = OldDoc#veil_document{record = OldFile#file{created = true}},
            {ok, _} = fslogic_objects:save_file(ChangedFile),

            #atom{value = ?VOK};
        {error, file_not_found} ->
            {ok, _} = fslogic_objects:get_file(FullFileName),
            #atom{value = ?VOK}
    end.


%% file_not_used/1
%% ====================================================================
%% @doc Marks the file as not-used by the FUSE (implicit last-release operation).
%% @end
-spec file_not_used(FullFileName :: string()) ->
    #atom{} | no_return().
%% ====================================================================
file_not_used(FullFileName) ->
    ?debug("file_not_used(FullFileName ~p)", [FullFileName]),

    ok = dao_lib:apply(dao_vfs, remove_descriptor, [{by_file_n_owner, {FullFileName, fslogic_context:get_fuse_id()}}], fslogic_context:get_protocol_version()),
    #atom{value = ?VOK}.


%% renew_file_location/1
%% ====================================================================
%% @doc Renew file location lock.
%% @end
-spec renew_file_location(FullFileName :: string()) ->
    #filelocationvalidity{} | no_return().
%% ====================================================================
renew_file_location(FullFileName) ->
    ?debug("renew_file_location(FullFileName ~p)", [FullFileName]),

    {ok, Descriptors} = dao_lib:apply(dao_vfs, list_descriptors, [{by_file_n_owner, {FullFileName, fslogic_context:get_fuse_id()}}, 10, 0], fslogic_context:get_protocol_version()),
    case length(Descriptors) of
        0 ->
            ?error("Error: can not renew file location for file: ~s, descriptor not found", [FullFileName]),
            #filelocationvalidity{answer = ?VENOENT, validity = 0};
        1 ->
            [VeilDoc | _] = Descriptors,
            Validity = ?LOCATION_VALIDITY,

            {ok, _} = fslogic_objects:save_file_descriptor(fslogic_context:get_protocol_version(), VeilDoc, Validity),
            #filelocationvalidity{answer = ?VOK, validity = Validity};
        _Many ->
            ?error("Error: can not renew file location for file: ~s, too many file descriptors", [FullFileName]),
            #filelocationvalidity{answer = ?VEREMOTEIO, validity = 0}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
