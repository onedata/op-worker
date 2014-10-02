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

-include("oneprovider_modules/dao/dao.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_file_location/2, get_file_location/3, get_new_file_location/3, create_file_ack/1, file_not_used/1, renew_file_location/1]).

%% ====================================================================
%% API functions
%% ====================================================================


%% get_file_location/2
%% ====================================================================
%% @doc Gets file location (implicit file open operation).
%% @end
-spec get_file_location(File :: string() | file_doc(), OpenMode :: string()) ->
    #filelocation{} | no_return().
%% ====================================================================
get_file_location(FileDoc = #db_document{record = #file{}}, ?UNSPECIFIED_MODE) ->
    get_file_location(FileDoc, none, ?UNSPECIFIED_MODE, false).


%% get_file_location/3
%% ====================================================================
%% @doc Gets file location (implicit file open operation). Allows to force-select ClusterProxy helper.
%% @end
-spec get_file_location(File :: string() | file_doc(), OpenMode :: string(), ForceClusterProxy :: boolean()) ->
    #filelocation{} | no_return().
%% ====================================================================
get_file_location(FullFileName, OpenMode, ForceClusterProxy) when is_list(FullFileName) ->
    {ok, FileDoc} = fslogic_objects:get_file(FullFileName),
    get_file_location(FileDoc, FullFileName, OpenMode, ForceClusterProxy).

%% get_file_location/4
%% ====================================================================
%% @doc Gets file location (implicit file open operation).
%% @end
-spec get_file_location(FileDoc :: file_doc(), FullFileName :: string(), OpenMode :: string(), ForceClusterProxy :: boolean()) ->
    #filelocation{} | no_return().
%% ====================================================================
get_file_location(FileDoc, FullFileName, OpenMode, ForceClusterProxy) ->
    ?info("get_file_location(~p, ~p, ~p, ~p)", [FileDoc, FullFileName, OpenMode, ForceClusterProxy]),
    Validity = ?LOCATION_VALIDITY,
    case FileDoc#db_document.record#file.type of
        ?REG_TYPE -> ok;
        UnSuppType ->
            ?error("Unsupported operation: get_file_location for file [UUID ~p] with type: ~p", [FileDoc#db_document.uuid, UnSuppType]),
            throw(?VENOTSUP)
    end,

    fslogic_file:fix_storage_owner(FileDoc),

    {ok, UserDoc} = fslogic_objects:get_user(),
    ok = fslogic_perms:check_file_perms(FullFileName,UserDoc,FileDoc,list_to_existing_atom(OpenMode)),

    {ok,_} = fslogic_objects:save_file_descriptor(fslogic_context:get_protocol_version(), FileDoc#db_document.uuid, fslogic_context:get_fuse_id(), Validity),

    FileDesc = FileDoc#db_document.record,
    FileLoc = fslogic_file:get_file_local_location(FileDesc),

    {ok, #space_info{space_id = SpaceId}} = fslogic_utils:get_space_info_for_path(FullFileName),

    {ok, #db_document{record = Storage}} = fslogic_objects:get_storage({uuid, FileLoc#file_location.storage_id}),

    {SH, File_id} = fslogic_utils:get_sh_and_id(fslogic_context:get_fuse_id(), Storage, FileLoc#file_location.file_id, SpaceId, ForceClusterProxy),
    #filelocation{storage_id = Storage#storage_info.id, file_id = File_id, validity = Validity,
        storage_helper_name = SH#storage_helper_info.name, storage_helper_args = SH#storage_helper_info.init_args}.


%% get_new_file_location/3
%% ====================================================================
%% @doc Gets new file location (implicit mknod operation).
%% @end
-spec get_new_file_location(FullFileName :: string(), Mode :: non_neg_integer(), ForceClusterProxy :: boolean()) ->
    #filelocation{} | no_return().
%% ====================================================================
get_new_file_location(FullFileName, Mode, ForceClusterProxy) ->
    ?info("get_new_file_location(FullFileName ~p, Mode: ~p, ForceClusterProxy: ~p)", [FullFileName, Mode, ForceClusterProxy]),

    NewFileName = fslogic_path:basename(FullFileName),
    ParentFileName = fslogic_path:strip_path_leaf(FullFileName),
    {ok, #db_document{record = #file{}} = ParentDoc} = fslogic_objects:get_file(ParentFileName),

    {ok, UserDoc} = fslogic_objects:get_user(),
    FileBaseName = fslogic_path:get_user_file_name(FullFileName, UserDoc),

    ok = fslogic_perms:check_file_perms(FileBaseName, UserDoc, ParentDoc, write),

    {ok, #space_info{space_id = SpaceId} = SpaceInfo} = fslogic_utils:get_space_info_for_path(FullFileName),

    {ok, StorageList} = dao_lib:apply(dao_vfs, list_storage, [], fslogic_context:get_protocol_version()),
    #db_document{uuid = UUID, record = #storage_info{} = Storage} = fslogic_storage:select_storage(fslogic_context:get_fuse_id(), StorageList),
    SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
    FileId = fslogic_storage:get_new_file_id(SpaceInfo, FileBaseName, UserDoc, SHI, fslogic_context:get_protocol_version()),
    FileLocation = #file_location{storage_id = UUID, file_id = FileId},

    {ok, UserID} = fslogic_context:get_user_id(),

    CTime = opn_utils:time(),

    FileRecordInit = #file{type = ?REG_TYPE, name = NewFileName, uid = UserID, parent = ParentDoc#db_document.uuid, perms = Mode, location = FileLocation, created = false},
    %% Async *times update
    FileRecord = fslogic_meta:update_meta_attr(FileRecordInit, times, {CTime, CTime, CTime}),

    Validity = ?LOCATION_VALIDITY,
    FCreateStatus = dao_lib:apply(dao_vfs, save_new_file, [FullFileName, FileRecord], fslogic_context:get_protocol_version()),

    case FCreateStatus of
        {ok, {waiting_file, ExistingWFile}} ->
            ExistingWFileUUID = ExistingWFile#db_document.uuid,
            fslogic_meta:update_parent_ctime(FileBaseName, CTime),
            {ok, _} = fslogic_objects:save_file_descriptor(fslogic_context:get_protocol_version(), ExistingWFileUUID, fslogic_context:get_fuse_id(), Validity),

            ExistingWFileRecord = ExistingWFile#db_document.record,
            ExistingWFileLocation= ExistingWFileRecord#file.location,

            {ok, #db_document{record = ExistingWFileStorage}} = fslogic_objects:get_storage({uuid, ExistingWFileLocation#file_location.storage_id}),
            {SH, File_id2} = fslogic_utils:get_sh_and_id(fslogic_context:get_fuse_id(), ExistingWFileStorage, ExistingWFileLocation#file_location.file_id, SpaceId, ForceClusterProxy),
            #storage_helper_info{name = ExistingWFileStorageSHName, init_args = ExistingWFileStorageSHArgs} = SH,
            #filelocation{storage_id = Storage#storage_info.id, file_id = File_id2, validity = Validity, storage_helper_name = ExistingWFileStorageSHName, storage_helper_args = ExistingWFileStorageSHArgs};
        {ok, FileUUID} ->
            fslogic_meta:update_parent_ctime(FileBaseName, CTime),
            {ok, _} = fslogic_objects:save_file_descriptor(fslogic_context:get_protocol_version(), FileUUID, fslogic_context:get_fuse_id(), Validity),

            {SH, File_id2} = fslogic_utils:get_sh_and_id(fslogic_context:get_fuse_id(), Storage, FileId, SpaceId, ForceClusterProxy),
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
        {ok, #db_document{record = #file{} = OldFile} = OldDoc} ->
            ChangedFile = OldDoc#db_document{record = OldFile#file{created = true}},
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
            [DbDoc | _] = Descriptors,
            Validity = ?LOCATION_VALIDITY,

            {ok, _} = fslogic_objects:save_file_descriptor(fslogic_context:get_protocol_version(), DbDoc, Validity),
            #filelocationvalidity{answer = ?VOK, validity = Validity};
        _Many ->
            ?error("Error: can not renew file location for file: ~s, too many file descriptors", [FullFileName]),
            #filelocationvalidity{answer = ?VEREMOTEIO, validity = 0}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
