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
-module(fslogic_req_regular).
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
-include("veil_modules/fslogic/fslogic.hrl").
-include("logging.hrl").

%% API
-export([get_file_location/1, get_new_file_location/2, create_file_ack/1, file_not_used/1, renew_file_location/1]).

%% ====================================================================
%% API functions
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

    {ok, #veil_document{record = Storage}} = fslogic_objects:get_storage({id, FileLoc#file_location.storage_id}),

    {SH, File_id} = fslogic_utils:get_sh_and_id(fslogic_context:get_fuse_id(), Storage, FileLoc#file_location.file_id),
    #filelocation{storage_id = Storage#storage_info.id, file_id = File_id, validity = Validity,
        storage_helper_name = SH#storage_helper_info.name, storage_helper_args = SH#storage_helper_info.init_args};
get_file_location(FullFileName) ->
    {ok, FileDoc} = fslogic_objects:get_file(FullFileName),
    get_file_location(FileDoc).

get_new_file_location(FullFileName, Mode) ->
    {ParentFound, ParentInfo} = fslogic_utils:get_parent_and_name_from_path(FullFileName, fslogic_context:get_protocol_version()),
    {UserDocStatus, UserDoc} = fslogic_objects:get_user(),
    FileBaseName = fslogic_utils:get_user_file_name(FullFileName, UserDoc),
        case ParentFound of
            ok ->
                {FileName, Parent} = ParentInfo,
                {PermsStat, PermsOK} = fslogic_utils:check_file_perms(FileBaseName, UserDocStatus, UserDoc, Parent, write),
                case PermsStat of
                    ok ->
                        case PermsOK of
                            true ->
                                {ok, StorageList} = dao_lib:apply(dao_vfs, list_storage, [], fslogic_context:get_protocol_version()),
                                case fslogic_storage:select_storage(fslogic_context:get_fuse_id(), StorageList) of
                                    #veil_document{uuid = UUID, record = #storage_info{} = Storage} ->
                                        SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
                                        File_id = fslogic_utils:get_new_file_id(FileBaseName, UserDoc, SHI, fslogic_context:get_protocol_version()),
                                        FileLocation = #file_location{storage_id = UUID, file_id = File_id},

                                        {UserIdStatus, UserId} = case {UserDocStatus, UserDoc} of
                                                                     {ok, _} -> {ok, UserDoc#veil_document.uuid};
                                                                     {error, get_user_id_error} -> {ok, ?CLUSTER_USER_ID};
                                                                     _ -> {UserDocStatus, UserDoc}
                                                                 end,

                                        case UserIdStatus of
                                            ok ->
                                                CTime = fslogic_utils:time(),

                                                Groups = fslogic_utils:get_group_owner(FileBaseName), %% Get owner group name based on file access path

                                                FileRecordInit = #file{type = ?REG_TYPE, name = FileName, uid = UserId, gids = Groups, parent = Parent#veil_document.uuid, perms = Mode, location = FileLocation, created = false},
                                                %% Async *times update
                                                FileRecord = fslogic_meta:update_meta_attr(FileRecordInit, times, {CTime, CTime, CTime}),

                                                Status = dao_lib:apply(dao_vfs, save_new_file, [FullFileName, FileRecord], fslogic_context:get_protocol_version()),
                                                Validity = ?LOCATION_VALIDITY,
                                                case Status of
                                                    {ok, {waiting_file, ExistingWFile}} ->
                                                        ExistingWFileUUID = ExistingWFile#veil_document.uuid,
                                                        fslogic_meta:update_parent_ctime(FileBaseName, CTime),
                                                        {ExistingWFileStatus2, _} = fslogic_objects:save_file_descriptor(fslogic_context:get_protocol_version(), ExistingWFileUUID, fslogic_context:get_fuse_id(), Validity),
                                                        case ExistingWFileStatus2 of
                                                            ok ->
                                                                ExistingWFileRecord = ExistingWFile#veil_document.record,
                                                                ExistingWFileLocation= ExistingWFileRecord#file.location,

                                                                case dao_lib:apply(dao_vfs, get_storage, [{uuid, ExistingWFileLocation#file_location.storage_id}], fslogic_context:get_protocol_version()) of
                                                                    {ok, #veil_document{record = ExistingWFileStorage}} ->
                                                                        {SH, File_id2} = fslogic_utils:get_sh_and_id(fslogic_context:get_fuse_id(), ExistingWFileStorage, ExistingWFileLocation#file_location.file_id),
                                                                        #storage_helper_info{name = ExistingWFileStorageSHName, init_args = ExistingWFileStorageSHArgs} = SH,
                                                                        #filelocation{storage_id = Storage#storage_info.id, file_id = File_id2, validity = Validity, storage_helper_name = ExistingWFileStorageSHName, storage_helper_args = ExistingWFileStorageSHArgs};
                                                                    ExistingWFileBadStatus2 ->
                                                                        lager:error([{mod, ?MODULE}], "Error: cannot get storage for existing waiting file location: ~p", [ExistingWFileBadStatus2]),
                                                                        #filelocation{answer = ?VEREMOTEIO, storage_id = -1, file_id = "", validity = 0}
                                                                end;
                                                            BadStatus2 ->
                                                                lager:error([{mod, ?MODULE}], "Error: cannot save file_descriptor document: ~p", [BadStatus2]),
                                                                #filelocation{answer = ?VEREMOTEIO, storage_id = -1, file_id = "", validity = 0}
                                                        end;
                                                    {ok, FileUUID} ->
                                                        fslogic_meta:update_parent_ctime(FileBaseName, CTime),
                                                        {Status2, _TmpAns2} = fslogic_objects:save_file_descriptor(fslogic_context:get_protocol_version(), FileUUID, fslogic_context:get_fuse_id(), Validity),
                                                        case Status2 of
                                                            ok ->
                                                                {SH, File_id2} = fslogic_utils:get_sh_and_id(fslogic_context:get_fuse_id(), Storage, File_id),
                                                                #storage_helper_info{name = SHName, init_args = SHArgs} = SH,
                                                                #filelocation{storage_id = Storage#storage_info.id, file_id = File_id2, validity = Validity, storage_helper_name = SHName, storage_helper_args = SHArgs};
                                                            _BadStatus2 ->
                                                                lager:error([{mod, ?MODULE}], "Error: cannot save file_descriptor document: ~p", [_BadStatus2]),
                                                                #filelocation{answer = ?VEREMOTEIO, storage_id = -1, file_id = "", validity = 0}
                                                        end;
                                                    {error, file_exists} ->
                                                        #filelocation{answer = ?VEEXIST, storage_id = -1, file_id = "", validity = 0};
                                                    _BadStatus ->
                                                        lager:error([{mod, ?MODULE}], "Error: cannot save file document: ~p", [_BadStatus]),
                                                        #filelocation{answer = ?VEREMOTEIO, storage_id = -1, file_id = "", validity = 0}
                                                end;
                                            _ ->
                                                lager:error([{mod, ?MODULE}], "Error: user doc error for file ~p, error ~p", [FullFileName, {UserDocStatus, UserDoc}]),
                                                #filelocation{answer = ?VEREMOTEIO, storage_id = -1, file_id = "", validity = 0}
                                        end;
                                    {error, SelectError} ->
                                        lager:error([{mod, ?MODULE}], "Error: can not get storage information: ~p", [SelectError]),
                                        #filelocation{answer = ?VEREMOTEIO, storage_id = -1, file_id = "", validity = 0}
                                end;
                            false ->
                                lager:warning("Creating file without permissions: ~p", [FileName]),
                                #filelocation{answer = ?VEPERM, storage_id = -1, file_id = "", validity = 0}
                        end;
                    _ ->
                        lager:warning("Cannot create file. Reason: ~p:~p", [PermsStat, PermsOK]),
                        #filelocation{answer = ?VEPERM, storage_id = -1, file_id = "", validity = 0}
                end;
            _ParentError ->
                lager:error([{mod, ?MODULE}], "Error: can not get parent for file: ~p", [FullFileName]),
                #filelocation{answer = ?VEREMOTEIO, storage_id = -1, file_id = "", validity = 0}
        end.

create_file_ack(FullFileName) ->
    case fslogic_objects:get_waiting_file(FullFileName) of
        {ok, #veil_document{record = #file{} = OldFile} = OldDoc} ->
            ChangedFile = OldDoc#veil_document{record = OldFile#file{created = true}},
            case dao_lib:apply(dao_vfs, save_file, [ChangedFile], fslogic_context:get_protocol_version()) of
                {ok, _} ->
                    #atom{value = ?VOK};
                Other ->
                    lager:warning("Cannot save file document. Reason: ~p", [Other]),
                    #atom{value = ?VEREMOTEIO}
            end;
        {error, file_not_found} ->
            case fslogic_objects:get_file(FullFileName) of
                {ok, _} ->
                    #atom{value = ?VOK};
                _ ->
                    lager:warning("Cannot find waiting file: ~p", [FullFileName]),
                    #atom{value = ?VENOENT}
            end;
        UnknownError ->
            ?error("create_file_ack unknown error: ~p", [UnknownError]),
            #atom{value = ?VEREMOTEIO}
    end.

file_not_used(FullFileName) ->
    ok = dao_lib:apply(dao_vfs, remove_descriptor, [{by_file_n_owner, {FullFileName, fslogic_context:get_fuse_id()}}], fslogic_context:get_protocol_version()),
    #atom{value = ?VOK}.

renew_file_location(FullFileName) ->
    {Status, TmpAns} = dao_lib:apply(dao_vfs, list_descriptors, [{by_file_n_owner, {FullFileName, fslogic_context:get_fuse_id()}}, 10, 0], fslogic_context:get_protocol_version()),
    case Status of
        ok ->
            case length(TmpAns) of
                0 ->
                    lager:error([{mod, ?MODULE}], "Error: can not renew file location for file: ~s, descriptor not found", [FullFileName]),
                    #filelocationvalidity{answer = ?VENOENT, validity = 0};
                1 ->
                    [VeilDoc | _] = TmpAns,
                    Validity = ?LOCATION_VALIDITY,

                    {Status2, _TmpAns2} = fslogic_objects:save_file_descriptor(fslogic_context:get_protocol_version(), VeilDoc, Validity),
                    case Status2 of
                        ok ->
                            #filelocationvalidity{answer = ?VOK, validity = Validity};
                        _BadStatus2 ->
                            lager:error([{mod, ?MODULE}], "Error: can not renew file location for file: ~s", [FullFileName]),
                            #filelocationvalidity{answer = ?VEREMOTEIO, validity = 0}
                    end;
                _Many ->
                    lager:error([{mod, ?MODULE}], "Error: can not renew file location for file: ~s, too many file descriptors", [FullFileName]),
                    #filelocationvalidity{answer = ?VEREMOTEIO, validity = 0}
            end;
        _Other ->
            lager:error([{mod, ?MODULE}], "Error: can not renew file location for file: ~s", [FullFileName]),
            #filelocationvalidity{answer = ?VEREMOTEIO, validity = 0}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
