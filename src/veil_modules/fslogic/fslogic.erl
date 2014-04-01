%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour to provide
%% functionality of file system logic.
%% @end
%% ===================================================================

-module(fslogic).
-behaviour(worker_plugin_behaviour).

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

-define(LOCATION_VALIDITY, 60*15).

-define(FILE_COUNTING_BASE, 256).

%% Which fuse operations (messages) are allowed to operate on base group directory ("/groups")
-define(GROUPS_BASE_ALLOWED_ACTIONS,    [getfileattr, updatetimes, getfilechildren]).

%% Which fuse operations (messages) are allowed to operate on second level group directory (e.g. "/groups/grpName")
-define(GROUPS_ALLOWED_ACTIONS,         [getfileattr, getnewfilelocation, createdir, updatetimes, createlink, getfilechildren]).

%% ====================================================================
%% API
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).
-export([get_full_file_name/1, get_file/3, get_user_id/0, get_user_root/0, get_user_root/1, get_user_groups/2, get_user_doc/0]).

%% ====================================================================
%% Test API
%% ====================================================================
-ifdef(TEST).
%% eunit
-export([handle_fuse_message/3, verify_file_name/1]).
%% ct
-export([get_files_number/3, create_dirs/4]).
-endif.

%% ====================================================================
%% API functions
%% ====================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1
-spec init(Args :: term()) -> list().
%% ====================================================================
init(_Args) ->
  Pid = self(),
  {ok, CleaningInterval} = application:get_env(veil_cluster_node, fslogic_cleaning_period),
  erlang:send_after(CleaningInterval * 1000, Pid, {timer, {asynch, 1, {delete_old_descriptors, Pid}}}),
  {ok, FilesSizeUpdateInterval} = application:get_env(veil_cluster_node, user_files_size_view_update_period),
  erlang:send_after(FilesSizeUpdateInterval * 1000, Pid, {timer, {asynch, 1, {update_user_files_size_view, Pid}}}),
  [].

%% handle/2
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1. <br/>
%% Processes standard worker requests (e.g. ping) and requests from FUSE.
%% @end
-spec handle(ProtocolVersion :: term(), Request :: term()) -> Result when
  Result :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, is_write_enabled) ->
  case user_logic:get_user({dn, get(user_id)}) of
    {ok, UserDoc} ->
      case user_logic:get_quota(UserDoc) of
        {ok, #quota{exceeded = Exceeded}} when is_binary(Exceeded) -> not(Exceeded);
        Error ->
          ?warning("cannot get quota doc for user with dn: ~p, Error: ~p", [get(user_id), Error]),
          false
      end;
    Error ->
      ?warning("cannot get user with dn: ~p, Error: ~p", [get(user_id), Error]),
      false
  end;

handle(_ProtocolVersion, healthcheck) ->
	ok;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

%% For tests
handle(ProtocolVersion, {delete_old_descriptors_test, Time}) ->
  handle_test(ProtocolVersion, {delete_old_descriptors_test, Time});

handle(ProtocolVersion, {update_user_files_size_view, Pid}) ->
  update_user_files_size_view(ProtocolVersion),
  {ok, Interval} = application:get_env(veil_cluster_node, user_files_size_view_update_period),
  erlang:send_after(Interval * 1000, Pid, {timer, {asynch, 1, {update_user_files_size_view, Pid}}}),
  ok;

handle(_ProtocolVersion, {answer_test_message, FuseID, Message}) ->
  request_dispatcher:send_to_fuse(FuseID, #testchannelanswer{message = Message}, "fuse_messages"),
  ok;

handle(ProtocolVersion, {delete_old_descriptors, Pid}) ->
  {Megaseconds,Seconds, _Microseconds} = os:timestamp(),
  Time = 1000000*Megaseconds + Seconds - 15,
  delete_old_descriptors(ProtocolVersion, Time),
  {ok, Interval} = application:get_env(veil_cluster_node, fslogic_cleaning_period),
  erlang:send_after(Interval * 1000, Pid, {timer, {asynch, ProtocolVersion, {delete_old_descriptors, Pid}}}),
  ok;

handle(ProtocolVersion, {getfilelocation_uuid, UUID}) ->
  {DocFindStatus, FileDoc} = dao_lib:apply(dao_vfs, get_file, [{uuid, UUID}], ProtocolVersion),
  getfilelocation(ProtocolVersion, DocFindStatus, FileDoc, ?CLUSTER_FUSE_ID);

handle(ProtocolVersion, {getfileattr, UUID}) ->
  {DocFindStatus, FileDoc} = dao_lib:apply(dao_vfs, get_file, [{uuid, UUID}], ProtocolVersion),
  getfileattr(ProtocolVersion, DocFindStatus, FileDoc);

handle(ProtocolVersion, Record) when is_record(Record, fusemessage) ->
    try
        handle_fuse_message(ProtocolVersion, Record#fusemessage.input, get(fuse_id))
    catch
        ErrorRet -> ErrorRet
    end;

handle(ProtocolVersion, {internal_call, Record}) ->
    try
        handle_fuse_message(ProtocolVersion, Record, ?CLUSTER_FUSE_ID)
    catch
        ErrorRet -> ErrorRet
    end;

handle(_ProtocolVersion, Record) when is_record(Record, callback) ->
  Answer = case Record#callback.action of
    channelregistration ->
      try
        gen_server:call({global, ?CCM}, {addCallback, Record#callback.fuse, Record#callback.node, Record#callback.pid}, 1000)
      catch
        _:_ ->
          error
      end;
    channelclose ->
      try
        gen_server:call({global, ?CCM}, {delete_callback, Record#callback.fuse, Record#callback.node, Record#callback.pid}, 1000)
      catch
        _:_ ->
          error
      end
  end,
  #atom{value = atom_to_list(Answer)};

%% Handle requests that have wrong structure.
handle(_ProtocolVersion, _Msg) ->
  wrong_request.

%% handle_test/3
%% ====================================================================
%% @doc Handles calls used during tests
-spec handle_test(ProtocolVersion :: term(), Request :: term()) -> Result when
  Result :: atom().
%% ====================================================================
-ifdef(TEST).
handle_test(ProtocolVersion, {delete_old_descriptors_test, Time}) ->
  delete_old_descriptors(ProtocolVersion, Time),
  ok.
-else.
handle_test(_ProtocolVersion, _Request) ->
  not_supported_in_normal_mode.
-endif.

%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0
-spec cleanup() -> ok.
%% ====================================================================
cleanup() ->
  ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% handle_fuse_message/3
%% ====================================================================
%% @doc Processes requests from FUSE.
%% @end
-spec handle_fuse_message(ProtocolVersion :: term(), Record :: tuple(), FuseID :: string()) -> Result when
  Result :: term().
%% ====================================================================
handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, updatetimes) ->
    {FileNameFindingAns, FileName} = get_full_file_name(Record#updatetimes.file_logic_name, updatetimes),
    case FileName of
        [?PATH_SEPARATOR] ->
            lager:warning("Trying to update times for root directory. FuseID: ~p, Request: ~p. Aborting.", [FuseID, Record]),
            throw(invalid_updatetimes_request);
        _ -> ok
    end,
    lager:debug("Updating times for file: ~p, Request: ~p", [FileName, Record]),
    case FileNameFindingAns of
        ok ->
            case get_file(ProtocolVersion, FileName, FuseID) of
                {ok, #veil_document{record = #file{} = File} = Doc} ->
                    File1 = fslogic_utils:update_meta_attr(File, times, {Record#updatetimes.atime, Record#updatetimes.mtime}),
                    File2 = fslogic_utils:update_meta_attr(File1, ctime, Record#updatetimes.ctime),

                    Status = string:equal(File2#file.meta_doc, File#file.meta_doc),
                    if
                        Status -> #atom{value = ?VOK};
                        true ->
                            %% TODO it may cause a problem with other operations (confilicts in DB) - it should be checked
                            dao_lib:apply(dao_vfs, {asynch, save_file}, [Doc#veil_document{record = File2}], ProtocolVersion),
                            #atom{value = ?VOK}
                    end;
                Other ->
                    lager:error("updatetimes: fslogic could not get file ~p due to: ~p", [FileName, Other]),
                    #atom{value = ?VEREMOTEIO}
            end;
        ?VEPERM -> #atom{value = ?VEPERM};
        _ -> #atom{value = ?VEREMOTEIO}
    end;
handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, changefileowner) ->
    {UserDocStatus, UserDoc} = get_user_doc(),
    {FileNameFindingAns, FileName} = get_full_file_name(Record#changefileowner.file_logic_name, changefileowner, UserDocStatus, UserDoc),
    case FileNameFindingAns of
      ok ->
        case get_file(ProtocolVersion, FileName, FuseID) of
            {ok, #veil_document{record = #file{} = File} = Doc} ->
              {PermsStat, PermsOK} = check_file_perms(FileName, UserDocStatus, UserDoc, Doc, root),
              case PermsStat of
                ok ->
                  case PermsOK of
                    true ->
                      {Return, NewFile} =
                          case dao_lib:apply(dao_users, get_user, [{login, Record#changefileowner.uname}], ProtocolVersion) of
                              {ok, #veil_document{record = #user{}, uuid = UID}} ->
                                  {?VOK, File#file{uid = UID}};
                              {error, user_not_found} ->
                                  lager:warning("chown: cannot find user with name ~p. lTrying UID (~p) lookup...", [Record#changefileowner.uname, Record#changefileowner.uid]),
                                  case dao_lib:apply(dao_users, get_user, [{uuid, integer_to_list(Record#changefileowner.uid)}], ProtocolVersion) of
                                      {ok, #veil_document{record = #user{}, uuid = UID1}} ->
                                          {?VOK, File#file{uid = UID1}};
                                      {error, {not_found, missing}} ->
                                          lager:warning("chown: cannot find user with uid ~p", [Record#changefileowner.uid]),
                                          {?VEINVAL, File};
                                      {error, Reason1} ->
                                          lager:error("chown: cannot find user with uid ~p due to error: ~p", [Record#changefileowner.uid, Reason1]),
                                          {?VEREMOTEIO, File}
                                  end;
                              {error, Reason1} ->
                                  lager:error("chown: cannot find user with uid ~p due to error: ~p", [Record#changefileowner.uid, Reason1]),
                                  {?VEREMOTEIO, File}
                          end,

                      case Return of
                          ?VOK ->
                              NewFile1 = fslogic_utils:update_meta_attr(NewFile, ctime, fslogic_utils:time()),
                              case dao_lib:apply(dao_vfs, save_file, [Doc#veil_document{record = NewFile1}], ProtocolVersion) of
                                  {ok, _} -> #atom{value = ?VOK};
                                  Other1 ->
                                      lager:error("fslogic could not save file ~p due to: ~p", [FileName, Other1]),
                                      #atom{value = ?VEREMOTEIO}
                              end;
                          OtherReturn ->
                              #atom{value = OtherReturn}
                      end;
                    false ->
                      lager:warning("Changing file's owner without permissions: ~p", [FileName]),
                      #atom{value = ?VEPERM}
                  end;
                _ ->
                  lager:warning("Cannot check permissions of file. Reason: ~p:~p", [PermsStat, PermsOK]),
                  #atom{value = ?VEREMOTEIO}
              end;
            {error, file_not_found} -> #atom{value = ?VENOENT};
            Other ->
                lager:error("fslogic could not get file ~p due to: ~p", [FileName, Other]),
                #atom{value = ?VEREMOTEIO}
        end;
      ?VEPERM -> #atom{value = ?VEPERM};
      _  -> #atom{value = ?VEREMOTEIO}
    end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, changefilegroup) ->
    {FileNameFindingAns, FileName} = get_full_file_name(Record#changefilegroup.file_logic_name, changefilegroup),
    case FileNameFindingAns of
      ok ->
        case get_file(ProtocolVersion, FileName, FuseID) of
            {ok, #veil_document{record = #file{} = File} = Doc} ->
                {Return, NewFile} = {?VENOTSUP, File}, %% @TODO: not implemented, add permission checking during the implementation

                case Return of
                    ?VOK ->
                        NewFile1 = fslogic_utils:update_meta_attr(NewFile, ctime, fslogic_utils:time()),
                        case dao_lib:apply(dao_vfs, save_file, [Doc#veil_document{record = NewFile1}], ProtocolVersion) of
                            {ok, _} -> #atom{value = ?VOK};
                            Other1 ->
                                lager:error("fslogic could not save file ~p due to: ~p", [FileName, Other1]),
                                #atom{value = ?VEREMOTEIO}
                        end;
                    OtherReturn ->
                        #atom{value = OtherReturn}
                end;
            {error, file_not_found} -> #atom{value = ?VENOENT};
            Other ->
                lager:error("fslogic could not get file ~p due to: ~p", [FileName, Other]),
                #atom{value = ?VEREMOTEIO}
        end;
      ?VEPERM -> #atom{value = ?VEPERM};
      _  -> #atom{value = ?VEREMOTEIO}
    end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, changefileperms) ->
    {UserDocStatus, UserDoc} = get_user_doc(),
    {FileNameFindingAns, FileName} = get_full_file_name(Record#changefileperms.file_logic_name, changefileperms, UserDocStatus, UserDoc),
    case FileNameFindingAns of
      ok ->
        case get_file(ProtocolVersion, FileName, FuseID) of
            {ok, #veil_document{record = #file{} = File} = Doc} ->
              {PermsStat, PermsOK} = check_file_perms(FileName, UserDocStatus, UserDoc, Doc),
              case PermsStat of
                ok ->
                  case PermsOK of
                    true ->
                      File1 = fslogic_utils:update_meta_attr(File, ctime, fslogic_utils:time()),
                      NewFile = Doc#veil_document{record = File1#file{perms = Record#changefileperms.perms}},
                      case dao_lib:apply(dao_vfs, save_file, [NewFile], ProtocolVersion) of
                          {ok, _} -> #atom{value = ?VOK};
                          Other1 ->
                              lager:error("fslogic could not save file ~p due to: ~p", [FileName, Other1]),
                              #atom{value = ?VEREMOTEIO}
                      end;
                    false ->
                      lager:warning("Changing file's perms file without permissions: ~p", [FileName]),
                      #atom{value = ?VEPERM}
                  end;
                _ ->
                  lager:warning("Cannot check permissions of file. Reason: ~p:~p", [PermsStat, PermsOK]),
                  #atom{value = ?VEREMOTEIO}
              end;
            {error, file_not_found} -> #atom{value = ?VENOENT};
            Other ->
                lager:error("fslogic could not get file ~p due to: ~p", [FileName, Other]),
                #atom{value = ?VEREMOTEIO}
        end;
      ?VEPERM -> #atom{value = ?VEPERM};
      _  -> #atom{value = ?VEREMOTEIO}
    end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getfileattr) ->
    {FileNameFindingAns, FileName} = get_full_file_name(Record#getfileattr.file_logic_name, getfileattr),
    case FileNameFindingAns of
      ok ->
        lager:debug("FileAttr for ~p", [FileName]),
        {DocFindStatus, FileDoc} = get_file(ProtocolVersion, FileName, FuseID),
        getfileattr(ProtocolVersion, DocFindStatus, FileDoc);
      ?VEPERM -> #fileattr{answer = ?VEPERM, mode = 0, uid = -1, gid = -1, atime = 0, ctime = 0, mtime = 0, type = ""};
      _  -> #fileattr{answer = ?VEREMOTEIO, mode = 0, uid = -1, gid = -1, atime = 0, ctime = 0, mtime = 0, type = ""}
    end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getfilelocation) ->
    {FileNameFindingAns, File} = get_full_file_name(Record#getfilelocation.file_logic_name, getfilelocation),
    case FileNameFindingAns of
      ok ->
        {DocFindStatus, FileDoc} = get_file(ProtocolVersion, File, FuseID),
        getfilelocation(ProtocolVersion, DocFindStatus, FileDoc, FuseID);
      ?VEPERM -> #filelocation{answer = ?VEPERM, storage_id = -1, file_id = "", validity = 0};
      _  -> #filelocation{answer = ?VEREMOTEIO, storage_id = -1, file_id = "", validity = 0}
    end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getnewfilelocation) ->
    {UserDocStatus, UserDoc} = get_user_doc(),
    FileBaseName = Record#getnewfilelocation.file_logic_name,
    {FileNameFindingAns, File} = get_full_file_name(FileBaseName, getnewfilelocation, UserDocStatus, UserDoc),
    case FileNameFindingAns of
      ok ->
        {ParentFound, ParentInfo} = fslogic_utils:get_parent_and_name_from_path(File, ProtocolVersion),
        case ParentFound of
          ok ->
            {FileName, Parent} = ParentInfo,
            {PermsStat, PermsOK} = check_file_perms(FileBaseName, UserDocStatus, UserDoc, Parent, write),
            case PermsStat of
              ok ->
                case PermsOK of
                  true ->
                    {ok, StorageList} = dao_lib:apply(dao_vfs, list_storage, [], ProtocolVersion),
                    case fslogic_storage:select_storage(FuseID, StorageList) of
                      #veil_document{uuid = UUID, record = #storage_info{} = Storage} ->
                        SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
                        File_id = get_new_file_id(FileBaseName, UserDoc, SHI, ProtocolVersion),
                        FileLocation = #file_location{storage_id = UUID, file_id = File_id},

                        {UserIdStatus, UserId} = case {UserDocStatus, UserDoc} of
                                                   {ok, _} -> {ok, UserDoc#veil_document.uuid};
                                                   {error, get_user_id_error} -> {ok, ?CLUSTER_USER_ID};
                                                   _ -> {UserDocStatus, UserDoc}
                                                 end,

                        case UserIdStatus of
                          ok ->
                            CTime = fslogic_utils:time(),

                            Groups = get_group_owner(FileBaseName), %% Get owner group name based on file access path

                            FileRecordInit = #file{type = ?REG_TYPE, name = FileName, uid = UserId, gids = Groups, parent = Parent#veil_document.uuid, perms = Record#getnewfilelocation.mode, location = FileLocation, created = false},
                            %% Async *times update
                            FileRecord = fslogic_utils:update_meta_attr(FileRecordInit, times, {CTime, CTime, CTime}),

                            Status = dao_lib:apply(dao_vfs, save_new_file, [File, FileRecord], ProtocolVersion),
                            Validity = ?LOCATION_VALIDITY,
                            case Status of
                              {ok, {waiting_file, ExistingWFile}} ->
                                ExistingWFileUUID = ExistingWFile#veil_document.uuid,
                                update_parent_ctime(Record#getnewfilelocation.file_logic_name, CTime),
                                {ExistingWFileStatus2, _} = save_file_descriptor(ProtocolVersion, ExistingWFileUUID, FuseID, Validity),
                                case ExistingWFileStatus2 of
                                  ok ->
                                    ExistingWFileRecord = ExistingWFile#veil_document.record,
                                    ExistingWFileLocation= ExistingWFileRecord#file.location,

                                    case dao_lib:apply(dao_vfs, get_storage, [{uuid, ExistingWFileLocation#file_location.storage_id}], ProtocolVersion) of
                                      {ok, #veil_document{record = ExistingWFileStorage}} ->
                                        {SH, File_id2} = get_sh_and_id(FuseID, ExistingWFileStorage, ExistingWFileLocation#file_location.file_id),
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
                                update_parent_ctime(Record#getnewfilelocation.file_logic_name, CTime),
                                {Status2, _TmpAns2} = save_file_descriptor(ProtocolVersion, FileUUID, FuseID, Validity),
                                case Status2 of
                                  ok ->
                                    {SH, File_id2} = get_sh_and_id(FuseID, Storage, File_id),
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
                            lager:error([{mod, ?MODULE}], "Error: user doc error for file ~p, error ~p", [File, {UserDocStatus, UserDoc}]),
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
            lager:error([{mod, ?MODULE}], "Error: can not get parent for file: ~p", [File]),
            #filelocation{answer = ?VEREMOTEIO, storage_id = -1, file_id = "", validity = 0}
        end;
      ?VEPERM -> #filelocation{answer = ?VEPERM, storage_id = -1, file_id = "", validity = 0};
      _  ->
        lager:error([{mod, ?MODULE}], "Error: can not get full file name: ~p, error ~p", [File, {FileNameFindingAns, File}]),
        #filelocation{answer = ?VEREMOTEIO, storage_id = -1, file_id = "", validity = 0}
    end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, createfileack) ->
  {FileNameFindingAns, File} = get_full_file_name(Record#createfileack.file_logic_name, createfileack),
  case FileNameFindingAns of
    ok ->
      case get_waiting_file(ProtocolVersion, File, FuseID) of
        {ok, #veil_document{record = #file{} = OldFile} = OldDoc} ->
          ChangedFile = OldDoc#veil_document{record = OldFile#file{created = true}},
          case dao_lib:apply(dao_vfs, save_file, [ChangedFile], ProtocolVersion) of
            {ok, _} ->
              #atom{value = ?VOK};
            Other ->
              lager:warning("Cannot save file document. Reason: ~p", [Other]),
              #atom{value = ?VEREMOTEIO}
          end;
        {error, file_not_found} ->
          case get_file(ProtocolVersion, File, FuseID) of
            {ok, _} ->
              #atom{value = ?VOK};
            _ ->
              lager:warning("Cannot find waiting file: ~p", [File]),
              #atom{value = ?VENOENT}
          end;
        _ -> #atom{value = ?VEREMOTEIO}
      end;
    ?VEPERM -> #atom{value = ?VEPERM};
    _  -> #atom{value = ?VEREMOTEIO}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, filenotused) ->
  {FileNameFindingAns, File} = get_full_file_name(Record#filenotused.file_logic_name, filenotused),
  case FileNameFindingAns of
    ok ->
      Status = dao_lib:apply(dao_vfs, remove_descriptor, [{by_file_n_owner, {File, FuseID}}], ProtocolVersion),
      case Status of
        ok -> #atom{value = ?VOK};
        _Other ->
          lager:error([{mod, ?MODULE}], "Error: for file not used message, file: ~s", [File, _Other]),
          #atom{value = ?VEREMOTEIO}
      end;
    ?VEPERM -> #atom{value = ?VEPERM};
    _  -> #atom{value = ?VEREMOTEIO}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, renewfilelocation) ->
  {FileNameFindingAns, File} = get_full_file_name(Record#renewfilelocation.file_logic_name, renewfilelocation),
  case FileNameFindingAns of
    ok ->
      {Status, TmpAns} = dao_lib:apply(dao_vfs, list_descriptors, [{by_file_n_owner, {File, FuseID}}, 10, 0], ProtocolVersion),
      case Status of
        ok ->
          case length(TmpAns) of
            0 ->
              lager:error([{mod, ?MODULE}], "Error: can not renew file location for file: ~s, descriptor not found", [File]),
              #filelocationvalidity{answer = ?VENOENT, validity = 0};
            1 ->
              [VeilDoc | _] = TmpAns,
              Validity = ?LOCATION_VALIDITY,

              {Status2, _TmpAns2} = save_file_descriptor(ProtocolVersion, VeilDoc, Validity),
              case Status2 of
                ok ->
                  #filelocationvalidity{answer = ?VOK, validity = Validity};
                _BadStatus2 ->
                  lager:error([{mod, ?MODULE}], "Error: can not renew file location for file: ~s", [File]),
                  #filelocationvalidity{answer = ?VEREMOTEIO, validity = 0}
              end;
            _Many ->
              lager:error([{mod, ?MODULE}], "Error: can not renew file location for file: ~s, too many file descriptors", [File]),
              #filelocationvalidity{answer = ?VEREMOTEIO, validity = 0}
          end;
        _Other ->
          lager:error([{mod, ?MODULE}], "Error: can not renew file location for file: ~s", [File]),
          #filelocationvalidity{answer = ?VEREMOTEIO, validity = 0}
      end;
    ?VEPERM -> #filelocationvalidity{answer = ?VEPERM, validity = 0};
    _  -> #filelocationvalidity{answer = ?VEREMOTEIO, validity = 0}
  end;

handle_fuse_message(ProtocolVersion, Record, _FuseID) when is_record(Record, createdir) ->
  {UserDocStatus, UserDoc} = get_user_doc(),
  {FileNameFindingAns, Dir} = get_full_file_name(Record#createdir.dir_logic_name, createdir, UserDocStatus, UserDoc),
  case FileNameFindingAns of
    ok ->
      {ParentFound, ParentInfo} = fslogic_utils:get_parent_and_name_from_path(Dir, ProtocolVersion),

      case ParentFound of
        ok ->
          {FileName, Parent} = ParentInfo,
          {PermsStat, PermsOK} = check_file_perms(Dir, UserDocStatus, UserDoc, Parent, write),
          case PermsStat of
            ok ->
              case PermsOK of
                true ->
                  {UserIdStatus, UserId} = get_user_id(),
                  case UserIdStatus of
                    ok ->
                      Groups = get_group_owner(Record#createdir.dir_logic_name), %% Get owner group name based on file access path

                      FileInit = #file{type = ?DIR_TYPE, name = FileName, uid = UserId, gids = Groups, parent = Parent#veil_document.uuid, perms = Record#createdir.mode},
                      %% Async *times update
                      CTime = fslogic_utils:time(),
                      File = fslogic_utils:update_meta_attr(FileInit, times, {CTime, CTime, CTime}),

                      {Status, TmpAns} = dao_lib:apply(dao_vfs, save_new_file, [Dir, File], ProtocolVersion),
                      case {Status, TmpAns} of
                        {ok, _} ->
                          update_parent_ctime(Record#createdir.dir_logic_name, CTime),
                          #atom{value = ?VOK};
                        {error, file_exists} ->
                          #atom{value = ?VEEXIST};
                        _BadStatus ->
                          lager:error([{mod, ?MODULE}], "Error: can not create dir: ~s, error: ~p", [Dir, _BadStatus]),
                          #atom{value = ?VEREMOTEIO}
                      end;
                    _ -> #atom{value = ?VEREMOTEIO}
                  end;
                false ->
                  lager:warning("Creating directory without permissions: ~p", [FileName]),
                  #atom{value = ?VEPERM}
              end;
            _ ->
              lager:warning("Cannot create directory. Reason: ~p:~p", [PermsStat, PermsOK]),
              #atom{value = ?VEREMOTEIO}
          end;
              _ParentError ->
                lager:error([{mod, ?MODULE}], "Error: can not create dir: ~s, parentInfo: ~p", [Dir, _ParentError]),
                #atom{value = ?VEREMOTEIO}
            end;
    ?VEPERM -> #atom{value = ?VEPERM};
    _  -> #atom{value = ?VEREMOTEIO}
  end;

handle_fuse_message(ProtocolVersion, Record, _FuseID) when is_record(Record, getfilechildren) ->
  {FileNameFindingAns, File} = get_full_file_name(Record#getfilechildren.dir_logic_name, getfilechildren),
  TokenizedPath = string:tokens(Record#getfilechildren.dir_logic_name, "/"),
  case FileNameFindingAns of
    ok ->
    {Num, Offset} =
        case {Record#getfilechildren.offset, TokenizedPath} of
            {0 = Off0, []} -> %% First iteration over "/" dir has to contain "groups" folder, so fetch `num - 1` files instead `num`
                {Record#getfilechildren.children_num - 1, Off0};
            {Off1, []} -> %% Next iteration over "/" dir has start one entry earlier, so fetch `num` files starting on `offset - 1`
                {Record#getfilechildren.children_num, Off1 - 1};
            {Off2, _} -> %% Non-root dir -> proceed normally
                {Record#getfilechildren.children_num, Off2}
        end,

      {Status, TmpAns} = dao_lib:apply(dao_vfs, list_dir, [File, Num, Offset], ProtocolVersion),
      case Status of
        ok ->
            Children = fslogic_utils:create_children_list(TmpAns),
            case {Record#getfilechildren.offset, TokenizedPath} of
                {0, []}    -> %% When asking about root, add virtual ?GROUPS_BASE_DIR_NAME entry
                    #filechildren{child_logic_name = Children ++ [?GROUPS_BASE_DIR_NAME]}; %% Only for offset = 0
                {_, [?GROUPS_BASE_DIR_NAME]} -> %% For group list query ignore DB result and generate list based on user's teams
                    Teams = user_logic:get_team_names({dn, get(user_id)}),
                    {_Head, Tail} = lists:split(min(Offset, length(Teams)), Teams),
                    {Ret, _} = lists:split(min(Num, length(Tail)), Tail),
                    #filechildren{child_logic_name = Ret};
                _ ->
                    #filechildren{child_logic_name = Children}
            end;
        _BadStatus ->
          lager:error([{mod, ?MODULE}], "Error: can not list files in dir: ~s", [File]),
          #filechildren{answer = ?VEREMOTEIO, child_logic_name = []}
      end;
    ?VEPERM -> #filechildren{answer = ?VEPERM, child_logic_name = []};
    _  -> #filechildren{answer = ?VEREMOTEIO, child_logic_name = []}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, deletefile) ->
  {UserDocStatus, UserDoc} = get_user_doc(),
  {FileNameFindingAns, File} = get_full_file_name(Record#deletefile.file_logic_name, deletefile, UserDocStatus, UserDoc),
  case FileNameFindingAns of
    ok ->
      {FindStatus, FindTmpAns} = get_file(ProtocolVersion, File, FuseID),

      case FindStatus of
        ok ->
          {PermsStat, PermsOK} = check_file_perms(File, UserDocStatus, UserDoc, FindTmpAns),
          case PermsStat of
            ok ->
              case PermsOK of
                true ->
                  FileDesc = FindTmpAns#veil_document.record,
                  {ChildrenStatus, ChildrenTmpAns} = case FileDesc#file.type of
                    ?DIR_TYPE ->
                      dao_lib:apply(dao_vfs, list_dir, [File, 10, 0], ProtocolVersion);
                    _OtherType -> {ok, []}
                  end,

                  case ChildrenStatus of
                    ok ->
                      case length(ChildrenTmpAns) of
                        0 ->
                          Status = dao_lib:apply(dao_vfs, remove_file, [File], ProtocolVersion),
                          case Status of
                            ok ->
                              update_parent_ctime(Record#deletefile.file_logic_name, fslogic_utils:time()),
                              #atom{value = ?VOK};
                            _BadStatus ->
                              lager:error([{mod, ?MODULE}], "Error: can not remove file: ~s", [File]),
                              #atom{value = ?VEREMOTEIO}
                          end;
                        _Other ->
                          lager:error([{mod, ?MODULE}], "Error: can not remove file (it has children): ~s", [File]),
                          #atom{value = ?VENOTEMPTY}
                    end;
                    _Other2 ->
                      lager:error([{mod, ?MODULE}], "Error: can not remove file (can not check children): ~s", [File]),
                      #atom{value = ?VEREMOTEIO}
                  end;
                false ->
                  lager:warning("Deleting file without permissions: ~p", [File]),
                  #atom{value = ?VEPERM}
              end;
            _ ->
              lager:warning("Cannot check permissions of file. Reason: ~p:~p", [PermsStat, PermsOK]),
              #atom{value = ?VEREMOTEIO}
          end;
        _FindError ->
          lager:error([{mod, ?MODULE}], "Error: can not remove file (can not check file type): ~s", [File]),
          #atom{value = ?VEREMOTEIO}
      end;
    ?VEPERM -> #atom{value = ?VEPERM};
    _  -> #atom{value = ?VEREMOTEIO}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, renamefile) ->
  {UserDocStatus, UserDoc} = get_user_doc(),
  {FileNameFindingAns, File} = get_full_file_name(Record#renamefile.from_file_logic_name, renamefile, UserDocStatus, UserDoc),
  {FileNameFindingAns2, NewFileName} = get_full_file_name(Record#renamefile.to_file_logic_name, renamefile, UserDocStatus, UserDoc),
  case {FileNameFindingAns, FileNameFindingAns2} of
    {ok, ok} ->
      case get_file(ProtocolVersion, File, FuseID) of
        {ok, #veil_document{record = #file{} = OldFile} = OldDoc} ->
          {PermsStat, PermsOK} = check_file_perms(File, UserDocStatus, UserDoc, OldDoc),
          case PermsStat of
            ok ->
              case PermsOK of
                true ->
                  case get_file(ProtocolVersion, NewFileName, FuseID) of
                    {error, file_not_found} ->
                      NewDir = fslogic_utils:strip_path_leaf(NewFileName),
                      case (OldFile#file.type =:= ?DIR_TYPE) and (string:str(NewDir, File) == 1) of
                        true ->
                          lager:warning("Moving dir ~p to its child: ~p", [File, NewDir]),
                          #atom{value = ?VEREMOTEIO};
                        false ->
                          case get_file(ProtocolVersion, NewDir, FuseID) of
                            {ok, #veil_document{uuid = NewParent} = NewParentDoc} ->
                              OldDir = fslogic_utils:strip_path_leaf(File),
                              case get_file(ProtocolVersion, OldDir, FuseID) of
                                {ok, OldParentDoc} ->
                                  {PermsStat1, PermsOK1} = check_file_perms(NewDir, UserDocStatus, UserDoc, NewParentDoc, write),
                                  {PermsStat2, PermsOK2} = check_file_perms(OldDir, UserDocStatus, UserDoc, OldParentDoc, write),
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
                                                              ?error("Cannot fetch storage (ID: ~p) information for file ~p. Reason: ~p", [StorageID, File, MReason]),
                                                              throw(gen_error_message(renamefile, ?VEREMOTEIO))
                                                      end,
                                                  SHInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage), %% Storage helper for cluster
                                                  NewFileID = get_new_file_id(NewFileName, UserDoc, SHInfo, ProtocolVersion),

                                                  %% Change group owner if needed
                                                  case get_group_owner(NewFileName) of
                                                      [] -> ok; %% Dont change group owner
                                                      [NewGroup | _] -> %% We are moving file to group folder -> change owner
                                                          case storage_files_manager:chown(SHInfo, FileID, "", NewGroup) of
                                                              ok -> ok;
                                                              MReason1 ->
                                                                  ?error("Cannot change group owner for file (ID: ~p) to ~p due to: ~p.", [FileID, NewGroup, MReason1]),
                                                                  throw(gen_error_message(renamefile, ?VEREMOTEIO))
                                                          end
                                                  end,

                                                  %% Move file to new location on storage
                                                  ActionR = storage_files_manager:mv(SHInfo, FileID, NewFileID),
                                                  _NewFile =
                                                      case ActionR of
                                                          ok -> OldFile#file{location = OldFile#file.location#file_location{file_id = NewFileID}};
                                                          MReason0 ->
                                                              ?error("Cannot move file (from ID ~p, to ID: ~p) on storage due to: ~p", [FileID, NewFileID, MReason0]),
                                                              throw(gen_error_message(renamefile, ?VEREMOTEIO))
                                                      end;
                                              (_) -> ok %% Dont move non-regular files
                                              end, %% end fun()

                                          %% Check if we need to move file on storage and do it when we do need it
                                          NewFile =
                                              case {string:tokens(Record#renamefile.from_file_logic_name, "/"), string:tokens(Record#renamefile.to_file_logic_name, "/")} of
                                                  {_, [?GROUPS_BASE_DIR_NAME, _InvalidTarget]} -> %% Moving into ?GROUPS_BASE_DIR_NAME dir is not allowed
                                                      ?info("Attemt to move file to base group directory. Query: ~p", [Record]),
                                                      throw(gen_error_message(renamefile, ?VEPERM));
                                                  {[?GROUPS_BASE_DIR_NAME, _InvalidSource], _} -> %% Moving from ?GROUPS_BASE_DIR_NAME dir is not allowed
                                                      ?info("Attemt to move base group directory. Query: ~p", [Record]),
                                                      throw(gen_error_message(renamefile, ?VEPERM));

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
                                              case get_group_owner(NewFileName) of %% Do we need to update group owner?
                                                  [] -> NewFile#file{parent = NewParent, name = fslogic_utils:basename(NewFileName)}; %% Dont change group owner
                                                  [_NewGroup | _] = GIDs -> %% We are moving file to group folder -> change owner
                                                      NewFile#file{parent = NewParent, name = fslogic_utils:basename(NewFileName), gids = GIDs}
                                              end,
                                          RenamedFile = fslogic_utils:update_meta_attr(RenamedFileInit, ctime, fslogic_utils:time()),
                                          Renamed = OldDoc#veil_document{record = RenamedFile},
                                          case dao_lib:apply(dao_vfs, save_file, [Renamed], ProtocolVersion) of
                                          {ok, _} ->
                                              CTime = fslogic_utils:time(),
                                              update_parent_ctime(Record#renamefile.from_file_logic_name, CTime),
                                              update_parent_ctime(Record#renamefile.to_file_logic_name, CTime),
                                              #atom{value = ?VOK};
                                          Other ->
                                              lager:warning("Cannot save file document. Reason: ~p", [Other]),
                                              #atom{value = ?VEREMOTEIO}
                                          end;
                                        _ ->
                                          lager:warning("Moving of file without permissions: ~p", [File]),
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
                      lager:warning("Destination file already exists: ~p", [File]),
                      #atom{value = ?VEEXIST};
                    _ -> #atom{value = ?VEREMOTEIO}
                  end;
                false ->
                  lager:warning("Renaming file without permissions: ~p", [File]),
                  #atom{value = ?VEPERM}
              end;
            _ ->
              lager:warning("Cannot check permissions of file. Reason: ~p:~p", [PermsStat, PermsOK]),
              #atom{value = ?VEREMOTEIO}
          end;
        {error, file_not_found} ->
          lager:warning("Cannot find source file: ~p", [File]),
          #atom{value = ?VENOENT};
        _ -> #atom{value = ?VEREMOTEIO}
      end;
    {?VEPERM, _} -> #atom{value = ?VEPERM};
    {_ , ?VEPERM} -> #atom{value = ?VEPERM};
    _  -> #atom{value = ?VEREMOTEIO}
  end;

%% Symbolic link creation. From - link name, To - path pointed by new link
handle_fuse_message(ProtocolVersion, Record, _FuseID) when is_record(Record, createlink) ->
  {UserDocStatus, UserDoc} = get_user_doc(),
  {FileNameFindingAns, From} = get_full_file_name(Record#createlink.from_file_logic_name, createlink, UserDocStatus, UserDoc),
%%   {FileNameFindingAns2, To} = get_full_file_name(Record#createlink.to_file_logic_name, createlink),
  To = Record#createlink.to_file_logic_name,
%%   case {FileNameFindingAns, FileNameFindingAns2} of
%%     {ok, ok} ->
  case FileNameFindingAns of
    ok ->
      {ParentFound, ParentInfo} = fslogic_utils:get_parent_and_name_from_path(From, ProtocolVersion),

      case ParentFound of
        ok ->
          {FileName, Parent} = ParentInfo,
          {PermsStat, PermsOK} = check_file_perms(From, UserDocStatus, UserDoc, Parent, write),
          case PermsStat of
            ok ->
              case PermsOK of
                true ->
                  UserId =
                    case UserDoc of
                      get_user_id_error -> ?CLUSTER_USER_ID;
                      _ -> UserDoc#veil_document.uuid
                    end,

                  Groups = get_group_owner(Record#createlink.from_file_logic_name), %% Get owner group name based on file access path

                  LinkDocInit = #file{type = ?LNK_TYPE, name = FileName, uid = UserId, gids = Groups, ref_file = To, parent = Parent#veil_document.uuid},
                  CTime = fslogic_utils:time(),
                  LinkDoc = fslogic_utils:update_meta_attr(LinkDocInit, times, {CTime, CTime, CTime}),

                  case dao_lib:apply(dao_vfs, save_new_file, [From, LinkDoc], ProtocolVersion) of
                    {ok, _} ->
                      update_parent_ctime(Record#createlink.from_file_logic_name, CTime),
                      #atom{value = ?VOK};
                    {error, file_exists} ->
                      lager:error("Cannot create link - file already exists: ~p", [From]),
                      #atom{value = ?VEEXIST};
                    {error, Reason} ->
                      lager:error("Cannot save link file (from ~p to ~p) due to error: ~p", [From, To, Reason]),
                      #atom{value = ?VEREMOTEIO}
                  end;
                false ->
                  lager:warning("Creation of link without permissions: from ~p to ~p", [From, To]),
                  #atom{value = ?VEPERM}
              end;
            _ ->
              lager:warning("Cannot create link. Reason: ~p:~p", [PermsStat, PermsOK]),
              #atom{value = ?VEREMOTEIO}
          end;
        {error, Reason1} ->
          lager:error("Cannot fetch file information for file ~p due to error: ~p", [From, Reason1]),
          #atom{value = ?VEREMOTEIO}
      end;
    ?VEPERM -> #atom{value = ?VEPERM};
    _  -> #atom{value = ?VEREMOTEIO}
  end;

%% Fetch link data (target path)
handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getlink) ->
  {FileNameFindingAns, File} = get_full_file_name(Record#getlink.file_logic_name, getlink),
  case FileNameFindingAns of
    ok ->
      case get_file(ProtocolVersion, File, FuseID) of
          {ok, #veil_document{record = #file{ref_file = Target}}} ->
%%               #linkinfo{file_logic_name = get_file_name_from_full_file_name(Target)};
              #linkinfo{file_logic_name = Target};
          {error, file_not_found} ->
              lager:error("Link ~p does not exist.", [File]),
              #linkinfo{answer = ?VENOENT, file_logic_name = ""};
          {error, Reason} ->
              lager:error("Cannot read link ~p due to error: ~p", [File, Reason]),
              #linkinfo{answer = ?VEREMOTEIO, file_logic_name = ""}
      end;
    ?VEPERM -> #linkinfo{answer = ?VEPERM, file_logic_name = ""};
    _  -> #linkinfo{answer = ?VEREMOTEIO, file_logic_name = ""}
  end;

%% Test message
handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, testchannel) ->
  Interval = Record#testchannel.answer_delay_in_ms,
  timer:apply_after(Interval, gen_server, cast, [?MODULE, {asynch, ProtocolVersion, {answer_test_message, FuseID, Record#testchannel.answer_message}}]),
  #atom{value = "ok"};

handle_fuse_message(ProtocolVersion, Record, _FuseID) when is_record(Record, getstatfs) ->
  case get_user_doc() of
    {ok, UserDoc} ->
      case user_logic:get_quota(UserDoc) of
        {ok, Quota} ->
          case user_logic:get_files_size(UserDoc#veil_document.uuid, ProtocolVersion) of
            {ok, Size} -> #statfsinfo{answer = ?VOK, quota_size = Quota#quota.size, files_size = Size};
            _ -> #statfsinfo{answer = ?VEREMOTEIO, quota_size = -1, files_size = -1}
          end;
        _ -> #statfsinfo{answer = ?VEREMOTEIO, quota_size = -1, files_size = -1}
      end;
    _ -> #statfsinfo{answer = ?VEREMOTEIO, quota_size = -1, files_size = -1}
  end.

%% save_file_descriptor/3
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE.
%% @end
-spec save_file_descriptor(ProtocolVersion :: term(), File :: record(), Validity :: integer()) -> Result when
  Result :: term().
%% ====================================================================

save_file_descriptor(ProtocolVersion, File, Validity) ->
  Descriptor = update_file_descriptor(File#veil_document.record, Validity),
  dao_lib:apply(dao_vfs, save_descriptor, [File#veil_document{record = Descriptor}], ProtocolVersion).


%% save_file_descriptor/4
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE.
%% @end
-spec save_file_descriptor(ProtocolVersion :: term(), Uuid::uuid(), FuseID :: string(), Validity :: integer()) -> Result when
  Result :: term().
%% ====================================================================

save_file_descriptor(ProtocolVersion, Uuid, FuseID, Validity) ->
  case FuseID of
    ?CLUSTER_FUSE_ID -> {ok, ok};
    _ ->
      Status = dao_lib:apply(dao_vfs, list_descriptors, [{by_uuid_n_owner, {Uuid, FuseID}}, 10, 0], ProtocolVersion),
      case Status of
        {ok, TmpAns} ->
          case length(TmpAns) of
            0 ->
              save_new_file_descriptor(ProtocolVersion, Uuid, FuseID, Validity);
            1 ->
              [VeilDoc | _] = TmpAns,
              save_file_descriptor(ProtocolVersion, VeilDoc, Validity);
            _Many ->
              lager:error([{mod, ?MODULE}], "Error: to many file descriptors for file uuid: ~p", [Uuid]),
              {error, "Error: too many file descriptors"}
          end;
        _Other -> _Other
      end
  end.

%% save_new_file_descriptor/4
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE.
%% @end
-spec save_new_file_descriptor(ProtocolVersion :: term(), Uuid::uuid(), FuseID :: string(), Validity :: integer()) -> Result when
  Result :: term().
%% ====================================================================

save_new_file_descriptor(ProtocolVersion, Uuid, FuseID, Validity) ->
  Descriptor = update_file_descriptor(#file_descriptor{file = Uuid, fuse_id = FuseID}, Validity),
  dao_lib:apply(dao_vfs, save_descriptor, [Descriptor], ProtocolVersion).

%% update_file_descriptor/2
%% ====================================================================
%% @doc Updates descriptor (record, not in DB)
%% @end
-spec update_file_descriptor(Descriptor :: record(),  Validity :: integer()) -> Result when
  Result :: record().
%% ====================================================================

update_file_descriptor(Descriptor, Validity) ->
  {Megaseconds,Seconds, _Microseconds} = os:timestamp(),
  Time = 1000000*Megaseconds + Seconds,
  Descriptor#file_descriptor{create_time = Time, validity_time = Validity}.


%% get_file/3
%% ====================================================================
%% @doc Gets file info from DB
%% @end
-spec get_file(ProtocolVersion :: term(), File :: string(), FuseID :: string()) -> Result when
  Result :: term().
%% ====================================================================
get_file(ProtocolVersion, File, FuseID) ->
  get_file_helper(ProtocolVersion, File, FuseID, get_file).

%% get_waiting_file/3
%% ====================================================================
%% @doc Gets file info about file that waits to be created at storage from DB
%% @end
-spec get_waiting_file(ProtocolVersion :: term(), File :: string(), FuseID :: string()) -> Result when
  Result :: term().
%% ====================================================================
get_waiting_file(ProtocolVersion, File, FuseID) ->
  get_file_helper(ProtocolVersion, File, FuseID, get_waiting_file).

%% get_file_helper/4
%% ====================================================================
%% @doc Gets file info from DB
%% @end
-spec get_file_helper(ProtocolVersion :: term(), File :: string(), FuseID :: string(), Fun :: atom()) -> Result when
  Result :: term().
%% ====================================================================
get_file_helper(ProtocolVersion, File, FuseID, Fun) ->
    lager:debug("get_file(File: ~p, FuseID: ~p)", [File, FuseID]),
    case string:tokens(File, "/") of
        [?GROUPS_BASE_DIR_NAME, GroupName | _] -> %% Check if group that user is tring to access is avaliable to him
            case get(user_id) of %% Internal call, allow all group access
                undefined   -> dao_lib:apply(dao_vfs, Fun, [File], ProtocolVersion);
                UserDN      -> %% Check if user has access to this group
                    Teams = user_logic:get_team_names({dn, UserDN}),
                    case lists:member(GroupName, Teams) of %% Does the user belong to the group?
                        true  -> dao_lib:apply(dao_vfs, Fun, [File], ProtocolVersion);
                        false -> {error, file_not_found} %% Assume that this file does not exists
                    end
            end;
        _ ->
            dao_lib:apply(dao_vfs, Fun, [File], ProtocolVersion)
    end.

%% delete_old_descriptors/3
%% ====================================================================
%% @doc Deletes old descriptors (older than Time)
%% @end
-spec delete_old_descriptors(ProtocolVersion :: term(), Time :: integer()) -> Result when
  Result :: term().
%% ====================================================================
delete_old_descriptors(ProtocolVersion, Time) ->
  Status = dao_lib:apply(dao_vfs, remove_descriptor, [{by_expired_before, Time}], ProtocolVersion),
  case Status of
    ok ->
      lager:info([{mod, ?MODULE}], "Old descriptors cleared"),
      ok;
    Other ->
      lager:error([{mod, ?MODULE}], "Error during clearing old descriptors: ~p", [Other]),
      Other
  end.

%% update_user_files_size_view
%% ====================================================================
%% @doc Updates user files size view in db
%% @end
-spec update_user_files_size_view(ProtocolVersion :: term()) -> term().
%% ====================================================================
update_user_files_size_view(ProtocolVersion) ->
  case dao_lib:apply(dao_users, update_files_size, [], ProtocolVersion) of
    ok ->
      lager:info([{mod, ?MODULE}], "User files size view updated"),
      ok;
    Other ->
      lager:error([{mod, ?MODULE}], "Error during updating user files size view: ~p", [Other]),
      Other
  end.

%% get_user_root/1
%% ====================================================================
%% @doc Gets user's root directory.
%% @end
-spec get_user_root(UserDoc :: term()) -> Result when
  Result :: {ok, RootDir} | {error, ErrorDesc},
  RootDir :: string(),
  ErrorDesc :: atom.
%% ====================================================================

get_user_root(UserDoc) ->
  UserRecord = UserDoc#veil_document.record,
  "/" ++ UserRecord#user.login.

%% get_user_doc/0
%% ====================================================================
%% @doc Gets user's doc from db.
%% @end
-spec get_user_doc() -> Result when
  Result :: {ok, UserDoc} | {error, ErrorDesc},
  UserDoc :: term(),
  ErrorDesc :: atom.
%% ====================================================================

get_user_doc() ->
  UserId = get(user_id),
  case UserId of
    undefined -> {error, get_user_id_error};
    DN ->
      user_logic:get_user({dn, DN})
  end.

%% get_user_root/2
%% ====================================================================
%% @doc Gets user's root directory.
%% @end
-spec get_user_root(UserDocStatus :: atom(), UserDoc :: term()) -> Result when
  Result :: {ok, RootDir} | {error, ErrorDesc},
  RootDir :: string(),
  ErrorDesc :: atom.
%% ====================================================================

get_user_root(UserDocStatus, UserDoc) ->
  case UserDocStatus of
    ok ->
      {ok, get_user_root(UserDoc)};
    _ ->
      case UserDoc of
        get_user_id_error -> {error, get_user_id_error};
        _ -> {error, get_user_error}
      end
  end.

%% get_user_root/0
%% ====================================================================
%% @doc Gets user's root directory.
%% @end
-spec get_user_root() -> Result when
  Result :: {ok, RootDir} | {error, ErrorDesc},
  RootDir :: string(),
  ErrorDesc :: atom.
%% ====================================================================

get_user_root() ->
  {UserDocStatus, UserDoc} = get_user_doc(),
  get_user_root(UserDocStatus, UserDoc).

%% get_user_groups/0
%% ====================================================================
%% @doc Gets user's group
%% @end
-spec get_user_groups(UserDocStatus :: atom(), UserDoc :: term()) -> Result when
  Result :: {ok, Groups} | {error, ErrorDesc},
  Groups :: list(),
  ErrorDesc :: atom.
%% ====================================================================

get_user_groups(UserDocStatus, UserDoc) ->
  case UserDocStatus of
    ok ->
      {ok, user_logic:get_team_names(UserDoc)};
    _ ->
      case UserDoc of
        get_user_id_error -> {error, get_user_groups_error};
        _ -> {error, get_user_error}
      end
  end.

%% get_user_id/0
%% ====================================================================
%% @doc Gets user's id.
%% @end
-spec get_user_id() -> Result when
  Result :: {ok, UserID} | {error, ErrorDesc},
  UserID :: term(),
  ErrorDesc :: atom.
%% ====================================================================
get_user_id() ->
  UserId = get(user_id),
  case UserId of
    undefined -> {ok, ?CLUSTER_USER_ID};
    DN ->
              {GetUserAns, User} = user_logic:get_user({dn, DN}),
              case GetUserAns of
                ok ->
                  {ok, User#veil_document.uuid};
                _ -> {error, get_user_error}
              end
  end.

%% get_full_file_name/1
%% ====================================================================
%% @doc Gets file's full name (user's root is added to name, but only when asking about non-group dir).
%% @end
-spec get_full_file_name(FileName :: string()) -> Result when
  Result :: {ok, FullFileName} | {error, ErrorDesc},
  FullFileName :: string(),
  ErrorDesc :: atom.
%% ====================================================================

get_full_file_name(FileName) ->
  get_full_file_name(FileName, cluster_request).

%% get_full_file_name/2
%% ====================================================================
%% @doc Gets file's full name (user's root is added to name, but only when asking about non-group dir).
%% @end
-spec get_full_file_name(FileName :: string(), Request :: atom()) -> Result when
  Result :: {ok, FullFileName} | {error, ErrorDesc},
  FullFileName :: string(),
  ErrorDesc :: atom.
%% ====================================================================

get_full_file_name(FileName, Request) ->
  {UserDocStatus, UserDoc} = get_user_doc(),
  get_full_file_name(FileName, Request, UserDocStatus, UserDoc).

%% get_full_file_name/4
%% ====================================================================
%% @doc Gets file's full name (user's root is added to name, but only when asking about non-group dir).
%% @end
-spec get_full_file_name(FileName :: string(), Request :: atom(), UserDocStatus :: atom(), UserDoc :: tuple()) -> Result when
  Result :: {ok, FullFileName} | {error, ErrorDesc},
  FullFileName :: string(),
  ErrorDesc :: atom.
%% ====================================================================

get_full_file_name(FileName, Request, UserDocStatus, UserDoc) ->
  case verify_file_name(FileName) of
    {error, Error} -> {Error, FileName};
    {ok, Tokens} ->
      VerifiedFileName = string:join(Tokens, "/"),
      case UserDocStatus of
        ok -> case assert_group_access(UserDoc, Request, VerifiedFileName) of
                ok ->
                  case Tokens of %% Map all /groups/* requests to root of the file system (i.e. dont add any prefix)
                    [?GROUPS_BASE_DIR_NAME | _] ->
                      {ok, VerifiedFileName};
                    _ ->
                      Root = get_user_root(UserDoc),
                      {ok, Root ++ "/" ++ VerifiedFileName}
                  end;
                _ -> {?VEPERM, ?VEPERM}
              end;
        _ ->
          case UserDoc of
            get_user_id_error -> {ok, VerifiedFileName};
            _ -> {user_doc_not_found, UserDoc}
          end
      end
  end.

%% Function is not used anymore
%% %% get_file_name_from_full_file_name/1
%% %% ====================================================================
%% %% @doc Gets file's name from full name (user's root is deleted to name).
%% %% @end
%% -spec get_file_name_from_full_file_name(FileName :: string()) -> Result when
%%   Result :: {ok, FileName} | {error, ErrorDesc},
%%   FileName :: string(),
%%   ErrorDesc :: atom.
%% %% ====================================================================
%%
%% get_file_name_from_full_file_name(FileName) ->
%%   case get(user_id) of
%%     undefined ->
%%       FileName;
%%     _ ->
%%       Pos = string:chr(FileName, $/),
%%       case Pos of
%%         1 ->
%%           FileName2 = string:substr(FileName, 2),
%%           Pos2 = string:chr(FileName2, $/),
%%           string:substr(FileName2, Pos2);
%%         _ ->
%%           string:substr(FileName, Pos)
%%       end
%%   end.

%% get_sh_and_id/3
%% ====================================================================
%% @doc Returns storage hel[per info and new file id (it may be changed for Cluster Proxy).
%% @end
-spec get_sh_and_id(FuseID :: string(), Storage :: term(), File_id :: string()) -> Result when
  Result :: {SHI, NewFileId},
  SHI :: term(),
  NewFileId :: string().
%% ====================================================================
get_sh_and_id(FuseID, Storage, File_id) ->
  SHI = fslogic_storage:get_sh_for_fuse(FuseID, Storage),
  #storage_helper_info{name = SHName, init_args = _SHArgs} = SHI,
  case SHName =:= "ClusterProxy" of
    true ->
      {SHI, integer_to_list(Storage#storage_info.id) ++ ?REMOTE_HELPER_SEPARATOR ++ File_id};
    false -> {SHI, File_id}
  end.

%% get_files_number/3
%% ====================================================================
%% @doc Returns number of user's or group's files
%% @end
-spec get_files_number(user | group, UUID :: uuid() | string(), ProtocolVersion :: integer()) -> Result when
  Result :: {ok, Sum} | {error, any()},
  Sum :: integer().
%% ====================================================================
get_files_number(Type, GroupName, ProtocolVersion) ->
    Ans = dao_lib:apply(dao_users, get_files_number, [Type, GroupName], ProtocolVersion),
    case Ans of
        {error, files_number_not_found} -> {ok, 0};
        _ -> Ans
    end.

%% get_new_file_id/5
%% ====================================================================
%% @doc Returns id for a new file
%% @end
-spec get_new_file_id(File :: string(), UserDoc :: term(), SHInfo :: term(), ProtocolVersion :: integer()) -> string().
%% ====================================================================
get_new_file_id(File, UserDoc, SHInfo, ProtocolVersion) ->
  {Root1, {CountStatus, FilesCount}} =
      case {string:tokens(File, "/"), UserDoc} of
          {[?GROUPS_BASE_DIR_NAME, GroupName | _], _} -> %% Group dir context
              {"/" ++ ?GROUPS_BASE_DIR_NAME ++ "/" ++ GroupName, get_files_number(group, GroupName, ProtocolVersion)};
          {_, get_user_id_error} -> {"/", {error, get_user_id_error}}; %% Unknown context
          _ -> {"/users/" ++ get_user_root(UserDoc), get_files_number(user, UserDoc#veil_document.uuid, ProtocolVersion)}
      end,
  File_id_beg = case Root1 of
    get_user_id_error -> "/";
    _ ->
      case CountStatus of
        ok -> create_dirs(FilesCount, ?FILE_COUNTING_BASE, SHInfo, Root1 ++ "/");
        _ -> Root1 ++ "/"
      end
  end,

  File_id_beg ++ dao_helper:gen_uuid() ++ re:replace(File, "/", "___", [global, {return,list}]).

%% create_dirs/4
%% ====================================================================
%% @doc Creates dir at storage for files (if needed). Returns the path
%% that contains created dirs.
%% @end
-spec create_dirs(Count :: integer(), CountingBase :: integer(), SHInfo :: term(), TmpAns :: string()) -> string().
%% ====================================================================
create_dirs(Count, CountingBase, SHInfo, TmpAns) ->
  case Count > CountingBase of
    true ->
      {S1,S2,S3} = now(),
      random:seed(S1,S2,S3),
      DirName = TmpAns ++ integer_to_list(random:uniform(CountingBase)) ++ "/",
      case storage_files_manager:mkdir(SHInfo, DirName) of
        ok -> create_dirs(Count div CountingBase, CountingBase, SHInfo, DirName);
        {error, dir_or_file_exists} -> create_dirs(Count div CountingBase, CountingBase, SHInfo, DirName);
        _ -> create_dirs(Count div CountingBase, CountingBase, SHInfo, TmpAns)
      end;
    false -> TmpAns
  end.

%% assert_group_access/1
%% ====================================================================
%% @doc Checks if operation given as parameter (one of fuse_messages) is allowed to be invoked in groups context.
%% @end
-spec assert_group_access(UserDoc :: tuple(), Request :: atom(), LogicalPath :: string()) -> ok | error.
%% ====================================================================
assert_group_access(_UserDoc, cluster_request, _LogicalPath) ->
  ok;

assert_group_access(UserDoc, Request, LogicalPath) ->
  assert_grp_access(UserDoc, Request, string:tokens(LogicalPath, "/")).

%% assert_grp_access/1
%% ====================================================================
%% @doc Checks if operation given as parameter (one of fuse_messages) is allowed to be invoked in groups context.
%% @end
-spec assert_grp_access(UserDoc :: tuple(), Request :: atom(), Path :: list()) -> ok | error.
%% ====================================================================
assert_grp_access(_UserDoc, Request, [?GROUPS_BASE_DIR_NAME]) ->
    case lists:member(Request, ?GROUPS_BASE_ALLOWED_ACTIONS) of
        false   -> error;
        true    -> ok
    end;
assert_grp_access(UserDoc, Request, [?GROUPS_BASE_DIR_NAME | Tail]) ->
    TailCheck = case Tail of
      [_GroupName] ->
        case lists:member(Request, ?GROUPS_ALLOWED_ACTIONS) of
          false   -> error;
          true    -> ok
        end;
      _ ->
        ok
    end,

    case TailCheck of
      ok ->
        UserTeams = user_logic:get_team_names(UserDoc),
        [GroupName2 | _] = Tail,
        case lists:member(GroupName2, UserTeams) of
          true    -> ok;
          false   -> error
        end;
      _ ->
        TailCheck
    end;
assert_grp_access(_, _, _) ->
    ok.

%% Not needed any more
%% %% extract_logical_path/1
%% %% ====================================================================
%% %% @doc Convinience method that returns logical file path affected by given operation.
%% %%      E.g. for "create file" operation, its parent is the one considered "affected".
%% %% @end
%% -spec extract_logical_path(Record :: tuple()) -> string() | no_return().
%% %% ====================================================================
%% extract_logical_path(#getfileattr{file_logic_name = Path}) ->
%%     Path;
%% extract_logical_path(#getfilelocation{file_logic_name = Path}) ->
%%     Path;
%% extract_logical_path(#deletefile{file_logic_name = Path}) ->
%%     Path;
%% extract_logical_path(#renamefile{from_file_logic_name = Path}) ->
%%     Path;
%% extract_logical_path(#getnewfilelocation{file_logic_name = Path}) ->
%%    fslogic_utils:strip_path_leaf(Path);
%% extract_logical_path(#filenotused{file_logic_name = Path}) ->
%%     Path;
%% extract_logical_path(#renewfilelocation{file_logic_name = Path}) ->
%%     Path;
%% extract_logical_path(#getfilechildren{dir_logic_name = Path}) ->
%%     Path;
%% extract_logical_path(#createdir{dir_logic_name = Path}) ->
%%     fslogic_utils:strip_path_leaf(Path);
%% extract_logical_path(#getlink{file_logic_name = Path}) ->
%%     Path;
%% extract_logical_path(#createlink{from_file_logic_name = Path}) ->
%%     fslogic_utils:strip_path_leaf(Path);
%% extract_logical_path(#changefileowner{file_logic_name = Path}) ->
%%     Path;
%% extract_logical_path(#changefilegroup{file_logic_name = Path}) ->
%%     Path;
%% extract_logical_path(#changefileperms{file_logic_name = Path}) ->
%%     Path;
%% extract_logical_path(#updatetimes{file_logic_name = Path}) ->
%%     Path;
%% extract_logical_path(#testchannel{}) ->
%%     "/";
%% extract_logical_path(Record) ->
%%     ?error("Unsupported record: ~p", [element(1, Record)]),
%%     throw({unsupported_record, element(1, Record)}).


%% gen_error_message/2
%% ====================================================================
%% @doc Convinience method that returns protobuf answer message that is build base on given error code
%%      and type of request.
%% @end
-spec gen_error_message(RecordName :: atom(), VeilError :: string()) -> tuple() | no_return().
%% ====================================================================
gen_error_message(getfileattr, Error) ->
    #fileattr{answer = Error, mode = 0, uid = -1, gid = -1, atime = 0, ctime = 0, mtime = 0, type = ""};
gen_error_message(getfilelocation, Error) ->
    #filelocation{answer = Error, storage_id = -1, file_id = "", validity = 0};
gen_error_message(getnewfilelocation, Error) ->
    #filelocation{answer = Error, storage_id = -1, file_id = "", validity = 0};
gen_error_message(filenotused, Error) ->
    #atom{value = Error};
gen_error_message(renamefile, Error) ->
    #atom{value = Error};
gen_error_message(deletefile, Error) ->
    #atom{value = Error};
gen_error_message(createdir, Error) ->
    #atom{value = Error};
gen_error_message(changefileowner, Error) ->
    #atom{value = Error};
gen_error_message(changefilegroup, Error) ->
    #atom{value = Error};
gen_error_message(changefileperms, Error) ->
    #atom{value = Error};
gen_error_message(updatetimes, Error) ->
    #atom{value = Error};
gen_error_message(createlink, Error) ->
    #atom{value = Error};
gen_error_message(renewfilelocation, Error) ->
    #filelocationvalidity{answer = Error, validity = 0};
gen_error_message(getfilechildren, Error) ->
    #filechildren{answer = Error, child_logic_name = []};
gen_error_message(getlink, Error) ->
    #linkinfo{answer = Error, file_logic_name = ""};
gen_error_message(testchannel, Error) ->
    #atom{value = Error};
gen_error_message(RecordName, _Error) ->
    ?error("Unsupported record: ~p", [RecordName]),
    throw({unsupported_record, RecordName}).


%% get_group_owner/1
%% ====================================================================
%% @doc Convinience method that returns list of group name(s) that are considered as default owner of file
%%      created with given path. E.g. when path like "/groups/gname/file1" is passed, the method will
%%      return ["gname"].
%% @end
-spec get_group_owner(FileBasePath :: string()) -> [string()].
%% ====================================================================
get_group_owner(FileBasePath) ->
    case string:tokens(FileBasePath, "/") of
        [?GROUPS_BASE_DIR_NAME, GroupName | _] -> [GroupName];
        _ -> []
    end.

%% getfilelocation/4
%% ====================================================================
%% @doc Returns information about storage helper used to
%% access file and file id at storage.
%% @end
-spec getfilelocation(ProtocolVersion :: term(), DocFindStatus :: atom(), FileDoc :: term(), FuseID :: string()) -> Result when
  Result :: {ok, FileLocation} | {ErrorGeneral, ErrorDetail},
  FileLocation :: term(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
getfilelocation(ProtocolVersion, DocFindStatus, FileDoc, FuseID) ->
  Validity = ?LOCATION_VALIDITY,
  case {DocFindStatus, FileDoc} of
    {ok, _} ->
      case FileDoc#veil_document.record#file.type of
        ?REG_TYPE ->
          {Status2, TmpAns2} = save_file_descriptor(ProtocolVersion, FileDoc#veil_document.uuid, FuseID, Validity),
          case Status2 of
            ok ->
              FileDesc = FileDoc#veil_document.record,
              FileLoc = FileDesc#file.location,
              case dao_lib:apply(dao_vfs, get_storage, [{uuid, FileLoc#file_location.storage_id}], ProtocolVersion) of
                {ok, #veil_document{record = Storage}} ->
                  {SH, File_id} = get_sh_and_id(FuseID, Storage, FileLoc#file_location.file_id),
                  #filelocation{storage_id = Storage#storage_info.id, file_id = File_id, validity = Validity,
                  storage_helper_name = SH#storage_helper_info.name, storage_helper_args = SH#storage_helper_info.init_args};
                Other ->
                  lager:error("Cannot fetch storage: ~p for file: ~p, reason: ~p", [FileLoc#file_location.storage_id, FileDoc#veil_document.uuid, Other]),
                  #filelocation{answer = ?VEREMOTEIO, storage_id = -1, file_id = "", validity = 0}
              end;
            _BadStatus2 ->
              lager:warning("Unknown fslogic error during descriptor saving: ~p", [TmpAns2]),
              #filelocation{answer = ?VEREMOTEIO, storage_id = -1, file_id = "", validity = 0}
          end;
        _ -> #filelocation{answer = ?VENOTSUP, storage_id = -1, file_id = "", validity = 0}
      end;
    {error, file_not_found} -> #filelocation{answer = ?VENOENT, storage_id = -1, file_id = "", validity = 0};
    _BadStatus ->
      lager:warning("Unknown fslogic error during doc finding: ~p", [_BadStatus]),
      #filelocation{answer = ?VEREMOTEIO, storage_id = -1, file_id = "", validity = 0}
  end.

%% getfileattr/3
%% ====================================================================
%% @doc Returns file's attributes.
%% @end
-spec getfileattr(ProtocolVersion :: term(), DocFindStatus :: atom(), FileDoc :: term()) -> Result when
  Result :: {ok, FileAttrs} | {ErrorGeneral, ErrorDetail},
  FileAttrs :: term(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
getfileattr(ProtocolVersion, DocFindStatus, FileDoc) ->
  case {DocFindStatus, FileDoc} of
    {ok, #veil_document{record = #file{} = File, uuid = FileUUID}} ->
      {Type, Size} =
        case File#file.type of
          ?DIR_TYPE -> {"DIR", 0};
          ?REG_TYPE ->
            FileLoc = File#file.location,
            S =
              case dao_lib:apply(dao_vfs, get_storage, [{uuid, FileLoc#file_location.storage_id}], ProtocolVersion) of
                {ok, #veil_document{record = Storage}} ->
                  {SH, File_id} = get_sh_and_id(?CLUSTER_FUSE_ID, Storage, FileLoc#file_location.file_id),
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
        case dao_lib:apply(dao_users, get_user, [{uuid, File#file.uid}], ProtocolVersion) of
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
                  "DIR" -> case dao_lib:apply(dao_vfs, count_subdirs, [{uuid, FileUUID}], ProtocolVersion) of
                               {ok, Sum} -> Sum + 2;
                               _Other ->
                                   lager:error([{mod, ?MODULE}], "Error: can not get number of links for file: ~s", [File]),
                                   -1
                           end;
                  "REG" -> 1;
                  _ -> -1
              end,

      #fileattr{answer = ?VOK, mode = File#file.perms, atime = ATime, ctime = CTime, mtime = MTime, type = Type, size = Size, uname = UName, gname = GName, uid = UID, gid = UID, links = Links};
    {error, file_not_found} ->
      lager:debug("FileAttr: ENOENT"),
      #fileattr{answer = ?VENOENT, mode = 0, uid = -1, gid = -1, atime = 0, ctime = 0, mtime = 0, type = "", links = -1};
    _ ->
      #fileattr{answer = ?VEREMOTEIO, mode = 0, uid = -1, gid = -1, atime = 0, ctime = 0, mtime = 0, type = "", links = -1}
  end.

%% check_file_perms/4
%% ====================================================================
%% @doc Checks if the user has permission to modify file (e,g. change owner).
%% @end
-spec check_file_perms(FileName :: string(), UserDocStatus :: atom(), UserDoc :: term(), FileDoc :: term()) -> Result when
  Result :: {ok, boolean()} | {error, ErrorDetail},
  ErrorDetail :: term().
%% ====================================================================
check_file_perms(FileName, UserDocStatus, UserDoc, FileDoc) ->
  check_file_perms(FileName, UserDocStatus, UserDoc, FileDoc, perms).

%% check_file_perms/5
%% ====================================================================
%% @doc Checks if the user has permission to modify file (e,g. change owner).
%% @end
-spec check_file_perms(FileName :: string(), UserDocStatus :: atom(), UserDoc :: term(), FileDoc :: term(), CheckType :: atom()) -> Result when
  Result :: {ok, boolean()} | {error, ErrorDetail},
  ErrorDetail :: term().
%% ====================================================================
check_file_perms(FileName, UserDocStatus, UserDoc, FileDoc, CheckType) ->
  case CheckType of
    root ->
      case {UserDocStatus, UserDoc} of
        {ok, _} ->
          {ok, false};
        {error, get_user_id_error} -> {ok, true};
        _ -> {UserDocStatus, UserDoc}
      end;
    _ ->
      case string:tokens(FileName, "/") of
        [?GROUPS_BASE_DIR_NAME | _] ->
          FileRecord = FileDoc#veil_document.record,
          CheckOwn = case CheckType of
                       perms -> true;
                       _ -> %write
                         case FileRecord#file.perms band ?WR_GRP_PERM of
                           0 -> true;
                           _ -> false
                         end
                     end,

          case CheckOwn of
            true ->
              case {UserDocStatus, UserDoc} of
                {ok, _} ->
                  UserUuid = UserDoc#veil_document.uuid,
                  {ok, FileRecord#file.uid =:= UserUuid};
                {error, get_user_id_error} -> {ok, true};
                _ -> {UserDocStatus, UserDoc}
              end;
            false ->
              {ok, true}
          end;
        _ ->
          {ok, true}
      end
  end.

%% Updates modification time for parent of Dir
update_parent_ctime(Dir, CTime) ->
    case fslogic_utils:strip_path_leaf(Dir) of
        [?PATH_SEPARATOR] -> ok;
        ParentPath -> gen_server:call(?Dispatcher_Name, {fslogic, 1, #veil_request{subject = get(user_id), request = {internal_call, #updatetimes{file_logic_name = ParentPath, mtime = CTime}}}})
    end.

%% Verify filename
%% (skip single dot in filename, return error when double dot in filename, return filename tokens otherwies)
verify_file_name(FileName) ->
  Tokens = lists:filter(fun(X) -> X =/= "." end, string:tokens(FileName, "/")),
  case lists:any(fun(X) -> X =:= ".." end, Tokens) of
    true -> {error, wrong_filename};
    _ -> {ok, Tokens}
  end.
