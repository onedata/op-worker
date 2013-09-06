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

%% TODO dodać sprawdzanie uprawnień, gdy będziemy mieli już autentykację klienta
%% (obecnie generowane warningi to efekt przekazywania parametrów, które będą
%% do tego potrzebne, a nie są obecnie używane)

-module(fslogic).
-behaviour(worker_plugin_behaviour).

-include("registered_names.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("files_common.hrl").
-include("fuse_messages_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("veil_modules/dao/dao_users.hrl").
-include_lib("veil_modules/dao/dao_types.hrl").

-define(LOCATION_VALIDITY, 60*15).
-define(CLUSTER_USER_ID, cluster).

%% ====================================================================
%% API
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

%% ====================================================================
%% Test API
%% ====================================================================
-ifdef(TEST).
-export([handle_fuse_message/3]).
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
  {ok, Interval} = application:get_env(veil_cluster_node, fslogic_cleaning_period),
  erlang:send_after(Interval * 1000, Pid, {timer, {asynch, 1, {delete_old_descriptors, Pid}}}),
  [].

%% handle/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1. <br/>
%% Processes standard worker requests (e.g. ping) and requests from FUSE.
%% @end
-spec handle(ProtocolVersion :: term(), Request :: term()) -> Result when
  Result :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(ProtocolVersion, {delete_old_descriptors_test, Time}) ->
  delete_old_descriptors(ProtocolVersion, Time),
  ok;

handle(ProtocolVersion, {delete_old_descriptors, Pid}) ->
  {Megaseconds,Seconds, _Microseconds} = os:timestamp(),
  Time = 1000000*Megaseconds + Seconds - 15,
  delete_old_descriptors(ProtocolVersion, Time),
  {ok, Interval} = application:get_env(veil_cluster_node, fslogic_cleaning_period),
  erlang:send_after(Interval * 1000, Pid, {timer, {asynch, ProtocolVersion, {delete_old_descriptors, Pid}}}),
  ok;

handle(ProtocolVersion, Record) when is_record(Record, fusemessage) ->
  handle_fuse_message(ProtocolVersion, Record#fusemessage.input, Record#fusemessage.id);

handle(ProtocolVersion, {internal_call, Record}) ->
  handle_fuse_message(ProtocolVersion, Record, non);

handle(_ProtocolVersion, _Msg) ->
  ok.

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
handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, changefileperms) ->
    {FileNameFindingAns, FileName} = get_full_file_name(Record#changefileperms.logic_file_name, FuseID),
    case FileNameFindingAns of
      ok ->
        case get_file(ProtocolVersion, FileName, FuseID) of
            {ok, #veil_document{record = #file{} = File} = Doc} ->
                NewFile = Doc#veil_document{record = File#file{perms = Record#changefileperms.perms}},
                case dao_lib:apply(dao_vfs, save_file, [NewFile], ProtocolVersion) of
                    {ok, _} -> #atom{value = ?VOK};
                    Other1 ->
                        lager:error("fslogic could not save file ~p due to: ~p", [FileName, Other1]),
                        #atom{value = ?VEIO}
                end;
            {error, file_not_found} -> #atom{value = ?VENOENT};
            Other ->
                lager:error("fslogic could not get file ~p due to: ~p", [FileName, Other]),
                #atom{value = ?VEIO}
        end;
      _  -> #atom{value = ?VEIO}
    end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getfileattr) ->
    {FileNameFindingAns, FileName} = get_full_file_name(Record#getfileattr.file_logic_name, FuseID),
    case FileNameFindingAns of
      ok ->
        lager:debug("FileAttr for ~p", [FileName]),
        case get_file(ProtocolVersion, FileName, FuseID) of
            {ok, #veil_document{record = #file{} = File}} ->
                Type =
                    case File#file.type of
                        ?DIR_TYPE -> "DIR";
                        ?REG_TYPE -> "REG";
                        ?LNK_TYPE -> "LNK";
                        _ -> "UNK"
                    end,
                %% TODO: *time are not implemented because whole file_meta isnt working yet
                #fileattr{answer = ?VOK, mode = File#file.perms, uid = -1, gid = -1, atime = 0, ctime = 0, mtime = 0, nlink = 0, type = Type};
            {error, file_not_found} ->
                lager:debug("FileAttr: ENOENT"),
                #fileattr{answer = ?VENOENT, mode = 0, uid = -1, gid = -1, atime = 0, ctime = 0, mtime = 0, nlink = 0, type = ""};
            _ ->
                #fileattr{answer = ?VEIO, mode = 0, uid = -1, gid = -1, atime = 0, ctime = 0, mtime = 0, nlink = 0, type = ""}
        end;
      _  -> #fileattr{answer = ?VEIO, mode = 0, uid = -1, gid = -1, atime = 0, ctime = 0, mtime = 0, nlink = 0, type = ""}
    end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getfilelocation) ->
    {FileNameFindingAns, File} = get_full_file_name(Record#getfilelocation.file_logic_name, FuseID),
    case FileNameFindingAns of
      ok ->
        {Status, TmpAns} = get_file(ProtocolVersion, File, FuseID),
        Validity = ?LOCATION_VALIDITY,
        case {Status, TmpAns} of
            {ok, _} ->
            case TmpAns#veil_document.record#file.type of
                ?REG_TYPE ->
                    {Status2, TmpAns2} = save_file_descriptor(ProtocolVersion, File, TmpAns#veil_document.uuid, FuseID, Validity),
                    case Status2 of
                    ok ->
                        FileDesc = TmpAns#veil_document.record,
                        FileLoc = FileDesc#file.location,
                        case dao_lib:apply(dao_vfs, get_storage, [FileLoc#file_location.storage_id], ProtocolVersion) of
                            {ok, #veil_document{record = Storage}} ->
                                SH = fslogic_storage:get_sh_for_fuse(FuseID, Storage),
                                #filelocation{storage_id = Storage#storage_info.id, file_id = FileLoc#file_location.file_id, validity = Validity,
                                              storage_helper_name = SH#storage_helper_info.name, storage_helper_args = SH#storage_helper_info.init_args};
                            Other ->
                                lager:error("Cannot fetch storage: ~p for file: ~p, reason: ~p", [FileLoc#file_location.storage_id, TmpAns#veil_document.uuid, Other]),
                                #filelocation{answer = ?VEIO, storage_id = -1, file_id = "", validity = 0}
                        end;
                    _BadStatus2 ->
                        lager:warning("Unknown fslogic error: ~p", [TmpAns2]),
                        #filelocation{answer = ?VEIO, storage_id = -1, file_id = "", validity = 0}
                    end;
                _ -> #filelocation{answer = ?VENOTSUP, storage_id = -1, file_id = "", validity = 0}
            end;
        {error, file_not_found} -> #filelocation{answer = ?VENOENT, storage_id = -1, file_id = "", validity = 0};
            _BadStatus ->
              lager:warning("Unknown fslogic error: ~p", [_BadStatus]),
              #filelocation{answer = ?VEIO, storage_id = -1, file_id = "", validity = 0}
        end;
      _  -> #filelocation{answer = ?VEIO, storage_id = -1, file_id = "", validity = 0}
    end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getnewfilelocation) ->
    {FileNameFindingAns, File} = get_full_file_name(Record#getnewfilelocation.file_logic_name, FuseID),
    case FileNameFindingAns of
      ok ->
        {FindStatus, FindTmpAns} = get_file(ProtocolVersion, File, FuseID),
        case FindStatus of
            ok -> #filelocation{answer = ?VEEXIST, storage_id = -1, file_id = "", validity = 0};
            error -> case FindTmpAns of
                file_not_found ->
                {ParentFound, ParentInfo} = fslogic_utils:get_parent_and_name_from_path(File, ProtocolVersion),

                case ParentFound of
                    ok ->
                        {FileName, Parent} = ParentInfo,
                        {ok, StorageList} = dao_lib:apply(dao_vfs, list_storage, [], ProtocolVersion),
                        case fslogic_storage:select_storage(FuseID, StorageList) of
                            #veil_document{uuid = UUID, record = #storage_info{} = Storage} ->
                                File_id = "real_location_of___" ++ re:replace(File, "/", "___", [global, {return,list}]),
                                FileLocation = #file_location{storage_id = UUID, file_id = File_id},

                                {UserIdStatus, UserId} = get_user_id(FuseID),
                                case UserIdStatus of
                                  ok ->
                                    FileRecord = #file{type = ?REG_TYPE, name = FileName, uid = UserId, size = 0, parent = Parent, perms = Record#getnewfilelocation.mode, location = FileLocation},

                                    Status = dao_lib:apply(dao_vfs, save_file, [FileRecord], ProtocolVersion),
                                    Validity = ?LOCATION_VALIDITY,
                                    case Status of
                                        {ok, FileUUID} ->
                                            {Status2, _TmpAns2} = save_file_descriptor(ProtocolVersion, File, FileUUID, FuseID, Validity),
                                            case Status2 of
                                              ok ->
                                                  #storage_helper_info{name = SHName, init_args = SHArgs} = fslogic_storage:get_sh_for_fuse(FuseID, Storage),
                                                  #filelocation{storage_id = Storage#storage_info.id, file_id = File_id, validity = Validity, storage_helper_name = SHName, storage_helper_args = SHArgs};
                                              _BadStatus2 ->
                                              lager:error([{mod, ?MODULE}], "Error: cannot save file_descriptor document: ~p", [_BadStatus2]),
                                                #filelocation{answer = ?VEIO, storage_id = -1, file_id = "", validity = 0}
                                            end;
                                        _BadStatus ->
                                          lager:error([{mod, ?MODULE}], "Error: cannot save file document: ~p", [_BadStatus]),
                                          #filelocation{answer = ?VEIO, storage_id = -1, file_id = "", validity = 0}
                                    end;
                                  _ ->
                                    #filelocation{answer = ?VEIO, storage_id = -1, file_id = "", validity = 0}
                                end;
                            {error, SelectError} ->
                                lager:error([{mod, ?MODULE}], "Error: can not get storage information: ~p", [SelectError]),
                                #filelocation{answer = ?VEIO, storage_id = -1, file_id = "", validity = 0}
                        end;
                    _ParentError -> #filelocation{answer = ?VEIO, storage_id = -1, file_id = "", validity = 0}
                end;
              _Other ->
                lager:error([{mod, ?MODULE}], "Error: can not create new file: ~s, can not chceck if file exists", [File]),
                #filelocation{answer = ?VEIO, storage_id = -1, file_id = "", validity = 0}
            end;
            _Other2 ->
              lager:error([{mod, ?MODULE}], "Error: can not create new file: ~s, can not chceck if file exists", [File]),
              #filelocation{answer = ?VEIO, storage_id = -1, file_id = "", validity = 0}
        end;
      _  -> #filelocation{answer = ?VEIO, storage_id = -1, file_id = "", validity = 0}
    end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, filenotused) ->
  {FileNameFindingAns, File} = get_full_file_name(Record#filenotused.file_logic_name, FuseID),
  case FileNameFindingAns of
    ok ->
      Status = dao_lib:apply(dao_vfs, remove_descriptor, [{by_file_n_owner, {File, FuseID}}], ProtocolVersion),
      case Status of
        ok -> #atom{value = ?VOK};
        _Other ->
          lager:error([{mod, ?MODULE}], "Error: for file not used message, file: ~s", [File, _Other]),
          #atom{value = ?VEIO}
      end;
    _  -> #atom{value = ?VEIO}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, renewfilelocation) ->
  {FileNameFindingAns, File} = get_full_file_name(Record#renewfilelocation.file_logic_name, FuseID),
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
                  #filelocationvalidity{answer = ?VEIO, validity = 0}
              end;
            _Many ->
              lager:error([{mod, ?MODULE}], "Error: can not renew file location for file: ~s, too many file descriptors", [File]),
              #filelocationvalidity{answer = ?VEIO, validity = 0}
          end;
        _Other ->
          lager:error([{mod, ?MODULE}], "Error: can not renew file location for file: ~s", [File]),
          #filelocationvalidity{answer = ?VEIO, validity = 0}
      end;
    _  -> #filelocationvalidity{answer = ?VEIO, validity = 0}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, createdir) ->
  {FileNameFindingAns, Dir} = get_full_file_name(Record#createdir.dir_logic_name, FuseID),
  case FileNameFindingAns of
    ok ->
      {FindStatus, FindTmpAns} = get_file(ProtocolVersion, Dir, FuseID),
      case FindStatus of
        ok -> #atom{value = ?VEEXIST};
        error -> case FindTmpAns of
            file_not_found ->
            {ParentFound, ParentInfo} = fslogic_utils:get_parent_and_name_from_path(Dir, ProtocolVersion),

            case ParentFound of
              ok ->
                {FileName, Parent} = ParentInfo,
                {UserIdStatus, UserId} = get_user_id(FuseID),
                case UserIdStatus of
                  ok ->
                    File = #file{type = ?DIR_TYPE, name = FileName, uid = UserId, parent = Parent, perms = Record#createdir.mode},

                    {Status, _TmpAns} = dao_lib:apply(dao_vfs, save_file, [File], ProtocolVersion),
                    case Status of
                      ok ->
                        #atom{value = ?VOK};
                      _BadStatus ->
                        lager:error([{mod, ?MODULE}], "Error: can not create dir: ~s, error: ~p", [Dir, _BadStatus]),
                        #atom{value = ?VEIO}
                    end;
                  _ -> #atom{value = ?VEIO}
                end;
              _ParentError ->
                lager:error([{mod, ?MODULE}], "Error: can not create dir: ~s, parentInfo: ~p", [Dir, _ParentError]),
                #atom{value = ?VEIO}
            end;

           _Other ->
            lager:error([{mod, ?MODULE}], "Error: can not create dir: ~s, can not chceck if dir exists", [Dir]),
             #atom{value = ?VEIO}
          end;
        _Other2 ->
          lager:error([{mod, ?MODULE}], "Error: can not create new dir: ~s, can not chceck if dir exists", [Dir]),
          #atom{value = ?VEIO}
      end;
    _  -> #atom{value = ?VEIO}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getfilechildren) ->
  {FileNameFindingAns, File} = get_full_file_name(Record#getfilechildren.dir_logic_name, FuseID),
  case FileNameFindingAns of
    ok ->
      Num = Record#getfilechildren.children_num,
      Offset = Record#getfilechildren.offset,

      {Status, TmpAns} = dao_lib:apply(dao_vfs, list_dir, [File, Num, Offset], ProtocolVersion),
      case Status of
        ok ->
          Children = fslogic_utils:create_children_list(TmpAns),
          #filechildren{child_logic_name = Children};
        _BadStatus ->
          lager:error([{mod, ?MODULE}], "Error: can not list files in dir: ~s", [File]),
          #filechildren{answer = ?VEIO, child_logic_name = [""]}
      end;
    _  -> #filechildren{answer = ?VEIO, child_logic_name = [""]}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, deletefile) ->
  {FileNameFindingAns, File} = get_full_file_name(Record#deletefile.file_logic_name, FuseID),
  case FileNameFindingAns of
    ok ->
      {FindStatus, FindTmpAns} = get_file(ProtocolVersion, File, FuseID),

      case FindStatus of
        ok ->
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
                      #atom{value = ?VOK};
                    _BadStatus ->
                      lager:error([{mod, ?MODULE}], "Error: can not remove file: ~s", [File]),
                      #atom{value = ?VEIO}
                  end;
                _Other ->
                  lager:error([{mod, ?MODULE}], "Error: can not remove file (it has children): ~s", [File]),
                  #atom{value = ?VENOTEMPTY}
            end;
            _Other2 ->
              lager:error([{mod, ?MODULE}], "Error: can not remove file (can not check children): ~s", [File]),
              #atom{value = ?VEIO}
          end;
        _FindError ->
          lager:error([{mod, ?MODULE}], "Error: can not remove file (can not check file type): ~s", [File]),
          #atom{value = ?VEIO}
      end;
    _  -> #atom{value = ?VEIO}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, renamefile) ->
  {FileNameFindingAns, File} = get_full_file_name(Record#renamefile.from_file_logic_name, FuseID),
  {FileNameFindingAns2, NewFileName} = get_full_file_name(Record#renamefile.to_file_logic_name, FuseID),
  case {FileNameFindingAns, FileNameFindingAns2} of
    {ok, ok} ->
      case get_file(ProtocolVersion, File, FuseID) of
        {ok, #veil_document{record = #file{} = OldFile} = OldDoc} ->
          case get_file(ProtocolVersion, NewFileName, FuseID) of
            {error, file_not_found} ->
              case get_file(ProtocolVersion, fslogic_utils:strip_path_leaf(NewFileName), FuseID) of
                {ok, #veil_document{uuid = NewParent}} ->
                  Renamed = OldDoc#veil_document{record =
                      OldFile#file{parent = NewParent, name = fslogic_utils:basename(NewFileName)}},
                  case dao_lib:apply(dao_vfs, save_file, [Renamed], ProtocolVersion)  of
                    {ok, _} -> #atom{value = ?VOK};
                    Other ->
                      lager:warning("Cannot save file document. Reason: ~p", [Other]),
                      #atom{value = ?VEIO}
                  end;
                {error, file_not_found} ->
                  lager:warning("Cannot find destination dir: ~p", [fslogic_utils:strip_path_leaf(NewFileName)]),
                  #atom{value = ?VENOENT};
                _ -> #atom{value = ?VEIO}
              end;
            {ok, #veil_document{}} ->
              lager:warning("Destination file already exists: ~p", [File]),
              #atom{value = ?VEEXIST};
            _ -> #atom{value = ?VEIO}
          end;
        {error, file_not_found} ->
          lager:warning("Cannot find source file: ~p", [File]),
          #atom{value = ?VENOENT};
        _ -> #atom{value = ?VEIO}
      end;
    _  -> #atom{value = ?VEIO}
  end;

%% Symbolic link creation. From - link name, To - path pointed by new link
handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, createlink) ->
  {FileNameFindingAns, From} = get_full_file_name(Record#createlink.from_file_logic_name, FuseID),
  {FileNameFindingAns2, To} = get_full_file_name(Record#createlink.to_file_logic_name, FuseID),
  case {FileNameFindingAns, FileNameFindingAns2} of
    {ok, ok} ->
      case get_file(ProtocolVersion, From, FuseID) of
          {error, file_not_found} ->
              case get_file(ProtocolVersion, fslogic_utils:strip_path_leaf(From), FuseID) of
                  {ok, #veil_document{uuid = Parent}} ->
                      LinkDoc = #file{type = ?LNK_TYPE, name = fslogic_utils:basename(From), ref_file = To, size = 0, parent = Parent},
                      case dao_lib:apply(dao_vfs, save_file, [LinkDoc], ProtocolVersion) of
                          {ok, _} ->
                              #atom{value = ?VOK};
                          {error, Reason} ->
                              lager:error("Cannot save link file (from ~p to ~p) due to error: ~p", [From, To, Reason]),
                              #atom{value = ?VEIO}
                      end;
                  {error, file_not_found} ->
                      lager:error("Cannot create link ~p because parent directory does not exist.", [From]),
                      #atom{value = ?VENOENT};
                  {error, Reason1} ->
                      lager:error("Cannot fetch file information for file ~p due to error: ~p", [fslogic_utils:strip_path_leaf(From), Reason1]),
                      #atom{value = ?VEIO}
              end;
          {ok, #veil_document{}} ->
              lager:error("Cannot create link - file already exists: ~p", [From]),
              #atom{value = ?VEEXIST};
          _ -> #atom{value = ?VEIO}
      end;
    _  -> #atom{value = ?VEIO}
  end;

%% Fetch link data (target path)
handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getlink) ->
  {FileNameFindingAns, File} = get_full_file_name(Record#getlink.file_logic_name, FuseID),
  case FileNameFindingAns of
    ok ->
      case get_file(ProtocolVersion, File, FuseID) of
          {ok, #veil_document{record = #file{ref_file = Target}}} ->
              #linkinfo{file_logic_name = get_file_name_from_full_file_name(Target, FuseID)};
          {error, file_not_found} ->
              lager:error("Link ~p does not exist.", [File]),
              #linkinfo{answer = ?VENOENT, file_logic_name = ""};
          {error, Reason} ->
              lager:error("Cannot read link ~p due to error: ~p", [File, Reason]),
              #linkinfo{answer = ?VEIO, file_logic_name = ""}
      end;
    _  -> #linkinfo{answer = ?VEIO, file_logic_name = ""}
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


%% save_file_descriptor/5
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE.
%% @end
-spec save_file_descriptor(ProtocolVersion :: term(), File :: string(), Uuid::uuid(), FuseID :: string(), Validity :: integer()) -> Result when
  Result :: term().
%% ====================================================================

save_file_descriptor(ProtocolVersion, File, Uuid, FuseID, Validity) ->
  case FuseID of
    non -> {ok, ok};
    _ ->
      Status = dao_lib:apply(dao_vfs, list_descriptors, [{by_file_n_owner, {File, FuseID}}, 10, 0], ProtocolVersion),
      case Status of
        {ok, TmpAns} ->
          case length(TmpAns) of
            0 ->
              save_new_file_descriptor(ProtocolVersion, File, Uuid, FuseID, Validity);
            1 ->
              [VeilDoc | _] = TmpAns,
              save_file_descriptor(ProtocolVersion, VeilDoc, Validity);
            _Many ->
              lager:error([{mod, ?MODULE}], "Error: to many file descriptors for file: ~s", [File]),
              {error, "Error: too many file descriptors"}
          end;
        _Other -> _Other
      end
  end.

%% save_new_file_descriptor/5
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE.
%% @end
-spec save_new_file_descriptor(ProtocolVersion :: term(), File :: string(), Uuid::uuid(), FuseID :: string(), Validity :: integer()) -> Result when
  Result :: term().
%% ====================================================================

save_new_file_descriptor(ProtocolVersion, _File, Uuid, FuseID, Validity) ->
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
  lager:debug("get_file(File: ~p, FuseID: ~p)", [File, FuseID]),
  dao_lib:apply(dao_vfs, get_file, [File], ProtocolVersion).

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
  UserId = get(user_id),
  case UserId of
    {rdnSequence, RDNSequence} ->
      DN = user_logic:rdn_sequence_to_dn_string(RDNSequence),
      {GetUserAns, User} = user_logic:get_user({dn, DN}),
      case GetUserAns of
        ok ->
          UserRecord = User#veil_document.record,
          {ok, UserRecord#user.login};
        _ -> {error, get_user_error}
      end;
    _ -> {error, get_user_id_error}
  end.

%% get_user_id/1
%% ====================================================================
%% @doc Gets user's id.
%% @end
-spec get_user_id(FuseID :: string()) -> Result when
  Result :: {ok, UserID} | {error, ErrorDesc},
  UserID :: term(),
  ErrorDesc :: atom.
%% ====================================================================
get_user_id(FuseID) ->
  case FuseID of
    non -> {ok, ?CLUSTER_USER_ID};
    _ ->
      UserId = get(user_id),
      case UserId of
        {rdnSequence, RDNSequence} ->
          DN = user_logic:rdn_sequence_to_dn_string(RDNSequence),
          {GetUserAns, User} = user_logic:get_user({dn, DN}),
          case GetUserAns of
            ok ->
              {ok, User#veil_document.uuid};
            _ -> {error, get_user_error}
          end;
        _ -> {error, get_user_id_error}
      end
  end.

%% get_full_file_name/2
%% ====================================================================
%% @doc Gets file's full name (user's root is added to name).
%% @end
-spec get_full_file_name(FileName :: string(), FuseID :: string()) -> Result when
  Result :: {ok, FullFileName} | {error, ErrorDesc},
  FullFileName :: string(),
  ErrorDesc :: atom.
%% ====================================================================

get_full_file_name(FileName, FuseID) ->
  case FuseID of
    non ->
      {ok, FileName};
    _ ->
      [Beg | _] = FileName,
      {RootAns, Root} = get_user_root(),
      case RootAns of
        ok ->
          NewFileName = case Beg of
            $/ -> Root ++ FileName;
            _ -> Root ++ "/" ++ FileName
          end,
          {ok, NewFileName};
        _ -> {root_not_found, RootAns}
      end
  end.

%% get_file_name_from_full_file_name/2
%% ====================================================================
%% @doc Gets file's name from full name (user's root is deleted to name).
%% @end
-spec get_file_name_from_full_file_name(FileName :: string(), FuseID :: string()) -> Result when
  Result :: {ok, FileName} | {error, ErrorDesc},
  FileName :: string(),
  ErrorDesc :: atom.
%% ====================================================================

get_file_name_from_full_file_name(FileName, FuseID) ->
  case FuseID of
    non ->
      {ok, FileName};
    _ ->
      Pos = string:chr(FileName, $/),
      case Pos of
        1 ->
          FileName2 = string:substr(FileName, 2),
          Pos2 = string:chr(FileName2, $/),
          string:substr(FileName2, Pos2);
        _ ->
          string:substr(FileName, Pos)
      end
  end.