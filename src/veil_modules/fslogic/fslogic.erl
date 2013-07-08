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
-include_lib("veil_modules/dao/dao.hrl").
-include_lib("files_common.hrl").
-include("fuse_messages_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("veil_modules/dao/dao_types.hrl").

-define(LOCATION_VALIDITY, 60*15).
-define(FILE_NOT_FOUND_MESSAGE, "Error: file_not_found").
-define(FILE_ALREADY_EXISTS, "Error: file_already_exists").

%% TODO zrobić okresowe sprawdzanie drskryptorów i usuwanie przestarzałych z bazy
%% potrzebna w dao funkcja listująca wszystkie deskryptory
%% TODO dodać wszędzie logowanie w przypadku błędów

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1
-spec init(Args :: term()) -> list().
%% ====================================================================
init(_Args) ->
  Pid = self(),
  {ok, Interval} = application:get_env(veil_cluster_node, fslogic_cleaning_period),
  erlang:send_after(Interval, Pid, {timer, {asynch, 1, {delete_old_descriptors, Pid}}}),
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
  erlang:send_after(Interval, Pid, {timer, {asynch, ProtocolVersion, {delete_old_descriptors, Pid}}}),
  ok;

handle(ProtocolVersion, Record) when is_record(Record, fusemessage) ->
  handle_fuse_message(ProtocolVersion, Record#fusemessage.input, Record#fusemessage.id);

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
handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getfilelocation) ->
  File = Record#getfilelocation.file_logic_name,
  {Status, TmpAns} = get_file(ProtocolVersion, File, FuseID),

  Validity = ?LOCATION_VALIDITY,
  case Status of
    ok ->
      {Status2, TmpAns2} = save_file_descriptor(ProtocolVersion, File, TmpAns#veil_document.uuid, FuseID, Validity),
      case Status2 of
        ok ->
          FileDesc = TmpAns#veil_document.record,
          FileLoc = FileDesc#file.location,
          #filelocation{storage_helper = FileLoc#file_location.storage_helper_id, file_id = FileLoc#file_location.file_id, validity = Validity};
        _BadStatus2 -> #filelocation{storage_helper = TmpAns2, file_id = "Error", validity = 0}
      end;
    _BadStatus -> #filelocation{storage_helper = TmpAns, file_id = "Error", validity = 0}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getnewfilelocation) ->
  File = Record#getnewfilelocation.file_logic_name,
  {FindStatus, FindTmpAns} = get_file(ProtocolVersion, File, FuseID),
  case FindStatus of
    ok -> #filelocation{storage_helper = ?FILE_ALREADY_EXISTS, file_id = "Error", validity = 0};
    error -> case FindTmpAns of
        ?FILE_NOT_FOUND_MESSAGE ->
        {ParentFound, ParentInfo} = get_parent_and_name_from_path(File, ProtocolVersion),

        case ParentFound of
          ok ->
            {FileName, Parent} = ParentInfo,
            Storage_helper = "helper1",
            File_id = "real_location_of___" ++ re:replace(File, "/", "___", [global, {return,list}]),
            FileLocation = #file_location{storage_helper_id = Storage_helper, file_id = File_id},
            FileRecord = #file{type = ?REG_TYPE, name = FileName, size = 0, parent = Parent, location = FileLocation},

            Pid = self(),
            Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 1, {vfs, save_file, [FileRecord]}}),
            {Status, TmpAns} = wait_for_dao_ans(Ans, FileName, 1, "save_file"),

            Validity = ?LOCATION_VALIDITY,
            case Status of
              ok ->
                {Status2, TmpAns2} = save_file_descriptor(ProtocolVersion, File, TmpAns, FuseID, Validity),
                case Status2 of
                  ok ->
                    #filelocation{storage_helper = Storage_helper, file_id = File_id, validity = Validity};
                  _BadStatus2 -> #filelocation{storage_helper = TmpAns2, file_id = "Error", validity = 0}
                end;
              _BadStatus -> #filelocation{storage_helper = TmpAns, file_id = "Error", validity = 0}
            end;
          _ParentError -> #filelocation{storage_helper = ParentInfo, file_id = "Error", validity = 0}
        end;
      _Other ->
        lager:error([{mod, ?MODULE}], "Error: can not create new file: ~s, can not chceck if file exists", [File]),
        #filelocation{storage_helper = "Error: can not chceck if file exists", file_id = "Error", validity = 0}
    end;
    _Other2 ->
      lager:error([{mod, ?MODULE}], "Error: can not create new file: ~s, can not chceck if file exists", [File]),
      #filelocation{storage_helper = "Error: can not chceck if file exists", file_id = "Error", validity = 0}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, filenotused) ->
  File = Record#filenotused.file_logic_name,
  Pid = self(),
  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 40, {vfs, remove_descriptor, [{by_file_n_owner, {File, FuseID}}]}}),
  {Status, TmpAns} = wait_for_dao_ans(Ans, File, 40, "remove_descriptor"),
  case Status of
    ok -> #atom{value = "ok"};
    _Other ->
      lager:error([{mod, ?MODULE}], "Error: for file not used message, file: ~s", [File]),
      #atom{value = TmpAns}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, renewfilelocation) ->
  File = Record#renewfilelocation.file_logic_name,
  Pid = self(),
  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 50, {vfs, list_descriptors, [{by_file_n_owner, {File, FuseID}}, 10, 0]}}),
  {Status, TmpAns} = wait_for_dao_ans(Ans, File, 50, "list_descriptors"),
  case Status of
    ok ->
      case length(TmpAns) of
        0 ->
          lager:error([{mod, ?MODULE}], "Error: can not renew file location for file: ~s, descriptor not found", [File]),
          #filelocationvalidity{answer = "Error: file descriptor not found", validity = 0};
        1 ->
          [VeilDoc | _] = TmpAns,
          Validity = ?LOCATION_VALIDITY,

          {Status2, TmpAns2} = save_file_descriptor(ProtocolVersion, VeilDoc, Validity),
          case Status2 of
            ok ->
              #filelocationvalidity{answer = "ok", validity = Validity};
            _BadStatus2 ->
              lager:error([{mod, ?MODULE}], "Error: can not renew file location for file: ~s", [File]),
              #filelocationvalidity{answer = TmpAns2, validity = 0}
          end;
        _Many ->
          lager:error([{mod, ?MODULE}], "Error: can not renew file location for file: ~s, too many file descriptors", [File]),
          #filelocationvalidity{answer = "Error: too many file descriptors", validity = 0}
      end;
    _Other ->
      lager:error([{mod, ?MODULE}], "Error: can not renew file location for file: ~s", [File]),
      #filelocationvalidity{answer = TmpAns, validity = 0}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, createdir) ->
  Dir = Record#createdir.dir_logic_name,
  {FindStatus, FindTmpAns} = get_file(ProtocolVersion, Dir, FuseID),
  case FindStatus of
    ok -> #atom{value = ?FILE_ALREADY_EXISTS};
    error -> case FindTmpAns of
        ?FILE_NOT_FOUND_MESSAGE ->
        {ParentFound, ParentInfo} = get_parent_and_name_from_path(Dir, ProtocolVersion),

        case ParentFound of
          ok ->
            {FileName, Parent} = ParentInfo,
            File = #file{type = ?DIR_TYPE, name = FileName, parent = Parent},

            Pid = self(),
            Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 60, {vfs, save_file, [File]}}),
            {Status, TmpAns} = wait_for_dao_ans(Ans, FileName, 60, "save_file"),

            case Status of
              ok ->
                #atom{value = "ok"};
              _BadStatus ->
                lager:error([{mod, ?MODULE}], "Error: can not create dir: ~s", [Dir]),
                #atom{value = TmpAns}
            end;
          _ParentError ->
            lager:error([{mod, ?MODULE}], "Error: can not create dir: ~s", [Dir]),
            #atom{value = ParentInfo}
        end;

       _Other ->
        lager:error([{mod, ?MODULE}], "Error: can not create dir: ~s, can not chceck if dir exists", [Dir]),
         #atom{value = "Error: can not chceck if dir exists"}
      end;
    _Other2 ->
      lager:error([{mod, ?MODULE}], "Error: can not create new dir: ~s, can not chceck if dir exists", [Dir]),
      #atom{value = "Error: can not chceck if dir exists"}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getfilechildren) ->
  File = Record#getfilechildren.dir_logic_name,
  Num = Record#getfilechildren.children_num,
  Offset = Record#getfilechildren.offset,

  Pid = self(),
  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 70, {vfs, list_dir, [File, Num, Offset]}}),
  {Status, TmpAns} = wait_for_dao_ans(Ans, File, 70, "list_dir"),

  Children = case Status of
    ok -> create_children_list(TmpAns);
    _BadStatus ->
      lager:error([{mod, ?MODULE}], "Error: can not list files in dir: ~s", [File]),
      [TmpAns]
  end,

  #filechildren{child_logic_name = Children};

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, deletefile) ->
  File = Record#deletefile.file_logic_name,
  {FindStatus, FindTmpAns} = get_file(ProtocolVersion, File, FuseID),

  case FindStatus of
    ok ->
      Pid = self(),
      FileDesc = FindTmpAns#veil_document.record,
      {ChildrenStatus, ChildrenTmpAns} = case FileDesc#file.type of
        ?DIR_TYPE ->
          Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 130, {vfs, list_dir, [File, 1000, 0]}}),
          wait_for_dao_ans(Ans, File, 130, "list_dir");
        _OtherType -> {ok, []}
      end,

      case ChildrenStatus of
        ok -> case length(ChildrenTmpAns) of
          0 ->
            Ans2 = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 110, {vfs, remove_file, [File]}}),
            {Status, TmpAns} = wait_for_dao_ans(Ans2, File, 110, "remove_file"),

            case Status of
              ok ->
                #atom{value = "ok"};
              _BadStatus ->
                lager:error([{mod, ?MODULE}], "Error: can not remove file: ~s", [File]),
                #atom{value = TmpAns}
            end;
          _Other ->
            lager:error([{mod, ?MODULE}], "Error: can not remove file (it has children): ~s", [File]),
            #atom{value = "Error: children exist"}
          end;
        _Other2 ->
          lager:error([{mod, ?MODULE}], "Error: can not remove file (can not check children): ~s", [File]),
          #atom{value = ChildrenTmpAns}
      end;
    _FindError ->
      lager:error([{mod, ?MODULE}], "Error: can not remove file (can not check file type): ~s", [File]),
      #atom{value = FindTmpAns}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, renamefile) ->
  File = Record#renamefile.from_file_logic_name,
  NewFileName = Record#renamefile.to_file_logic_name,

  Pid = self(),
  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 80, {vfs, rename_file, [File, NewFileName]}}),
  {Status, TmpAns} = wait_for_dao_ans(Ans, File, 80, "rename_file"),

  case Status of
    ok ->
      #atom{value = "ok"};
    _BadStatus ->
      lager:error([{mod, ?MODULE}], "Error: can not rename file: ~s", [File]),
      #atom{value = TmpAns}
  end.

%% save_file_descriptor/3
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE.
%% @end
-spec save_file_descriptor(ProtocolVersion :: term(), File :: record(), Validity :: integer()) -> Result when
  Result :: term().
%% ====================================================================

save_file_descriptor(ProtocolVersion, File, Validity) ->
  Pid = self(),
  Descriptor = update_file_descriptor(File#veil_document.record, Validity),

  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 100, {vfs, save_descriptor, [File#veil_document{record = Descriptor}]}}),
  wait_for_dao_ans(Ans, File, 100, "save_descriptor").


%% save_file_descriptor/5
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE.
%% @end
-spec save_file_descriptor(ProtocolVersion :: term(), File :: string(), Uuid::uuid(), FuseID :: string(), Validity :: integer()) -> Result when
  Result :: term().
%% ====================================================================

save_file_descriptor(ProtocolVersion, File, Uuid, FuseID, Validity) ->
  Pid = self(),
  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 20, {vfs, list_descriptors, [{by_file_n_owner, {File, FuseID}}, 10, 0]}}),

  {Status, TmpAns} = wait_for_dao_ans(Ans, File, 20, "list_descriptors"),
  case Status of
    ok ->
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
    _Other -> {Status, TmpAns}
  end.

save_new_file_descriptor(ProtocolVersion, File, Uuid, FuseID, Validity) ->
  Pid = self(),
  Descriptor = update_file_descriptor(#file_descriptor{file = Uuid, fuse_id = FuseID}, Validity),
  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 100, {vfs, save_descriptor, [Descriptor]}}),
  wait_for_dao_ans(Ans, File, 100, "save_descriptor").

update_file_descriptor(Descriptor, Validity) ->
  {Megaseconds,Seconds, _Microseconds} = os:timestamp(),
  Time = 1000000*Megaseconds + Seconds,
  Descriptor#file_descriptor{create_time = Time, validity_time = Validity}.

%% wait_for_dao_ans/4
%% ====================================================================
%% @doc Waits for answer from dao and analysis answers from gen_server
%% (first parameter) and from dao (checks possible errors).
%% @end
-spec wait_for_dao_ans(Ans :: term(), File :: string(), MessageId :: integer(), LogMessage :: string()) -> Result when
  Result :: term().
%% ====================================================================

wait_for_dao_ans(Ans, File, MessageId, LogMessage) ->
  case Ans of
    ok ->
      receive
        {worker_answer, MessageId, ok} -> {ok, ok};
        {worker_answer, MessageId, {ok, DaoAns}} -> {ok, DaoAns};
        {worker_answer, MessageId, {error, file_not_found}} -> {error, ?FILE_NOT_FOUND_MESSAGE};
        Ans2 ->
          lager:error([{mod, ?MODULE}], "Error: wrong dao answer for: " ++ LogMessage ++ ", file: ~s, answer: ~p", [File, Ans2]),
          {error, "Error: wrong dao answer for: " ++ LogMessage}
      after 1000 ->
        lager:error([{mod, ?MODULE}], "Error: dao timeout for: " ++ LogMessage ++ ", file: ~s", [File]),
        {error, "Error: dao timeout for: " ++ LogMessage}
      end;
    Other ->
      lager:error([{mod, ?MODULE}], "Error: dispatcher error for: " ++ LogMessage ++ ", file: ~s, error: ~p", [File, Other]),
      {error, "Error: dispatcher for: " ++ LogMessage}
  end.

%% get_parent_and_name_from_path/2
%% ====================================================================
%% @doc Gets parent uuid and file name on the basis of absolute path.
%% @end
-spec get_parent_and_name_from_path(Path :: string(), ProtocolVersion :: term()) -> Result when
  Result :: tuple().
%% ====================================================================

get_parent_and_name_from_path(Path, ProtocolVersion) ->
  Pos = string:rchr(Path, $/),
  case Pos of
    0 -> {ok, {Path, ""}};
    _Other ->
      File = string:substr(Path, Pos + 1),
      Parent = string:substr(Path, 1, Pos -1),

      case Parent =:= "" of
        true -> {ok, {File, ""}};
        false ->
          Pid = self(),
          Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 1000, {vfs, get_file, [Parent]}}),

          {Status, TmpAns} = wait_for_dao_ans(Ans, Parent, 1000, "get_file"),
          case Status of
            ok -> {ok, {File, TmpAns#veil_document.uuid}};
            _BadStatus ->
              lager:error([{mod, ?MODULE}], "Error: cannot find parent for path: ~s", [Path]),
              {error, "Error: cannot find parent: " ++ TmpAns}
          end
      end
  end.

%% create_children_list/1
%% ====================================================================
%% @doc Creates list of children logical names on the basis of list with
%% veil_documents that describe children.
%% @end
-spec create_children_list(Files :: list()) -> Result when
  Result :: term().
%% ====================================================================

create_children_list(Files) ->
  create_children_list(Files, []).

%% create_children_list/2
%% ====================================================================
%% @doc Creates list of children logical names on the basis of list with
%% veil_documents that describe children.
%% @end
-spec create_children_list(Files :: list(), TmpAns :: list()) -> Result when
  Result :: term().
%% ====================================================================

create_children_list([], Ans) ->
  Ans;

create_children_list([File | Rest], Ans) ->
  FileDesc = File#veil_document.record,
  create_children_list(Rest, [FileDesc#file.name | Ans]).

get_file(ProtocolVersion, File, FuseID) ->
  Pid = self(),
  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 30, {vfs, get_file, [File]}}),
  wait_for_dao_ans(Ans, File, 30, "get_file").

delete_old_descriptors(ProtocolVersion, Time) ->
  Pid = self(),
  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 13, {vfs, remove_descriptor, [{by_expired_before, Time}]}}),

  {Status, _TmpAns} = wait_for_dao_ans(Ans, "all_files", 13, "remove_descriptor"),
  case Status of
    ok ->
      lager:info([{mod, ?MODULE}], "Old descriptors cleared");
    _Other -> lager:error([{mod, ?MODULE}], "Error during clearing old descriptors")
  end.
