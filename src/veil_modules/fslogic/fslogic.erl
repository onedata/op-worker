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

-define(LOCATION_VALIDITY, 60*15).

%% TODO zrobić okresowe sprawdzanie drskryptorów i usuwanie przestarzałych z bazy
%% potrzebna w dao funkcja listująca wszystkie deskryptory

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

init(_Args) ->
	[].

handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(ProtocolVersion, Record) when is_record(Record, fusemessage) ->
  handle_fuse_message(ProtocolVersion, Record#fusemessage.input, Record#fusemessage.id);

handle(_ProtocolVersion, _Msg) ->
	ok.

cleanup() ->
	ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% TODO zastanowić się nad formą odpowiedzi o niesistniejący plik (teraz zwraca błąd)
handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getfilelocation) ->
  File = Record#getfilelocation.file_logic_name,

  Pid = self(),
  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 1, {vfs, get_file, [File]}}),
  {Status, TmpAns} = wait_for_dao_ans(Ans, File, 1, "get_file"),

  Validity = ?LOCATION_VALIDITY,
  case Status of
    ok ->
      {Status2, TmpAns2} = save_file_descriptor(ProtocolVersion, File, FuseID, Validity),
      case Status2 of
        ok ->
          FileDesc = TmpAns#veil_document.record,
          FileLoc = FileDesc#file.location,
          #filelocation{storage_helper = FileLoc#file_location.storage_helper_id, file_id = FileLoc#file_location.file_id, validity = Validity};
        _BadStatus2 -> #filelocation{storage_helper = TmpAns2, file_id = "Error", validity = 0}
      end;
    _BadStatus -> #filelocation{storage_helper = TmpAns, file_id = "Error", validity = 0}
  end;

%% TODO zabezpieczyć na wypadek gdyby pytano o istniejący plik
handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getnewfilelocation) ->
  {FileName, Parent} = get_parent_and_name_from_path(Record#getnewfilelocation.file_logic_name),
  Storage_helper = "helper1",
  File_id = "real_location_of" ++ FileName,
  FileLocation = #file_location{storage_helper_id = Storage_helper, file_id = File_id},
  File = #file{type = ?REG_TYPE, name = FileName, size = 0, parent = Parent, location = FileLocation},

  Pid = self(),
  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 1, {vfs, save_file, [File]}}),
  {Status, TmpAns} = wait_for_dao_ans(Ans, FileName, 1, "save_file"),

  Validity = ?LOCATION_VALIDITY,
  case Status of
    ok ->
      {Status2, TmpAns2} = save_file_descriptor(ProtocolVersion, File, FuseID, Validity),
      case Status2 of
        ok ->
          #filelocation{storage_helper = Storage_helper, file_id = File_id, validity = Validity};
        _BadStatus2 -> #filelocation{storage_helper = TmpAns2, file_id = "Error", validity = 0}
      end;
    _BadStatus -> #filelocation{storage_helper = TmpAns, file_id = "Error", validity = 0}
  end;

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, filenotused) ->
  %% TODO - skasować desekryptor z bazy
  %% potrzebna w dao funkcja, która wyszukuje deskryptor po parze {plik, fuse_id},
  %% ewentualnie funkcja, która kasuje deskryptory posadające odpowiednią wartość pola {plik, fuse_id}
  #atom{value = "ok"};

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, renewfilelocation) ->
  %% TODO - sprawdzić czy fuse ma prawa do pliku i jeśli tak to odnowić desekryptor w bazie
  %% potrzebna w dao funkcja, która wyszukuje deskryptor po parze {plik, fuse_id}
  #filelocationvalidity{answer = "ok", validity = ?LOCATION_VALIDITY};

handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, createdir) ->
  {FileName, Parent} = get_parent_and_name_from_path(Record#createdir.dir_logic_name),
  File = #file{type = ?DIR_TYPE, name = FileName, parent = Parent},

  Pid = self(),
  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 1, {vfs, save_file, [File]}}),
  {Status, TmpAns} = wait_for_dao_ans(Ans, FileName, 1, "save_file"),

  case Status of
    ok ->
      #atom{value = "ok"};
    _BadStatus -> #atom{value = TmpAns}
  end;

%% TODO zrobić obsługę większej ilości katalogów niż 1000
handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getfilechildren) ->
  File = Record#getfilechildren.dir_logic_name,

  Pid = self(),
  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 1, {vfs, list_dir, [File, 1000, 0]}}),
  {Status, TmpAns} = wait_for_dao_ans(Ans, File, 1, "list_dir"),

  Locations = case Status of
    ok -> create_file_location_list(TmpAns, ?LOCATION_VALIDITY);
    _BadStatus -> [#filelocation{storage_helper = TmpAns, file_id = "Error", validity = 0}]
  end,

  #filechildren{child_location = Locations}.

%% handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, deletefile) ->
%%   File = Record#deletefile.file_logic_name,
%%
%%   Pid = self(),
%%   Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 1, {vfs, remove_file, [File]}}),
%%   {Status, TmpAns} = wait_for_dao_ans(Ans, File, 1, "remove_file"),
%%
%%   Validity = ?LOCATION_VALIDITY,
%%   case Status of
%%     ok ->
%%       {Status2, TmpAns2} = save_file_descriptor(ProtocolVersion, File, FuseID, Validity),
%%       case Status2 of
%%         ok ->
%%           FileDesc = TmpAns#veil_document.record,
%%           FileLoc = FileDesc#file.location,
%%           #filelocation{storage_helper = FileLoc#file_location.storage_helper_id, file_id = FileLoc#file_location.file_id, validity = Validity};
%%         _BadStatus2 -> #filelocation{storage_helper = TmpAns2, file_id = "Error", validity = 0}
%%       end;
%%     _BadStatus -> #filelocation{storage_helper = TmpAns, file_id = "Error", validity = 0}
%%   end;

save_file_descriptor(ProtocolVersion, File, FuseID, Validity) ->
  Pid = self(),

  {Megaseconds,Seconds, _Microseconds} = os:timestamp(),
  Time = 1000000*Megaseconds + Seconds,
  Descriptor = #file_descriptor{file = File, fuse_id = FuseID, create_time = Time, expire_time = Validity},

  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 100, {vfs, save_descriptor, [Descriptor]}}),
  wait_for_dao_ans(Ans, File, 100, "save_descriptor").

wait_for_dao_ans(Ans, File, MessageId, LogMessage) ->
  case Ans of
    ok ->
      receive
        {worker_answer, MessageId, {ok, DaoAns}} -> {ok, DaoAns};
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

get_parent_and_name_from_path(Path) ->
  Pos = string:rchr(Path, $/),
  case Pos of
    0 -> {Path, ""};
    _Other -> {string:substr(Path, Pos + 1), string:substr(Path, 1, Pos -1)}
  end.

create_file_location_list(Files, Validity) ->
  create_file_location_list(Files, [], Validity).

create_file_location_list([], Ans, _Validity) ->
  Ans;

create_file_location_list([File | Rest], Ans, Validity) ->
  FileDesc = File#veil_document.record,
  FileLoc = FileDesc#file.location,
  AnsPart = #filelocation{storage_helper = FileLoc#file_location.storage_helper_id, file_id = FileLoc#file_location.file_id, validity = Validity},
  create_file_location_list(Rest, [AnsPart | Ans], Validity).