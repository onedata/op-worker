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

-define(LOCATION_VALIDITY, 60*15).

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

%%TODO zrobić okresowe sprawdzanie drskryptorów i usuwanie przestarzałych z bazy
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
    _BadStatus -> TmpAns
  end;


handle_fuse_message(ProtocolVersion, Record, FuseID) when is_record(Record, getnewfilelocation) ->
  FileName = Record#getnewfilelocation.file_logic_name,
  Storage_helper = "helper1",
  File_id = "real_location_of" ++ FileName,
  FileLocation = #file_location{storage_helper_id = Storage_helper, file_id = File_id},
  File = #file{type = 1, name = FileName, size = 0, parent = "", location = FileLocation},

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
    _BadStatus -> TmpAns
  end.

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
          {error, #filelocation{storage_helper = "Error: wrong dao answer for: " ++ LogMessage, file_id = "Error", validity = 0}}
      after 1000 ->
        lager:error([{mod, ?MODULE}], "Error: dao timeout for: " ++ LogMessage ++ ", file: ~s", [File]),
        {error, #filelocation{storage_helper = "Error: dao timeout for: " ++ LogMessage, file_id = "Error", validity = 0}}
      end;
    Other ->
      lager:error([{mod, ?MODULE}], "Error: dispatcher error for: " ++ LogMessage ++ ", file: ~s, error: ~p", [File, Other]),
      {error, #filelocation{storage_helper = "Error: dispatcher for: " ++ LogMessage, file_id = "Error", validity = 0}}
  end.