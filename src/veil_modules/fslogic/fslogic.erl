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

%% dodać logowanie błędów laggerem i zwracanie tylko uproszczonej wersji
handle(ProtocolVersion, create_file) ->
  FileLocation = #file_location{storage_helper_id = "helper1", file_id = "abcd"},
  File = #file{type = 1, name = "ab", size = 0, parent = "", location = FileLocation},

  Pid = self(),
  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 1, {vfs, save_file, [File]}}),
  case Ans of
    ok ->
      receive
        {worker_answer, 1, {ok, _UUID}} -> file_creation_ok;
        Ans2 -> {file_creation, wrong_dao_ans, Ans2}
      after 1000 ->
        timeout
      end;
    Other -> {file_creation, dispatcher_error, Other}
  end;

handle(ProtocolVersion, get_file) ->
  Pid = self(),
  Ans = gen_server:call(?Dispatcher_Name, {dao, ProtocolVersion, Pid, 1, {vfs, get_file, [{absolute_path, "/ab"}]}}),
  case Ans of
    ok ->
      receive
        {worker_answer, 1, {ok, FileDoc}} -> {ok, FileDoc};
        Ans2 -> {get_file, wrong_dao_ans, Ans2}
      after 1000 ->
        timeout
      end;
    Other -> {get_file, dispatcher_error, Other}
  end;

handle(_ProtocolVersion, _Msg) ->
	ok.

cleanup() ->
	ok.
