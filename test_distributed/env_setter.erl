%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module contains functions useful for distributed tests
%% e.g. function that sets environment before start, function that
%% synchronizes virtual machines etc.
%% @end
%% ===================================================================

-module(env_setter).

-include("registered_names.hrl").

-export([start_test/0, stop_test/0, start_app/1, stop_app/0, synch_nodes/1, synch_nodes/2]).

%% ====================================================================
%% API functions
%% ====================================================================

%% start_test/0
%% ====================================================================
%% @doc This function ets environment for application.
-spec start_test() -> ok.
%% ====================================================================

start_test() ->
  code:add_path("../../../ebin"),
  {ok, Dirs} = file:list_dir("../../../deps"),
  Deps = list_deps(Dirs, "../../../deps/", "/ebin", []),
  code:add_paths(Deps),
  lager:start(),
  ssl:start(),
  application:start(ranch),
  ok = application:load(?APP_Name).

%% stop_test/0
%% ====================================================================
%% @doc This function clears after the test.
-spec stop_test() -> ok.
%% ====================================================================

stop_test() ->
  application:stop(ranch),
  application:stop(lager),
  application:stop(ssl),
  application:stop(crypto),
  application:stop(public_key),
  ok = application:unload(?APP_Name).

%% start_app/1
%% ====================================================================
%% @doc This function starts the application ands sets environment
%% variables for it.
-spec start_app(Vars :: list()) -> ok.
%% ====================================================================

start_app(Vars) ->
  set_env_vars(Vars),
  ok = application:start(?APP_Name).

%% stop_app/0
%% ====================================================================
%% @doc This function stops the application.
-spec stop_app() -> ok.
%% ====================================================================

stop_app() ->
  ok = application:stop(?APP_Name).

%% synch_nodes/1
%% ====================================================================
%% @doc This function synchronizes nodes.
-spec synch_nodes(Nodes :: list()) -> NumberOfSynchronizedNodes when
  NumberOfSynchronizedNodes :: integer().
%% ====================================================================

synch_nodes(Nodes) ->
  synch_nodes(Nodes, true).

%% synch_nodes/2
%% ====================================================================
%% @doc This function synchronizes nodes.
-spec synch_nodes(Nodes :: list(), NodesStart :: boolean()) -> NumberOfSynchronizedNodes when
  NumberOfSynchronizedNodes :: integer().
%% ====================================================================

synch_nodes(Nodes, NodesStart) ->
  Pid = self(),
  global:re_register_name(node(), Pid),
  case NodesStart of
    true ->
      timer:sleep(1000 * length(Nodes)), %% time for other nodes start
      lists:foreach(fun(N) -> net_adm:ping(N) end, Nodes),
      timer:sleep(1000 * length(Nodes)); %% nodes need time to exchange information
    _Other -> ok
  end,
  lists:foreach(fun(W) -> global:send(W, init) end, Nodes),
  waitForAns(length(Nodes), init, 2000).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% list_deps/4
%% ====================================================================
%% @doc This function lists the directories with dependencies of application.
-spec list_deps(Dirs :: list(), Beg :: string(), End  :: string(), Ans :: list()) -> FinalAns when
  FinalAns :: list().
%% ====================================================================

list_deps([], _Beg, _End, Ans) ->
  Ans;

list_deps([D | Dirs], Beg, End, Ans) ->
  list_deps(Dirs, Beg, End, [Beg ++ D ++ End | Ans]).

%% set_env_vars/1
%% ====================================================================
%% @doc This function sets environment variables for application.
-spec set_env_vars(EnvVars :: list()) -> ok.
%% ====================================================================

set_env_vars([]) ->
  ok;

set_env_vars([{Variable, Value} | Vars]) ->
  application:set_env(?APP_Name, Variable, Value),
  set_env_vars(Vars).

%% waitForAns/3
%% ====================================================================
%% @doc This function waits for ProcNum messages
-spec waitForAns(ProcNum :: integer(), Message :: term(), Timeout :: integer()) -> NumberOfMessages when
  NumberOfMessages :: integer().
%% ====================================================================

waitForAns(ProcNum, Message, Timeout) ->
  waitForAns(ProcNum, 0, Message, Timeout).

%% waitForAns/4
%% ====================================================================
%% @doc This function waits for ProcNum messages
-spec waitForAns(ProcNum :: integer(), TmpAns :: integer(), Message :: term(), Timeout :: integer()) -> NumberOfMessages when
  NumberOfMessages :: integer().
%% ====================================================================

waitForAns(0, Ans, _Message, _Timeout) ->
  Ans;
waitForAns(ProcNum, Ans, Message, Timeout) ->
  receive
    Message -> waitForAns(ProcNum-1, Ans + 1, Message, Timeout)
  after
    Timeout -> Ans
  end.