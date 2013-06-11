-module(env_setter).

-include("registered_names.hrl").

-export([start_test/0, stop_test/0, start_app/1, stop_app/0, synch_nodes/1, synch_nodes/2]).

start_test() ->
  code:add_path("../../../ebin"),
  {ok, Dirs} = file:list_dir("../../../deps"),
  Deps = list_deps(Dirs, "../../../deps/", "/ebin", []),
  code:add_paths(Deps),
  lager:start(),
  ssl:start(),
  ok = application:start(ranch),
  ok = application:load(?APP_Name).

stop_test() ->
  ok = application:stop(ranch),
  ok = application:unload(?APP_Name).

start_app(Vars) ->
  set_env_vars(Vars),
  ok = application:start(?APP_Name).

stop_app() ->
  ok = application:stop(?APP_Name).

synch_nodes(Nodes) ->
  synch_nodes(Nodes, true).

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

list_deps([], _Beg, _End, Ans) ->
  Ans;

list_deps([D | Dirs], Beg, End, Ans) ->
  list_deps(Dirs, Beg, End, [Beg ++ D ++ End | Ans]).

set_env_vars([]) ->
  ok;

set_env_vars([{Variable, Value} | Vars]) ->
  application:set_env(?APP_Name, Variable, Value),
  set_env_vars(Vars).

waitForAns(ProcNum, Message, Timeout) ->
  waitForAns(ProcNum, 0, Message, Timeout).
waitForAns(0, Ans, _Message, _Timeout) ->
  Ans;
waitForAns(ProcNum, Ans, Message, Timeout) ->
  receive
    Message -> waitForAns(ProcNum-1, Ans + 1, Message, Timeout)
  after
    Timeout -> Ans
  end.