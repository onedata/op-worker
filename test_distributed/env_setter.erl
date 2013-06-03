-module(env_setter).

-include("registered_names.hrl").

-export([start_test/0, stop_test/0, start_app/1, stop_app/0]).

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
  ok = application:stop(ranch).

start_app(Vars) ->
  set_env_vars(Vars),
  ok = application:start(?APP_Name).

stop_app() ->
  ok = application:stop(?APP_Name).

list_deps([], _Beg, _End, Ans) ->
  Ans;

list_deps([D | Dirs], Beg, End, Ans) ->
  list_deps(Dirs, Beg, End, [Beg ++ D ++ End | Ans]).

set_env_vars([]) ->
  ok;

set_env_vars([{Variable, Value} | Vars]) ->
  application:set_env(?APP_Name, Variable, Value),
  set_env_vars(Vars).
