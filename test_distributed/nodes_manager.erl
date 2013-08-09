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

-module(nodes_manager).

-include("registered_names.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([start_test_on_nodes/1, start_test_on_nodes/2, stop_nodes/1, start_local_test/0, start_app_on_nodes/2, stop_app_on_nodes/1, stop_local_test/0]).
-export([start_deps/0, start_app/1, stop_deps/0, stop_app/0, start_deps_for_tester_node/0, stop_deps_for_tester_node/0]).

%% ====================================================================
%% API functions
%% ====================================================================

%% start_deps_for_tester_node/0
%% ====================================================================
%% @doc Starts dependencies needed by tester node (node that does not
%% host application but coordinates test).
-spec start_deps_for_tester_node() -> Result when
  Result ::  ok | {error, Reason},
  Reason :: term().
%% ====================================================================
start_deps_for_tester_node() ->
  %% SASL reboot/start in order to disable TTY logging
  %% Normally `error_logger:tty(false)` should be enough, but some apps could start SASL anyway without our boot options
  application:stop(sasl),
  application:unload(sasl),
  application:load(sasl),
  application:set_env(sasl, sasl_error_logger, false),
  application:start(sasl),
  error_logger:tty(false),

  %% Start all deps
  ssl:start().

%% stop_deps_for_tester_node/0
%% ====================================================================
%% @doc Stops dependencies needed by tester node (node that does not
%% host application but coordinates test).
-spec stop_deps_for_tester_node() -> Result when
  Result ::  ok | {error, Reason},
  Reason :: term().
%% ====================================================================
stop_deps_for_tester_node() ->
  application:stop(ssl),
  application:stop(crypto),
  application:stop(public_key).

%% start_local_test/0
%% ====================================================================
%% @doc Starts dependencies needed when test is run locally on one node.
-spec start_local_test() -> Result when
  Result ::  ok | {error, Reason},
  Reason :: term().
%% ====================================================================
start_local_test() ->
  set_deps(),
  start_deps().

%% start_test_on_nodes/1
%% ====================================================================
%% @doc Starts nodes needed for test.
-spec start_test_on_nodes(NodesNum :: integer()) -> Result when
  Result ::  list().
%% ====================================================================
start_test_on_nodes(NodesNum) ->
  start_test_on_nodes(NodesNum, false).

%% start_test_on_nodes/2
%% ====================================================================
%% @doc Starts nodes needed for test.
-spec start_test_on_nodes(NodesNum :: integer(), Verbose :: boolean()) -> Result when
  Result ::  list().
%% ====================================================================
start_test_on_nodes(NodesNum, Verbose) ->
  set_deps(),

  case NodesNum > 0 of
    true ->
      Host = get_host(),
      Nodes = create_nodes_description(Host, NodesNum),
      start_nodes(Nodes, [], Verbose);
    false -> []
  end.

%% start_app_on_nodes/2
%% ====================================================================
%% @doc Starts application (with arguments) on nodes.
-spec start_app_on_nodes(Nodes :: list(), Args  :: list()) -> Result when
  Result ::  list().
%% ====================================================================
start_app_on_nodes([], _Args) ->
  [];

start_app_on_nodes([Node | Nodes], [Arg | Args]) ->
  Deps = rpc:call(Node, nodes_manager, start_deps, []),
  App = rpc:call(Node, nodes_manager, start_app, [Arg]),
  Ans = case (Deps =:= ok) and (App =:= ok) of
    true -> ok;
    false -> error
  end,
  [Ans | start_app_on_nodes(Nodes, Args)].

%% stop_app_on_nodes/1
%% ====================================================================
%% @doc Stops application on nodes.
-spec stop_app_on_nodes(Nodes :: list()) -> Result when
  Result ::  list().
%% ====================================================================
stop_app_on_nodes([]) ->
  [];

stop_app_on_nodes([Node | Nodes]) ->
  App = rpc:call(Node, nodes_manager, stop_app, []),
  Deps = rpc:call(Node, nodes_manager, stop_deps, []),
  Ans = case (Deps =:= ok) and (App =:= ok) of
    true -> ok;
    false -> error
  end,
  [Ans | stop_app_on_nodes(Nodes)].

%% stop_nodes/1
%% ====================================================================
%% @doc Stops nodes.
-spec stop_nodes(Nodes :: list()) -> ok.
%% ====================================================================
stop_nodes([]) ->
  ok;

stop_nodes([Node | Nodes]) ->
  slave:stop(Node),
  stop_nodes(Nodes).

%% stop_local_test/0
%% ====================================================================
%% @doc Stops dependencies needed when test is run locally on one node.
-spec stop_local_test() -> Result when
  Result ::  ok | error.
%% ====================================================================
stop_local_test() ->
  App = stop_app(),
  Deps = stop_deps(),
  case (Deps =:= ok) and (App =:= ok) of
    true -> ok;
    false -> error
  end.

%% start_deps/0
%% ====================================================================
%% @doc This function sets environment for application.
-spec start_deps() -> ok.
%% ====================================================================
start_deps() ->
  stop_deps(), %% Stop all applications (if any)

  application:start(sasl),
  lager:start(),
  ssl:start(),
  application:start(os_mon),
  application:start(ranch),
  application:start(nprocreg),
  application:start(cowboy),
  application:start(nitrogen_core),
  application:start(simple_bridge),
  application:start(mimetypes),
  application:start(ibrowse),
  application:load(?APP_Name).

%% stop_deps/0
%% ====================================================================
%% @doc This function clears after the test.
-spec stop_deps() -> ok.
%% ====================================================================

stop_deps() ->
  application:stop(ranch),
  application:stop(os_mon),
  application:stop(ssl),
  application:stop(crypto),
  application:stop(public_key),
  application:stop(nprocreg),
  application:stop(cowboy),
  application:stop(lager),
  application:stop(sasl),
  application:stop(nitrogen_core),
  application:stop(mimetypes),
  application:stop(simple_bridge),
  application:stop(ibrowse),
  try
    stop_app()
  catch
    _:_ -> ok
  end,
  application:unload(?APP_Name).

%% start_app/1
%% ====================================================================
%% @doc This function starts the application ands sets environment
%% variables for it.
-spec start_app(Vars :: list()) -> ok.
%% ====================================================================

start_app(Vars) ->
  set_env_vars([{nif_prefix, './'}, {ca_dir, './cacerts/'}] ++ Vars),
  application:stop(?APP_Name), %% Make sure that veil_cluster isn't running before starting new instance
  application:start(?APP_Name).

%% stop_app/0
%% ====================================================================
%% @doc This function stops the application.
-spec stop_app() -> ok.
%% ====================================================================

stop_app() ->
  application:stop(?APP_Name).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% start_nodes/3
%% ====================================================================
%% @doc Starts nodes needed for test.
-spec start_nodes(NodesNames:: list(), TmpAns :: list(), Verbose :: boolean()) -> Result when
  Result ::  list().
%% ====================================================================
start_nodes([], Ans, _Verbose) ->
  Ans;

start_nodes([{NodeName, Host} | Nodes], Ans, Verbose) ->
  {TmpAns, Node} = case Verbose of
    true -> slave:start(Host, NodeName, make_code_path() ++ " -setcookie \"" ++ atom_to_list(erlang:get_cookie()) ++ "\"");
    _ -> slave:start(Host, NodeName, make_code_path() ++ " -noshell -setcookie \"" ++ atom_to_list(erlang:get_cookie()) ++ "\"")
  end,
  case TmpAns of
    ok -> start_nodes(Nodes, [Node | Ans], Verbose);
    _ -> start_nodes(Nodes, [error | Ans], Verbose)
  end.

%% create_nodes_description/2
%% ====================================================================
%% @doc Creates description of nodes needed for test.
-spec create_nodes_description(Host:: atom(), Counter :: integer()) -> Result when
  Result ::  list().
%% ====================================================================
create_nodes_description(_Host, 0) ->
  [];

create_nodes_description(Host, Counter) ->
  Desc = {list_to_atom("slave" ++ integer_to_list(Counter)), Host},
  [Desc | create_nodes_description(Host, Counter - 1)].

%% make_code_path/0
%% ====================================================================
%% @doc Returns host name.
%% @end
-spec get_host() -> atom().
%% ====================================================================
get_host() ->
  Node = atom_to_list(node()),
  [_, Host] = string:tokens(Node, "@"),
  list_to_atom(Host).

%% make_code_path/0
%% ====================================================================
%% @doc Returns current code path string, formatted as erlang slave node argument.
%% @end
-spec make_code_path() -> string().
%% ====================================================================
make_code_path() ->
  lists:foldl(fun(Node, Path) -> " -pa " ++ Node ++ Path end,
    [], code:get_path()).

%% set_deps/0
%% ====================================================================
%% @doc Sets paths to dependencies.
%% @end
-spec set_deps() -> Result when
  Result :: true | {error, What},
  What :: bad_directory | bad_path.
%% ====================================================================
set_deps() ->
  timer:sleep(1000),
  code:add_path("./ebin"),
  {ok, Dirs} = file:list_dir("./deps"),
  Deps = list_deps(Dirs, "./deps/", "/ebin", []),
  code:add_paths(Deps).

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