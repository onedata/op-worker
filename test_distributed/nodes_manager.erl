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
-include("nodes_manager.hrl").
-include("modules_and_args.hrl").

-define(VIEW_REBUILDING_TIME, 2000).
-define(FUSE_SESSION_EXP_TIME, 8000).
-define(REQUEST_HANDLING_TIME, 1000).

%% ====================================================================
%% API
%% ====================================================================
-export([start_test_on_nodes/1, start_test_on_nodes/2, stop_nodes/1, start_test_on_local_node/0, start_app_on_nodes/2, stop_app_on_nodes/1, stop_test_on_local_nod/0, check_start_assertions/1]).
-export([start_test_on_nodes_with_dist_app/2, start_test_on_nodes_with_dist_app/3, start_node/2, stop_node/1]).
-export([start_deps/0, start_app/2, start_app_local/1, stop_deps/0, stop_app/1, stop_app_local/0, start_deps_for_tester_node/0, stop_deps_for_tester_node/0, get_db_node/0]).

%% Functions to use instead of timer
-export([wait_for_cluster_cast/0, wait_for_cluster_cast/1, wait_for_nodes_registration/1, wait_for_cluster_init/0, wait_for_cluster_init/1, wait_for_state_loading/0, wait_for_db_reaction/0, wait_for_fuse_session_exp/0, wait_for_request_handling/0]).

%% ====================================================================
%% API functions
%% ====================================================================

%% check_start_assertions/0
%% ====================================================================
%% @doc Checks if test was initialized properly.
-spec check_start_assertions(Config :: term()) -> ok.
%% ====================================================================
check_start_assertions(Config) ->
  Assertions = ?config(assertions, Config),
  lists:foreach(fun({Exp, Real}) -> ?assertEqual(Exp, Real) end, Assertions).

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

%% start_test_on_local_node/0
%% ====================================================================
%% @doc Starts dependencies needed when test is run locally on one node.
-spec start_test_on_local_node() -> Result when
  Result ::  ok | {error, Reason},
  Reason :: term().
%% ====================================================================
start_test_on_local_node() ->
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
      Nodes = create_nodes_description(Host, [], NodesNum),

      Params = case Verbose of
        true -> lists:map(fun(_) -> "" end, Nodes);
        _ -> lists:map(fun(_) -> " -noshell" end, Nodes)
      end,

      start_nodes(Nodes, Params);
    false -> []
  end.

%% start_test_on_nodes_with_dist_app/2
%% ====================================================================
%% @doc Starts nodes needed for test.
-spec start_test_on_nodes_with_dist_app(NodesNum :: integer(), CCMNum :: integer()) -> Result when
  Result ::  list().
%% ====================================================================
start_test_on_nodes_with_dist_app(NodesNum, CCMNum) ->
  start_test_on_nodes_with_dist_app(NodesNum, CCMNum, false).

%% start_test_on_nodes_with_dist_app/3
%% ====================================================================
%% @doc Starts nodes needed for test.
-spec start_test_on_nodes_with_dist_app(NodesNum :: integer(), CCMNum :: integer(), Verbose :: boolean()) -> Result when
  Result ::  list().
%% ====================================================================
start_test_on_nodes_with_dist_app(NodesNum, CCMNum, Verbose) ->
  set_deps(),

  case NodesNum > 0 of
    true ->
      Host = get_host(),
      Nodes = create_nodes_description(Host, [], NodesNum),

      DistNodes = create_dist_nodes_list(Nodes, CCMNum),
      DistAppDesc = create_dist_app_description(DistNodes),
      Params = create_nodes_params_for_dist_nodes(Nodes, DistNodes, DistAppDesc),

      Params2 = case Verbose of
                 true -> lists:map(fun(P) -> P end, Params);
                 _ -> lists:map(fun(P) -> " -noshell " ++ P end, Params)
               end,

      {start_nodes(Nodes, Params2), Params2};
    false -> []
  end.

%% start_app/2
%% ====================================================================
%% @doc Starts application (with arguments) on node.
-spec start_app(Node :: atom(), Args  :: list()) -> Result when
  Result ::  list().
%% ====================================================================
start_app(Node, Args) ->
  rpc:call(Node, nodes_manager, start_app_local, [Args]).

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
  App = start_app(Node, Arg),
  Ans = case (Deps =:= ok) and (App =:= ok) of
    true -> ok;
    false -> error
  end,
  [Ans | start_app_on_nodes(Nodes, Args)].

%% stop_app/1
%% ====================================================================
%% @doc Stops application on node.
-spec stop_app(Node :: atom()) -> Result when
  Result ::  list().
%% ====================================================================
stop_app(Node) ->
  rpc:call(Node, nodes_manager, stop_app_local, []).

%% stop_app_on_nodes/1
%% ====================================================================
%% @doc Stops application on nodes.
-spec stop_app_on_nodes(Nodes :: list()) -> Result when
  Result ::  list().
%% ====================================================================
stop_app_on_nodes([]) ->
  [];

stop_app_on_nodes([Node | Nodes]) ->
  App = stop_app(Node),
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
  stop_node(Node),
  stop_nodes(Nodes).

%% stop_test_on_local_nod/0
%% ====================================================================
%% @doc Stops dependencies needed when test is run locally on one node.
-spec stop_test_on_local_nod() -> Result when
  Result ::  ok | error.
%% ====================================================================
stop_test_on_local_nod() ->
  App = stop_app_local(),
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

  % Load configuration from sys.config so that lager starts with proper configuration
  application:load(lager),
  {ok, [Data]} = file:consult("sys.config"),
  Config = proplists:get_value(lager, Data),
  lists:foreach(
    fun(Key) -> 
      application:set_env(lager, Key, proplists:get_value(Key, Config)) 
    end, proplists:get_keys(Config)),
  lager:start(),


  ssl:start(),
  application:start(os_mon),
  application:start(ranch),
  application:start(nprocreg),
  application:start(cowboy),
  application:start(n2o),
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
  application:stop(n2o),
  application:stop(mimetypes),
  application:stop(ibrowse),
  application:unload(?APP_Name).

%% start_app_local/1
%% ====================================================================
%% @doc This function starts the application ands sets environment
%% variables for it.
-spec start_app_local(Vars :: list()) -> ok.
%% ====================================================================

start_app_local(Vars) ->
  set_env_vars([{nif_prefix, './'}, {ca_dir, './cacerts/'}] ++ Vars),
  application:stop(?APP_Name), %% Make sure that veil_cluster isn't running before starting new instance
  application:start(?APP_Name).

%% stop_app_local/0
%% ====================================================================
%% @doc This function stops the application.
-spec stop_app_local() -> ok.
%% ====================================================================

stop_app_local() ->
  application:stop(?APP_Name).

%% get_db_node/0
%% ====================================================================
%% @doc This function returns db node.
-spec get_db_node() -> atom().
%% ====================================================================

get_db_node() ->
  Node = atom_to_list(node()),
  [_, Host] = string:tokens(Node, "@"),
  list_to_atom("db@" ++ Host).

%% start_node/2
%% ====================================================================
%% @doc Starts node with params.
-spec start_node(Node :: atom(), Params :: string()) -> Result when
  Result ::  {Ans, Node},
  Ans :: term(),
  Node :: atom().
%% ====================================================================

start_node(Node, Params) ->
  NodeStr = atom_to_list(Node),
  [BegStr, HostStr] = string:tokens(NodeStr, "@"),
  Host = list_to_atom(HostStr),
  NodeName = list_to_atom(BegStr),

  start_node(NodeName, Host, Params).

%% stop_node/1
%% ====================================================================
%% @doc Stops node.
-spec stop_node(Node :: atom()) -> ok.
%% ====================================================================

stop_node(Node) ->
  slave:stop(Node).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% start_node/3
%% ====================================================================
%% @doc Starts node with params.
-spec start_node(NodeName :: atom(), Host :: atom(), Params :: string()) -> Result when
  Result ::  {Ans, Node},
  Ans :: term(),
  Node :: atom().
%% ====================================================================
start_node(NodeName, Host, Params) ->
  slave:start(Host, NodeName, make_code_path() ++ " -setcookie \"" ++ atom_to_list(erlang:get_cookie()) ++ "\"" ++ Params).

%% start_nodes/2
%% ====================================================================
%% @doc Starts nodes needed for test.
-spec start_nodes(NodesNames:: list(), AdditionalParams :: string()) -> Result when
  Result ::  list().
%% ====================================================================

start_nodes([], _AdditionalParams) ->
  [];

start_nodes([{NodeName, Host} | Nodes], [Param | AdditionalParams]) ->
  Pid = self(),
  spawn(fun() ->
    {TmpAns, Node} = start_node(NodeName, Host, Param),
    Pid ! {NodeName, TmpAns, Node}
  end),
  OtherNodesAns = start_nodes(Nodes, AdditionalParams),
  Node3 = receive
    {NodeName, TmpAns2, Node2} ->
      case TmpAns2 of
        ok -> Node2;
        _ -> error
      end
  after 5000 ->
    error
  end,
  [Node3 | OtherNodesAns].

%% create_nodes_description/3
%% ====================================================================
%% @doc Creates description of nodes needed for test.
-spec create_nodes_description(Host:: atom(), TmpAns :: list(), Counter :: integer()) -> Result when
  Result ::  list().
%% ====================================================================
create_nodes_description(_Host, Ans, 0) ->
  Ans;

create_nodes_description(Host, Ans, Counter) ->
  Desc = {list_to_atom("slave" ++ integer_to_list(Counter)), Host},
  create_nodes_description(Host, [Desc | Ans], Counter - 1).

%% create_dist_nodes_list/2
%% ====================================================================
%% @doc Creates list of nodes for distributed application
-spec create_dist_nodes_list(Nodes:: list(), DistNodesNum :: integer()) -> Result when
  Result ::  list().
%% ====================================================================

create_dist_nodes_list(_, 0) ->
  [];

create_dist_nodes_list([{NodeName, Host} | Nodes], DistNodesNum) ->
  Node = "'" ++ atom_to_list(NodeName) ++ "@" ++ atom_to_list(Host) ++ "'",
  [Node | create_dist_nodes_list(Nodes, DistNodesNum - 1)].

%% create_dist_app_description/1
%% ====================================================================
%% @doc Creates description of distributed application
-spec create_dist_app_description(DistNodes:: list()) -> Result when
  Result ::  string().
%% ====================================================================

create_dist_app_description(DistNodes) ->
  [Main | Rest] = DistNodes,
  RestString = lists:foldl(fun(N, TmpAns) ->
    case TmpAns of
      "" -> N;
      _ -> TmpAns ++ ", " ++ N
    end
  end, "", Rest),
  "\"[{veil_cluster_node, 1000, [" ++ Main ++ ", {" ++ RestString ++ "}]}]\"".

%% create_nodes_params_for_dist_nodes/3
%% ====================================================================
%% @doc Creates list of nodes for distributed application
-spec create_nodes_params_for_dist_nodes(Nodes:: list(), DistNodes :: list(), DistAppDescription :: string()) -> Result when
  Result ::  list().
%% ====================================================================

create_nodes_params_for_dist_nodes([], _DistNodes, _DistAppDescription) ->
  [];

create_nodes_params_for_dist_nodes([{NodeName, Host}  | Nodes], DistNodes, DistAppDescription) ->
  Node = "'" ++ atom_to_list(NodeName) ++ "@" ++ atom_to_list(Host) ++ "'",
  case lists:member(Node, DistNodes) of
    true ->
      SynchNodes = lists:delete(Node, DistNodes),
      SynchNodesString = lists:foldl(fun(N, TmpAns) ->
        case TmpAns of
          "" -> N;
          _ -> TmpAns ++ ", " ++ N
        end
      end, "", SynchNodes),
      Param = " -kernel distributed " ++ DistAppDescription ++ " -kernel sync_nodes_mandatory \"[" ++ SynchNodesString ++ "]\" -kernel sync_nodes_timeout 30000 ",
      [Param | create_nodes_params_for_dist_nodes(Nodes, DistNodes, DistAppDescription)];
    false -> ["" | create_nodes_params_for_dist_nodes(Nodes, DistNodes, DistAppDescription)]
  end.

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
  code:add_path("../ebin"),
  {ok, Dirs} = file:list_dir("../deps"),
  Deps = list_deps(Dirs, "../deps/", "/ebin", []),
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

%% wait_for_cluster_cast/0
%% ====================================================================
%% @doc Wait until cluster processes last cast.
%% @end
-spec wait_for_cluster_cast() -> ok | no_return().
%% ====================================================================
wait_for_cluster_cast() ->
  wait_for_cluster_cast({global, ?CCM}).

%% wait_for_cluster_cast/1
%% ====================================================================
%% @doc Wait until cluster processes last cast.
%% @end
-spec wait_for_cluster_cast(GenServ :: term()) -> ok | no_return().
%% ====================================================================
wait_for_cluster_cast(GenServ) ->
  timer:sleep(100),
  Ans = try
    gen_server:call(GenServ, check, 10000)
  catch
    E1:E2 ->
      {exception, E1, E2}
  end,
  ?assertEqual(ok, Ans).

%% wait_for_nodes_registration/1
%% ====================================================================
%% @doc Wait until all nodes will be registered.
%% @end
-spec wait_for_nodes_registration(NodesNum :: integer()) -> ok | no_return().
%% ====================================================================
wait_for_nodes_registration(NodesNum) ->
  wait_for_nodes_registration(NodesNum, 20).

%% wait_for_nodes_registration/2
%% ====================================================================
%% @doc Wait until all nodes will be registered.
%% @end
-spec wait_for_nodes_registration(NodesNum :: integer(), TriesNum :: integer()) -> ok | no_return().
%% ====================================================================
wait_for_nodes_registration(NodesNum, 0) ->
  ?assertEqual(NodesNum, check_nodes()),
  ok;

wait_for_nodes_registration(NodesNum, TriesNum) ->
  case check_nodes() of
    NodesNum -> ok;
    _ ->
      timer:sleep(500),
      wait_for_nodes_registration(NodesNum, TriesNum - 1)
  end.

%% check_nodes/0
%% ====================================================================
%% @doc Get number of registered nodes.
%% @end
-spec check_nodes() -> Ans when
  Ans :: integer() | {exception, E1, E2},
  E1 :: term(),
  E2 :: term().
%% ====================================================================
check_nodes() ->
  try
    length(gen_server:call({global, ?CCM}, get_nodes, 1000))
  catch
    E1:E2 ->
      {exception, E1, E2}
  end.

%% check_init/1
%% ====================================================================
%% @doc Check if cluster is initialized properly.
%% @end
-spec check_init(ModulesNum :: integer()) -> Ans when
  Ans :: boolean() | {exception, E1, E2},
  E1 :: term(),
  E2 :: term().
%% ====================================================================
check_init(ModulesNum) ->
  try
    {WList, StateNum} = gen_server:call({global, ?CCM}, get_workers, 1000),
    case length(WList) >= ModulesNum of
      true ->
        timer:sleep(500),
        Nodes = gen_server:call({global, ?CCM}, get_nodes, 1000),
        {_, CStateNum} = gen_server:call({global, ?CCM}, get_callbacks, 1000),
        CheckNode = fun(Node, TmpAns) ->
          StateNum2 = gen_server:call({?Dispatcher_Name, Node}, get_state_num, 1000),
          {_, CStateNum2} = gen_server:call({?Dispatcher_Name, Node}, get_callbacks, 1000),
          case (StateNum == StateNum2) and (CStateNum == CStateNum2) of
            true -> TmpAns;
            false -> false
          end
        end,
        lists:foldl(CheckNode, true, Nodes);
      false ->
        false
    end
  catch
    E1:E2 ->
      {exception, E1, E2}
  end.

%% wait_for_cluster_init/0
%% ====================================================================
%% @doc Wait until cluster is initialized properly.
%% @end
-spec wait_for_cluster_init() -> Ans when
  Ans :: boolean() | {exception, E1, E2},
  E1 :: term(),
  E2 :: term().
%% ====================================================================
wait_for_cluster_init() ->
  wait_for_cluster_init(0).

%% wait_for_cluster_init/1
%% ====================================================================
%% @doc Wait until cluster is initialized properly.
%% @end
-spec wait_for_cluster_init(ModulesNum :: integer()) -> Ans when
  Ans :: boolean() | {exception, E1, E2},
  E1 :: term(),
  E2 :: term().
%% ====================================================================
wait_for_cluster_init(ModulesNum) ->
  wait_for_cluster_init(ModulesNum + length(?Modules_With_Args), 20).

%% wait_for_cluster_init/2
%% ====================================================================
%% @doc Wait until cluster is initialized properly.
%% @end
-spec wait_for_cluster_init(ModulesNum :: integer(), TriesNum :: integer()) -> Ans when
  Ans :: boolean() | {exception, E1, E2},
  E1 :: term(),
  E2 :: term().
%% ====================================================================
wait_for_cluster_init(ModulesNum, 0) ->
%%   ?assert(check_init(ModulesNum))
  check_init(ModulesNum);

wait_for_cluster_init(ModulesNum, TriesNum) ->
  case check_init(ModulesNum) of
    true -> true;
    _ ->
      timer:sleep(500),
      wait_for_cluster_init(ModulesNum, TriesNum - 1)
  end.

%% wait_for_db_reaction/0
%% ====================================================================
%% @doc Give DB time for processing request.
%% @end
-spec wait_for_db_reaction() -> ok.
%% ====================================================================
wait_for_db_reaction() ->
  timer:sleep(?VIEW_REBUILDING_TIME).

%% wait_for_fuse_session_exp/0
%% ====================================================================
%% @doc Give FUSE session time to expire.
%% @end
-spec wait_for_fuse_session_exp() -> ok.
%% ====================================================================
wait_for_fuse_session_exp() ->
  timer:sleep(?FUSE_SESSION_EXP_TIME).

%% wait_for_request_handling/0
%% ====================================================================
%% @doc Give cluster time for request handling.
%% @end
-spec wait_for_request_handling() -> ok.
%% ====================================================================
wait_for_request_handling() ->
  timer:sleep(?REQUEST_HANDLING_TIME).

%% check_state_loading/0
%% ====================================================================
%% @doc Check if state is loaded from DB.
%% @end
-spec check_state_loading() -> Ans when
  Ans :: boolean() | {exception, E1, E2},
  E1 :: term(),
  E2 :: term().
%% ====================================================================
check_state_loading() ->
  try
    gen_server:call({global, ?CCM}, check_state_loaded, 1000)
  catch
    E1:E2 ->
      {exception, E1, E2}
  end.

%% wait_for_state_loading/0
%% ====================================================================
%% @doc Wait until state is loaded from DB.
%% @end
-spec wait_for_state_loading() -> ok | no_return().
%% ====================================================================
wait_for_state_loading() ->
  wait_for_state_loading(20).

%% wait_for_state_loading/1
%% ====================================================================
%% @doc Wait until state is loaded from DB.
%% @end
-spec wait_for_state_loading(TriesNum :: integer()) -> ok | no_return().
%% ====================================================================
wait_for_state_loading(0) ->
  ?assert(check_state_loading());

wait_for_state_loading(TriesNum) ->
  case check_state_loading() of
    true -> true;
    _ ->
      timer:sleep(500),
      wait_for_state_loading(TriesNum - 1)
  end.