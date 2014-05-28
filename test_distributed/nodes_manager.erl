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

%% Uncomment for verbosity
%% -define(verbose, 1).

%% ====================================================================
%% API
%% ====================================================================
-export([check_start_assertions/1]).
-export([start_test_on_nodes_with_dist_app/2, start_test_on_nodes_with_dist_app/3, start_node/2, stop_node/1]).

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
-endif.

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
  after 30000 ->
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
  ?assert(check_init(ModulesNum));

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