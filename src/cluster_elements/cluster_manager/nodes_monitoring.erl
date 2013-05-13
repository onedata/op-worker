%% Copyright
-module(nodes_monitoring).
-author("michal").
-include("registered_names.hrl").

%% API
-export([monitoring_loop/1, monitoring_loop/2, start_monitoring_loop/2]).



%% monitoring_loop/1
%% ====================================================================
%% @doc Loop that monitors if nodes are alive.
-spec monitoring_loop(Flag) -> ok when
  Flag :: on | off.
%% ====================================================================
start_monitoring_loop(Flag, Nodes) ->
  {ok, spawn_link(?MODULE, monitoring_loop, [Flag, Nodes])}.

monitoring_loop(Flag, Nodes) ->
  case Flag of
    on ->
      change_monitoring(Nodes, true);
    off -> ok
  end,
  monitoring_loop(on).

monitoring_loop(Flag) ->
  1 = 1,
  Pid = self(),
  lager:info([{mod, ?MODULE}], "aaa loop: ~s", [Pid]),
  lager:info([{mod, ?MODULE}], "monitoring_loop"),
  receive
    {nodedown, Node} ->
      lager:info([{mod, ?MODULE}], "aaa2 loop: ~s", [Pid]),
      case Flag of
        on ->
          erlang:monitor_node(Node, false),
          gen_server:cast({global, ?CCM}, {node_down, Node});
        off -> ok
      end,
      lager:info([{mod, ?MODULE}], "aaa3 loop: ~s", [Pid]),
      monitoring_loop(Flag);
    {monitor_node, Node} ->
      lager:info([{mod, ?MODULE}], "aaa4 loop: ~s", [Pid]),
      case Flag of
        on ->
          erlang:monitor_node(Node, true);
        off -> ok
      end,
      lager:info([{mod, ?MODULE}], "aaa5 loop: ~s", [Pid]),
      monitoring_loop(Flag);
    {off, Nodes} ->
      lager:info([{mod, ?MODULE}], "aaa6 loop: ~s", [Pid]),
      case Flag of
        on ->
          change_monitoring(Nodes, false);
        off -> ok
      end,
      lager:info([{mod, ?MODULE}], "aaa7 loop: ~s", [Pid]),
      monitoring_loop(off);
    {on, Nodes} ->
      lager:info([{mod, ?MODULE}], "aaa8 loop: ~s", [Pid]),
      case Flag of
        off ->
          change_monitoring(Nodes, true);
        on -> ok
      end,
      lager:info([{mod, ?MODULE}], "aaa9 loop: ~s", [Pid]),
      monitoring_loop(on);
    {get_version, Reply_Pid} ->
      lager:info([{mod, ?MODULE}], "aaa loop10: ~s", [Pid]),
      Reply_Pid ! {monitor_process_version, node_manager:check_vsn()},
      lager:info([{mod, ?MODULE}], "aaa loop11: ~s", [Pid]),
      monitoring_loop(Flag);
    switch_code ->
      lager:info([{mod, ?MODULE}], "aaa loop12: ~s", [Pid]),
      ?MODULE:monitoring_loop(Flag);
    exit ->
      lager:info([{mod, ?MODULE}], "aaa loop13: ~s", [Pid]),
      ok
  end.

%% change_monitoring/2
%% ====================================================================
%% @doc Starts or stops monitoring of nodes.
-spec change_monitoring(Nodes, Flag) -> ok when
  Nodes :: list(),
  Flag :: boolean().
%% ====================================================================
change_monitoring([], _Flag) ->
  ok;

change_monitoring([Node | Nodes], Flag) ->
  erlang:monitor_node(Node, Flag),
  change_monitoring(Nodes, Flag).