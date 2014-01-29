%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour to provide
%% functionality of rule engines manager (update of rules in all types
%% of rule engines).
%% @end
%% ===================================================================

-module(rule_manager).
-behaviour(worker_plugin_behaviour).

-include("registered_names.hrl").
-include_lib("veil_modules/cluster_rengine/cluster_rengine.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

-define(RULE_MANAGER_ETS, rule_manager).

init(_Args) ->
  ets:new(rule_manager, [named_table, public, set, {read_concurrency, true}]),
  ets:new(rule_manager_id, [named_table, set]),
  ets:insert(rule_manager_id, {current_id, 1}),

  FunctionOnSave = fun(_Event) ->
    function_on_save
  end,

  ets:insert(?RULE_MANAGER_ETS, {save_event, [FunctionOnSave]}),
	[].

handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(_ProtocolVersion, get_event_handlers) ->
  %% for that moment it does not make sense to copy values from one ets to another, but it's just mock implementation
  %% end the only purpose is to keep rule_manager and cluster_rengine separate
  %% in real implementation event handlers for rule_manager will be stored in db

  ets:match(?RULE_MANAGER_ETS, {'$1', '$2'});

handle(_ProtocolVersion, {add_event_handler, {Event, EventHandlerMapFun, EventHandlerDispMapFun, EventHandler}}) ->
  NewEventItem = {tree, generate_tree_name(), EventHandlerMapFun, EventHandlerDispMapFun, EventHandler},
  case ets:lookup(?RULE_MANAGER_ETS, Event) of
    [] -> ets:insert(?RULE_MANAGER_ETS, {Event, [NewEventItem]});
    EventHandlers -> ets:insert(?CLUSTER_RENGINE, {Event, [NewEventItem | EventHandlers]})
  end,
  gen_server:cast({global, ?CCM}, {cluster_regine, 1, {clear_cache, Event}});
  %%worker_host:clear_cache({?EVENT_HANDLERS_CACHE, Event});

handle(_ProtocolVersion, {get_event_handlers, Event}) ->
  ets:lookup(?RULE_MANAGER_ETS, Event);

handle(_ProtocolVersion, _Msg) ->
  ok.

cleanup() ->
	ok.

%% Helper functions

generate_tree_name() ->
  Id = ets:lookup(rule_manager_id, current_id),
  ets:insert(rule_manager_id, {current_id, Id + 1}),
  list_to_atom("event_" ++ integer_to_list(Id)).