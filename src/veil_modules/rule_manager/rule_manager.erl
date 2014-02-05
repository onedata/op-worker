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

-include("logging.hrl").
-include("registered_names.hrl").
-include_lib("veil_modules/cluster_rengine/cluster_rengine.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

-define(RULE_MANAGER_ETS, rule_manager).
-define(HANDLER_TREE_ID_ETS, handler_tree_id_ets).

init(_Args) ->
  ets:new(?RULE_MANAGER_ETS, [named_table, public, set, {read_concurrency, true}]),
  ets:new(?HANDLER_TREE_ID_ETS, [named_table, public, set]),
  ets:insert(?HANDLER_TREE_ID_ETS, {current_id, 1}),

  FunctionOnSave = fun(_Event) ->
    function_on_save
  end,

  ets:insert(?RULE_MANAGER_ETS, {save_event, [FunctionOnSave]}),
	[].

handle(_ProtocolVersion, ping) ->
  ?info("in the rulemanager ping"),
  pong;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(_ProtocolVersion, get_event_handlers) ->
  %% for that moment it does not make sense to copy values from one ets to another, but it's just mock implementation
  %% end the only purpose is to keep rule_manager and cluster_rengine separate
  %% in real implementation event handlers for rule_manager will be stored in db

  ets:match(?RULE_MANAGER_ETS, {'$1', '$2'});

handle(_ProtocolVersion, {add_event_handler, {EventType, Item}}) ->
  ?debug("add_event_handler new"),

  NewEventItem = case Item#event_handler_item.processing_method of
                   tree -> Item#event_handler_item{tree_id = generate_tree_name()};
                   _ -> Item
                 end,

  case ets:lookup(?RULE_MANAGER_ETS, EventType) of
    [] -> ets:insert(?RULE_MANAGER_ETS, {EventType, [NewEventItem]});
    EventHandlers -> ets:insert(?RULE_MANAGER_ETS, {EventType, [NewEventItem | EventHandlers]})
  end,
  gen_server:call(?Dispatcher_Name, {cluster_regine, 1, {clear_cache, EventType}}),

  ?debug("add_event_handler finished");
  %%worker_host:clear_cache({?EVENT_HANDLERS_CACHE, Event});

handle(_ProtocolVersion, {get_event_handlers, Event}) ->
  ?info("eventhandlers: ~p~n", [ets:lookup(?RULE_MANAGER_ETS, Event)]),

  EventHandlerItems = ets:lookup(?RULE_MANAGER_ETS, Event),
  Res = case EventHandlerItems of
          [{_EventType, ItemsList}] -> ItemsList;
          _ -> []
        end,
  {ok, Res};

handle(_ProtocolVersion, _Msg) ->
  ?debug("rule_manager default handler"),
  ok.

cleanup() ->
	ok.

%% Helper functions

generate_tree_name() ->
  [{_, Id}] = ets:lookup(?HANDLER_TREE_ID_ETS, current_id),
  ?info("generarte_tree_name: ~p~n", [Id]),
  ets:insert(?HANDLER_TREE_ID_ETS, {current_id, Id + 1}),
  list_to_atom("event_" ++ integer_to_list(Id)).