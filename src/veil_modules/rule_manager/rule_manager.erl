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
-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/dao_helper.hrl").

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
  ?info("some pong"),
  pong;

handle(_ProtocolVersion, healthcheck) ->
	ok;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(_ProtocolVersion, get_event_handlers) ->
  %% for that moment it does not make sense to copy values from one ets to another, but it's just mock implementation
  %% end the only purpose is to keep rule_manager and cluster_rengine separate
  %% in real implementation event handlers for rule_manager will be stored in db

  ets:match(?RULE_MANAGER_ETS, {'$1', '$2'});

handle(_ProtocolVersion, {add_event_handler, {EventType, EventHandlerItem}}) ->
  ?info("entering add_event_handler"),
  NewEventItem = case EventHandlerItem#event_handler_item.processing_method of
                   tree -> EventHandlerItem#event_handler_item{tree_id = generate_tree_name()};
                   _ -> EventHandlerItem
                 end,

  case ets:lookup(?RULE_MANAGER_ETS, EventType) of
    [] -> ets:insert(?RULE_MANAGER_ETS, {EventType, [NewEventItem]});
    [{_EventType, EventHandlers}] -> ets:insert(?RULE_MANAGER_ETS, {EventType, [NewEventItem | EventHandlers]})
  end,
%%   gen_server:call(?Dispatcher_Name, {cluster_regine, 1, {clear_cache, EventType}}),

  worker_host:clear_cache({?EVENT_TREES_MAPPING, EventType}),
  notify_producers(),

  ?info("New handler for event ~p registered.", [EventType]),
  ok;

handle(_ProtocolVersion, {get_event_handlers, EventType}) ->
  ?info("get_event_handlers for event"),
  EventHandlerItems = ets:lookup(?RULE_MANAGER_ETS, EventType),
  Res = case EventHandlerItems of
          [{_EventType, ItemsList}] -> ItemsList;
          _ -> []
        end,
  {ok, Res};

handle(_ProtocolVersion, _Msg) ->
  ok.

cleanup() ->
	ok.

%% Helper functions

notify_producers() ->
  Rows = fetch_rows(?FUSE_CONNECTIONS_VIEW, #view_query_args{}),
  FuseIds = lists:map(fun(#view_row{key = FuseId}) -> FuseId end, Rows),
  UniqueFuseIds = sets:to_list(sets:from_list(FuseIds)),

  ?info("Notify producers: ~p", [UniqueFuseIds]),
  lists:foreach(fun(FuseId) -> request_dispatcher:send_to_fuse(FuseId, {testchannelanswer, "test"}, "fuse_messages") end, UniqueFuseIds).

generate_tree_name() ->
  [{_, Id}] = ets:lookup(?HANDLER_TREE_ID_ETS, current_id),
  ets:insert(?HANDLER_TREE_ID_ETS, {current_id, Id + 1}),
  list_to_atom("event_" ++ integer_to_list(Id)).

fetch_rows(ViewName, QueryArgs) ->
  case dao:list_records(ViewName, QueryArgs) of
    {ok, #view_result{rows = Rows}} ->
      Rows;
    Error ->
      ?error("Invalid view response: ~p", [Error]),
      throw(invalid_data)
  end.
