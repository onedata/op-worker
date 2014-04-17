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
-include("veil_modules/dao/dao_helper.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/cluster_rengine/cluster_rengine.hrl").

-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

%% name of ets that store event_handler_item records registered in rule_manager as values and event types as keys
-define(RULE_MANAGER_ETS, rule_manager).

%% name of ets that store eventstreamconfig records (from fuse_messages_pb)
%% TODO: consider using worker_host state instead
-define(PRODUCERS_RULES_ETS, producers_rules).

%% name of ets for tree id generation
%% TODO: consider using worker_host state instead
-define(HANDLER_TREE_ID_ETS, handler_tree_id_ets).

init(_Args) ->
  ets:new(?RULE_MANAGER_ETS, [named_table, public, set, {read_concurrency, true}]),
  ets:new(?PRODUCERS_RULES_ETS, [named_table, public, set, {read_concurrency, true}]),
  ets:new(?HANDLER_TREE_ID_ETS, [named_table, public, set]),
  ets:insert(?HANDLER_TREE_ID_ETS, {current_id, 1}),
	[].

handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, healthcheck) ->
	ok;

%% handler called by veilclient on intitialization - returns config for veilclient
handle(_ProtocolVersion, event_producer_config_request) ->
  Configs = case ets:lookup(?PRODUCERS_RULES_ETS, producer_configs) of
              [{_Key, ProducerConfigs}] -> ProducerConfigs;
              _ -> []
            end,
  #eventproducerconfig{event_streams_configs = Configs};

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(_ProtocolVersion, get_event_handlers) ->
  MatchingItems = ets:match(?RULE_MANAGER_ETS, {'$1', '$2'}),

  %% ets:match returns list of lists - convert it to list of tuples
  Res = lists:map(fun(Item) -> list_to_tuple(Item) end, MatchingItems),
  {ok, Res};

handle(_ProtocolVersion, {add_event_handler, {EventType, EventHandlerItem}}) ->
  handle(_ProtocolVersion, {add_event_handler, {EventType, EventHandlerItem, #eventstreamconfig{}}});

handle(_ProtocolVersion, {add_event_handler, {EventType, EventHandlerItem, ProducerConfig}}) ->
  NewEventItem = case EventHandlerItem#event_handler_item.processing_method of
                   tree -> EventHandlerItem#event_handler_item{tree_id = generate_tree_name()};
                   _ -> EventHandlerItem
                 end,

  case ets:lookup(?RULE_MANAGER_ETS, EventType) of
    [] -> ets:insert(?RULE_MANAGER_ETS, {EventType, [NewEventItem]});
    [{_EventType, EventHandlers}] -> ets:insert(?RULE_MANAGER_ETS, {EventType, [NewEventItem | EventHandlers]})
  end,

  register_producer_config(ProducerConfig),

  notify_cluster_rengines(NewEventItem, EventType),
  notify_producers(ProducerConfig, EventType),

  ?info("New handler for event ~p registered.", [EventType]),
  ok;

handle(_ProtocolVersion, {register_producer_config, ProducerConfig}) ->
  register_producer_config(ProducerConfig),
  notify_fuses(ProducerConfig);

handle(_ProtocolVersion, {get_event_handlers, EventType}) ->
  EventHandlerItems = ets:lookup(?RULE_MANAGER_ETS, EventType),
  Res = case EventHandlerItems of
          [{_EventType, ItemsList}] -> ItemsList;
          _ -> []
        end,
  {ok, Res};

handle(_ProtocolVersion, register_default_rules) ->
  {ok, QuotaCheckFreq} = application:get_env(?APP_Name, quota_check_freq),
  register_default_rules(QuotaCheckFreq);

handle(_ProtocolVersion, _Msg) ->
  ok.

cleanup() ->
	ok.

%% ====================================================================
%% Helper functions
%% ====================================================================

%% notify_cluster_rengines/2
%% ====================================================================
%% @doc Notify all cluster_rengines about EventHandlerItem for EventType
%% @end
-spec notify_cluster_rengines(EventHandlerItem :: event_handler_item(), EventType :: atom()) -> ok | error.
%% ====================================================================
notify_cluster_rengines(EventHandlerItem, EventType) ->
  Ans = gen_server:call({global, ?CCM}, {update_cluster_rengines, EventType, EventHandlerItem}),
  case Ans of
    ok -> ok;
    _ ->
      ?warning("Cannot nofify cluster_rengines"),
      error
  end.

%% notify_producers/2
%% ====================================================================
%% @doc Notify event producers about new ProducerConfig.
%% @end
-spec notify_producers(ProducerConfig :: #eventstreamconfig{}, EventType :: string()) -> term().
%% ====================================================================
notify_producers(ProducerConfig, EventType) ->
  notify_fuses(ProducerConfig),

  %% notify logical_files_manager
  gen_server:cast({global, ?CCM}, {notify_lfm, EventType, true}).

register_producer_config(ProducerConfig) ->
  case ets:lookup(?PRODUCERS_RULES_ETS, producer_configs) of
    [] -> ets:insert(?PRODUCERS_RULES_ETS, {producer_configs, [ProducerConfig]});
    [{_, ListOfConfigs}] -> ets:insert(?PRODUCERS_RULES_ETS, {producer_configs, [ProducerConfig | ListOfConfigs]})
  end.

%% notify_fuses/1
%% ====================================================================
%% @doc Notify all fuses about new ProducerConfig.
%% @end
-spec notify_fuses(ProducerConfig :: #eventstreamconfig{}) -> term().
%% ====================================================================
notify_fuses(ProducerConfig) ->
  Rows = fetch_rows(?FUSE_CONNECTIONS_VIEW, #view_query_args{}),
  FuseIds = lists:map(fun(#view_row{key = FuseId}) -> FuseId end, Rows),
  UniqueFuseIds = sets:to_list(sets:from_list(FuseIds)),

  lists:foreach(fun(FuseId) -> request_dispatcher:send_to_fuse(FuseId, ProducerConfig, "fuse_messages") end, UniqueFuseIds).

%% generate_tree_name/0
%% ====================================================================
%% @doc Generate tree name for subprocess tree.
%% @end
-spec generate_tree_name() -> atom().
%% ====================================================================
generate_tree_name() ->
  [{_, Id}] = ets:lookup(?HANDLER_TREE_ID_ETS, current_id),
  ets:insert(?HANDLER_TREE_ID_ETS, {current_id, Id + 1}),
  list_to_atom("event_" ++ integer_to_list(Id)).

%% Helper function for fetching rows from view
fetch_rows(ViewName, QueryArgs) ->
  case dao:list_records(ViewName, QueryArgs) of
    {ok, #view_result{rows = Rows}} ->
      Rows;
    Error ->
      ?error("Invalid view response: ~p", [Error]),
      throw(invalid_data)
  end.

%% register_default_rules/1
%% ====================================================================
%% @doc Register rules that should be registered just after cluster startup.
%% @end
-spec register_default_rules(WriteBytesThreshold :: integer()) -> ok.
%% ====================================================================
register_default_rules(WriteBytesThreshold) ->
  rule_definitions:register_quota_exceeded_handler(),
  rule_definitions:register_rm_event_handler(),
  rule_definitions:register_for_write_events(WriteBytesThreshold),
  rule_definitions:register_for_truncate_events(),
  ?info("default rule_manager rules registered"),
  ok.