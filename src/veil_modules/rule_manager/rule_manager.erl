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

-module(
rule_manager).
-behaviour(worker_plugin_behaviour).
-include("logging.hrl").
-include("registered_names.hrl").
-include("veil_modules/dao/dao_helper.hrl").
-include("veil_modules/dao/dao.hrl").
-include("communication_protocol_pb.hrl").

-include("logging.hrl").
-include("registered_names.hrl").
-include_lib("veil_modules/cluster_rengine/cluster_rengine.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/dao_helper.hrl").

-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

%% functions for manual tests
-export([register_quota_exceeded_handler/0, change_quota/2, register_rm_event_handler/0, prepare/2,
  register_for_write_stats/1, register_for_read_stats/1, register_for_quota_events/1]).

-define(RULE_MANAGER_ETS, rule_manager).
-define(PRODUCERS_RULES_ETS, producers_rules).
-define(HANDLER_TREE_ID_ETS, handler_tree_id_ets).

-define(ProtocolVersion, 1).
-define(VIEW_UPDATE_DELAY, 5000).

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

  %% todo: eventually it needs to be implemented differently
  case ets:lookup(?PRODUCERS_RULES_ETS, producer_configs) of
    [] -> ets:insert(?PRODUCERS_RULES_ETS, {producer_configs, [ProducerConfig]});
    [{_, ListOfConfigs}] -> ets:insert(?PRODUCERS_RULES_ETS, {producer_configs, [ProducerConfig | ListOfConfigs]})
  end,

  notify_cluster_rengines(NewEventItem, EventType),
  notify_producers(ProducerConfig, EventType),

  ?info("New handler for event ~p registered.", [EventType]),
  ok;

handle(_ProtocolVersion, {get_event_handlers, EventType}) ->
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

notify_cluster_rengines(EventHandlerItem, EventType) ->
  Ans = gen_server:call({global, ?CCM}, {update_cluster_rengines, EventType, EventHandlerItem}),
  case Ans of
    ok -> ok;
    _ ->
      ?warning("Cannot nofify cluster_rengines"),
      error
  end.

notify_producers(ProducerConfig, EventType) ->
  Rows = fetch_rows(?FUSE_CONNECTIONS_VIEW, #view_query_args{}),
  FuseIds = lists:map(fun(#view_row{key = FuseId}) -> FuseId end, Rows),
  UniqueFuseIds = sets:to_list(sets:from_list(FuseIds)),

  lists:foreach(fun(FuseId) -> request_dispatcher:send_to_fuse(FuseId, ProducerConfig, "fuse_messages") end, UniqueFuseIds),

  gen_server:cast({global, ?CCM}, {notify_lfm, EventType, true}).

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

%% ====================================================================
%% Functions for manual tests
%% ====================================================================
register_quota_exceeded_handler() ->
  EventHandler = fun(Event) ->
    UserDn = proplists:get_value(user_dn, Event),
    worker_host:send_to_user({dn, UserDn}, #atom{value = "write_disabled"}, "communication_protocol", 1),
    gen_server:cast({global, ?CCM}, {update_user_write_enabled, UserDn, false})
  end,
  EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},
  gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {quota_exceeded_event, EventItem}}}).

%% For test purposes
register_rm_event_handler() ->
  EventHandler = fun(Event) ->
    ?info("RmEvent Handler"),
    QuotaExceededFn = fun() ->
      UserDn = proplists:get_value(user_dn, Event),
      case user_logic:quota_exceeded({dn, UserDn}, ?ProtocolVersion) of
        false ->
          worker_host:send_to_user({dn, UserDn}, #atom{value = "write_enabled"}, "communication_protocol", ?ProtocolVersion),
          gen_server:cast({global, ?CCM}, {update_user_write_enabled, UserDn, true}),
          false;
        _ ->
          true
      end
    end,

    Exceeded = QuotaExceededFn(),

    %% if quota is exceeded check quota one more time after 5s - in meanwhile db view might get reloaded
    case Exceeded of
      true ->
        spawn(fun() ->
          receive
            _ -> ok
          after ?VIEW_UPDATE_DELAY ->
            QuotaExceededFn()
          end
        end);
      _ ->
        ok
    end
  end,

  EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},
  EventFilter = #eventfilterconfig{field_name = "type", desired_value = "rm_event"},
  EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},
  gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {rm_event, EventItem, EventFilterConfig}}}).

register_for_quota_events(WriteStatsEventsMultiplier) ->
  EventHandler = fun(Event) ->
    ?info("Write EventHandler2 ~p", [node(self())]),
    UserDn = proplists:get_value(user_dn, Event),
    case user_logic:quota_exceeded({dn, UserDn}, ?ProtocolVersion) of
      true ->
        send_event([{type, quota_exceeded_event}, {user_dn, UserDn}]),
        ?info("Quota exceeded event emited");
      _ ->
        ok
    end
  end,

  EventHandlerMapFun = fun(WriteEv) ->
    UserDnString = proplists:get_value(user_dn, WriteEv),
    case UserDnString of
      undefined -> ok;
      _ -> string:len(UserDnString)
    end
  end,

  EventHandlerDispMapFun = fun(WriteEv) ->
    UserDnString = proplists:get_value(user_dn, WriteEv),
    case UserDnString of
      undefined -> ok;
      _ ->
        UserIdInt = string:len(UserDnString),
        UserIdInt div 10
    end
  end,
  Config = #event_stream_config{config = #aggregator_config{field_name = user_dn, fun_field_name = "_count", threshold = WriteStatsEventsMultiplier}},
  EventItem = #event_handler_item{processing_method = tree, handler_fun = EventHandler, map_fun = EventHandlerMapFun, disp_map_fun = EventHandlerDispMapFun, config = Config},
  gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {quota_write_event, EventItem}}}).

register_for_read_stats(Bytes) ->
  EventHandler = fun(Event) ->
    Count = proplists:get_value(count, Event),
    ?info("Read stats: ~p", [Count])
  end,
  EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},

  EventFilter = #eventfilterconfig{field_name = "type", desired_value = "read_event"},
  EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},
  EventAggregator = #eventaggregatorconfig{field_name = "type", threshold = Bytes, sum_field_name = "bytes"},
  EventAggregatorConfig = #eventstreamconfig{aggregator_config = EventAggregator, wrapped_config = EventFilterConfig},

  gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {read_event, EventItem, EventAggregatorConfig}}}).

register_for_write_stats(Bytes) ->
  EventHandler = fun(Event) ->
    Count = proplists:get_value(count, Event),
    ?info("Write stats: ~p", [Count]),

    NewEvent1 = proplists:delete(type, Event),
    QuotaEvent = [{type, quota_write_event} | NewEvent1],
    send_event(QuotaEvent)
  end,
  EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},

  EventFilter = #eventfilterconfig{field_name = "type", desired_value = "write_event"},
  EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},
  EventAggregator = #eventaggregatorconfig{field_name = "type", threshold = Bytes, sum_field_name = "bytes"},
  EventAggregatorConfig = #eventstreamconfig{aggregator_config = EventAggregator, wrapped_config = EventFilterConfig},

  gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {write_event, EventItem, EventAggregatorConfig}}}).

%% For test purposes
change_quota(UserLogin, NewQuotaInBytes) ->
  {ok, UserDoc} = user_logic:get_user({login, UserLogin}),
  user_logic:update_quota(UserDoc, #quota{size = NewQuotaInBytes}).

send_event(Event) ->
  gen_server:call(?Dispatcher_Name, {cluster_rengine, ?ProtocolVersion, {event_arrived, Event}}).

prepare(QuotaBytes, StatsBytes) ->
  register_quota_exceeded_handler(),
  register_rm_event_handler(),
  register_for_write_stats(StatsBytes),
  register_for_quota_events(2),
  register_for_read_stats(StatsBytes),
  change_quota("plgmsitko", QuotaBytes).