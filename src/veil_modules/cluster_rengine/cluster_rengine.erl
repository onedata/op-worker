%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour to provide
%% the functionality of rule engine which triggers admin/user defined
%% rules when events appears.
%% @end
%% ===================================================================

-module(cluster_rengine).
-behaviour(worker_plugin_behaviour).

-include("logging.hrl").
-include_lib("veil_modules/cluster_rengine/cluster_rengine.hrl").
-include("registered_names.hrl").
-include("records.hrl").

-include("veil_modules/dao/dao_vfs.hrl").
-include("veil_modules/dao/dao.hrl").

-include("fuse_messages_pb.hrl").
-include("communication_protocol_pb.hrl").

-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/dao_helper.hrl").
-include_lib("veil_modules/dao/dao_types.hrl").

-define(PROCESSOR_ETS_NAME, "processor_ets_name").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

%% functions for manual tests
-export([register_mkdir_handler/0, register_mkdir_handler_aggregation/1, register_write_event_handler/1, register_quota_exceeded_handler/0,
         send_mkdir_event/0, delete_file/1, change_quota/2, register_rm_event_handler/0, prepare/0]).

-ifdef(TEST).
-compile([export_all]).
-endif.

init(_Args) ->
  worker_host:create_simple_cache(?EVENT_HANDLERS_CACHE),
  worker_host:create_simple_cache(?EVENT_TREES_MAPPING),
	[].

handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, #eventmessage{type = Type}) ->
  Event = [{type, list_to_atom(Type)}, {user_dn, get(user_id)}, {fuse_id, get(fuse_id)}],
  handle(1, {event_arrived, Event});

handle(_ProtocolVersion, healthcheck) ->
	ok;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(ProtocolVersion, {final_stage_tree, TreeId, Event}) ->
  EventType = proplists:get_value(type, Event),
  SleepNeeded = update_event_handler(ProtocolVersion, EventType),
  % from my observations it takes about 200ms until disp map fun is registered in cluster_manager
  case SleepNeeded of
    true -> timer:sleep(600);
    _ -> ok
  end,
  gen_server:call({cluster_rengine, node()}, {asynch, 1, {final_stage_tree, TreeId, Event}});


handle(ProtocolVersion, {event_arrived, Event}) ->
  handle(ProtocolVersion, {event_arrived, Event, false});
handle(ProtocolVersion, {event_arrived, Event, SecondTry}) ->
  EventType = proplists:get_value(type, Event),
  case ets:lookup(?EVENT_TREES_MAPPING, EventType) of
    [] ->
      case SecondTry of
        true ->
          ok;
        false ->
          ?info("cluster_rengine - CACHE MISS: ~p", [self()]),
          % did not found mapping for event and first try - update caches for this event and try one more time
          SleepNeeded = update_event_handler(ProtocolVersion, EventType),
          % from my observations it takes about 200ms until disp map fun is registered in cluster_manager
          case SleepNeeded of
            true -> timer:sleep(600);
            _ -> ok
          end,
          handle(ProtocolVersion, {event_arrived, Event, true})
      end;
    % mapping for event found - forward event
    [{_, EventToTreeMappings}] ->
      ForwardEvent = fun(EventToTreeMapping) ->
        case EventToTreeMapping of
          {tree, TreeId} ->
            % forward event to subprocess tree
            gen_server:call({?Dispatcher_Name, node()}, {cluster_rengine, 1, {final_stage_tree, TreeId, Event}});
          {standard, HandlerFun} ->
            HandlerFun(Event)
        end
      end,
      lists:foreach(ForwardEvent, EventToTreeMappings)
  end;

% handles standard (non sub tree) event processing
handle(_ProtocolVersion, {final_stage, HandlerId, Event}) ->
  HandlerItem = ets:lookup(?EVENT_HANDLERS_CACHE, HandlerId),
  case HandlerItem of
    [] ->
      % we do not have to worry about updating cache here. Doing nothing is ok because this event is the result of forward
      % of event_arrived so mapping had to exist in moment of forwarding. Only situation when this code executes is
      % when between forward and calling final_stage cache has been cleared - in that situation doing nothing is ok.
      ok;
    [{_HandlerId, #event_handler_item{handler_fun = HandlerFun}}] ->
      HandlerFun(Event)
  end.

cleanup() ->
	ok.

% inner functions

save_to_caches(EventType, EventHandlerItems) ->
  EntriesForHandlers = lists:map(
    fun(#event_handler_item{processing_method = ProcessingMethod, tree_id = TreeId, handler_fun = HandlerFun}) ->
      case ProcessingMethod of
        tree -> {tree, TreeId};
        _ -> {standard, HandlerFun}
      end
    end,
    EventHandlerItems),
  ets:insert(?EVENT_TREES_MAPPING, {EventType, EntriesForHandlers}),

  HandlerItemsForTree = lists:filter(fun(#event_handler_item{tree_id = TreeId}) -> TreeId /= undefined end, EventHandlerItems),
  lists:foreach(fun(#event_handler_item{tree_id = TreeId} = EventHandlerItem) ->
    case ets:lookup(?EVENT_HANDLERS_CACHE, TreeId) of
      [] -> ets:insert(?EVENT_HANDLERS_CACHE, {TreeId, EventHandlerItem});
      _ -> ok
    end
  end, HandlerItemsForTree).

% returns if during update at least one process tree has been registered
update_event_handler(ProtocolVersion, EventType) ->
  gen_server:call(?Dispatcher_Name, {rule_manager, ProtocolVersion, self(), {get_event_handlers, EventType}}),

  receive
    {ok, EventHandlers} ->
      case EventHandlers of
        [] ->
          %% no registered events - insert empty list
          ets:insert(?EVENT_TREES_MAPPING, {EventType, []}),
          ok;
        EventHandlersList ->
          CheckIfTreeNeeded = fun(#event_handler_item{processing_method = ProcessingMethod, tree_id = TreeId}) ->
            ((ProcessingMethod =:= tree) and (ets:lookup(?EVENT_HANDLERS_CACHE, TreeId) == [])) end,
          EventsHandlersForTree = lists:filter(CheckIfTreeNeeded, EventHandlersList),
          save_to_caches(EventType, EventHandlersList),
          create_process_tree_for_handlers(EventsHandlersForTree),
          length(EventsHandlersForTree) > 0
      end;
    _ ->
      ?warning("rule_manager sent back unexpected structure")
    after 1000 ->
      ?warning("rule manager did not replied")
  end.

create_process_tree_for_handlers(EventHandlersList) ->
  lists:foreach(fun create_process_tree_for_handler/1, EventHandlersList).

create_process_tree_for_handler(#event_handler_item{tree_id = TreeId, map_fun = MapFun, handler_fun = HandlerFun, config = #event_stream_config{config = ActualConfig} = Config}) ->
  ProcFun = case ActualConfig of
              undefined ->
                fun(ProtocolVersion, {final_stage_tree, TreeId2, Event}) ->
                    HandlerFun(Event)
                end;
              _ ->
                FromConfigFun = fun_from_config(Config),
                case element(1, ActualConfig) of
                  aggregator_config ->
                    fun(ProtocolVersion, {final_stage_tree, TreeId2, Event}, EtsName) ->
                      case FromConfigFun(ProtocolVersion, {final_stage_tree, TreeId2, Event}, EtsName) of
                        non -> ok;
                        Ev -> HandlerFun(Ev)
                      end
                    end;
                  _ ->
                    fun(ProtocolVersion, {final_stage_tree, TreeId2, Event}) ->
                      case FromConfigFun(ProtocolVersion, {final_stage_tree, TreeId2, Event}) of
                        non -> ok;
                        Ev -> HandlerFun(Ev)
                      end
                    end
                end
            end,

  NewMapFun = fun({_, _TreeId, Event}) ->
    MapFun(Event)
  end,

  RM = get_request_map_fun(),
  DM = get_disp_map_fun(),

  Node = erlang:node(self()),

  LocalCacheName = list_to_atom(atom_to_list(TreeId) ++ "_local_cache"),
  case ActualConfig of
    undefined -> gen_server:call({cluster_rengine, Node}, {register_sub_proc, TreeId, 2, 2, ProcFun, NewMapFun, RM, DM});
    _ -> case element(1, ActualConfig) of
     aggregator_config -> gen_server:call({cluster_rengine, Node}, {register_sub_proc, TreeId, 2, 2, ProcFun, NewMapFun, RM, DM, LocalCacheName});
     _ -> gen_server:call({cluster_rengine, Node}, {register_sub_proc, TreeId, 2, 2, ProcFun, NewMapFun, RM, DM})
    end
  end.
%%   nodes_manager:wait_for_cluster_cast({cluster_rengine, Node}),

fun_from_config(#event_stream_config{config = ActualConfig, wrapped_config = WrappedConfig}) ->
  WrappedFun = case WrappedConfig of
    undefined -> non;
    _ -> fun_from_config(WrappedConfig)
  end,

  case element(1, ActualConfig) of
    aggregator_config ->
      fun(ProtocolVersion, {final_stage_tree, TreeId, Event}, EtsName) ->
        ?info("-------- Aggregator fun: ~p", [Event]),
        InitCounterIfNeeded = fun(EtsName, Key) ->
          case ets:lookup(EtsName, Key) of
            [] -> ets:insert(EtsName, {Key, 0});
            _ -> ok
          end
        end,

        ActualEvent = case WrappedFun of
          non -> Event;
          _ -> WrappedFun(ProtocolVersion, {final_stage_tree, TreeId, Event}, EtsName)
        end,

        case ActualEvent of
          non -> non;
          _ ->
            FieldName = ActualConfig#aggregator_config.field_name,
            FieldValue = proplists:get_value(FieldName, ActualEvent, {}),
            FunFieldName = ActualConfig#aggregator_config.fun_field_name,
            Incr = proplists:get_value(FunFieldName, ActualEvent, 1),
            ?info("-------- Aggregator fun incr: ~p, ~p, ~p", [Incr, FieldName, FieldValue]),

            case FieldValue of
              FieldValue2 when not is_tuple(FieldValue2) ->
                Key = "sum_" ++ FieldValue,
                InitCounterIfNeeded(EtsName, Key),
                [{_Key, Val}] = ets:lookup(EtsName, Key),
                NewValue = Val + Incr,
                ?info("-------- Aggregator fun new Value: ~p", [NewValue]),
                case NewValue >= ActualConfig#aggregator_config.threshold of
                  true ->
                    ?info("-------- Aggregator fun new Value: TRUE"),
                    ets:insert(EtsName, {Key, 0}),
                    case proplists:get_value(ans_pid, ActualEvent) of
                      undefined -> [{FieldName, FieldValue}, {FunFieldName, NewValue}];
                      _ -> [{FieldName, FieldValue}, {FunFieldName, NewValue}, {ans_pid, proplists:get_value(ans_pid, ActualEvent)}]
                    end;
                  _ ->
                    ?info("-------- Aggregator fun new Value: FALSE"),
                    ets:insert(EtsName, {Key, NewValue}),
                    non
                end;
              _ -> non
            end
          end
        end;
    filter_config ->
      fun(ProtocolVersion, {final_stage_tree, TreeId, Event}, EtsName) ->
        ActualEvent = case WrappedFun of
          non -> Event;
          _ -> WrappedFun(ProtocolVersion, {final_stage_tree, TreeId, Event}, EtsName)
        end,

        case ActualEvent of
          non -> non;
          _ ->
            FieldName = ActualConfig#filter_config.field_name,
            FieldValue = proplists:get_value(FieldName, ActualEvent, {}),
            case FieldValue =:= ActualConfig#filter_config.desired_value of
              true -> ActualEvent;
              _ -> non
            end
        end
      end;
    _ ->
      ?warning("Unknown type of stream event config: ~p", [element(1, ActualConfig)])
  end.

get_request_map_fun() ->
  fun
    ({final_stage_tree, TreeId, _Event}) ->
      TreeId;
    (_) ->
      non
  end.

get_disp_map_fun() ->
  fun({final_stage_tree, TreeId, Event}) ->
    EventHandlerFromEts = ets:lookup(?EVENT_HANDLERS_CACHE, TreeId),
    case EventHandlerFromEts of
      [] ->
        % if we proceeded here it may be the case, when final_stage_tree is being processed by another node and
        % this node does not have the most recent version of handler for given event. So try to update
        update_event_handler(1, proplists:get_value(type, Event)),
        EventHandler = ets:lookup(?EVENT_HANDLERS_CACHE, TreeId),

        case EventHandler of
          [] -> non;
          [{_Ev2, #event_handler_item{disp_map_fun = FetchedDispMapFun2}}] ->
            FetchedDispMapFun2(Event)
        end,

        % it may happen only if cache has been cleared between forwarding and calling DispMapFun - do nothing
        non;
      [{_Ev, #event_handler_item{disp_map_fun = FetchedDispMapFun}}] ->
        FetchedDispMapFun(Event)
    end;
    (_) ->
      non
  end.

%% For test purposes
register_mkdir_handler() ->
  EventHandler = fun(#mkdir_event{user_id = UserId, ans_pid = AnsPid}) ->
    ?info("Mkdir EventHandler ~p", [node(self())]),
    delete_file("plgmsitko/todelete")
  end,

  EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler}, %, map_fun = EventHandlerMapFun, disp_map_fun = EventHandlerDispMapFun, config = ProcessingConfig},

  EventFilter = #eventfilterconfig{field_name = "type", desired_value = "mkdir_event"},
  EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},
  gen_server:call({?Dispatcher_Name, node()}, {rule_manager, 1, self(), {add_event_handler, {mkdir_event, EventItem, EventFilterConfig}}}).

%% For test purposes
register_mkdir_handler_aggregation(InitCounter) ->
  EventHandler = fun(#mkdir_event{user_id = UserId, ans_pid = AnsPid}) ->
    ?info("Mkdir EventHandler aggregation ~p", [node(self())]),
    delete_file("plgmsitko/todelete")
  end,

  EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},

  EventFilter = #eventfilterconfig{field_name = "type", desired_value = "mkdir_event"},
  EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},

  EventAggregator = #eventaggregatorconfig{field_name = "type", threshold = InitCounter, sum_field_name = "count"},
  EventAggregatorConfig = #eventstreamconfig{aggregator_config = EventAggregator, wrapped_config = EventFilterConfig},
  gen_server:call({?Dispatcher_Name, node()}, {rule_manager, 1, self(), {add_event_handler, {mkdir_event, EventItem, EventAggregatorConfig}}}).

%% For test purposes
register_write_event_handler(InitCounter) ->
  EventHandler = fun(Event) ->
    ?info("Write EventHandler ~p", [node(self())]),
    UserDn = proplists:get_value(user_dn, Event),
    FuseId = proplists:get_value(fuse_id, Event),
    case user_logic:quota_exceeded({dn, UserDn}) of
      true ->
        gen_server:call({?Dispatcher_Name, node()}, {cluster_rengine, 1, {event_arrived, [{type, quota_exceeded_event}, {user_dn, UserDn}, {fuse_id, FuseId}]}}),
        ?info("Quota exceeded event emited");
      _ ->
        ok
    end
  end,

  EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},

  EventFilter = #eventfilterconfig{field_name = "type", desired_value = "write_event"},
  EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},

  EventAggregator = #eventaggregatorconfig{field_name = "type", threshold = InitCounter, sum_field_name = "count"},
  EventAggregatorConfig = #eventstreamconfig{aggregator_config = EventAggregator, wrapped_config = EventFilterConfig},
  gen_server:call({?Dispatcher_Name, node()}, {rule_manager, 1, self(), {add_event_handler, {write_event, EventItem, EventAggregatorConfig}}}).

%% For test purposes
register_quota_exceeded_handler() ->
  EventHandler = fun(Event) ->
    UserDn = proplists:get_value(user_dn, Event),
    FuseId = proplists:get_value(fuse_id, Event),
    ?info("quota exceeded event for user: ~p", [UserDn]),
    request_dispatcher:send_to_fuse(FuseId, #atom{value = "write_disabled"}, "communication_protocol")
  end,
  EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},
  gen_server:call({?Dispatcher_Name, node()}, {rule_manager, 1, self(), {add_event_handler, {quota_exceeded_event, EventItem}}}).

%% For test purposes
register_rm_event_handler() ->
  EventHandler = fun(Event) ->
    ?info("RmEvent Handler"),
    UserDn = proplists:get_value(user_dn, Event),
    FuseId = proplists:get_value(fuse_id, Event),
    case user_logic:quota_exceeded({dn, UserDn}) of
      false -> request_dispatcher:send_to_fuse(FuseId, #atom{value = "write_enabled"}, "communication_protocol");
      _ -> ok
    end
  end,

  EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},
  EventFilter = #eventfilterconfig{field_name = "type", desired_value = "rm_event"},
  EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},
  gen_server:call({?Dispatcher_Name, node()}, {rule_manager, 1, self(), {add_event_handler, {rm_event, EventItem, EventFilterConfig}}}).

%% For test purposes
send_mkdir_event() ->
  MkdirEvent = #mkdir_event{user_id = "123"},
  gen_server:call({?Dispatcher_Name, node()}, {cluster_rengine, 1, {event_arrived, MkdirEvent}}).

%% For test purposes
delete_file(FilePath) ->
  rpc:call(node(), dao_lib, apply, [dao_vfs, remove_file, [FilePath], 1]).

%% For test purposes
change_quota(UserLogin, NewQuotaInBytes) ->
  {ok, UserDoc} = user_logic:get_user({login, UserLogin}),
  user_logic:update_quota(UserDoc, #quota{size = NewQuotaInBytes}).

%% For test purposes
prepare() ->
  register_write_event_handler(1),
  register_quota_exceeded_handler(),
  register_rm_event_handler(),
  change_quota("plgmsitko", 100).