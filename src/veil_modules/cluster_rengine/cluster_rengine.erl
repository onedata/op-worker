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

-ifdef(TEST).
-compile([export_all]).
-endif.

init(_Args) ->
  ets:new(?EVENT_HANDLERS_CACHE, [named_table, public, set, {read_concurrency, true}]),
  ets:new(?EVENT_TREES_MAPPING, [named_table, public, set, {read_concurrency, true}]),
	[].

handle(_ProtocolVersion, ping) ->
  pong;

handle(ProtocolVersion, #eventmessage{type = Type, count = Count}) ->
  Event = [{type, list_to_atom(Type)}, {user_dn, get(user_id)}, {fuse_id, get(fuse_id)}, {count, Count}],
  handle(ProtocolVersion, {event_arrived, Event});

handle(_ProtocolVersion, healthcheck) ->
	ok;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(_ProtocolVersion, {final_stage_tree, _TreeId, _Event}) ->
  ?warning("cluster_rengine final_stage_tree handler should be always called in subprocess tree process");

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
          ?info("cluster_rengine - CACHE MISS: ~p, ~p", [node(), EventType]),
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
            gen_server:call({?Dispatcher_Name, node()}, {cluster_rengine, ProtocolVersion, {final_stage_tree, TreeId, Event}});
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
          create_process_tree_for_handlers(ProtocolVersion, EventsHandlersForTree),
          length(EventsHandlersForTree) > 0
      end;
    _ ->
      ?warning("rule_manager sent back unexpected structure")
    after 1000 ->
      ?warning("rule manager did not replied")
  end.

create_process_tree_for_handlers(ProtocolVersion, EventHandlersList) ->
  CreateProcessTreeForHandler = fun(HandlersList) -> create_process_tree_for_handler(ProtocolVersion, HandlersList) end,
  lists:foreach(CreateProcessTreeForHandler, EventHandlersList).

create_process_tree_for_handler(ProtocolVersion, #event_handler_item{tree_id = TreeId, map_fun = MapFun, handler_fun = HandlerFun, config = #event_stream_config{config = ActualConfig} = Config}) ->
  ProcFun = case ActualConfig of
              undefined ->
                fun(_ProtocolVersion, {final_stage_tree, TreeId2, Event}) ->
                    HandlerFun(Event)
                end;
              _ ->
                FromConfigFun = fun_from_config(Config),
                case element(1, ActualConfig) of
                  aggregator_config ->
                    fun(ProtocolVersion2, {final_stage_tree, TreeId2, Event}, EtsName) ->
                      case FromConfigFun(ProtocolVersion2, {final_stage_tree, TreeId2, Event}, EtsName) of
                        non -> ok;
                        Ev -> HandlerFun(Ev)
                      end
                    end;
                  _ ->
                    fun(ProtocolVersion2, {final_stage_tree, TreeId2, Event}) ->
                      case FromConfigFun(ProtocolVersion2, {final_stage_tree, TreeId2, Event}) of
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
  DM = get_disp_map_fun(ProtocolVersion),

  Node = erlang:node(self()),

  LocalCacheName = list_to_atom(atom_to_list(TreeId) ++ "_local_cache"),
  case ActualConfig of
    undefined -> gen_server:call({cluster_rengine, Node}, {register_sub_proc, TreeId, 2, 2, ProcFun, NewMapFun, RM, DM});
    _ -> case element(1, ActualConfig) of
     aggregator_config -> gen_server:call({cluster_rengine, Node}, {register_sub_proc, TreeId, 2, 2, ProcFun, NewMapFun, RM, DM, LocalCacheName});
     _ -> gen_server:call({cluster_rengine, Node}, {register_sub_proc, TreeId, 2, 2, ProcFun, NewMapFun, RM, DM})
    end
  end.

fun_from_config(#event_stream_config{config = ActualConfig, wrapped_config = WrappedConfig}) ->
  WrappedFun = case WrappedConfig of
    undefined -> non;
    _ -> fun_from_config(WrappedConfig)
  end,

  case element(1, ActualConfig) of
    aggregator_config ->
      fun(ProtocolVersion, {final_stage_tree, TreeId, Event}, EtsName) ->
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

            case FieldValue of
              FieldValue2 when not is_tuple(FieldValue2) ->
                Key = "sum_" ++ FieldValue,
                InitCounterIfNeeded(EtsName, Key),
                [{_Key, Val}] = ets:lookup(EtsName, Key),
                NewValue = Val + Incr,
                case NewValue >= ActualConfig#aggregator_config.threshold of
                  true ->
                    ets:insert(EtsName, {Key, 0}),
                    case proplists:get_value(ans_pid, ActualEvent) of
                      undefined -> [{FieldName, FieldValue}, {FunFieldName, NewValue}];
                      _ -> [{FieldName, FieldValue}, {FunFieldName, NewValue}, {ans_pid, proplists:get_value(ans_pid, ActualEvent)}]
                    end;
                  _ ->
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

get_disp_map_fun(ProtocolVersion) ->
  fun({final_stage_tree, TreeId, Event}) ->
    EventHandlerFromEts = ets:lookup(?EVENT_HANDLERS_CACHE, TreeId),
    case EventHandlerFromEts of
      [] ->
        % if we proceeded here it may be the case, when final_stage_tree is being processed by another node and
        % this node does not have the most recent version of handler for given event. So try to update
        update_event_handler(ProtocolVersion, proplists:get_value(type, Event)),
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
