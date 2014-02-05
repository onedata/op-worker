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

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

init(_Args) ->
  worker_host:create_simple_cache(?EVENT_HANDLERS_CACHE),
  worker_host:create_simple_cache(?EVENT_TREES_MAPPING),
	ok.

handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(_ProtocolVersion, {clear_cache, Event}) ->
  clear_events_cache(Event),
  worker_host:clear_cache({?EVENT_TREES_MAPPING, Event});

handle(ProtocolVersion, {event_arrived, {AnsPid, Event}}) ->
  handle(ProtocolVersion, {event_arrived, {AnsPid, Event}, false});
handle(ProtocolVersion, {event_arrived, {AnsPid, Event}, SecondTry}) ->
  EventType = element(1, Event),
  case ets:lookup(?EVENT_TREES_MAPPING, EventType) of
    [] ->
      case SecondTry of
        true ->
          ok;
        false ->
          % did not found mapping for event and first try - update caches for this event and try one more time
          update_event_handler(EventType),
          handle(ProtocolVersion, {event_arrived, {AnsPid, Event}, true})
      end;
    % mapping for event found - forward event
    [{_, EventToTreeMappings}] ->
      ForwardEvent = fun(EventToTreeMapping) ->
        case EventToTreeMapping of
          {tree, TreeId} ->
            ?info("forwarding to tree"),
            % forward event to subprocess tree
            gen_server:call({?Dispatcher_Name, erlang:node(self())}, {cluster_rengine, 1, {final_stage_tree, TreeId, AnsPid, Event}});
          {non, HandlerFun} ->
            io:format("normal processing with ioformat~n"),
            ?info("normal processing"),
            HandlerFun(Event)
        end
      end,
      lists:foreach(ForwardEvent, EventToTreeMappings)
  end;

% handles standard (non sub tree) event processing
handle(_ProtocolVersion, {final_stage, HandlerId, AnsPid, Event}) ->
  HandlerItem = ets:lookup(?EVENT_HANDLERS_CACHE, HandlerId),
  case HandlerItem of
    [] ->
      % we do not have to worry about updating cache here. Doing nothing is ok because this event is the result of forward
      % of event_arrived so mapping had to exist in moment of forwarding. Only situation when this code executes is
      % when between forward and calling final_stage cache has been cleared - in that situation doing nothing is ok.
      ok;
    [{_HandlerId, #event_handler_item{handler_fun = HandlerFun}}] ->
      Res =  HandlerFun(Event),
      AnsPid ! {ok, Res}
  end.

cleanup() ->
	ok.

% inner functions

clear_events_cache(Event) ->
  EventToTreeMapping = ets:lookup(?EVENT_TREES_MAPPING, element(1, Event)),
  case EventToTreeMapping of
    [] -> ok;
    [{_Ev, TreesNames}] ->
      DeleteEntry = fun(TreeName) ->
        ets:delete(?EVENT_HANDLERS_CACHE, TreeName)
      end,
      lists:foreach(DeleteEntry, TreesNames)
  end.

save_to_caches(EventType, EventHandlerItems) ->
  EntriesForHandlers = lists:map(
    fun(#event_handler_item{processing_method = ProcessingMethod, tree_id = TreeId, handler_fun = HandlerFun}) ->
      case ProcessingMethod of
        tree -> {tree, TreeId};
        _ -> {non, HandlerFun}
      end
    end,
    EventHandlerItems),
  ets:insert(?EVENT_TREES_MAPPING, {EventType, EntriesForHandlers}),

  lists:foreach(fun(#event_handler_item{tree_id = TreeId, map_fun = MapFun, disp_map_fun = DispMapFun, handler_fun = HandlerFun}) ->
    ets:insert(?EVENT_HANDLERS_CACHE, {TreeId, {MapFun, DispMapFun, HandlerFun}})
  end, EventHandlerItems).

update_event_handler(EventType) ->
  gen_server:call(?Dispatcher_Name, {rule_manager, 1, self(), {get_event_handlers, EventType}}),

  receive
    {ok, EventHandlers} ->
      case EventHandlers of
        [] ->
          % no registered events - insert empty list
          ets:insert(?EVENT_TREES_MAPPING, {EventType, []}),
          ok;
        EventHandlersList ->
          CheckIfTreeNeeded = fun(#event_handler_item{processing_method = ProcessingMethod}) -> ProcessingMethod =:= tree end,
          EventsHandlersForTree = lists:filter(CheckIfTreeNeeded, EventHandlersList),
          create_process_tree_for_handlers(EventsHandlersForTree),
          save_to_caches(EventType, EventHandlersList)
      end;
    _ ->
      ?warning("rule_manager sent back unexpected structure")
    after 1000 ->
      ?warning("rule manager did not replied")
  end.

create_process_tree_for_handlers(EventHandlersList) ->
  lists:foreach(fun create_process_tree_for_handler/1, EventHandlersList).

create_process_tree_for_handler(#event_handler_item{tree_id = TreeId, map_fun = MapFun, handler_fun = HandlerFun}) ->
  ProcFun = fun(_ProtocolVersion, {final_stage_tree, _TreeId, AnsPid, Event}) ->
    Res = HandlerFun(Event),
    AnsPid ! {ok, Res}
  end,

  NewMapFun = fun({_ProtocolVersion, {_, _TreeId, _AnsPid, Event}}) ->
    MapFun(Event)
  end,

  RM = get_request_map_fun(),
  DM = get_disp_map_fun(),

  Node = erlang:node(self()),

  ?info("wolamy register_sub_proc"),
  gen_server:call({cluster_rengine, Node}, {register_sub_proc, TreeId, 2, 2, ProcFun, NewMapFun, RM, DM}).

get_request_map_fun() ->
  fun
    ({final_stage_tree, TreeId, _AnsPid, _Event}) ->
      ?warning("request map fun success"),
      TreeId;
    (_) ->
      ?warning("request map fun fail"),
      non
  end.

get_disp_map_fun() ->
  fun({cluster_rengine, final_stage_tree, {TreeId, _AnsPid, EventRecord}}) ->
    EventHandlerFromEts = ets:lookup(?EVENT_HANDLERS_CACHE, TreeId),
    case EventHandlerFromEts of
      [] ->
        % it may happen only if cache has been cleared between forwarding and calling DispMapFun - do nothing
        ok;
      [{_Ev, #event_handler_item{disp_map_fun = FetchedDispMapFun}}] ->
        FetchedDispMapFun(EventRecord)
    end
  end.