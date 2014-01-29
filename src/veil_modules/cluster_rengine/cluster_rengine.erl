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

-include_lib("veil_modules/cluster_rengine/cluster_rengine.hrl").
-include("registered_names.hrl").
-include("records.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

init(_Args) ->
  worker_host:create_simple_cache(?EVENT_HANDLERS_CACHE),
	ok.

handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(_ProtocolVersion, {clear_cache, Event}) ->
  clear_events_cache(Event),
  worker_host:clear_cache({?EVENT_TREES_MAPPING, Event});

handle(ProtocolVersion, {event_arrived, {AnsPid, Event}}) ->
  handle(ProtocolVersion, {event_arrived, {AnsPid, Event, false}});
handle(ProtocolVersion, {event_arrived, {AnsPid, Event}, SecondTry}) ->
  case ets:lookup(?EVENT_TREES_MAPPING, Event) of
    [] ->
      case SecondTry of
        true ->
          ok;
        false ->
          % did not found mapping for event and first try - update caches for this event and try one more time
          update_event_handler(Event),
          handle(ProtocolVersion, {event_arrived, {AnsPid, Event}}, true)
      end;
    % mapping for event found - forward event
    [{Ev, EventToTreeMappings}] ->
      ForwardEvent = fun(EventToTreeMapping) ->
        case EventToTreeMapping of
          {tree, TreeId} ->
            % forward event to subprocess tree
            gen_server:cast({global, ?CCM}, {cluster_rengine, 1, {final_stage_tree, TreeId, AnsPid, Event}});
          {non} ->
            % forward event to standard plugin handle
            gen_server:cast({global, ?CCM}, {cluster_rengine, 1, {final_stage, AnsPid, Event}})
        end
      end,
      lists:foreach(ForwardEvent, EventToTreeMappings)
  end.

% handles standard (non sub tree) event processing
handle(ProtocolVersion, {final_stage, HandlerId, AnsPid, Event}) ->
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

save_to_caches(Event, EventItems) ->
  TreesIdsForEvent = lists:map(fun(EventHandlerItem) -> element(2, EventHandlerItem) end, EventItems),
  ets:insert(?EVENT_TREES_MAPPING, {Event, TreesIdsForEvent}),

  lists:foreach(fun(#event_handler_item{tree_id = TreeId, map_fun = MapFun, disp_map_fun = DispMapFun, handler_fun = HandlerFun}) ->
    ets:insert(?EVENT_HANDLERS_CACHE, {TreeId, {MapFun, DispMapFun, HandlerFun}})
  end, EventItems).

update_event_handler(Event) ->
  EventHandlers = gen_server:call({global, ?CCM}, {rule_manager, 1, {get_event_handlers, Event}}),
  case EventHandlers of
    [] ->
      % no registered events - nothing more to do
      ok;
    [{_, EventHandlersList}] ->
      create_process_tree_for_handlers(Event, EventHandlersList),
      save_to_caches(Event, EventHandlersList)
  end.

create_process_tree_for_handlers(Event, EventHandlersList) ->
  SubProcList = generate_sub_proc_list_for_handlers(Event, EventHandlersList),

  RequestMap = fun
    ({cluster_rengine, final_stage_tree, {TreeId, AnsPid, Event}}) ->
      TreeId;
    (_) -> non
  end,

  DispMapFun = fun({cluster_rengine, final_stage_tree, {TreeId, AnsPid, Event}}) ->
    EventHandlerFromEts = ets:lookup(?EVENT_HANDLERS_CACHE, TreeId),
    case EventHandlerFromEts of
      [] ->
        % it may happen only if cache has been cleared between forwarding and calling DispMapFun - do nothing
        ok;
      [{_Ev, #event_handler_item{disp_map_fun = FetchedDispMapFun}}] ->
        FetchedDispMapFun(Event)
    end
  end,

  #initial_host_description{request_map = RequestMap, dispatcher_request_map = DispMapFun, sub_procs = SubProcList, plug_in_state = ok}.

generate_sub_proc_list_for_handlers(Event, EventHandlersList) ->
  TreeElements = lists:map(generate_sub_proc_list_for_handler/1, EventHandlersList),
  worker_host:generate_sub_proc_list(TreeElements).

generate_sub_proc_list_for_handler(#event_handler_item{tree_id = TreeId, map_fun = MapFun, handler_fun = HandlerFun}) ->
  ProcFun = fun(ProtocolVersion, {final_stage_tree, TreeId, AnsPid, Event}) ->
    Res = HandlerFun(Event),
    AnsPid ! {ok, Res}
  end,

  NewMapFun = fun({_, _, {_, _, _, Event}}) ->
    MapFun(Event)
  end,
  {TreeId, 2, 2, ProcFun, NewMapFun}.