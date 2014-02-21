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

-define(PROCESSOR_ETS_NAME, "processor_ets_name").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0, log/1]).

%% functions for manual tests
-export([register_mkdir_handler/1, send_mkdir_event/0, delete_file/0]).

init(_Args) ->
  worker_host:create_simple_cache(?EVENT_HANDLERS_CACHE),
  worker_host:create_simple_cache(?EVENT_TREES_MAPPING),
	ok.

handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, event) ->
  ?info("Some message from client"),
  handle(1, {event_arrived, #mkdir_event{user_id = "123"}});

handle(_ProtocolVersion, healthcheck) ->
	ok;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(_ProtocolVersion, {log, Msg}) ->
  log(Msg);

handle(_ProtocolVersion, {log, Msg, Args}) ->
  log(Msg, Args);

handle(ProtocolVersion, {final_stage_tree, TreeId, Event}) ->
  EventType = element(1, Event),
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
  ?info("--- !!event_arrived ~p", [element(1, Event)]),
  EventType = element(1, Event),
  case ets:lookup(?EVENT_TREES_MAPPING, EventType) of
    [] ->
      ?info("ffff: ~p", [node()]),
      case SecondTry of
        true ->
          ok;
        false ->
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
            ?info("forwarding to tree ~p", [node()]),
            % forward event to subprocess tree
%%             gen_server:call({cluster_rengine, node()}, {asynch, 1, {final_stage_tree, TreeId, Event}});
            gen_server:call({?Dispatcher_Name, node()}, {cluster_rengine, 1, {final_stage_tree, TreeId, Event}});
%%           gen_server:call({cluster_rengine, node()}, {asynch, 1, {final_stage_tree, TreeId, Event}});
          {standard, HandlerFun} ->
            ?info("normal processing ~p", [node()]),
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
          % no registered events - insert empty list
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

create_process_tree_for_handler(#event_handler_item{tree_id = TreeId, map_fun = MapFun, handler_fun = HandlerFun, config = Config}) ->
  GetEventProcessorEtsName = fun() ->
    %% In Erlang doc there is some warning about using pid_to_list in application code.
    %% It seems (but only seems) to be be used correctly here because we will store only local pids here.
    %% Still it may be better idea to do some changes to worker_host to prevent from doing "strange" things.
    list_to_atom(?PROCESSOR_ETS_NAME ++ pid_to_list(self()))
  end,

  CreateEventProcessEtsIfNeeded = fun(EtsName, InitCounter) ->
    try
      ?info("ETsname: ~p", [EtsName]),
      ets:new(EtsName, [named_table, private, set]),
      ets:insert(EtsName, {counter, InitCounter})
    catch
      error:badarg -> ok
    end
  end,


  ProcFun = case Config#processing_config.init_counter of
    undefined ->
      fun(_ProtocolVersion, {final_stage_tree, _TreeId, Event}) ->
        ?info("handler fun !!! without aggr"),
        HandlerFun(Event)
      end;
    InitCounter ->
      fun(_ProtocolVersion, {final_stage_tree, _TreeId, Event}) ->
        ?info("--<<>>-- handler fun !!! with aggr: ~p", [node()]),
        EtsName = GetEventProcessorEtsName(),
        ?info("--<<>>-- handler fun !!! with aggr2: ~p", [EtsName]),
        CreateEventProcessEtsIfNeeded(EtsName, InitCounter),
        CurrentCounter = ets:update_counter(EtsName, counter, {2, -1, 1, InitCounter}),
        ?info("....... Current counter: ~p", [CurrentCounter]),
        case CurrentCounter of
          InitCounter ->
            ?info("counter dobity"),
            HandlerFun(Event);
          _ -> ok
        end
      end
    end,

  NewMapFun = fun({_ProtocolVersion, {_, _TreeId, Event}}) ->
    ?info("NewMapFun!!!!!----"),
    MapFun(Event)
  end,

  RM = get_request_map_fun(),
  DM = get_disp_map_fun(),

  Node = erlang:node(self()),

  ?info("wolamy register_sub_proc"),
  gen_server:call({cluster_rengine, Node}, {register_sub_proc, TreeId, 2, 2, ProcFun, NewMapFun, RM, DM}),
%%   nodes_manager:wait_for_cluster_cast({cluster_rengine, Node}),
  ?info("--- po register_sub_proc").

get_request_map_fun() ->
  fun
    ({final_stage_tree, TreeId, _Event}) ->
      ?info("request_map---------"),
      TreeId;
    (_) ->
      ?info("request map fun fail"),
      non
  end.

get_disp_map_fun() ->
  fun({final_stage_tree, TreeId, Event}) ->
    ?info("disp_map_fun---------********"),
    EventHandlerFromEts = ets:lookup(?EVENT_HANDLERS_CACHE, TreeId),
    case EventHandlerFromEts of
      [] ->
        % if we proceeded here it may be the case, when final_stage_tree is being processed by another node and
        % this node does not have the most recent version of handler for given event. So try to update
        update_event_handler(1, element(1, Event)),
        EventHandler = ets:lookup(?EVENT_HANDLERS_CACHE, TreeId),

        case EventHandler of
          [] -> non;
          [{_Ev2, #event_handler_item{disp_map_fun = FetchedDispMapFun2}}] ->
            ?info("disp_map_fun---------******** EXTRA"),
            FetchedDispMapFun2(Event)
        end,

        % it may happen only if cache has been cleared between forwarding and calling DispMapFun - do nothing
        non;
      [{_Ev, #event_handler_item{disp_map_fun = FetchedDispMapFun}}] ->
        FetchedDispMapFun(Event)
    end;
    (_) ->
      ?info("disp_map_fun---------????????"),
      non
  end.

insert_to_ets_set(EtsName, Key, ItemToInsert) ->
  case ets:lookup(EtsName, Key) of
    [] -> ets:insert(EtsName, {Key, [ItemToInsert]});
    PreviousItems -> ets:insert(EtsName, {Key, [ItemToInsert | PreviousItems]})
  end.


%% For test purposes
register_mkdir_handler(InitCounter) ->
%%   EventHandlerMapFun = fun(#mkdir_event{user_id = UserIdString}) ->
%%     string_to_integer(UserIdString)
%%   end,
%%
%%   EventHandlerDispMapFun = fun (#mkdir_event{user_id = UserId}) ->
%%     UserIdInt = string_to_integer(UserId),
%%     UserIdInt div 100
%%   end,

  EventHandler = fun(#mkdir_event{user_id = UserId, ans_pid = AnsPid}) ->
    ?info("Mkdir EventHandler ~p", [node(self())]),
    delete_file()
  end,

%%   ProcessingConfig = #processing_config{init_counter = InitCounter},
  EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler}, %, map_fun = EventHandlerMapFun, disp_map_fun = EventHandlerDispMapFun, config = ProcessingConfig},
  gen_server:call({?Dispatcher_Name, node()}, {rule_manager, 1, self(), {add_event_handler, {mkdir_event, EventItem}}}).

send_mkdir_event() ->
  MkdirEvent = #mkdir_event{user_id = "123"},
  gen_server:call({?Dispatcher_Name, node()}, {cluster_rengine, 1, {event_arrived, MkdirEvent}}).

string_to_integer(SomeString) ->
  {SomeInteger, _} = string:to_integer(SomeString),
  case SomeString of
    error ->
      throw(badarg);
    _ -> SomeInteger
  end.

delete_file() ->
  rpc:call(node(), dao_lib, apply, [dao_vfs, remove_file, ["plgmsitko/todelete"], 1]).

log(Msg) ->
  ?info(Msg).

log(Msg, Arg) ->
  ?info("LOGGGG ::: ~p: ~p", [Msg, Arg]).