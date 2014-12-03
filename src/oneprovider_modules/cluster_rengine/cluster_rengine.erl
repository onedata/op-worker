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

-include_lib("ctool/include/logging.hrl").
-include_lib("oneprovider_modules/cluster_rengine/cluster_rengine.hrl").
-include("registered_names.hrl").
-include("records.hrl").

-include("oneprovider_modules/dao/dao_vfs.hrl").
-include("oneprovider_modules/dao/dao.hrl").

-include("fuse_messages_pb.hrl").
-include("communication_protocol_pb.hrl").

-include_lib("oneprovider_modules/dao/dao.hrl").
-include_lib("dao/include/dao_helper.hrl").
-include_lib("oneprovider_modules/dao/dao_types.hrl").

-define(PROCESSOR_ETS_NAME, "processor_ets_name").
-define(PROTOCOL_VERSION, 1).

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

%% utils functions
-export([send_event/2]).

-ifdef(TEST).
-compile([export_all]).
-endif.

init(_Args) ->
    ets:new(?EVENT_HANDLERS_CACHE, [named_table, public, set, {read_concurrency, true}]),
    ets:new(?EVENT_TREES_MAPPING, [named_table, public, set, {read_concurrency, true}]),

    %% no need to wait for rule_manager - if it starts after cluster_rengine it will notify about handlers
    %% send_after just to do it in another process
    Pid = self(),
    erlang:send_after(10, Pid, {timer, {asynch, ?PROTOCOL_VERSION, configure_event_handlers}}),
    ok.

handle(_ProtocolVersion, ping) ->
    pong;

handle(ProtocolVersion, EventMessage) when is_record(EventMessage, eventmessage) ->
    Properties = lists:zip(EventMessage#eventmessage.numeric_properties_keys, EventMessage#eventmessage.numeric_properties_values)
        ++ lists:zip(EventMessage#eventmessage.string_properties_keys, EventMessage#eventmessage.string_properties_values)
        ++ [{"blocks", lists:map(fun({_, Offset, Size}) -> {Offset, Size} end, EventMessage#eventmessage.block)},
            {"sequence_number", EventMessage#eventmessage.sequence_number}],

    AdditionalProperties = [{"user_dn", fslogic_context:get_user_dn()}, {"fuse_id", get(fuse_id)}],
    Event = Properties ++ AdditionalProperties,
    ?debug("Event from client arrived, type: ~p", [proplists:lookup("type", Event)]),
    handle(ProtocolVersion, {event_arrived, Event});

handle(_ProtocolVersion, healthcheck) ->
    ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

%% Get all EventHandlerItems registered in rule_manager and create process tree for each of them.
handle(ProtocolVersion, configure_event_handlers) ->
    gen_server:call(?Dispatcher_Name, {rule_manager, ProtocolVersion, self(), get_event_handlers}),
    receive
        {ok, EventHandlers} ->
            ?debug("New event handlers: ~p", [EventHandlers]),
            lists:foreach(fun({EventType, EventHandlerItems}) ->
                lists:foreach(fun(EventHandlerItem) ->
                    update_event_handler(ProtocolVersion, EventType, EventHandlerItem)
                end, EventHandlerItems)
            end, EventHandlers),
            ok;
        Other ->
            ?warning("rule_manager get_event_handlers handler sent back unexpected structure ~p", [Other]),
            error
    after 1000 ->
        ?info("rule_manager get_event_handlers handler did not reply"),
        error
    end;

handle(_ProtocolVersion, {final_stage_tree, _TreeId, _Event}) ->
    ?warning("cluster_rengine final_stage_tree handler should be always called in subprocess tree process");

handle(ProtocolVersion, {update_cluster_rengine, EventType, EventHandlerItem}) ->
    ?info("--- cluster_rengines update_cluster_rengine"),
    update_event_handler(ProtocolVersion, EventType, EventHandlerItem);

handle(ProtocolVersion, {event_arrived, Event}) ->
    EventType = proplists:get_value("type", Event),
    case ets:lookup(?EVENT_TREES_MAPPING, EventType) of
        [] ->
            ?warning("No handler for event of type: ~p", [EventType]),
            ok;
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

%% Handle requests that have wrong structure.
handle(_ProtocolVersion, _Msg) ->
    ?warning("Wrong request: ~p", [_Msg]),
    wrong_request.

cleanup() ->
    ok.

% inner functions

% returns if during update at least one process tree has been registered
update_event_handler(ProtocolVersion, EventType, #event_handler_item{processing_method = ProcessingMethod, tree_id = TreeId} = EventHandlerItem) ->
    case {ProcessingMethod, ets:lookup(?EVENT_HANDLERS_CACHE, TreeId)} of
        {tree, []} -> create_process_tree_for_handler(ProtocolVersion, EventHandlerItem);
        _ -> ok
    end,
    save_to_caches(EventType, EventHandlerItem).

save_to_caches(EventType, #event_handler_item{processing_method = ProcessingMethod, tree_id = TreeId, handler_fun = HandlerFun} = EventHandlerItem) ->
    EventHandlerEntry = case ProcessingMethod of
                            tree -> {tree, TreeId};
                            _ -> {standard, HandlerFun}
                        end,
    EventHandlerEntries = case ets:lookup(?EVENT_TREES_MAPPING, EventType) of
                              [{_Key, Value}] -> Value;
                              _ -> []
                          end,
    ets:insert(?EVENT_TREES_MAPPING, {EventType, [EventHandlerEntry | EventHandlerEntries]}),

    case ProcessingMethod of
        tree -> case ets:lookup(?EVENT_HANDLERS_CACHE, TreeId) of
                    [] -> ets:insert(?EVENT_HANDLERS_CACHE, {TreeId, EventHandlerItem});
                    _ -> ok
                end;
        _ -> ok
    end.

create_process_tree_for_handler(ProtocolVersion, #event_handler_item{tree_id = TreeId, map_fun = MapFun, handler_fun = HandlerFun, config = #event_stream_config{config = ActualConfig} = Config}) ->
    ?info("Creation of process tree: ~p with config ~p", [TreeId, Config]),
    ProcFun = case ActualConfig of
                  undefined ->
                      fun(_ProtocolVersion, {final_stage_tree, _TreeId2, Event}) ->
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
        undefined ->
            gen_server:call({cluster_rengine, Node}, {register_or_update_sub_proc, TreeId, 2, 2, ProcFun, NewMapFun, RM, DM});
        _ -> case element(1, ActualConfig) of
                 aggregator_config ->
                     gen_server:call({cluster_rengine, Node}, {register_or_update_sub_proc, TreeId, 2, 2, ProcFun, NewMapFun, RM, DM, LocalCacheName});
                 _ ->
                     gen_server:call({cluster_rengine, Node}, {register_or_update_sub_proc, TreeId, 2, 2, ProcFun, NewMapFun, RM, DM})
             end
    end.

%% fun_from_config/1
%% ====================================================================
%% @doc Returns function that can be called as proc_fun in sub_proc.
%% Additionally returned function will satisfy another requirement - it
%% returns non if processed event should not be processed further. Otherwise
%% it returns event (in the form of proplist) that should be processed further.
%% Returned function is construced according to event_stream_config passed
%% as the argument.
%% @end
-spec fun_from_config(#event_stream_config{}) -> fun().
%% ====================================================================
fun_from_config(#event_stream_config{config = ActualConfig, wrapped_config = WrappedConfig}) ->
    WrappedFun = case WrappedConfig of
                     undefined -> non;
                     _ -> fun_from_config(WrappedConfig)
                 end,

    case element(1, ActualConfig) of
        aggregator_config ->
            fun(ProtocolVersion, {final_stage_tree, TreeId, Event}, EtsName) ->
                InitCounterIfNeeded = fun(Key) ->
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
                                InitCounterIfNeeded(Key),
                                [{_Key, Val}] = ets:lookup(EtsName, Key),
                                NewValue = Val + Incr,
                                case NewValue >= ActualConfig#aggregator_config.threshold of
                                    true ->
                                        ets:insert(EtsName, {Key, 0}),
                                        case proplists:get_value("ans_pid", ActualEvent) of
                                            undefined -> [{FieldName, FieldValue}, {FunFieldName, NewValue}];
                                            AnsPid ->
                                                [{FieldName, FieldValue}, {FunFieldName, NewValue}, {"ans_pid", AnsPid}]
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

get_disp_map_fun(_ProtocolVersion) ->
    fun({final_stage_tree, TreeId, Event}) ->
        EventHandlerFromEts = ets:lookup(?EVENT_HANDLERS_CACHE, TreeId),
        case EventHandlerFromEts of
            [] ->
                non;
            [{_Ev, #event_handler_item{disp_map_fun = FetchedDispMapFun}}] ->
                FetchedDispMapFun(Event)
        end;
        (_) ->
            non
    end.

send_event(ProtocolVersion, Event) ->
    gen_server:call(?Dispatcher_Name, {cluster_rengine, ProtocolVersion, {event_arrived, Event}}).