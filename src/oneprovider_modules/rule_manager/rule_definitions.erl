%% ==================================================================
%% @author Michal Sitko
%% @copyright (C) 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ==================================================================
%% @doc:
%% This module contains rule definitions, most of them are supposed
%% to be registered on cluster startup by register_default_rules
%% function.
%% @end
%% ==================================================================

-module(rule_definitions).

-include_lib("ctool/include/logging.hrl").
-include("registered_names.hrl").
-include_lib("dao/include/dao_helper.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/cluster_rengine/cluster_rengine.hrl").

-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").

%% API
-export([register_quota_exceeded_handler/0, register_rm_event_handler/0, register_for_write_events/1, register_for_truncate_events/0, register_read_for_stats_events/1, register_write_for_stats_events/1]).
-export([register_write_for_available_blocks_events/1, register_truncate_for_avaialable_blocks_events/0]).

-define(ProtocolVersion, 1).
-define(VIEW_UPDATE_DELAY, 5000).

%% ====================================================================
%% Rule definitions
%% ====================================================================
register_quota_exceeded_handler() ->
    EventHandler = fun(Event) ->
        UserDn = proplists:get_value("user_dn", Event),
        ?debug("Quota exceeded for user: ~p", [UserDn]),
        change_write_enabled(UserDn, false)
    end,
    EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},

    %% no client configuration needed - register event handler
    gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {"quota_exceeded_event", EventItem}}}).

register_rm_event_handler() ->
    EventHandler = get_rm_event_handler(),
    EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},

    %% client configuration
    EventFilter = #eventfilterconfig{field_name = "type", desired_value = "rm_event"},
    EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},

    gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {"rm_event", EventItem, EventFilterConfig}}}).

register_for_truncate_events() ->
    EventHandler = get_rm_event_handler(),
    EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},

    %% client configuration
    EventFilter = #eventfilterconfig{field_name = "type", desired_value = "truncate_event"},
    EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},

    gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {"truncate_event", EventItem, EventFilterConfig}}}).

register_truncate_for_avaialable_blocks_events() ->
    EventHandler = get_truncate_for_avaialbale_blocks_handler(),
    EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},

    %% client configuration
    EventFilter = #eventfilterconfig{field_name = "type", desired_value = "truncate_event"},
    EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},

    EventTransformer = #eventtransformerconfig{field_names_to_replace = ["type"], values_to_replace = ["truncate_event"], new_values = ["truncate_for_available_blocks"]},
    EventTransformerConfig = #eventstreamconfig{transformer_config = EventTransformer, wrapped_config = EventFilterConfig},

    gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {"truncate_for_available_blocks", EventItem, EventTransformerConfig}}}).

%% Registers handler which will be called every Bytes will be written.
register_for_write_events(Bytes) ->
    EventHandler = fun(Event) ->
        UserDn = proplists:get_value("user_dn", Event),
        try
            case user_logic:quota_exceeded({dn, UserDn}, ?ProtocolVersion) of
                true ->
                    cluster_rengine:send_event(?ProtocolVersion, [{"type", "quota_exceeded_event"}, {"user_dn", UserDn}]);
                _ ->
                    ok
            end
        catch
            throw:{cannot_fetch_user, Error} ->
                ?warning("cannot fetch user when checking if quota exceeded for user_dn: ~p, error: ~p", [UserDn, Error])
        end
    end,

    EventHandlerMapFun = get_standard_map_fun(),
    EventHandlerDispMapFun = get_standard_disp_map_fun(),
    EventItem = #event_handler_item{processing_method = tree, handler_fun = EventHandler, map_fun = EventHandlerMapFun, disp_map_fun = EventHandlerDispMapFun, config = #event_stream_config{config = #aggregator_config{field_name = "user_dn", fun_field_name = "bytes", threshold = Bytes}}},

    %% client configuration
    EventFilter = #eventfilterconfig{field_name = "type", desired_value = "write_event"},
    EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},
    EventAggregator = #eventaggregatorconfig{field_name = "type", threshold = Bytes, sum_field_name = "bytes"},
    EventAggregatorConfig = #eventstreamconfig{aggregator_config = EventAggregator, wrapped_config = EventFilterConfig},

    gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {"write_event", EventItem, EventAggregatorConfig}}}).

%% Registers handler which will be called every Bytes will be written.
register_write_for_available_blocks_events(BytesThreshold) ->
    EventHandler = get_write_for_available_blocks_handler(),

    EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},

    EventFilter = #eventfilterconfig{field_name = "type", desired_value = "write_event"},
    EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},
    EventAggregator = #eventaggregatorconfig{field_name = "type", threshold = BytesThreshold, sum_field_name = "bytes"},
    EventAggregatorConfig = #eventstreamconfig{aggregator_config = EventAggregator, wrapped_config = EventFilterConfig},

    EventTransformer = #eventtransformerconfig{field_names_to_replace = ["type"], values_to_replace = ["write_event"], new_values = ["write_for_available_blocks"]},
    EventTransformerConfig = #eventstreamconfig{transformer_config = EventTransformer, wrapped_config = EventAggregatorConfig},
    gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {"write_for_available_blocks", EventItem, EventTransformerConfig}}}).

register_write_for_stats_events(BytesThreshold) ->
    EventHandler = fun(Event) ->
        Bytes = proplists:get_value("bytes", Event),
        case is_integer(Bytes) of
            true ->
                gen_server:cast({global, ?CCM}, {update_storage_write_b, Bytes});
            _ -> ?error("Write for stats handler received wrong data: ~p, ~p", [Bytes, Event])
        end
    end,

    EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},

    EventFilter = #eventfilterconfig{field_name = "type", desired_value = "write_event"},
    EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},
    EventAggregator = #eventaggregatorconfig{field_name = "type", threshold = BytesThreshold, sum_field_name = "bytes"},
    EventAggregatorConfig = #eventstreamconfig{aggregator_config = EventAggregator, wrapped_config = EventFilterConfig},

    EventTransformer = #eventtransformerconfig{field_names_to_replace = ["type"], values_to_replace = ["write_event"], new_values = ["write_for_stats"]},
    EventTransformerConfig = #eventstreamconfig{transformer_config = EventTransformer, wrapped_config = EventAggregatorConfig},
    gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {"write_for_stats", EventItem, EventTransformerConfig}}}).

register_read_for_stats_events(BytesThreshold) ->
    EventHandler = fun(Event) ->
        Bytes = proplists:get_value("bytes", Event),
        case is_integer(Bytes) of
            true -> gen_server:cast({global, ?CCM}, {update_storage_read_b, Bytes});
            _ -> ?error("Read for stats handler received wrong data: ~p, ~p", [Bytes, Event])
        end
    end,

    EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},

    EventFilter = #eventfilterconfig{field_name = "type", desired_value = "read_event"},
    EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},
    EventAggregator = #eventaggregatorconfig{field_name = "type", threshold = BytesThreshold, sum_field_name = "bytes"},
    EventAggregatorConfig = #eventstreamconfig{aggregator_config = EventAggregator, wrapped_config = EventFilterConfig},

    gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {"read_event", EventItem, EventAggregatorConfig}}}).

%% ====================================================================
%% Helper functions.
%% ====================================================================

change_write_enabled(UserDn, true) ->
    worker_host:send_to_user({dn, UserDn}, #atom{value = "write_enabled"}, "communication_protocol", ?ProtocolVersion),
    gen_server:cast({global, ?CCM}, {update_user_write_enabled, UserDn, true});
change_write_enabled(UserDn, false) ->
    worker_host:send_to_user({dn, UserDn}, #atom{value = "write_disabled"}, "communication_protocol", ?ProtocolVersion),
    gen_server:cast({global, ?CCM}, {update_user_write_enabled, UserDn, false}).


get_standard_map_fun() ->
    fun(WriteEv) ->
        UserDnString = proplists:get_value("user_dn", WriteEv),
        case UserDnString of
            undefined -> ok;
            _ -> string:len(UserDnString)
        end
    end.

get_standard_disp_map_fun() ->
    fun(WriteEv) ->
        UserDnString = proplists:get_value("user_dn", WriteEv),
        case UserDnString of
            undefined -> ok;
            _ ->
                UserIdInt = string:len(UserDnString),
                UserIdInt div 10
        end
    end.

get_rm_event_handler() ->
    fun(Event) ->

        CheckQuotaExceeded = fun(UserDn) ->
            try
                case user_logic:quota_exceeded({dn, UserDn}, ?ProtocolVersion) of
                    false ->
                        change_write_enabled(UserDn, true),
                        false;
                    _ ->
                        true
                end
            catch
                throw:{cannot_fetch_user, Error} ->
                    ?warning("cannot fetch user when checking if quota exceeded for user_dn: ~p, error: ~p", [UserDn, Error])
            end
        end,

        %% this function returns boolean true only if check quota is needed and quota is exceeded
        CheckQuotaIfNeeded = fun(UserDn) ->
            case user_logic:get_user({dn, UserDn}) of
                {ok, UserDoc} ->
                    case user_logic:get_quota(UserDoc) of
                        {ok, #quota{exceeded = true}} ->
                            %% calling CheckQuota causes view reloading so we call it only when needed (quota has been already exceeded)
                            CheckQuotaExceeded(UserDn);
                        _ -> false
                    end;
                Error ->
                    ?warning("cannot get user with dn: ~p, Error: ~p", [UserDn, Error]),
                    false
            end
        end,

        UserDn = proplists:get_value("user_dn", Event),
        Exceeded = CheckQuotaIfNeeded(UserDn),

        %% if quota is exceeded check quota one more time after 5s - in meanwhile db view might get reloaded
        case Exceeded of
            true ->
                spawn(fun() ->
                    receive
                        _ -> ok
                    after ?VIEW_UPDATE_DELAY ->
                        CheckQuotaExceeded(UserDn)
                    end
                end);
            _ ->
                ok
        end
    end.

get_write_for_available_blocks_handler() ->
    fun(Event) ->
        ?info("WRITE"),
        ?dump(Event),
        FuseId = proplists:get_value("fuse_id", Event),
        SequenceNumber = proplists:get_value("sequence_number", Event),
        Blocks = proplists:get_value("blocks", Event),
        FullFileName = proplists:get_value("filePath", Event),
        Context = proplists:get_value("user_context", Event),
        case Context of
            undefined -> ok;
            _ -> fslogic_context:set_user_context(Context)
        end,
        fslogic_context:set_fuse_id(FuseId),
        lists:foreach(fun({Offset, Size}) -> ok = logical_files_manager:mark_as_modified(FullFileName, FuseId, SequenceNumber, Offset, Size) end, Blocks)
    end.

get_truncate_for_avaialbale_blocks_handler() ->
    fun(Event) ->
        ?info("TRUNCATE"),
        ?dump(Event),
        FuseId = proplists:get_value("fuse_id", Event),
        SequenceNumber = proplists:get_value("sequence_number", Event),
        NewSize = proplists:get_value("newSize", Event),
        FullFileName = proplists:get_value("filePath", Event),
        Context = proplists:get_value("user_context", Event),
        case Context of
            undefined -> ok;
            _ -> fslogic_context:set_user_context(Context)
        end,
        fslogic_context:set_fuse_id(FuseId),
        ok = logical_files_manager:mark_as_truncated(FullFileName, FuseId, SequenceNumber, NewSize)
    end.