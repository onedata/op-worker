%%%--------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module implements worker_plugin_behaviour callbacks.
%%% This module handles monitoring using rrd.
%%% @end
%%%--------------------------------------------------------------------
-module(monitoring_worker).
-behaviour(worker_plugin_behaviour).

-author("Michal Wrona").

-include("global_definitions.hrl").
-include("modules/events/streams.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("modules/monitoring/rrd_definitions.hrl").

%% API
-export([init/1, handle/1, cleanup/0]).


%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State :: worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    case session_manager:create_monitoring_session(?MONITORING_SESSION_ID) of
        {ok, _} ->
            Sub = #subscription{
                id = ?MONITORING_SUB_ID,
                object = undefined,
                event_stream = #event_stream_definition{
                    metadata = 0,
                    init_handler = fun(_, _, _) -> #{} end,
                    terminate_handler = fun(_) -> ok end,
                    event_handler = fun monitoring_event:handle_monitoring_events/2,
                    admission_rule = fun
                        (#event{object = #storage_used_updated{}}) -> true;
                        (#event{object = #space_info_updated{}}) -> true;
                        (#event{object = #file_operations_statistics{}}) -> true;
                        (#event{object = #rtransfer_statistics{}}) -> true;
                        (_) -> false
                    end,
                    aggregation_rule = fun monitoring_event:aggregate_monitoring_events/2,
                    transition_rule = fun(Meta, _) -> Meta end,
                    emission_rule = fun(_) -> false end,
                    emission_time = timer:seconds(?STEP_IN_SECONDS)
                }
            },
            case event:subscribe(Sub) of
                {ok, _} -> ok;
                {error, already_exists} -> ok
            end;
        {error, already_exists} -> ok
    end,

    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck | term(),
    Result :: nagios_handler:healthcheck_response() | ok | {ok, Response} |
    {error, Reason} | pong,
    Response :: term(),
    Reason :: term().
handle(ping) ->
    pong;
handle(healthcheck) ->
    ok;

%%--------------------------------------------------------------------
%% @doc
%% Exports monitoring state using rrdtool xport.
%% @end
%%--------------------------------------------------------------------
handle({export, MonitoringId, Step, Format}) ->
    case monitoring_state:exists(MonitoringId) of
        true ->
            rrd_utils:export_rrd(MonitoringId, Step, Format);
        false ->
            {error, ?ENOENT}
    end;

handle(_Request) ->
    ?log_bad_request(_Request),
    {error, wrong_request}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
