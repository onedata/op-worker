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
    event:subscribe(#subscription{
        id = ?MONITORING_SUB_ID,
        type = #monitoring_subscription{time_threshold = ?STEP_IN_SECONDS}
    }, ?ROOT_SESS_ID),

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
