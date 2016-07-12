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
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("modules/monitoring/rrd_definitions.hrl").

%% API
-export([init/1, handle/1, cleanup/0]).

-define(START_RETRY_TIMEOUT, timer:seconds(60)).
-define(FIRST_UPDATE_TIMEOUT, timer:seconds(60)).
-define(START_RETRY_LIMIT, 3).

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
    case monitoring_init_state:list() of
        {ok, Docs} ->
            lists:foreach(fun(#document{value = #monitoring_init_state{
                monitoring_id = MonitoringId}}) ->
                    erlang:send_after(?FIRST_UPDATE_TIMEOUT, monitoring_worker,
                            {timer, {update, MonitoringId}})
            end, Docs);
        {error, Reason} ->
            ?error_stacktrace("Cannot restart monitoring - ~p", [Reason])
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
%% Starts monitoring for given monitoring id.
%% @end
%%--------------------------------------------------------------------
handle({start, MonitoringId}) ->
    worker_proxy:call(monitoring_worker, {restart, MonitoringId, 0});

%%--------------------------------------------------------------------
%% @doc
%% Tries to starts monitoring for given monitoring id.
%% In case of failure tries again after START_RETRY_TIMEOUT and will try
%% to start monitoring START_RETRY_LIMIT times.
%% @end
%%--------------------------------------------------------------------
handle({restart, MonitoringId, Count}) ->
    try monitoring_utils:start(MonitoringId) of
        already_exists ->
            ok;
        ok ->
            worker_proxy:cast(monitoring_worker,
                {update, MonitoringId}),
            ok
    catch
        T:M ->
            CurrentCount = Count + 1,
            ?error_stacktrace(
                "Cannot start monitoring for ~w, tries number: ~b - ~p:~p",
                [MonitoringId, CurrentCount, T, M]),

            case CurrentCount of
                ?START_RETRY_LIMIT ->
                    ok;
                _ ->
                    erlang:send_after(?START_RETRY_TIMEOUT, monitoring_worker,
                        {timer, {restart, MonitoringId, CurrentCount}}),
                    ok
            end
    end;

%%--------------------------------------------------------------------
%% @doc
%% Stops monitoring for given monitoring id.
%% @end
%%--------------------------------------------------------------------
handle({stop, MonitoringId}) ->
    case monitoring_state:get(MonitoringId) of
        {ok, #document{value = State}} ->
            {ok, _} = monitoring_state:save(#document{key = MonitoringId,
                value = State#monitoring_state{active = false}}),
            ok;
        _ -> ok
    end;

%%--------------------------------------------------------------------
%% @doc
%% Updates rrd file with monitoring data for given monitoring id.
%% @end
%%--------------------------------------------------------------------
handle({update, MonitoringId}) ->
    case monitoring_state:get(MonitoringId) of
        {ok, #document{value = #monitoring_state{active = true,
            monitoring_interval = MonitoringInterval} = MonitoringState}} ->

            try monitoring_utils:update(MonitoringId, MonitoringState) of
                {ok, UpdatedMonitoringState} ->
                    monitoring_state:save(#document{key = MonitoringId,
                        value = UpdatedMonitoringState})
            catch
                T:M ->
                    ?error_stacktrace(
                        "Cannot update monitoring state for ~w - ~p:~p",
                        [MonitoringId, T, M])
            end,

            erlang:send_after(MonitoringInterval, monitoring_worker,
                {timer, {update, MonitoringId}}),
            ok;

        {ok, #document{value = #monitoring_state{active = false}}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end;

%%--------------------------------------------------------------------
%% @doc
%% Updates monitoring state buffer for given monitoring id.
%% @end
%%--------------------------------------------------------------------
handle({update_buffer_state, MonitoringId, Value}) ->
    case monitoring_state:exists(MonitoringId) of
        true ->
            monitoring_utils:update_buffer_state(MonitoringId, Value);
        false ->
            worker_proxy:call(monitoring_worker, {start, MonitoringId}),
            monitoring_utils:update_buffer_state(MonitoringId, Value)
    end;

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
