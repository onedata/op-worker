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

-define(START_RETRY_TIMEOUT, timer:seconds(1)).
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
    {ok, Docs} = monitoring_state:decoded_list(),
    ThisProviderId = oneprovider:get_provider_id(),

    lists:foreach(fun({#monitoring_id{provider_id = ProviderId} = MonitoringId,
        #monitoring_state{monitoring_interval = Interval}}) ->
        case ProviderId of
            ThisProviderId ->
                erlang:send_after(Interval, monitoring_worker, {timer, {update, MonitoringId}});
            _ -> ok
        end
    end, Docs),

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

handle({start, MonitoringId}) ->
    worker_proxy:call(monitoring_worker,
        {restart, MonitoringId, 0});

handle({restart, MonitoringId, Count}) ->
    try rrd_utils:create_rrd(MonitoringId) of
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

handle({stop, MonitoringId}) ->
    {ok, #document{value = State}} = monitoring_state:get(MonitoringId),
    {ok, _} = monitoring_state:save(#document{key = MonitoringId,
        value = State#monitoring_state{active = false}}),
    ok;

handle({update, MonitoringId}) ->
    case monitoring_state:get(MonitoringId) of
        {ok, #document{value = #monitoring_state{active = true}}} ->

            MonitoringInterval = try update(MonitoringId) of
                {ok, #monitoring_state{monitoring_interval = Interval}} ->
                    Interval
            catch
                T:M ->

                    ?error_stacktrace(
                        "Cannot update monitoring state for ~w - ~p:~p",
                        [MonitoringId, T, M]),

                    {ok, #monitoring_state{monitoring_interval = Interval}} =
                        monitoring_state:get(MonitoringId),
                    Interval
            end,

            erlang:send_after(MonitoringInterval, monitoring_worker,
                {timer, {update, MonitoringId}}),
            ok;

        {ok, #monitoring_state{active = false}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end;

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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates rrd with value corresponding to metric type.
%% @end
%%--------------------------------------------------------------------
-spec update(#monitoring_id{}) -> {ok, #monitoring_state{}}.
update(#monitoring_id{main_subject_type = space, main_subject_id = SpaceId,
    metric_type = storage_used} = MonitoringId) ->
    {ok, #document{value = #space_quota{current_size = CSize}}} =
        space_quota:get(SpaceId),

    {ok, State} = rrd_utils:update_rrd(MonitoringId, CSize),
    {ok, State};

update(#monitoring_id{main_subject_type = space, main_subject_id = SpaceId,
    metric_type = storage_quota} = MonitoringId) ->
    {ok, #document{value = #space_info{providers_supports = ProvSupport}}} =
        space_info:get(SpaceId),
    SupSize = proplists:get_value(oneprovider:get_provider_id(), ProvSupport, 0),

    {ok, State} = rrd_utils:update_rrd(MonitoringId, SupSize),
    {ok, State}.
