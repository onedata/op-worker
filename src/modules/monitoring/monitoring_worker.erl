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

    lists:foreach(fun({SubjectType, SubjectId, MetricType, ProviderId,
        #monitoring_state{monitoring_interval = Interval}}) ->
        case ProviderId of
            ThisProviderId ->
                erlang:send_after(Interval, monitoring_worker,
                    {timer, {update, SubjectType, SubjectId, MetricType}});
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

handle({start, SubjectType, SubjectId, MetricType}) ->
    case rrd_utils:create_rrd(SubjectType, SubjectId, MetricType) of
        already_exists ->
            ok;
        ok ->
            worker_proxy:cast(monitoring_worker,
                {update, SubjectType, SubjectId, MetricType}),
            ok
    end;

handle({stop, SubjectType, SubjectId, MetricType}) ->
    {ok, State} = monitoring_state:get(SubjectType, SubjectId, MetricType),
    {ok, _} = monitoring_state:save(SubjectType, SubjectId, MetricType,
        State#monitoring_state{active = false}),
    ok;

handle({update, SubjectType, SubjectId, MetricType}) ->
    case monitoring_state:get(SubjectType, SubjectId, MetricType) of
        {ok, #monitoring_state{active = true}} ->
            MonitoringInterval = try update(SubjectType, SubjectId, MetricType) of
                {ok, #monitoring_state{monitoring_interval = Interval}} ->
                    Interval
            catch
                T:M ->
                    ?error_stacktrace(
                        "Cannot update monitoring state for {~s, ~s, ~s} - ~p:~p",
                        [SubjectType, SubjectId, MetricType, T, M]),

                    {ok, #monitoring_state{monitoring_interval = Interval}} =
                        monitoring_state:get(SubjectType, SubjectId, MetricType),
                    Interval
            end,
            erlang:send_after(MonitoringInterval, monitoring_worker,
                {timer, {update, SubjectType, SubjectId, MetricType}}),
            ok;
        {ok, #monitoring_state{active = false}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end;

handle({export, SubjectType, SubjectId, MetricType, Step, Format, ProviderId}) ->
    case monitoring_state:exists(SubjectType, SubjectId, MetricType) of
        true ->
            rrd_utils:export_rrd(SubjectType, SubjectId, MetricType, Step,
                Format, ProviderId);
        false ->
            {error, ?ENOENT}
    end;

handle({export, SubjectType, SubjectId, MetricType, Step, Format}) ->
    case monitoring_state:exists(SubjectType, SubjectId, MetricType) of
        true ->
            rrd_utils:export_rrd(SubjectType, SubjectId, MetricType, Step,
                Format, oneprovider:get_provider_id());
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
-spec update(atom(), datastore:id(), atom()) -> {ok, #monitoring_state{}}.
update(space, SpaceId, storage_used) ->
    {ok, #document{value = #space_quota{current_size = CSize}}} =
        space_quota:get(SpaceId),

    {ok, State} = rrd_utils:update_rrd(space, SpaceId, storage_used, CSize),
    {ok, State};

update(space, SpaceId, storage_quota) ->
    {ok, #document{value = #space_info{providers_supports = ProvSupport}}} =
        space_info:get(SpaceId),
    SupSize = proplists:get_value(oneprovider:get_provider_id(), ProvSupport, 0),

    {ok, State} = rrd_utils:update_rrd(space, SpaceId, storage_quota, SupSize),
    {ok, State}.
