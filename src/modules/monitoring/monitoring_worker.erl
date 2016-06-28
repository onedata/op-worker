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
-define(START_RETRY_LIMIT, 3).
-define(COUNTER_LIMIT, 4294967296). %% 2^32

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
    case monitoring_init_state:decoded_list() of
        {ok, Docs} ->
            lists:foreach(fun({MonitoringId, #monitoring_init_state{
                monitoring_interval = Interval}}) ->
                    erlang:send_after(Interval, monitoring_worker,
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
    try start(MonitoringId) of
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

            try update(MonitoringId, MonitoringState) of
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
            update_buffer_state(MonitoringId, Value);
        false ->
            worker_proxy:call(monitoring_worker, {start, MonitoringId}),
            update_buffer_state(MonitoringId, Value)
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates rrd with optional between updates state.
%% @end
%%--------------------------------------------------------------------
-spec start(#monitoring_id{}) -> ok | already_exists.

start(#monitoring_id{main_subject_type = space, metric_type = storage_used,
    secondary_subject_type = user} = MonitoringId) ->
    rrd_utils:create_rrd(MonitoringId, #{storage_used => 0});

start(#monitoring_id{main_subject_type = space, metric_type = data_access} = MonitoringId) ->
    rrd_utils:create_rrd(MonitoringId,
        #{read_counter => 0, write_counter =>0});

start(#monitoring_id{main_subject_type = space, metric_type = block_access} = MonitoringId) ->
    rrd_utils:create_rrd(MonitoringId,
        #{read_operations_counter => 0, write_operations_counter =>0});

start(MonitoringId) ->
    rrd_utils:create_rrd(MonitoringId, #{}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates rrd with value corresponding to metric type.
%% @end
%%--------------------------------------------------------------------
-spec update(#monitoring_id{}, #monitoring_state{}) -> {ok, #monitoring_state{}}.
update(#monitoring_id{main_subject_type = space,
    metric_type = storage_used, secondary_subject_type = user} = MonitoringId,
    #monitoring_state{state_buffer = StateBuffer} = MonitoringState) ->

    CurrentSize = maps:get(storage_used, StateBuffer),
    {ok, State} = rrd_utils:update_rrd(MonitoringId, MonitoringState, [CurrentSize]),
    {ok, State};

update(#monitoring_id{main_subject_type = space, main_subject_id = SpaceId,
    metric_type = storage_used} = MonitoringId, MonitoringState) ->
    {ok, #document{value = #space_quota{current_size = CurrentSize}}} =
        space_quota:get(SpaceId),

    {ok, State} = rrd_utils:update_rrd(MonitoringId, MonitoringState, [CurrentSize]),
    {ok, State};

update(#monitoring_id{main_subject_type = space, main_subject_id = SpaceId,
    metric_type = storage_quota} = MonitoringId, MonitoringState) ->
    {ok, #document{value = #space_info{providers_supports = ProvSupport}}} =
        space_info:get(SpaceId),
    SupSize = proplists:get_value(oneprovider:get_provider_id(), ProvSupport, 0),

    {ok, State} = rrd_utils:update_rrd(MonitoringId, MonitoringState, [SupSize]),
    {ok, State};

update(#monitoring_id{main_subject_type = space, main_subject_id = SpaceId,
    metric_type = connected_users} = MonitoringId, MonitoringState) ->
    {ok, #document{value = #space_info{users = Users}}} = space_info:get(SpaceId),

    {ok, State} = rrd_utils:update_rrd(MonitoringId, MonitoringState, [length(Users)]),
    {ok, State};

update(#monitoring_id{main_subject_type = space, metric_type = data_access} = MonitoringId,
    #monitoring_state{state_buffer = StateBuffer} = MonitoringState) ->
    ReadCount = maps:get(read_counter, StateBuffer),
    WriteCount = maps:get(write_counter, StateBuffer),

    {ok, State} = rrd_utils:update_rrd(MonitoringId, MonitoringState, [ReadCount, WriteCount]),
    {ok, State};

update(#monitoring_id{main_subject_type = space, metric_type = block_access} = MonitoringId,
    #monitoring_state{state_buffer = StateBuffer} = MonitoringState) ->
    ReadCount = maps:get(read_operations_counter, StateBuffer),
    WriteCount = maps:get(write_operations_counter, StateBuffer),

    {ok, State} = rrd_utils:update_rrd(MonitoringId, MonitoringState, [ReadCount, WriteCount]),
    {ok, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates monitoring buffer state.
%% @end
%%--------------------------------------------------------------------
-spec update_buffer_state(#monitoring_id{}, term()) -> no_return().
update_buffer_state(MonitoringId, Value) ->
    monitoring_state:run_synchronized(MonitoringId, fun() ->
        {ok, #document{value = MonitoringState}} = monitoring_state:get(MonitoringId),
        update_buffer_state(MonitoringId, MonitoringState, Value)
    end).

-spec update_buffer_state(#monitoring_id{}, #monitoring_state{}, term()) -> no_return().
update_buffer_state(#monitoring_id{main_subject_type = space, metric_type = storage_used,
    secondary_subject_type = user} = MonitoringId,
    #monitoring_state{state_buffer = StateBuffer}, Value) ->

    CurrentSize = maps:get(storage_used, StateBuffer),
    {ok, _} = monitoring_state:update(MonitoringId,
        #{state_buffer => #{storage_used => CurrentSize + Value}});

update_buffer_state(#monitoring_id{main_subject_type = space, metric_type = data_access} =
    MonitoringId, #monitoring_state{state_buffer = StateBuffer}, Value) ->

    ReadCount = maps:get(read_counter, StateBuffer),
    WriteCount = maps:get(write_counter, StateBuffer),

    ReadUpdate = maps:get(read_counter, Value, 0),
    WriteUpdate = maps:get(write_counter, Value, 0),

    UpdatedReadCount = (ReadCount + ReadUpdate) rem ?COUNTER_LIMIT,
    UpdatedWriteCount = (WriteCount + WriteUpdate) rem ?COUNTER_LIMIT,

    {ok, _} = monitoring_state:update(MonitoringId,
        #{state_buffer => #{read_counter => UpdatedReadCount,
            write_counter => UpdatedWriteCount}});

update_buffer_state(#monitoring_id{main_subject_type = space, metric_type = block_access} =
    MonitoringId, #monitoring_state{state_buffer = StateBuffer}, Value) ->

    ReadCount = maps:get(read_operations_counter, StateBuffer),
    WriteCount = maps:get(write_operations_counter, StateBuffer),

    ReadUpdate = maps:get(read_operations_counter, Value, 0),
    WriteUpdate = maps:get(write_operations_counter, Value, 0),

    UpdatedReadCount = (ReadCount + ReadUpdate) rem ?COUNTER_LIMIT,
    UpdatedWriteCount = (WriteCount + WriteUpdate) rem ?COUNTER_LIMIT,

    {ok, _} = monitoring_state:update(MonitoringId,
        #{state_buffer => #{read_operations_counter => UpdatedReadCount,
            write_operations_counter => UpdatedWriteCount}}).

