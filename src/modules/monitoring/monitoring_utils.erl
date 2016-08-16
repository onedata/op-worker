%%%--------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module contains utils functions used to start and
%%% update monitoring.
%%% @end
%%%--------------------------------------------------------------------
-module(monitoring_utils).
-author("Michal Wrona").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("modules/monitoring/rrd_definitions.hrl").

%% API
-export([create_and_update/1, create_and_update/2, create/2, update/4]).

-define(BYTES_TO_BITS, 8).

%%--------------------------------------------------------------------
%% @doc
%% Creates RRD if not exists and updates it.
%% @end
%%--------------------------------------------------------------------
-spec create_and_update(#monitoring_id{}) -> ok.
create_and_update(MonitoringId) ->
    create_and_update(MonitoringId, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Creates RRD if not exists and updates RRD. Updates rrd at previous and
%% current PDP time slots. Updates at previous slot only if update at
%% this slot was not performed earlier.
%% @end
%%--------------------------------------------------------------------
-spec create_and_update(#monitoring_id{}, maps:map()) -> ok.
create_and_update(MonitoringId, UpdateValue) ->
    try
        CurrentTime = erlang:system_time(seconds),
        {PreviousPDPTime, CurrentPDPTime, WaitingTime} =
            case CurrentTime rem ?STEP_IN_SECONDS of
                0 ->
                    {CurrentTime - ?STEP_IN_SECONDS, CurrentTime, 0};
                Value ->
                    {CurrentTime - Value, CurrentTime - Value + ?STEP_IN_SECONDS,
                        ?STEP_IN_SECONDS - Value}
            end,

        ok = monitoring_utils:create(MonitoringId, PreviousPDPTime - ?STEP_IN_SECONDS),
        {ok, #document{value = #monitoring_state{last_update_time = LastUpdateTime} =
            MonitoringState}} = monitoring_state:get(MonitoringId),

        case LastUpdateTime =/= PreviousPDPTime of
            true ->
                ok = update(MonitoringId, MonitoringState, PreviousPDPTime, #{});
            false -> ok
        end,

        {ok, #document{value = UpdatedMonitoringState}} = monitoring_state:get(MonitoringId),
        timer:apply_after(timer:seconds(WaitingTime), monitoring_utils, update,
            [MonitoringId, UpdatedMonitoringState, CurrentPDPTime, UpdateValue]),
        ok
    catch
        T:M ->
            ?error_stacktrace("Cannot update monitoring state for ~w - ~p:~p",
                [MonitoringId, T, M])
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates rrd with optional initial buffer state.
%% @end
%%--------------------------------------------------------------------
-spec create(#monitoring_id{}, non_neg_integer()) -> ok.
create(#monitoring_id{main_subject_type = space, metric_type = storage_used,
    secondary_subject_type = user} = MonitoringId, CreationTime) ->
    rrd_utils:create_rrd(MonitoringId, #{storage_used => 0}, CreationTime);

create(MonitoringId, CreationTime) ->
    rrd_utils:create_rrd(MonitoringId, #{}, CreationTime).

%%--------------------------------------------------------------------
%% @doc
%% Updates rrd with value corresponding to metric type.
%% @end
%%--------------------------------------------------------------------
-spec update(#monitoring_id{}, #monitoring_state{}, non_neg_integer(), maps:map()) -> ok.
update(#monitoring_id{main_subject_type = space, metric_type = storage_used,
    secondary_subject_type = user} = MonitoringId, MonitoringState, UpdateTime, UpdateValue) ->

    #monitoring_state{state_buffer = StateBuffer} = MonitoringState,
    CurrentSize = maps:get(storage_used, StateBuffer),
    SizeDifference = maps:get(size_difference, UpdateValue, 0),
    NewSize = CurrentSize + SizeDifference,

    {ok, _} = monitoring_state:update(MonitoringId,
        #{state_buffer => #{storage_used => NewSize}}),

    ok = rrd_utils:update_rrd(MonitoringId, MonitoringState, UpdateTime, [NewSize]);

update(#monitoring_id{main_subject_type = space, main_subject_id = SpaceId,
    metric_type = storage_used} = MonitoringId, MonitoringState, UpdateTime, _UpdateValue) ->

    {ok, #document{value = #space_quota{current_size = CurrentSize}}} =
        space_quota:get(SpaceId),

    ok = rrd_utils:update_rrd(MonitoringId, MonitoringState, UpdateTime, [CurrentSize]);

update(#monitoring_id{main_subject_type = space, main_subject_id = SpaceId,
    metric_type = storage_quota} = MonitoringId, MonitoringState, UpdateTime, _UpdateValue) ->

    #monitoring_state{state_buffer = StateBuffer} = MonitoringState,
    {ok, #document{value = #space_info{providers_supports = ProvSupport}}} =
        space_info:get(SpaceId),
    SupSize = proplists:get_value(oneprovider:get_provider_id(), ProvSupport, 0),

    case maps:get(previous_value, StateBuffer, undefined) of
        SupSize ->
            ok;
        _ ->
            {ok, _} = monitoring_state:update(MonitoringId,
                #{state_buffer => #{previous_value => SupSize}}),
            ok = rrd_utils:update_rrd(MonitoringId, MonitoringState, UpdateTime, [SupSize])
    end;


update(#monitoring_id{main_subject_type = space, main_subject_id = SpaceId,
    metric_type = connected_users} = MonitoringId, MonitoringState, UpdateTime, _UpdateValue) ->

    {ok, #document{value = #space_info{users = Users}}} = space_info:get(SpaceId),
    ConnectedUsers = length(Users),
    #monitoring_state{state_buffer = StateBuffer} = MonitoringState,

    case maps:get(previous_value, StateBuffer, undefined) of
        ConnectedUsers ->
            ok;
        _ ->
            {ok, _} = monitoring_state:update(MonitoringId,
                #{state_buffer => #{previous_value => ConnectedUsers}}),
            ok = rrd_utils:update_rrd(MonitoringId, MonitoringState, UpdateTime, [ConnectedUsers])
    end;

update(#monitoring_id{main_subject_type = space, metric_type = data_access} =
    MonitoringId, MonitoringState, UpdateTime, UpdateValue) ->

    ReadCount = maps:get(read_counter, UpdateValue, 0),
    WriteCount = maps:get(write_counter, UpdateValue, 0),
    ok = rrd_utils:update_rrd(MonitoringId, MonitoringState, UpdateTime, [ReadCount, WriteCount]);

update(#monitoring_id{main_subject_type = space, metric_type = block_access} =
    MonitoringId, MonitoringState, UpdateTime, UpdateValue) ->

    ReadCount = maps:get(read_operations_counter, UpdateValue, 0),
    WriteCount = maps:get(write_operations_counter, UpdateValue, 0),
    ok = rrd_utils:update_rrd(MonitoringId, MonitoringState, UpdateTime, [ReadCount, WriteCount]);

update(#monitoring_id{main_subject_type = space, metric_type = remote_transfer} =
    MonitoringId, MonitoringState, UpdateTime, UpdateValue) ->

    TransferIn = maps:get(transfer_in, UpdateValue, 0) * ?BYTES_TO_BITS,
    ok = rrd_utils:update_rrd(MonitoringId, MonitoringState, UpdateTime, [TransferIn]).

