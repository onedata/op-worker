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

%% API
-export([start/1, update/2, update_buffer_state/2]).

-define(COUNTER_LIMIT, 4294967296). %% 2^32
-define(BYTES_TO_BITS, 8).

%%--------------------------------------------------------------------
%% @doc
%% Creates rrd with optional between updates state.
%% @end
%%--------------------------------------------------------------------
-spec start(#monitoring_id{}) -> ok | already_exists.
start(#monitoring_id{main_subject_type = space, metric_type = storage_used,
    secondary_subject_type = user} = MonitoringId) ->
    rrd_utils:create_rrd(MonitoringId, #{storage_used => 0});

start(#monitoring_id{main_subject_type = space, metric_type = data_access} = MonitoringId) ->
    rrd_utils:create_rrd(MonitoringId, #{read_counter => 0, write_counter =>0});

start(#monitoring_id{main_subject_type = space, metric_type = block_access} = MonitoringId) ->
    rrd_utils:create_rrd(MonitoringId,
        #{read_operations_counter => 0, write_operations_counter =>0});

start(#monitoring_id{main_subject_type = space, metric_type = remote_transfer} = MonitoringId) ->
    rrd_utils:create_rrd(MonitoringId, #{transfer_in => 0});

start(MonitoringId) ->
    rrd_utils:create_rrd(MonitoringId, #{}).

%%--------------------------------------------------------------------
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
    {ok, State};

update(#monitoring_id{main_subject_type = space, metric_type = remote_transfer} = MonitoringId,
    #monitoring_state{state_buffer = StateBuffer} = MonitoringState) ->
    TransferIn = maps:get(transfer_in, StateBuffer),

    {ok, State} = rrd_utils:update_rrd(MonitoringId, MonitoringState, [TransferIn]),
    {ok, State}.


%%--------------------------------------------------------------------
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
            write_operations_counter => UpdatedWriteCount}});

update_buffer_state(#monitoring_id{main_subject_type = space, metric_type = remote_transfer} =
    MonitoringId, #monitoring_state{state_buffer = StateBuffer}, Value) ->

    TransferIn = maps:get(transfer_in, StateBuffer),
    InUpdate = maps:get(transfer_in, Value, 0),

    UpdatedTransferIn = (TransferIn + InUpdate * ?BYTES_TO_BITS) rem ?COUNTER_LIMIT,

    {ok, _} = monitoring_state:update(MonitoringId,
        #{state_buffer => #{transfer_in => UpdatedTransferIn}}).

