%%%--------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module contains functions used to update
%%% monitoring buffer state.
%%% @end
%%%--------------------------------------------------------------------
-module(monitoring_updates).
-author("Michal Wrona").

-include("global_definitions.hrl").
-include("modules/events/definitions.hrl").

%% API
-export([handle_write_events_for_monitoring/2,
    handle_read_events_for_monitoring/2, update_storage_used/3]).

%%--------------------------------------------------------------------
%% @doc
%% Processes write events to update monitoring state.
%% @end
%%--------------------------------------------------------------------
-spec handle_write_events_for_monitoring(Evts :: [event:event()], Ctx :: #{}) ->
    ok.
handle_write_events_for_monitoring(Evts, #{session_id := SessId}) ->
    lists:foreach(fun(#event{key = FileGUID, counter = Counter, object = #write_event{size = Size}}) ->
        {_, SpaceId} = fslogic_uuid:unpack_file_guid(FileGUID),
        {ok, #document{value = #session{identity = #user_identity{
            user_id = UserId}}}} = session:get(SessId),

        worker_proxy:cast(monitoring_worker, {update_buffer_state, #monitoring_id{
            main_subject_type = space,
            main_subject_id = SpaceId,
            metric_type = block_access
        }, #{write_operations_counter => Counter}}),
        worker_proxy:cast(monitoring_worker, {update_buffer_state, #monitoring_id{
            main_subject_type = space,
            main_subject_id = SpaceId,
            metric_type = block_access,
            secondary_subject_type = user,
            secondary_subject_id = UserId
        }, #{write_operations_counter => Counter}}),

        worker_proxy:cast(monitoring_worker, {update_buffer_state, #monitoring_id{
            main_subject_type = space,
            main_subject_id = SpaceId,
            metric_type = data_access
        }, #{write_counter => Size}}),
        worker_proxy:cast(monitoring_worker, {update_buffer_state, #monitoring_id{
            main_subject_type = space,
            main_subject_id = SpaceId,
            metric_type = data_access,
            secondary_subject_type = user,
            secondary_subject_id = UserId
        }, #{write_counter => Size}})

    end, Evts),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Processes read events to update monitoring state.
%% @end
%%--------------------------------------------------------------------
-spec handle_read_events_for_monitoring(Evts :: [event:event()], Ctx :: #{}) ->
    ok.
handle_read_events_for_monitoring(Evts, #{session_id := SessId}) ->
    lists:foreach(fun(#event{key = FileGUID, counter = Counter, object = #read_event{size = Size}}) ->
        {_, SpaceId} = fslogic_uuid:unpack_file_guid(FileGUID),
        {ok, #document{value = #session{identity = #user_identity{
            user_id = UserId}}}} = session:get(SessId),

        worker_proxy:cast(monitoring_worker, {update_buffer_state, #monitoring_id{
            main_subject_type = space,
            main_subject_id = SpaceId,
            metric_type = block_access
        }, #{read_operations_counter => Counter}}),
        worker_proxy:cast(monitoring_worker, {update_buffer_state, #monitoring_id{
            main_subject_type = space,
            main_subject_id = SpaceId,
            metric_type = block_access,
            secondary_subject_type = user,
            secondary_subject_id = UserId
        }, #{read_operations_counter => Counter}}),

        worker_proxy:cast(monitoring_worker, {update_buffer_state, #monitoring_id{
            main_subject_type = space,
            main_subject_id = SpaceId,
            metric_type = data_access
        }, #{read_counter => Size}}),
        worker_proxy:cast(monitoring_worker, {update_buffer_state, #monitoring_id{
            main_subject_type = space,
            main_subject_id = SpaceId,
            metric_type = data_access,
            secondary_subject_type = user,
            secondary_subject_id = UserId
        }, #{read_counter => Size}})

    end, Evts),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Updates storage used monitoring value for given user and space.
%% @end
%%--------------------------------------------------------------------
-spec update_storage_used(datastore:id(), datastore:id(), integer()) -> ok.
update_storage_used(SpaceId, UserId, SizeDiff) ->
    worker_proxy:cast(monitoring_worker, {update_buffer_state, #monitoring_id{
        main_subject_type = space,
        main_subject_id = SpaceId,
        metric_type = storage_used,
        secondary_subject_type = user,
        secondary_subject_id = UserId
    }, SizeDiff}),
    ok.
