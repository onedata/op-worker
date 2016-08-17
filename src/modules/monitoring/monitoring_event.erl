%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module containing functions used to handle monitoring events.
%%% @end
%%%-------------------------------------------------------------------
-module(monitoring_event).
-author("Michal Wrona").

-include("modules/events/definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([emit_storage_used_updated/3, emit_space_info_updated/1,
    emit_read_statistics/4, emit_write_statistics/4,
    emit_rtransfer_statistics/2, emit_rtransfer_statistics/3,
    aggregate_monitoring_events/2, handle_monitoring_events/2]).


%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about storage usage update.
%% @end
%%--------------------------------------------------------------------
-spec emit_storage_used_updated(datastore:id(), datastore:id(), integer()) ->
    ok | {error, Reason :: term()}.
emit_storage_used_updated(SpaceId, UserId, SizeDifference) ->
    EventBase = #storage_used_updated{
        space_id = SpaceId,
        size_difference = SizeDifference
    },
    event:emit(#event{object = EventBase#storage_used_updated{user_id = undefined}}),
    event:emit(#event{object = EventBase#storage_used_updated{user_id = UserId}}).


%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about space info update.
%% @end
%%--------------------------------------------------------------------
-spec emit_space_info_updated(datastore:id()) -> ok | {error, Reason :: term()}.
emit_space_info_updated(SpaceId) ->
    event:emit(#event{object = #space_info_updated{space_id = SpaceId}}).


%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about file operations statistics.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_operations_statistics(datastore:id(), datastore:id(), non_neg_integer(),
    non_neg_integer(), non_neg_integer(), non_neg_integer()) ->
    ok | {error, Reason :: term()}.
emit_file_operations_statistics(SpaceId, UserId, DataAccessRead, DataAccessWrite,
    BlockAccessRead, BlockAccessWrite) ->
    EventBase = #file_operations_statistics{
        space_id = SpaceId,
        data_access_read = DataAccessRead,
        data_access_write = DataAccessWrite,
        block_access_read = BlockAccessRead,
        block_access_write = BlockAccessWrite
    },
    event:emit(#event{object = EventBase#file_operations_statistics{user_id = undefined}}),
    event:emit(#event{object = EventBase#file_operations_statistics{user_id = UserId}}).


%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about read operations statistics.
%% @end
%%--------------------------------------------------------------------
-spec emit_read_statistics(datastore:id(), datastore:id(), non_neg_integer(),
    non_neg_integer()) -> ok | {error, Reason :: term()}.
emit_read_statistics(SpaceId, UserId, DataAccessRead, BlockAccessRead) ->
    emit_file_operations_statistics(SpaceId, UserId, DataAccessRead, 0, BlockAccessRead, 0).

%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about write operations statistics.
%% @end
%%--------------------------------------------------------------------
-spec emit_write_statistics(datastore:id(), datastore:id(), non_neg_integer(),
    non_neg_integer()) -> ok | {error, Reason :: term()}.
emit_write_statistics(SpaceId, UserId, DataAccessWrite, BlockAccessWrite) ->
    emit_file_operations_statistics(SpaceId, UserId, 0, DataAccessWrite, 0,
        BlockAccessWrite).

%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about rtransfer statistics.
%% @end
%%--------------------------------------------------------------------
-spec emit_rtransfer_statistics(datastore:id(), datastore:id(), non_neg_integer()) ->
    ok | {error, Reason :: term()}.
emit_rtransfer_statistics(SpaceId, UserId, TransferIn) ->
    EventBase = #rtransfer_statistics {
        space_id = SpaceId,
        transfer_in = TransferIn
    },
    event:emit(#event{object = EventBase#rtransfer_statistics{user_id = undefined}}),
    case UserId of
        ?ROOT_USER_ID -> ok;
        _ ->
            event:emit(#event{object = EventBase#rtransfer_statistics{user_id = UserId}})
    end.


%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about rtransfer statistics.
%% @end
%%--------------------------------------------------------------------
-spec emit_rtransfer_statistics(fslogic_worker:ctx(), non_neg_integer()) ->
    ok | {error, Reason :: term()}.
emit_rtransfer_statistics(CTX, TransferIn) ->
    #fslogic_ctx{session = #session{identity = #user_identity{user_id = UserId}},
        space_id = SpaceId} = CTX,
    emit_rtransfer_statistics(SpaceId, UserId, TransferIn).

%%--------------------------------------------------------------------
%% @doc
%% Aggregates monitoring related events.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_monitoring_events(#event{}, #event{}) -> #event{}.
aggregate_monitoring_events(#event{object = #storage_used_updated{} = O1} = E1,
    #event{object = #storage_used_updated{} = O2} = E2) ->
    E1#event{
        counter = E1#event.counter + E2#event.counter,
        object = O2#storage_used_updated{
            size_difference = O1#storage_used_updated.size_difference +
                O2#storage_used_updated.size_difference
        }
    };

aggregate_monitoring_events(#event{object = #space_info_updated{} = O1} = E1,
    #event{object = #space_info_updated{}} = E2) ->
    E1#event{
        counter = E1#event.counter + E2#event.counter,
        object = O1
    };

aggregate_monitoring_events(#event{object = #file_operations_statistics{} = O1} = E1,
    #event{object = #file_operations_statistics{} = O2} = E2) ->
    E1#event{
        counter = E1#event.counter + E2#event.counter,
        object = O2#file_operations_statistics{
            data_access_read = O1#file_operations_statistics.data_access_read +
                O2#file_operations_statistics.data_access_read,
            data_access_write = O1#file_operations_statistics.data_access_write +
                O2#file_operations_statistics.data_access_write,
            block_access_read = O1#file_operations_statistics.block_access_read +
                O2#file_operations_statistics.block_access_read,
            block_access_write = O1#file_operations_statistics.block_access_write +
                O2#file_operations_statistics.block_access_write
        }
    };

aggregate_monitoring_events(#event{object = #rtransfer_statistics{} = O1} = E1,
    #event{object = #rtransfer_statistics{} = O2} = E2) ->
    E1#event{
        counter = E1#event.counter + E2#event.counter,
        object = O2#rtransfer_statistics{
            transfer_in = O1#rtransfer_statistics.transfer_in +
                O2#rtransfer_statistics.transfer_in
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Processes monitoring related events.
%% @end
%%--------------------------------------------------------------------
-spec handle_monitoring_events(Events :: [event:event()], Ctx :: #{}) ->
    [ok | {error, Reason :: term()}].
handle_monitoring_events(Events, Ctx) ->
    lists:map(fun handle_monitoring_event/1, Events).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes monitoring related event.
%% @end
%%--------------------------------------------------------------------
-spec handle_monitoring_event(Events :: event:event()) ->
    ok | {error, Reason :: term()}.
handle_monitoring_event(#event{object = #storage_used_updated{user_id = undefined,
    space_id = SpaceId}}) ->
    monitoring_utils:create_and_update(#monitoring_id{
        main_subject_type = space,
        main_subject_id = SpaceId,
        metric_type = storage_used
    });

handle_monitoring_event(#event{object = #storage_used_updated{} = Event}) ->
    #storage_used_updated{user_id = UserId, space_id = SpaceId,
        size_difference = SizeDifference} = Event,
    monitoring_utils:create_and_update(#monitoring_id{
        main_subject_type = space,
        main_subject_id = SpaceId,
        metric_type = storage_used,
        secondary_subject_type = user,
        secondary_subject_id = UserId
    }, #{size_difference => SizeDifference});

handle_monitoring_event(#event{object = #space_info_updated{space_id = SpaceId}}) ->
    monitoring_utils:create_and_update(#monitoring_id{
        main_subject_type = space,
        main_subject_id = SpaceId,
        metric_type = storage_quota
    }),
    monitoring_utils:create_and_update(#monitoring_id{
        main_subject_type = space,
        main_subject_id = SpaceId,
        metric_type = connected_users
    });

handle_monitoring_event(#event{object = #file_operations_statistics{} = Event}) ->
    #file_operations_statistics{
        space_id = SpaceId,
        user_id = UserId,
        data_access_read = DataAccessRead,
        data_access_write = DataAccessWrite,
        block_access_read = BlockAccessRead,
        block_access_write = BlockAccessWrite
    } = Event,
    MonitoringId = get_monitoring_id(SpaceId, UserId),

    monitoring_utils:create_and_update(MonitoringId#monitoring_id{
        metric_type = data_access
    }, #{read_counter => DataAccessRead, write_counter => DataAccessWrite}),

    monitoring_utils:create_and_update(MonitoringId#monitoring_id{
        metric_type = block_access
    }, #{read_operations_counter => BlockAccessRead,
        write_operations_counter => BlockAccessWrite});

handle_monitoring_event(#event{object = #rtransfer_statistics{} = Event}) ->
    #rtransfer_statistics{
        space_id = SpaceId,
        user_id = UserId,
        transfer_in = TransferIn
    } = Event,

    MonitoringId = get_monitoring_id(SpaceId, UserId),
    monitoring_utils:create_and_update(MonitoringId#monitoring_id{
        metric_type = remote_transfer}, #{transfer_in => TransferIn}).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc Returns monitoring id without metric for given user and space.
%% @end
%%--------------------------------------------------------------------
-spec get_monitoring_id(datastore:id(), datastore:id()) -> #monitoring_id{}.
get_monitoring_id(SpaceId, UserId) ->
    MonitoringIdWithSpace = #monitoring_id{
        main_subject_type = space,
        main_subject_id = SpaceId
    },
    case UserId of
        undefined ->
            MonitoringIdWithSpace;
        _ ->
            MonitoringIdWithSpace#monitoring_id{
                secondary_subject_id = UserId,
                secondary_subject_type = user
            }
    end.