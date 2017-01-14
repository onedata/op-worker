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
-include("modules/monitoring/events.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([emit_storage_used_updated/3, emit_od_space_updated/1,
    emit_file_read_statistics/4, emit_file_written_statistics/4,
    emit_rtransfer_statistics/3, aggregate_monitoring_events/2,
    handle_monitoring_events/2]).

-export_type([type/0]).

-type type() :: #storage_used_updated{} | #od_space_updated{} |
#file_operations_statistics{} | #rtransfer_statistics{}.

%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about storage usage update.
%% @end
%%--------------------------------------------------------------------
-spec emit_storage_used_updated(SpaceId :: od_space:id(), UserId :: od_user:id(),
    SizeDifference :: integer()) -> ok | {error, Reason :: term()}.
emit_storage_used_updated(SpaceId, UserId, SizeDifference) ->
    Type = #storage_used_updated{
        space_id = SpaceId,
        size_difference = SizeDifference
    },
    emit(#monitoring_event{type = Type#storage_used_updated{user_id = undefined}}),
    case UserId of
        ?ROOT_USER_ID -> ok;
        ?GUEST_USER_ID -> ok; % todo store guest statistics
        _ ->
            emit(#monitoring_event{type = Type#storage_used_updated{user_id = UserId}})
    end.


%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about space info update.
%% @end
%%--------------------------------------------------------------------
-spec emit_od_space_updated(SpaceId :: od_space:id()) -> ok | {error, Reason :: term()}.
emit_od_space_updated(SpaceId) ->
    emit(#monitoring_event{type = #od_space_updated{space_id = SpaceId}}).


%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about file operations statistics.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_operations_statistics(SpaceId :: od_space:id(), UserId :: od_user:id(),
    DataAccessRead :: non_neg_integer(), BlockAccessRead :: non_neg_integer(),
    DataAccessWrite :: non_neg_integer(), BlockAccessWrite :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
emit_file_operations_statistics(SpaceId, UserId, DataAccessRead, DataAccessWrite,
    BlockAccessRead, BlockAccessWrite) ->
    case UserId of
        ?ROOT_USER_ID -> ok;
        ?GUEST_USER_ID -> ok;
        _ ->
            Type = #file_operations_statistics{
                space_id = SpaceId,
                data_access_read = DataAccessRead,
                data_access_write = DataAccessWrite,
                block_access_read = BlockAccessRead,
                block_access_write = BlockAccessWrite
            },
            emit(#monitoring_event{type = Type#file_operations_statistics{user_id = undefined}}),
            emit(#monitoring_event{type = Type#file_operations_statistics{user_id = UserId}})
    end.


%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about read operations statistics.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_read_statistics(SpaceId :: od_space:id(), UserId :: od_user:id(),
    DataAccessRead :: non_neg_integer(), BlockAccessRead :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
emit_file_read_statistics(SpaceId, UserId, DataAccessRead, BlockAccessRead) ->
    emit_file_operations_statistics(SpaceId, UserId, DataAccessRead, 0, BlockAccessRead, 0).

%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about write operations statistics.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_written_statistics(SpaceId :: od_space:id(), UserId :: od_user:id(),
    DataAccessWrite :: non_neg_integer(), BlockAccessWrite :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
emit_file_written_statistics(SpaceId, UserId, DataAccessWrite, BlockAccessWrite) ->
    emit_file_operations_statistics(SpaceId, UserId, 0, DataAccessWrite, 0,
        BlockAccessWrite).

%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about rtransfer statistics.
%% @end
%%--------------------------------------------------------------------
-spec emit_rtransfer_statistics(SpaceId :: od_space:id(), UserId :: od_user:id(),
    TransferIn :: non_neg_integer()) -> ok | {error, Reason :: term()}.
emit_rtransfer_statistics(SpaceId, UserId, TransferIn) ->
    Type = #rtransfer_statistics{
        space_id = SpaceId,
        transfer_in = TransferIn
    },
    emit(#monitoring_event{type = Type#rtransfer_statistics{user_id = undefined}}),
    case UserId of
        ?GUEST_USER_ID -> ok;
        ?ROOT_USER_ID -> ok;
        _ ->
            emit(#monitoring_event{type = Type#rtransfer_statistics{user_id = UserId}})
    end.

%%--------------------------------------------------------------------
%% @doc
%% Aggregates monitoring related events.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_monitoring_events(OldEvt :: event:type(), Evt :: event:type()) ->
    NewEvt :: event:type().
aggregate_monitoring_events(#monitoring_event{type = #storage_used_updated{} = T1} = E1,
    #monitoring_event{type = #storage_used_updated{} = T2}) ->
    E1#monitoring_event{type = T2#storage_used_updated{
        size_difference = T1#storage_used_updated.size_difference +
            T2#storage_used_updated.size_difference
    }};

aggregate_monitoring_events(#monitoring_event{type = #od_space_updated{}} = E1,
    #monitoring_event{type = #od_space_updated{} = T2}) ->
    E1#monitoring_event{type = T2};

aggregate_monitoring_events(#monitoring_event{type = #file_operations_statistics{} = T1} = E1,
    #monitoring_event{type = #file_operations_statistics{} = T2}) ->
    E1#monitoring_event{type = T2#file_operations_statistics{
        data_access_read = T1#file_operations_statistics.data_access_read +
            T2#file_operations_statistics.data_access_read,
        data_access_write = T1#file_operations_statistics.data_access_write +
            T2#file_operations_statistics.data_access_write,
        block_access_read = T1#file_operations_statistics.block_access_read +
            T2#file_operations_statistics.block_access_read,
        block_access_write = T1#file_operations_statistics.block_access_write +
            T2#file_operations_statistics.block_access_write
    }};

aggregate_monitoring_events(#monitoring_event{type = #rtransfer_statistics{} = T1} = E1,
    #monitoring_event{type = #rtransfer_statistics{} = T2}) ->
    E1#monitoring_event{type = T2#rtransfer_statistics{
        transfer_in = T1#rtransfer_statistics.transfer_in +
            T2#rtransfer_statistics.transfer_in
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Processes monitoring related events.
%% @end
%%--------------------------------------------------------------------
-spec handle_monitoring_events(Evts :: [event:type()], Ctx :: maps:map()) ->
    [ok | {error, Reason :: term()}].
handle_monitoring_events(Evts, Ctx) ->
    Result = lists:map(fun handle_monitoring_event/1, Evts),
    case Ctx of
        #{notify := Fun} -> Fun(Result);
        _ -> ok
    end,
    Result.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes monitoring related event.
%% @end
%%--------------------------------------------------------------------
-spec handle_monitoring_event(Evts :: event:type()) ->
    ok | {error, Reason :: term()}.
handle_monitoring_event(#monitoring_event{type = #storage_used_updated{
    user_id = undefined, space_id = SpaceId}}) ->
    monitoring_utils:create_and_update(SpaceId, #monitoring_id{
        main_subject_type = space,
        main_subject_id = SpaceId,
        metric_type = storage_used
    });

handle_monitoring_event(#monitoring_event{type = #storage_used_updated{} = Evt}) ->
    SpaceId = Evt#storage_used_updated.space_id,
    monitoring_utils:create_and_update(SpaceId, #monitoring_id{
        main_subject_type = space,
        main_subject_id = SpaceId,
        metric_type = storage_used,
        secondary_subject_type = user,
        secondary_subject_id = Evt#storage_used_updated.user_id
    }, #{size_difference => Evt#storage_used_updated.size_difference});

handle_monitoring_event(#monitoring_event{type = #od_space_updated{space_id = SpaceId}}) ->
    monitoring_utils:create_and_update(SpaceId, #monitoring_id{
        main_subject_type = space,
        main_subject_id = SpaceId,
        metric_type = storage_quota
    }),
    monitoring_utils:create_and_update(SpaceId, #monitoring_id{
        main_subject_type = space,
        main_subject_id = SpaceId,
        metric_type = connected_users
    });

handle_monitoring_event(#monitoring_event{type = #file_operations_statistics{} = Evt}) ->
    #file_operations_statistics{
        space_id = SpaceId,
        user_id = UserId,
        data_access_read = DataAccessRead,
        data_access_write = DataAccessWrite,
        block_access_read = BlockAccessRead,
        block_access_write = BlockAccessWrite
    } = Evt,
    MonitoringId = get_monitoring_id(SpaceId, UserId),

    monitoring_utils:create_and_update(SpaceId, MonitoringId#monitoring_id{
        metric_type = data_access
    }, #{read_counter => DataAccessRead, write_counter => DataAccessWrite}),

    monitoring_utils:create_and_update(SpaceId, MonitoringId#monitoring_id{
        metric_type = block_access
    }, #{read_operations_counter => BlockAccessRead,
        write_operations_counter => BlockAccessWrite});

handle_monitoring_event(#monitoring_event{type = #rtransfer_statistics{} = Evt}) ->
    #rtransfer_statistics{
        space_id = SpaceId,
        user_id = UserId,
        transfer_in = TransferIn
    } = Evt,

    MonitoringId = get_monitoring_id(SpaceId, UserId),
    monitoring_utils:create_and_update(SpaceId, MonitoringId#monitoring_id{
        metric_type = remote_transfer}, #{transfer_in => TransferIn}).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private @doc Returns monitoring id without metric for given user and space.
%% @end
%%--------------------------------------------------------------------
-spec get_monitoring_id(SpaceId :: od_space:id(), UserId :: od_user:id()) ->
    Id :: #monitoring_id{}.
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

%%--------------------------------------------------------------------
%% @private @doc Emits event using the root session.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evt :: event:type()) -> ok | {error, Reason :: term()}.
emit(Evt) ->
    event:emit(Evt, ?ROOT_SESS_ID).
