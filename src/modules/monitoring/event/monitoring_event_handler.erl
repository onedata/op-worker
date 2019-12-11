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
-module(monitoring_event_handler).
-author("Michal Wrona").

-include("modules/events/definitions.hrl").
-include("modules/monitoring/events.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API Handling
-export([handle_monitoring_events/2]).
%% API Aggregation
-export([aggregate_monitoring_events/2]).

%%%===================================================================
%%% API Handling
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Processes monitoring related events.
%% @end
%%--------------------------------------------------------------------
-spec handle_monitoring_events(Evts :: [event:type()], Ctx :: map()) ->
    [ok | {error, Reason :: term()}].
handle_monitoring_events(Evts, Ctx) ->
    SpaceIds = get_space_ids(Evts),
    MissingEvents = missing_events(SpaceIds, Evts),
    Result = lists:map(fun maybe_handle_monitoring_event/1, Evts ++ MissingEvents),
    case Ctx of
        #{notify := Fun} -> Fun(Result);
        _ -> ok
    end,
    Result.

%%%===================================================================
%%% API Aggregation
%%%===================================================================

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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get list of SpaceIds of events
%% @end
%%--------------------------------------------------------------------
-spec get_space_ids(Evts :: [event:type()]) -> [od_space:id()].
get_space_ids([]) ->
    [];
get_space_ids([#monitoring_event{
    type = #storage_used_updated{space_id = SpaceId}
} | Rest]) ->
    [SpaceId | get_space_ids(Rest) -- [SpaceId]];
get_space_ids([#monitoring_event{
    type = #od_space_updated{space_id = SpaceId}
} | Rest]) ->
    [SpaceId | get_space_ids(Rest) -- [SpaceId]];
get_space_ids([#monitoring_event{
    type = #file_operations_statistics{space_id = SpaceId}
} | Rest]) ->
    [SpaceId | get_space_ids(Rest) -- [SpaceId]];
get_space_ids([#monitoring_event{
    type = #rtransfer_statistics{space_id = SpaceId}
} | Rest]) ->
    [SpaceId | get_space_ids(Rest) -- [SpaceId]].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generate events with empty values for each missing event category.
%% @end
%%--------------------------------------------------------------------
-spec missing_events([od_space:id()], [event:type()]) -> [event:type()].
missing_events([], _Evts) ->
    [];
missing_events([SpaceId | Rest], Evts) ->
    EmptyStorageUsedUpdated = #monitoring_event{type = #storage_used_updated{
        space_id = SpaceId,
        size_difference = 0
    }},
    MissingStorageUsedUpdated = missing_event(fun(#monitoring_event{
        type = #storage_used_updated{space_id = Id}
    }) -> SpaceId =:= Id;
        (_) -> false
    end, Evts, EmptyStorageUsedUpdated),

    EmptyOdSpaceUpdated = #monitoring_event{type = #od_space_updated{
        space_id = SpaceId
    }},
    MissingOdSpaceUpdated = missing_event(fun(#monitoring_event{
        type = #od_space_updated{space_id = Id}
    }) -> SpaceId =:= Id;
        (_) -> false
    end, Evts, EmptyOdSpaceUpdated ),

    EmptyFileOperationsStatistics = #monitoring_event{type = #file_operations_statistics{
        space_id = SpaceId
    }},
    MissingFileOperationsStatistics = missing_event(fun(#monitoring_event{
        type = #file_operations_statistics{space_id = Id}
    }) -> SpaceId =:= Id;
        (_) -> false
    end, Evts, EmptyFileOperationsStatistics ),

    EmptyRtransferStatistics = #monitoring_event{type = #rtransfer_statistics{
        space_id = SpaceId
    }},
    MissingRtransferStatistics = missing_event(fun(#monitoring_event{
        type = #rtransfer_statistics{space_id = Id}
    }) -> SpaceId =:= Id;
        (_) -> false
    end, Evts, EmptyRtransferStatistics ),

    MissingStorageUsedUpdated ++ MissingOdSpaceUpdated
        ++ MissingFileOperationsStatistics ++ MissingRtransferStatistics ++
        missing_events(Rest, Evts).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns [EmptyEvent] if none of given events fulfills given precondition.
%% Otherwise returns an empty list.
%% @end
%%--------------------------------------------------------------------
-spec missing_event(Precondition :: function(), Evts :: [event:type()],
    EmptyEvent :: event:type()) -> [event:type()].
missing_event(Precondition, Events, EmptyEvent) ->
    case lists:any(Precondition, Events) of
        true -> [];
        false -> [EmptyEvent]
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes monitoring related event if at least one of supporting
%% storages is NOT read-only.
%% @end
%%--------------------------------------------------------------------
-spec maybe_handle_monitoring_event(Evt :: event:type()) ->
    ok | {error, Reason :: term()}.
maybe_handle_monitoring_event(Evt = #monitoring_event{
    type = #storage_used_updated{space_id = SpaceId}
}) ->
    maybe_handle_monitoring_event(SpaceId, Evt);
maybe_handle_monitoring_event(Evt = #monitoring_event{
    type = #od_space_updated{space_id = SpaceId}
}) ->
    maybe_handle_monitoring_event(SpaceId, Evt);
maybe_handle_monitoring_event(Evt = #monitoring_event{
    type = #file_operations_statistics{space_id = SpaceId}
}) ->
    maybe_handle_monitoring_event(SpaceId, Evt);
maybe_handle_monitoring_event(Evt = #monitoring_event{
    type = #rtransfer_statistics{space_id = SpaceId}
}) ->
    maybe_handle_monitoring_event(SpaceId, Evt).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes monitoring related event if at least one of supporting
%% storages is NOT read-only.
%% @end
%%--------------------------------------------------------------------
-spec maybe_handle_monitoring_event(od_space:id(), Evt :: event:type()) ->
    ok | {error, Reason :: term()}.
maybe_handle_monitoring_event(SpaceId, Evt) ->
    case are_all_local_storages_readonly(SpaceId) of
        true -> ok;
        _ -> handle_monitoring_event(Evt)
    end.


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


%%--------------------------------------------------------------------
%% @private
%% @doc Returns monitoring id without metric for given user and space.
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
%% @private
%% @doc
%% Checks if all local storages supporting given space are readonly.
%% @end
%%--------------------------------------------------------------------
-spec are_all_local_storages_readonly(od_space:id()) -> boolean().
are_all_local_storages_readonly(SpaceId) ->
    {ok, StorageIds} = space_logic:get_local_storage_ids(SpaceId),
    lists:all(fun storage:is_readonly/1, StorageIds).
