%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% a couple of models for ember app:
%%%     - transfer
%%%     - on-the-fly-transfer
%%%     - transfer-time-stat
%%%     - transfer-current-stat
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_data_backend).
-behavior(data_backend_behaviour).
-author("Lukasz Opiola").

-include_lib("ctool/include/posix/errors.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([init/0, terminate/0]).
-export([find_record/2, find_all/1, query/2, query_record/2]).
-export([create_record/2, update_record/3, delete_record/2]).
-export([
    list_transfers/5, get_transfers_for_file/1,
    rerun_transfer/2, cancel_transfer/1
]).

%%%===================================================================
%%% data_backend_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback init/0.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback terminate/0.
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_record/2.
%% @end
%%--------------------------------------------------------------------
-spec find_record(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_record(<<"transfer">>, StateAndTransferId) ->
    transfer_record(StateAndTransferId);

find_record(<<"on-the-fly-transfer">>, TransferId) ->
    on_the_fly_transfer_record(TransferId);

find_record(<<"transfer-time-stat">>, StatId) ->
    transfer_time_stat_record(StatId);

find_record(<<"transfer-current-stat">>, TransferId) ->
    transfer_current_stat_record(TransferId).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(_ResourceType) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query/2.
%% @end
%%--------------------------------------------------------------------
-spec query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
query(_ResourceType, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query_record/2.
%% @end
%%--------------------------------------------------------------------
-spec query_record(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
query_record(_ResourceType, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"transfer">>, Data) ->
    SessionId = gui_session:get_session_id(),
    FileGuid = proplists:get_value(<<"dataSourceIdentifier">>, Data),
    ReplicatingProvider = gs_protocol:null_to_undefined(proplists:get_value(
        <<"replicatingProvider">>, Data
    )),

        

    EvictingProvider = gs_protocol:null_to_undefined(proplists:get_value(
        <<"evictingProvider">>, Data
    )),

    TransferType = case {ReplicatingProvider, EvictingProvider} of
        {undefined, ProviderId} when is_binary(ProviderId) -> eviction;
        {ProviderId, undefined} when is_binary(ProviderId) -> replication;
        {_, _} -> migration
    end,

    Result = case TransferType of
        replication ->
            logical_file_manager:schedule_file_replication(
                SessionId, {guid, FileGuid}, ReplicatingProvider
            );
        Type when Type == eviction orelse Type == migration ->
            logical_file_manager:schedule_replica_eviction(
                SessionId, {guid, FileGuid},
                EvictingProvider, ReplicatingProvider
            )
    end,

    case Result of
        {ok, TransferId} ->
            transfer_record(op_gui_utils:ids_to_association(
                ?WAITING_TRANSFERS_STATE, TransferId
            ));
        {error, ?EACCES} ->
            gui_error:unauthorized();
        {error, Error} ->
            ?error("Failed to schedule transfer{"
            "~n\tfile=~p,"
            "~n\ttype=~p,"
            "~n\treplicatingProvider=~p"
            "~n\tevictingProvider=~p},"
            "~n due to: ~p", [
                FileGuid, TransferType,
                ReplicatingProvider, EvictingProvider,
                Error
            ]),
            gui_error:internal_server_error()
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(_ResourceType, _Id, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(_ResourceType, _Id) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% Cancel transfer of given id.
%% @end
%%--------------------------------------------------------------------
-spec cancel_transfer(binary()) -> ok | gui_error:error_result().
cancel_transfer(StateAndTransferId) ->
    {_, TransferId} = op_gui_utils:association_to_ids(StateAndTransferId),
    case transfer:cancel(TransferId) of
        ok ->
            ok;
        {error, already_ended} ->
            gui_error:report_error(<<
                "Given transfer could not be cancelled "
                "because it has already ended."
            >>)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Rerun transfer of given id.
%% @end
%%--------------------------------------------------------------------
-spec rerun_transfer(SessionId :: session:id(), StateAndTransferId :: binary()) ->
    ok | gui_error:error_result().
rerun_transfer(SessionId, StateAndTransferId) ->
    {_, TransferId} = op_gui_utils:association_to_ids(StateAndTransferId),
    {ok, UserId} = session:get_user_id(SessionId),
    case transfer:rerun_ended(UserId, TransferId) of
        {ok, NewTransferId} ->
            {ok, [
                {<<"id">>, op_gui_utils:ids_to_association(
                    ?WAITING_TRANSFERS_STATE, NewTransferId
                )}
            ]};
        {error, not_ended} ->
            gui_error:report_error(<<
                "Given transfer could not be rerun "
                "because it has not ended yet."
            >>)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Lists transfers of given type in given space. Starting Id, Offset and Limit
%% can be provided.
%% @end
%%--------------------------------------------------------------------
-spec list_transfers(od_space:id(), State :: binary(),
    StartFromIndex :: null | transfer_links:link_key(),
    Offset :: integer(), Limit :: transfer:list_limit()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
list_transfers(SpaceId, State, StartFromIndex, Offset, Limit) ->
    StartFromLink = gs_protocol:null_to_undefined(StartFromIndex),
    {ok, TransferIds} = case State of
        ?WAITING_TRANSFERS_STATE ->
            transfer:list_waiting_transfers(SpaceId, StartFromLink, Offset, Limit);
        ?ONGOING_TRANSFERS_STATE ->
            transfer:list_ongoing_transfers(SpaceId, StartFromLink, Offset, Limit);
        ?ENDED_TRANSFERS_STATE ->
            transfer:list_ended_transfers(SpaceId, StartFromLink, Offset, Limit)
    end,
    {ok, [
        {<<"list">>, [op_gui_utils:ids_to_association(State, TId) || TId <- TransferIds]}
    ]}.


%%--------------------------------------------------------------------
%% @doc
%% Returns the Ids of all ongoing transfers for given file. Only the root file
%% of every transfer is tracked, so files nested in dir structures will not
%% show any transfer.
%% @end
%%--------------------------------------------------------------------
-spec get_transfers_for_file(fslogic_worker:file_guid()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
get_transfers_for_file(FileGuid) ->
    {ok, #{
        ongoing := Ongoing,
        ended := Ended
    }} = transferred_file:get_transfers(FileGuid),

    ResultMap = #{
        ongoing => [op_gui_utils:ids_to_association(?ONGOING_TRANSFERS_STATE, TId) || TId <- Ongoing],
        ended => [op_gui_utils:ids_to_association(?ENDED_TRANSFERS_STATE, TId) || TId <- Ended]
    },
    {ok, maps:to_list(ResultMap)}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant transfer record based on link and transfer id.
%% @end
%%--------------------------------------------------------------------
-spec transfer_record(binary()) -> {ok, proplists:proplist()}.
transfer_record(StateAndTransferId) ->
    {State, TransferId} = op_gui_utils:association_to_ids(StateAndTransferId),
    SessionId = gui_session:get_session_id(),
    {ok, TransferDoc = #document{value = Transfer = #transfer{
        replicating_provider = ReplicatingProviderId,
        evicting_provider = EvictingProviderId,
        file_uuid = FileUuid,
        path = Path,
        user_id = UserId,
        space_id = SpaceId,
        schedule_time = ScheduleTime,
        start_time = StartTime,
        index_name = IndexName,
        query_view_params = QueryViewParams
    }}} = transfer:get(TransferId),
    {DataSourceType, DataSourceIdentifier, DataSourceName} = case IndexName of
        undefined ->
            FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
            FileType = case logical_file_manager:stat(SessionId, {guid, FileGuid}) of
                {ok, #file_attr{type = ?DIRECTORY_TYPE}} -> <<"dir">>;
                {ok, _} -> <<"file">>;
                {error, ?ENOENT} -> <<"deleted">>;
                {error, _} -> <<"unknown">>
            end,
            {FileType, FileGuid, Path};
        _ ->
            {<<"index">>, index:id(IndexName, SpaceId), IndexName}
    end,
    QueryParams = case QueryViewParams of
        undefined -> null;
        _ -> {QueryViewParams}
    end,
    IsOngoing = transfer:is_ongoing(Transfer),
    FinishTime = case IsOngoing of
        true -> null;
        false -> Transfer#transfer.finish_time
    end,
    {ok, LinkKey} = transfer:get_link_key_by_state(TransferDoc, State),
    {ok, [
        {<<"id">>, StateAndTransferId},
        {<<"index">>, LinkKey},
        {<<"evictingProvider">>, gs_protocol:undefined_to_null(
            EvictingProviderId
        )},
        {<<"replicatingProvider">>, gs_protocol:undefined_to_null(
            ReplicatingProviderId
        )},
        {<<"isOngoing">>, IsOngoing},
        {<<"space">>, SpaceId},
        {<<"dataSourceType">>, DataSourceType},
        {<<"dataSourceIdentifier">>, DataSourceIdentifier},
        {<<"dataSourceName">>, DataSourceName},
        {<<"queryParams">>, QueryParams},
        {<<"systemUserId">>, UserId},
        {<<"startTime">>, StartTime},
        {<<"scheduleTime">>, ScheduleTime},
        {<<"finishTime">>, FinishTime},
        {<<"currentStat">>, TransferId},
        {<<"minuteStat">>, op_gui_utils:ids_to_association(
            ?JOB_TRANSFERS_TYPE, ?MINUTE_STAT_TYPE, TransferId)},
        {<<"hourStat">>, op_gui_utils:ids_to_association(
            ?JOB_TRANSFERS_TYPE, ?HOUR_STAT_TYPE, TransferId)},
        {<<"dayStat">>, op_gui_utils:ids_to_association(
            ?JOB_TRANSFERS_TYPE, ?DAY_STAT_TYPE, TransferId)},
        {<<"monthStat">>, op_gui_utils:ids_to_association(
            ?JOB_TRANSFERS_TYPE, ?MONTH_STAT_TYPE, TransferId)}
    ]}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant on-the-fly-transfer record based on record id
%% (combined provider is and space id).
%% @end
%%--------------------------------------------------------------------
-spec on_the_fly_transfer_record(RecordId :: binary()) ->
    {ok, proplists:proplist()}.
on_the_fly_transfer_record(RecordId) ->
    {ProviderId, SpaceId} = op_gui_utils:association_to_ids(RecordId),
    TransferStatsId = space_transfer_stats:key(
        ProviderId, ?ON_THE_FLY_TRANSFERS_TYPE, SpaceId
    ),

    {ok, [
        {<<"id">>, RecordId},
        {<<"replicatingProvider">>, ProviderId},
        {<<"minuteStat">>, op_gui_utils:ids_to_association(
            ?ON_THE_FLY_TRANSFERS_TYPE, ?MINUTE_STAT_TYPE, TransferStatsId)},
        {<<"hourStat">>, op_gui_utils:ids_to_association(
            ?ON_THE_FLY_TRANSFERS_TYPE, ?HOUR_STAT_TYPE, TransferStatsId)},
        {<<"dayStat">>, op_gui_utils:ids_to_association(
            ?ON_THE_FLY_TRANSFERS_TYPE, ?DAY_STAT_TYPE, TransferStatsId)},
        {<<"monthStat">>, op_gui_utils:ids_to_association(
            ?ON_THE_FLY_TRANSFERS_TYPE, ?MONTH_STAT_TYPE, TransferStatsId)}
    ]}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant transfer-time-stat record based on stat id
%% (combined transfer id and prefix defining time span of histograms).
%% @end
%%--------------------------------------------------------------------
-spec transfer_time_stat_record(RecordId :: binary()) ->
    {ok, proplists:proplist()}.
transfer_time_stat_record(RecordId) ->
    {TransferType, StatsType, Id} = op_gui_utils:association_to_ids(RecordId),
    {Histograms, StartTime, LastUpdate, TimeWindow} = prepare_histograms(
        TransferType, StatsType, Id
    ),

    SpeedCharts = transfer_histograms:to_speed_charts(
        Histograms, StartTime, LastUpdate, TimeWindow
    ),

    {ok, [
        {<<"id">>, RecordId},
        {<<"timestamp">>, LastUpdate},
        {<<"type">>, StatsType},
        {<<"stats">>, maps:to_list(SpeedCharts)}
    ]}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get and prepare padded and trimmed histograms of requested type from
%% transfer of given type and id.
%% @end
%%--------------------------------------------------------------------
-spec prepare_histograms(TransferType :: binary(), HistogramsType :: binary(),
    Id :: binary()
) -> {transfer_histograms:histograms(), StartTime :: non_neg_integer(),
    LastUpdate :: non_neg_integer(), TimeWindow :: non_neg_integer()}.
prepare_histograms(?JOB_TRANSFERS_TYPE, HistogramsType, TransferId) ->
    {ok, #document{value = T}} = transfer:get(TransferId),
    StartTime = T#transfer.start_time,

    % Return historical statistics of finished transfers intact. As for active
    % ones, pad them with zeroes to current time and erase recent n-seconds to
    % avoid fluctuations on charts
    {Histograms, LastUpdate, TimeWindow} = case transfer:is_ongoing(T) of
        false ->
            RequestedHistograms = get_histograms(T, HistogramsType),
            Window = transfer_histograms:type_to_time_window(HistogramsType),
            {RequestedHistograms, get_last_update(T), Window};
        true ->
            LastUpdates = T#transfer.last_update,
            CurrentTime = provider_logic:zone_time_seconds(),
            prepare_histograms(T, HistogramsType, CurrentTime, LastUpdates)
    end,
    {Histograms, StartTime, LastUpdate, TimeWindow};
prepare_histograms(?ON_THE_FLY_TRANSFERS_TYPE, HistogramsType, TransferStatsId) ->
    % Some functions from transfer_histograms module require specifying
    % start time parameter. But there is no conception of start time for
    % space_transfer_stats doc. So a long past value like 0 (year 1970) is used.
    StartTime = 0,
    CurrentTime = provider_logic:zone_time_seconds(),
    Fetched = space_transfer_stats:get(TransferStatsId),
    {Histograms, LastUpdate, TimeWindow} = case Fetched of
        {ok, TransferStats} ->
            LastUpdates = TransferStats#space_transfer_stats.last_update,
            prepare_histograms(
                TransferStats, HistogramsType, CurrentTime, LastUpdates
            );
        {error, not_found} ->
            % Return empty stats in case transfer stats document does not exist
            Window = transfer_histograms:type_to_time_window(HistogramsType),
            Timestamp = transfer_histograms:trim_timestamp(CurrentTime),
            {#{}, Timestamp, Window};
        {error, Error} ->
            ?error("Failed to retrieve Space Transfer Stats Document
                   of ID ~p due to: ~p", [TransferStatsId, Error]),
            error(Error)
    end,
    Pred = fun(_Provider, Histogram) -> lists:sum(Histogram) > 0 end,
    {maps:filter(Pred, Histograms), StartTime, LastUpdate, TimeWindow}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get histograms of requested type from given record. Pad them with zeroes
%% to current time and erase recent n-seconds to avoid fluctuations on charts
%% (due to synchronization between providers). To do that for type other than
%% minute one, it is required to calculate also mentioned minute hists
%% (otherwise it is not possible to trim histograms of other types).
%% @end
%%--------------------------------------------------------------------
-spec prepare_histograms(Stats :: #transfer{} | #space_transfer_stats{},
    HistogramsType :: binary(), CurrentTime :: non_neg_integer(),
    LastUpdates :: #{od_provider:id() => non_neg_integer()}
) ->
    {transfer_histograms:histograms(), Timestamp :: non_neg_integer(),
        TimeWindow :: non_neg_integer()}.
prepare_histograms(Stats, ?MINUTE_STAT_TYPE, CurrentTime, LastUpdates) ->
    Histograms = get_histograms(Stats, ?MINUTE_STAT_TYPE),
    Window = ?FIVE_SEC_TIME_WINDOW,
    PaddedHistograms = transfer_histograms:pad_with_zeroes(
        Histograms, Window, LastUpdates, CurrentTime
    ),
    {NewHistograms, NewTimestamp} = transfer_histograms:trim_min_histograms(
        PaddedHistograms, CurrentTime
    ),
    {NewHistograms, NewTimestamp, Window};

prepare_histograms(Stats, HistogramsType, CurrentTime, LastUpdates) ->
    MinHistograms = get_histograms(Stats, ?MINUTE_STAT_TYPE),
    RequestedHistograms = get_histograms(Stats, HistogramsType),
    TimeWindow = transfer_histograms:type_to_time_window(HistogramsType),

    PaddedMinHistograms = transfer_histograms:pad_with_zeroes(
        MinHistograms, ?FIVE_SEC_TIME_WINDOW, LastUpdates, CurrentTime
    ),
    PaddedRequestedHistograms = transfer_histograms:pad_with_zeroes(
        RequestedHistograms, TimeWindow, LastUpdates, CurrentTime
    ),
    {_, NewRequestedHistograms, NewTimestamp} =
        transfer_histograms:trim_histograms(PaddedMinHistograms,
            PaddedRequestedHistograms, TimeWindow, CurrentTime),

    {NewRequestedHistograms, NewTimestamp, TimeWindow}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant transfer-current-stat record based on transfer id.
%% @end
%%--------------------------------------------------------------------
-spec transfer_current_stat_record(transfer:id()) -> {ok, proplists:proplist()}.
transfer_current_stat_record(TransferId) ->
    {ok, #document{value = Transfer = #transfer{
        bytes_replicated = BytesReplicated,
        files_replicated = FilesReplicated,
        files_evicted = FilesEvicted
    }}} = transfer:get(TransferId),
    LastUpdate = get_last_update(Transfer),
    {ok, [
        {<<"id">>, TransferId},
        {<<"status">>, get_status(Transfer)},
        {<<"timestamp">>, LastUpdate},
        {<<"replicatedBytes">>, BytesReplicated},
        {<<"replicatedFiles">>, FilesReplicated},
        {<<"evictedFiles">>, FilesEvicted}
    ]}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns status of given transfer. Replaces active status with 'replicating'
%% for replication and 'evicting' for eviction.
%% In case of migration 'evicting' indicates that the transfer itself has
%% finished, but source replica eviction is still in progress.
%% @end
%%--------------------------------------------------------------------
-spec get_status(transfer:record()) ->
    transfer:status() | evicting | replicating.
get_status(T = #transfer{
    replication_status = completed,
    replicating_provider = P1,
    evicting_provider = P2
}) when is_binary(P1) andalso is_binary(P2) ->
    case T#transfer.eviction_status of
        scheduled -> evicting;
        enqueued -> evicting;
        active -> evicting;
        Status -> Status
    end;
get_status(T = #transfer{replication_status = skipped}) ->
    case T#transfer.eviction_status of
        active -> evicting;
        Status -> Status
    end;
get_status(#transfer{replication_status = active}) -> replicating;
get_status(#transfer{replication_status = Status}) -> Status.


-spec get_last_update(#transfer{}) -> non_neg_integer().
get_last_update(#transfer{start_time = StartTime, last_update = LastUpdateMap}) ->
    % It is possible that there is no last update, if 0 bytes were
    % transferred, in this case take the start time.
    lists:max([StartTime | maps:values(LastUpdateMap)]).


-spec get_histograms(TransferStats :: #transfer{} | #space_transfer_stats{},
    HistogramsType :: binary()) -> transfer_histograms:histograms().
get_histograms(TransferStats, ?MINUTE_STAT_TYPE) ->
    case TransferStats of
        #transfer{min_hist = Histograms} -> Histograms;
        #space_transfer_stats{min_hist = Histograms} -> Histograms
    end;
get_histograms(TransferStats, ?HOUR_STAT_TYPE) ->
    case TransferStats of
        #transfer{hr_hist = Histograms} -> Histograms;
        #space_transfer_stats{hr_hist = Histograms} -> Histograms
    end;
get_histograms(TransferStats, ?DAY_STAT_TYPE) ->
    case TransferStats of
        #transfer{dy_hist = Histograms} -> Histograms;
        #space_transfer_stats{dy_hist = Histograms} -> Histograms
    end;
get_histograms(TransferStats, ?MONTH_STAT_TYPE) ->
    case TransferStats of
        #transfer{mth_hist = Histograms} -> Histograms;
        #space_transfer_stats{mth_hist = Histograms} -> Histograms
    end.
