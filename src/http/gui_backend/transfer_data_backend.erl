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
find_record(<<"transfer">>, TransferId) ->
    transfer_record(TransferId);

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
    FileGuid = proplists:get_value(<<"file">>, Data),
    Migration = proplists:get_value(<<"migration">>, Data),
    MigrationSource = proplists:get_value(<<"migrationSource">>, Data),
    Destination = proplists:get_value(<<"destination">>, Data),
    {ok, TransferId} = case Migration of
        false ->
            logical_file_manager:schedule_file_replication(
                SessionId, {guid, FileGuid}, Destination
            );
        true ->
            logical_file_manager:schedule_replica_invalidation(
                SessionId, {guid, FileGuid}, MigrationSource, Destination
            )
    end,
    transfer_record(TransferId).


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


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant transfer record based on transfer id.
%% @end
%%--------------------------------------------------------------------
-spec transfer_record(transfer:id()) -> {ok, proplists:proplist()}.
transfer_record(TransferId) ->
    SessionId = gui_session:get_session_id(),
    {ok, #document{value = Transfer = #transfer{
        source_provider_id = SourceProviderId,
        target_provider_id = DestinationProviderId,
        file_uuid = FileUuid,
        path = Path,
        user_id = UserId,
        space_id = SpaceId,
        start_time = StartTime
    }}} = transfer:get(TransferId),
    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
    FileType = case logical_file_manager:stat(SessionId, {guid, FileGuid}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} -> <<"dir">>;
        {ok, _} -> <<"file">>;
        {error, ?ENOENT} -> <<"deleted">>;
        {error, _} -> <<"unknown">>
    end,
    IsOngoing = transfer_utils:is_ongoing(Transfer),
    FinishTime = case IsOngoing of
        true -> null;
        false -> Transfer#transfer.finish_time
    end,
    IsInvalidation = transfer_utils:is_invalidation(Transfer),
    {ok, [
        {<<"id">>, TransferId},
        {<<"migration">>, IsInvalidation},
        {<<"migrationSource">>, SourceProviderId},
        {<<"destination">>, utils:ensure_defined(DestinationProviderId, undefined, null)},
        {<<"isOngoing">>, IsOngoing},
        {<<"space">>, SpaceId},
        {<<"file">>, FileGuid},
        {<<"path">>, Path},
        {<<"fileType">>, FileType},
        {<<"systemUserId">>, UserId},
        {<<"startTime">>, StartTime},
        {<<"finishTime">>, FinishTime},
        {<<"currentStat">>, TransferId},
        {<<"minuteStat">>, op_gui_utils:ids_to_association(?MINUTE_STAT_TYPE, TransferId)},
        {<<"hourStat">>, op_gui_utils:ids_to_association(?HOUR_STAT_TYPE, TransferId)},
        {<<"dayStat">>, op_gui_utils:ids_to_association(?DAY_STAT_TYPE, TransferId)},
        {<<"monthStat">>, op_gui_utils:ids_to_association(?MONTH_STAT_TYPE, TransferId)}
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
        {<<"destination">>, ProviderId},
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
%% Get and prepare for further processing histograms of specified type
%% for transfer of given type and id.
%% @end
%%--------------------------------------------------------------------
-spec prepare_histograms(TransferType :: binary(), HistogramsType :: binary(),
    Id :: binary()
) -> {transfer_histograms:histograms(), StartTime :: non_neg_integer(),
    LastUpdate :: non_neg_integer(), TimeWindow :: non_neg_integer()}.
prepare_histograms(?JOB_TRANSFERS_TYPE, HistogramsType, TransferId) ->
    {ok, #document{value = T}} = transfer:get(TransferId),
    StartTime = T#transfer.start_time,

    % Return historical statistics of finished transfers intact. As for active ones,
    % pad them with zeroes to current time and erase recent n-seconds to avoid
    % fluctuations on charts due to synchronization of docs between providers
    {Histograms, LastUpdate, TimeWindow} = case transfer_utils:is_ongoing(T) of
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
    {ok, TransferStats} = space_transfer_stats:get(TransferStatsId),
    LastUpdates = TransferStats#space_transfer_stats.last_update,
    CurrentTime = provider_logic:zone_time_seconds(),
    StartTime = 0,
    {Histograms, LastUpdate, TimeWindow} = prepare_histograms(
        TransferStats, HistogramsType, CurrentTime, LastUpdates
    ),
    {Histograms, StartTime, LastUpdate, TimeWindow}.


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
-spec prepare_histograms(TransferStats :: #transfer{} | #space_transfer_stats{},
    HistogramsType :: binary(), CurrentTime :: non_neg_integer(),
    LastUpdates :: #{od_provider:id() => non_neg_integer()}
) ->
    {Histograms :: transfer_histograms:histograms(),
        Timestamp :: non_neg_integer(), TimeWindow :: non_neg_integer()}.
prepare_histograms(TransferStats, ?MINUTE_STAT_TYPE, CurrentTime, LastUpdates) ->
    Histograms = get_histograms(TransferStats, ?MINUTE_STAT_TYPE),
    Window = ?FIVE_SEC_TIME_WINDOW,
    PaddedHistograms = transfer_histograms:pad_with_zeroes(
        Histograms, Window, CurrentTime, LastUpdates
    ),
    {TrimmedHistograms, NewTimestamp} = transfer_histograms:trim_min_histograms(
        PaddedHistograms, CurrentTime
    ),
    {TrimmedHistograms, NewTimestamp, Window};

prepare_histograms(TransferStats, HistogramsType, CurrentTime, LastUpdates) ->
    MinHistograms = get_histograms(TransferStats, ?MINUTE_STAT_TYPE),
    RequestedHistograms = get_histograms(TransferStats, HistogramsType),
    TimeWindow = transfer_histograms:type_to_time_window(HistogramsType),

    PaddedMinHistograms = transfer_histograms:pad_with_zeroes(
        MinHistograms, ?FIVE_SEC_TIME_WINDOW, CurrentTime, LastUpdates
    ),
    PaddedRequestedHistograms = transfer_histograms:pad_with_zeroes(
        RequestedHistograms, TimeWindow, CurrentTime, LastUpdates
    ),
    {_, TrimmedRequestedHistograms, NewTimestamp} = transfer_histograms:trim(
        PaddedMinHistograms, PaddedRequestedHistograms, TimeWindow, CurrentTime
    ),

    {TrimmedRequestedHistograms, NewTimestamp, TimeWindow}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant transfer-current-stat record based on transfer id.
%% @end
%%--------------------------------------------------------------------
-spec transfer_current_stat_record(transfer:id()) -> {ok, proplists:proplist()}.
transfer_current_stat_record(TransferId) ->
    {ok, #document{value = Transfer = #transfer{
        bytes_transferred = BytesTransferred,
        files_transferred = FilesTransferred
    }}} = transfer:get(TransferId),
    LastUpdate = get_last_update(Transfer),
    {ok, [
        {<<"id">>, TransferId},
        {<<"status">>, get_status(Transfer)},
        {<<"timestamp">>, LastUpdate},
        {<<"transferredBytes">>, BytesTransferred},
        {<<"transferredFiles">>, FilesTransferred}
    ]}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns status of given transfer. Adds one special status 'invalidating' to
%% indicate that the transfer itself has finished, but source replica
%% invalidation is still in progress (concerns only replica migration transfers).
%% @end
%%--------------------------------------------------------------------
-spec get_status(transfer:record()) -> transfer:status() | invalidating.
get_status(T = #transfer{invalidate_source_replica = true, status = completed}) ->
    case T#transfer.invalidation_status of
        completed -> completed;
        skipped -> completed;
        cancelled -> cancelled;
        failed -> failed;
        scheduled -> invalidating;
        active -> invalidating
    end;
get_status(T = #transfer{invalidate_source_replica = true, status = skipped}) ->
    case T#transfer.invalidation_status of
        completed -> completed;
        skipped -> skipped;
        cancelled -> cancelled;
        failed -> failed;
        scheduled -> invalidating;
        active -> invalidating
    end;
get_status(#transfer{
    status = active,
    files_processed = 0,
    bytes_transferred = 0
}) ->
    % transfer will be visible in GUI as active when files_processed counter > 0
    scheduled;
get_status(#transfer{status = Status}) ->
    Status.


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
