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
%%%     - space-transfers-time-stat
%%%     - space-transfers-provider-map
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
%% Returns a client-compliant transfer-time-stat record based on stat id
%% (combined transfer id and prefix defining time span of histograms).
%% @end
%%--------------------------------------------------------------------
-spec transfer_time_stat_record(StatId :: binary()) -> {ok, proplists:proplist()}.
transfer_time_stat_record(StatId) ->
    {TypePrefix, TransferId} = op_gui_utils:association_to_ids(StatId),
    {ok, #document{value = T}} = transfer:get(TransferId),
    StartTime = T#transfer.start_time,

    % Return historical statistics of finished transfers intact. As for active ones,
    % pad them with zeroes to current time and erase recent n-seconds to avoid
    % fluctuations on charts due to synchronization of docs between providers
    {CurrentHistograms, LastUpdate, TimeWindow} = case transfer_utils:is_ongoing(T) of
        false ->
            {Histograms, Window} = case TypePrefix of
                ?MINUTE_STAT_TYPE -> {T#transfer.min_hist, ?FIVE_SEC_TIME_WINDOW};
                ?HOUR_STAT_TYPE -> {T#transfer.hr_hist, ?MIN_TIME_WINDOW};
                ?DAY_STAT_TYPE -> {T#transfer.dy_hist, ?HOUR_TIME_WINDOW};
                ?MONTH_STAT_TYPE -> {T#transfer.mth_hist, ?DAY_TIME_WINDOW}
            end,
            SpeedChartLen = transfer_histograms:window_to_speed_chart_len(Window),
            RequestedHistograms = maps:map(fun(_Provider, Histogram) ->
                lists:sublist(Histogram, SpeedChartLen)
            end, Histograms),
            {RequestedHistograms, get_last_update(T), Window};
        true ->
            LastUpdates = T#transfer.last_update,
            CurrentTime = provider_logic:zone_time_seconds(),
            MinStats = {T#transfer.min_hist, ?FIVE_SEC_TIME_WINDOW},
            Stats = [MinStats | case TypePrefix of
                ?MINUTE_STAT_TYPE -> [];
                ?HOUR_STAT_TYPE -> [{T#transfer.hr_hist, ?MIN_TIME_WINDOW}];
                ?DAY_STAT_TYPE -> [{T#transfer.dy_hist, ?HOUR_TIME_WINDOW}];
                ?MONTH_STAT_TYPE -> [{T#transfer.mth_hist, ?DAY_TIME_WINDOW}]
            end],
            PaddedStats = lists:map(fun({Histograms, Window}) ->
                PaddedHistograms = transfer_histograms:pad(
                    Histograms, Window, CurrentTime, LastUpdates
                ),
                {PaddedHistograms, Window}
            end, Stats),
            {TrimmedStats, TrimmedTime} = transfer_histograms:trim(
                PaddedStats, CurrentTime
            ),
            {RequestedHistograms, Window} = lists:last(TrimmedStats),
            {RequestedHistograms, TrimmedTime, Window}
    end,

    % Calculate bytes per sec histograms
    SpeedCharts = maps:map(fun(_ProviderId, Histogram) ->
        transfer_histograms:histogram_to_speed_chart(
            Histogram, StartTime, LastUpdate, TimeWindow
        )
    end, CurrentHistograms),

    {ok, [
        {<<"id">>, StatId},
        {<<"timestamp">>, LastUpdate},
        {<<"type">>, TypePrefix},
        {<<"stats">>, maps:to_list(SpeedCharts)}
    ]}.


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
