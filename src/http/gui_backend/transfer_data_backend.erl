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

-type timestamp() :: non_neg_integer().
-type histograms() :: maps:map(od_provider:id(), [non_neg_integer()]).

%% API
-export([init/0, terminate/0]).
-export([find_record/2, find_all/1, query/2, query_record/2]).
-export([create_record/2, update_record/3, delete_record/2]).

-ifdef(TEST).
%% Export for unit testing
-export([histogram_to_speed_chart/4]).
-endif.

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
    {CurrentStats, LastUpdate, TimeWindow} = case transfer_utils:is_ongoing(T) of
        false ->
            ChartLen = transfer_histogram:stats_type_to_speed_chart_len(TypePrefix),
            StripFun = fun(_Provider, Hist) -> lists:sublist(Hist, ChartLen) end,
            {Histograms, Window} = case TypePrefix of
                ?MINUTE_STAT_TYPE -> {T#transfer.min_hist, ?FIVE_SEC_TIME_WINDOW};
                ?HOUR_STAT_TYPE -> {T#transfer.hr_hist, ?MIN_TIME_WINDOW};
                ?DAY_STAT_TYPE -> {T#transfer.dy_hist, ?HOUR_TIME_WINDOW};
                ?MONTH_STAT_TYPE -> {T#transfer.mth_hist, ?DAY_TIME_WINDOW}
            end,
            {maps:map(StripFun, Histograms), get_last_update(T), Window};
        true ->
            LastUpdates = T#transfer.last_update,
            CurrentTime = provider_logic:zone_time_seconds(),
            MinStats = {T#transfer.min_hist, ?MINUTE_STAT_TYPE},
            Stats = [MinStats | case TypePrefix of
                ?MINUTE_STAT_TYPE -> [];
                ?HOUR_STAT_TYPE -> [{T#transfer.hr_hist, ?HOUR_STAT_TYPE}];
                ?DAY_STAT_TYPE -> [{T#transfer.dy_hist, ?DAY_STAT_TYPE}];
                ?MONTH_STAT_TYPE -> [{T#transfer.mth_hist, ?MONTH_STAT_TYPE}]
            end],
            PaddedStats = pad_stats(Stats, CurrentTime, LastUpdates),
            {TrimmedStats, TrimmedTime} = trim_stats(PaddedStats, CurrentTime),
            {RequestedHistograms, _} = lists:last(TrimmedStats),
            Window = transfer_histogram:stats_type_to_time_window(TypePrefix),
            {RequestedHistograms, TrimmedTime, Window}
    end,

    % Calculate bytes per sec histograms
    VelocityStats = maps:map(fun(_ProviderId, Histogram) ->
        histogram_to_speed_chart(Histogram, StartTime, LastUpdate, TimeWindow)
    end, CurrentStats),

    {ok, [
        {<<"id">>, StatId},
        {<<"timestamp">>, LastUpdate},
        {<<"type">>, TypePrefix},
        {<<"stats">>, maps:to_list(VelocityStats)}
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
    files_processed = 0
}) ->
    % transfer will be visible in GUI as active when files_processed counter > 0
    scheduled;
get_status(#transfer{status = Status}) ->
    Status.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Pad ongoing transfer histograms with zeroes if no bytes were send recently.
%% @end
%%--------------------------------------------------------------------
-spec pad_stats(Stats :: [{Histograms :: histograms(), StatsType :: binary()}],
    CurrentTime :: timestamp(), LastUpdates :: #{od_provider:id() => timestamp()}
) ->
    [{histograms(), binary()}].
pad_stats(Stats, CurrentTime, LastUpdates) ->
    lists:map(fun({Histograms, StatsType}) ->
        TimeWindow = transfer_histogram:stats_type_to_time_window(StatsType),
        ZeroedHist = transfer_histogram:new_time_slot_histogram(
            CurrentTime, TimeWindow
        ),
        PaddedHistograms = maps:map(fun(Provider, Hist) ->
            LastUpdate = maps:get(Provider, LastUpdates),
            TimeSlotHist = time_slot_histogram:new(LastUpdate, TimeWindow, Hist),
            NewHist = time_slot_histogram:merge(ZeroedHist, TimeSlotHist),
            time_slot_histogram:get_histogram_values(NewHist)
        end, Histograms),
        {PaddedHistograms, StatsType}
    end, Stats).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Erase recent 30s of histograms to avoid fluctuations on charts.
%% @end
%%--------------------------------------------------------------------
-spec trim_stats(Stats :: [{Histograms :: histograms(), StatsType :: binary()}],
    CurrentTime :: timestamp()
) ->
    {[{histograms(), binary()}], timestamp()}.
trim_stats([{MinStats, _} | _] = Stats, CurrentTime) when map_size(MinStats) == 0 ->
    NewTimestamp = transfer_histogram:trim_timestamp(CurrentTime),
    {Stats, NewTimestamp};

trim_stats([{MinStats, ?MINUTE_STAT_TYPE}], CurrentTime) ->
    NewTimestamp = transfer_histogram:trim_timestamp(CurrentTime),
    MinSlotsToRemove = ?MIN_HIST_LENGTH - ?MIN_SPEED_HIST_LENGTH,
    TrimFun = fun(_Provider, Histogram) ->
        {_, NewHistogram} = lists:split(MinSlotsToRemove, Histogram),
        NewHistogram
    end,
    {[{maps:map(TrimFun, MinStats), ?MINUTE_STAT_TYPE}], NewTimestamp};

trim_stats([{MinStats, _}, {RequestedStats, StatsType}], CurrentTime) ->
    NewTimestamp = transfer_histogram:trim_timestamp(CurrentTime),
    TimeWindow = transfer_histogram:stats_type_to_time_window(StatsType),
    TrimFun = fun(OldHist1, [FstSlot, SndSlot | Rest]) ->
        MinSlotsToRemove = ?MIN_HIST_LENGTH - ?MIN_SPEED_HIST_LENGTH,
        {RemovedSlots, NewHist1} = lists:split(MinSlotsToRemove, OldHist1),
        RemovedBytes = lists:sum(RemovedSlots),
        OldHist2 = case RemovedBytes > FstSlot of
            true -> [0, SndSlot - (RemovedBytes - FstSlot) | Rest];
            false -> [FstSlot - RemovedBytes, SndSlot | Rest]
        end,
        Hist2 = case (CurrentTime div TimeWindow) == (NewTimestamp div TimeWindow) of
            true -> lists:droplast(OldHist2);
            false -> OldHist2
        end,
        Len = transfer_histogram:stats_type_to_speed_chart_len(StatsType),
        NewHist2 = lists:sublist(Hist2, Len),
        {NewHist1, NewHist2}
    end,

    {NewMinStats, NewRequestedStats} = maps:fold(
        fun(Provider, Hist1, {OldMinStats, OldRequestedStats}) ->
            Hist2 = maps:get(Provider, RequestedStats),
            {NewHist1, NewHist2} = TrimFun(Hist1, Hist2),
            {
                OldMinStats#{Provider => NewHist1},
                OldRequestedStats#{Provider => NewHist2}
            }
        end, {#{}, #{}}, MinStats
    ),
    NewStats = [
        {NewMinStats, ?MINUTE_STAT_TYPE},
        {NewRequestedStats, StatsType}
    ],
    {NewStats, NewTimestamp}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calculates average speed chart based on bytes histogram.
%% The produced array contains integers indicating speed (B/s) and 'null' atoms
%% whenever measurement from given time slot does not exist.
%% The values have the following meaning:
%%  [0] - anticipated current speed if current trend wouldn't change, at the
%%      moment of End timestamp
%%  [1..length-2] - calculated average speeds at the moments in the beginning of
%%      every time slot, i.e. timestamps which satisfy:
%%      Timestamp % TimeWindow = 0
%%  [length-1] - calculated average speed for the starting point of the transfer,
%%      at the moment of Start timestamp
%% NOTE: time slots are created for absolute time rather than actual duration
%% of the transfer. For example, for hour time window, time slots would be:
%%  8:00:00 - 8:59:59
%%  9:00:00 - 9:59:59
%%  etc.
%% A transfer that started at 8:55 and ended at 9:05 would have measurements
%% for both above time slots (when hour window is considered).
%% @end
%%--------------------------------------------------------------------
-spec histogram_to_speed_chart(histogram:histogram(), Start :: non_neg_integer(),
    End :: non_neg_integer(), Window :: non_neg_integer()) ->
    [non_neg_integer() | null].
histogram_to_speed_chart([First | Rest], Start, End, Window) when (End div Window) == (Start div Window) ->
    % First value must be handled in a special way, because the newest time slot
    % might have not passed yet.
    % In this case, we only have measurements from one time window. We take the
    % average speed and create a simple chart with two, same measurements.
    FirstSlotDuration = End - Start + 1,
    Speed = round(First / FirstSlotDuration),
    [Speed, Speed | lists:duplicate(length(Rest), null)];
histogram_to_speed_chart([First | Rest = [Previous | _]], Start, End, Window) ->
    % First value must be handled in a special way, because the newest time slot
    % might have not passed yet.
    % In this case, we have at least two measurements. Current speed is
    % calculated based on anticipated measurements in the future if current
    % trend was kept.
    Duration = End - Start + 1,
    FirstSlotDuration = min(Duration, (End rem Window) + 1),
    PreviousSlotDuration = min(Window, Duration - FirstSlotDuration),
    FirstSpeed = First / FirstSlotDuration,
    Average = (First + Previous) / (FirstSlotDuration + PreviousSlotDuration),
    % Calculate the current speed as would be if current trend did not change,
    % scale by the duration of the window.
    AnticipatedCurrentSpeed = Average + (FirstSpeed - Average) * FirstSlotDuration / Window,
    % The above might yield a negative value
    CurrentSpeed = max(0, round(AnticipatedCurrentSpeed)),
    % Calculate where the chart span starts
    ChartStart = max(Start, End - speed_chart_span(Window) + 1),
    [CurrentSpeed | histogram_to_speed_chart(First, Rest, Start, End, Window, ChartStart)].

-spec histogram_to_speed_chart(CurrentValue :: non_neg_integer(),
    histogram:histogram(), Start :: non_neg_integer(), End :: non_neg_integer(),
    Window :: non_neg_integer(), ChartStart :: non_neg_integer()) ->
    [non_neg_integer() | null].
histogram_to_speed_chart(Current, [Previous | Rest], Start, End, Window, ChartStart) ->
    Duration = End - ChartStart + 1,
    CurrentSlotDuration = min(Duration, (End rem Window) + 1),
    DurationWithoutCurrent = Duration - CurrentSlotDuration,
    PreviousSlotDuration = min(Window, DurationWithoutCurrent),
    % Check if we are considering the last two slots
    case DurationWithoutCurrent =< Window of
        true ->
            % Calculate the time that passed when collecting data for the
            % previous slot. It might be shorter than the window if the transfer
            % started anywhere in the middle of the window. It might also be different
            % than PreviousSlotDuration, if some of the measurement duration does not
            % fit the chart.
            PreviousSlotMeasurementTime = min(End - Start + 1 - CurrentSlotDuration, Window),
            Speed = (Current + Previous) / (CurrentSlotDuration + PreviousSlotMeasurementTime),
            % Scale the previous speed depending how much time of the window
            % will not fit the chart.
            PreviousSpeed = Previous / PreviousSlotMeasurementTime,
            PreviousScaled = Speed + PreviousSlotDuration / PreviousSlotMeasurementTime * (PreviousSpeed - Speed),
            [round(Speed), round(PreviousScaled) | lists:duplicate(length(Rest), null)];
        false ->
            Speed = round((Current + Previous) / (CurrentSlotDuration + PreviousSlotDuration)),
            [Speed | histogram_to_speed_chart(Previous, Rest, Start, End - CurrentSlotDuration, Window, ChartStart)]
    end.


-spec speed_chart_span(TimeWindow :: non_neg_integer()) -> non_neg_integer().
speed_chart_span(?FIVE_SEC_TIME_WINDOW) -> 60;
speed_chart_span(?MIN_TIME_WINDOW) -> 3600;
speed_chart_span(?HOUR_TIME_WINDOW) -> 86400; % 24 hours
speed_chart_span(?DAY_TIME_WINDOW) -> 2592000. % 30 days


-spec get_last_update(#transfer{}) -> non_neg_integer().
get_last_update(#transfer{start_time = StartTime, last_update = LastUpdateMap}) ->
    % It is possible that there is no last update, if 0 bytes were
    % transferred, in this case take the start time.
    lists:max([StartTime | maps:values(LastUpdateMap)]).
