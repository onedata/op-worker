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
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

-define(MINUTE_STAT_TYPE, <<"minute">>).
-define(HOUR_STAT_TYPE, <<"hour">>).
-define(DAY_STAT_TYPE, <<"day">>).
-define(MONTH_STAT_TYPE, <<"month">>).

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
        target_provider_id = Destination,
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
    FinishTime = case transfer:is_ongoing(Transfer) of
        true -> null;
        false -> Transfer#transfer.finish_time
    end,
    IsMigration = transfer:is_migrating(Transfer),
    MigrationSource = case IsMigration of
        true -> SourceProviderId;
        false -> null
    end,
    {ok, [
        {<<"id">>, TransferId},
        {<<"migration">>, IsMigration},
        {<<"migrationSource">>, MigrationSource},
        {<<"destination">>, Destination},
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
%% (combined transfer id and prefix defining time span of histograms.
%% @end
%%--------------------------------------------------------------------
-spec transfer_time_stat_record(StatId :: binary()) -> {ok, proplists:proplist()}.
transfer_time_stat_record(StatId) ->
    {TypePrefix, TransferId} = op_gui_utils:association_to_ids(StatId),
    {ok, #document{value = T}} = transfer:get(TransferId),
    {Histograms, TimeWindow} = case TypePrefix of
        ?MINUTE_STAT_TYPE -> {T#transfer.min_hist, ?FIVE_SEC_TIME_WINDOW};
        ?HOUR_STAT_TYPE -> {T#transfer.hr_hist, ?MIN_TIME_WINDOW};
        ?DAY_STAT_TYPE -> {T#transfer.dy_hist, ?HR_TIME_WINDOW};
        ?MONTH_STAT_TYPE -> {T#transfer.mth_hist, ?DY_TIME_WINDOW}
    end,
    StartTime = T#transfer.start_time,
    LastUpdateMap = T#transfer.last_update,
    {Timestamp, ShiftedHistograms} = case transfer:is_ongoing(T) of
        true ->
            % Shift the histograms to show up-to-date values
            CurrentTime = time_utils:zone_time_seconds(),
            ShiftedHists = maps:map(fun(ProviderId, HistogramValues) ->
                LastUpdate = maps:get(ProviderId, LastUpdateMap),
                shift_histogram(HistogramValues, LastUpdate, CurrentTime, TimeWindow)
            end, Histograms),
            {CurrentTime, ShiftedHists};
        false ->
            % Do not shift - we want historical values, last update time is the
            % end. It is possible that there is no last update, if 0 bytes were
            % transferred, in this case take the start time.
            LastUpdate = lists:max([StartTime | maps:values(LastUpdateMap)]),
            {LastUpdate, Histograms}
    end,
    % Calculate bytes per sec histograms
    StatsMap = maps:map(fun(_ProviderId, HistogramValues) ->
        histogram_to_speed_chart(HistogramValues, StartTime, Timestamp, TimeWindow)
    end, ShiftedHistograms),
    {ok, [
        {<<"id">>, StatId},
        {<<"timestamp">>, Timestamp},
        {<<"type">>, TypePrefix},
        {<<"stats">>, maps:to_list(StatsMap)}
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
        start_time = StartTime,
        last_update = LastUpdateMap,
        bytes_transferred = BytesTransferred,
        files_transferred = FilesTransferred,
        min_hist = MinHistograms
    }}} = transfer:get(TransferId),
    CurrentTime = time_utils:zone_time_seconds(),
    BytesPerSec = maps:to_list(maps:map(fun(ProviderId, HistogramValues) ->
        LastUpdate = maps:get(ProviderId, LastUpdateMap),
        % Only first three values matter when calculating current speed
        [V1, V2, V3 | _] = shift_histogram(
            HistogramValues, LastUpdate, CurrentTime, ?FIVE_SEC_TIME_WINDOW
        ),
        % Take the second value as current speed (the first one changes very
        % dynamically and does not show current trend from past couple of
        % seconds).
        [_, CurrentSpeed | _] = histogram_to_speed_chart(
            [V1, V2, V3], StartTime, CurrentTime, ?FIVE_SEC_TIME_WINDOW
        ),
        CurrentSpeed
    end, MinHistograms)),
    {ok, [
        {<<"id">>, TransferId},
        {<<"status">>, get_status(Transfer)},
        {<<"timestamp">>, CurrentTime},
        {<<"transferredBytes">>, BytesTransferred},
        {<<"transferredFiles">>, FilesTransferred},
        {<<"bytesPerSec">>, BytesPerSec}
    ]}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns status of given transfer. Adds one special status 'finalizing' to
%% indicate that the transfer itself has finished, but source replica
%% invalidation is still in progress (concerns only replica migration transfers).
%% @end
%%--------------------------------------------------------------------
-spec get_status(transfer:record()) -> transfer:status() | finalizing.
get_status(T = #transfer{invalidate_source_replica = true, status = completed}) ->
    case T#transfer.invalidation_status of
        completed -> completed;
        skipped -> completed;
        cancelled -> cancelled;
        failed -> failed;
        scheduled -> finalizing;
        active -> finalizing
    end;
get_status(#transfer{status = Status}) ->
    Status.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Shifts given histogram based on its last update and current time.
%% @end
%%--------------------------------------------------------------------
-spec shift_histogram(histogram:histogram(), LastUpdate :: non_neg_integer(),
    CurrentTime :: non_neg_integer(), TimeWindow :: non_neg_integer()) ->
    histogram:histogram().
shift_histogram(HistogramValues, LastUpdate, CurrentTime, TimeWindow) ->
    Histogram = time_slot_histogram:new(LastUpdate, TimeWindow, HistogramValues),
    ShiftedHistogram = time_slot_histogram:increment(Histogram, CurrentTime, 0),
    time_slot_histogram:get_histogram_values(ShiftedHistogram).


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
    ThisWindowDuration = End - Start + 1,
    Speed = round(First / ThisWindowDuration),
    [Speed, Speed | lists:duplicate(length(Rest) - 1, null)];
histogram_to_speed_chart([First | Rest = [Second | _]], Start, End, Window) ->
    % First value must be handled in a special way, because the newest time slot
    % might have not passed yet.
    % In this case, we have at least two measurements). Current speed is
    % calculated based on anticipated measurements in the future if current
    % trend was kept.
    Duration = End - Start + 1,
    FirstSlotDuration = min(Duration, (End rem Window) + 1),
    SecondSlotDuration = min(Window, Duration - FirstSlotDuration),
    FirstSpeed = First / FirstSlotDuration,
    SecondSpeed = Second / SecondSlotDuration,
    % Calculate the current speed as would be if current trend did not change.
    AnticipatedNextSpeed = 2 * FirstSpeed - SecondSpeed,
    AnticipatedSpeed = FirstSpeed +
        (AnticipatedNextSpeed - FirstSpeed) *
            (Window / 2 + FirstSlotDuration) / Window,
    % The above might yield a negative value
    CurrentSpeed = max(0, round(AnticipatedSpeed)),
    % The rest is calculated in a simpler manner.
    [CurrentSpeed | histogram_to_speed_chart(First, Rest, Start, End, Window)].

-spec histogram_to_speed_chart(CurrentValue :: non_neg_integer(),
    histogram:histogram(), Start :: non_neg_integer(), End :: non_neg_integer(),
    Window :: non_neg_integer()) -> [non_neg_integer() | null].
histogram_to_speed_chart(_Last, [], _, _, _) ->
    % Last value should not be included (as one is added to the beginning)
    [];
histogram_to_speed_chart(_, Rest, Start, End, _) when End < Start ->
    % Pad with nulls, but skip the last value
    lists:duplicate(length(Rest), null);
histogram_to_speed_chart(Current, [Previous | Rest], Start, End, Window) ->
    Duration = End - Start + 1,
    CurrentSlotDuration = min(Duration, (End rem Window) + 1),
    PreviousSlotDuration = min(Window, Duration - CurrentSlotDuration),
    CurrentSpeed = Current / CurrentSlotDuration,
    Speed = case PreviousSlotDuration =< 0 of
        true ->
            % This is the last measured time window, ignore the previous value
            CurrentSpeed;
        false ->
            PreviousSpeed = Previous / PreviousSlotDuration,
            (CurrentSpeed + PreviousSpeed) / 2
    end,
    [round(Speed) | histogram_to_speed_chart(Previous, Rest, Start, End - CurrentSlotDuration, Window)].

