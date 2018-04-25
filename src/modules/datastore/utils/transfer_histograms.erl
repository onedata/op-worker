%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Basic utils for transfer histograms.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_histograms).
-author("Bartosz Walkowicz").
-author("Lukasz Opiola").

-include("modules/datastore/transfer.hrl").

% possible types are: <<"minute">>, <<"hour">>, <<"day">> and <<"month">>.
-type type() :: binary().
-type histograms() :: #{od_provider:id() => histogram:histogram()}.
-type timestamp() :: non_neg_integer().
-type timestamps() :: #{od_provider:id() => non_neg_integer()}.

%% API
-export([
    new/2, update/6,
    pad_with_zeroes/4,
    trim_min_histograms/2, trim_histograms/4, trim_timestamp/1,
    type_to_time_window/1, type_to_hist_length/1,
    to_speed_charts/4
]).

-ifdef(TEST).
%% Export for unit testing
-export([histogram_to_speed_chart/4]).
-endif.

%%%===================================================================
%%% API
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% Creates a new transfer_histograms based on specified type and
%% bytes per provider.
%% @end
%%-------------------------------------------------------------------
-spec new(BytesPerProvider :: #{od_provider:id() => non_neg_integer()},
    HistogramsType :: type()) -> histograms().
new(BytesPerProvider, HistogramsType) ->
    HistogramLength = type_to_hist_length(HistogramsType),
    maps:map(fun(_ProviderId, Bytes) ->
        histogram:increment(histogram:new(HistogramLength), Bytes)
    end, BytesPerProvider).


%%-------------------------------------------------------------------
%% @doc
%% Updates transfer_histograms for specified providers.
%% Specified CurrentTime is assumed to be greater than LastUpdate.
%% @end
%%-------------------------------------------------------------------
-spec update(BytesPerProvider :: #{od_provider:id() => non_neg_integer()},
    histograms(), HistogramsType :: type(), LastUpdates :: timestamps(),
    StartTime :: timestamp(), CurrentTime :: timestamp()
) ->
    histograms().
update(BytesPerProvider, Histograms, HistogramsType,
    LastUpdates, StartTime, CurrentTime
) ->
    Window = type_to_time_window(HistogramsType),
    HistogramLength = type_to_hist_length(HistogramsType),
    maps:fold(fun(ProviderId, Bytes, OldHistograms) ->
        Histogram = case maps:find(ProviderId, OldHistograms) of
            {ok, OldHistogram} ->
                LastUpdate = maps:get(ProviderId, LastUpdates, StartTime),
                ShiftSize = (CurrentTime div Window) - (LastUpdate div Window),
                histogram:shift(OldHistogram, ShiftSize);
            error ->
                histogram:new(HistogramLength)
        end,
        OldHistograms#{ProviderId => histogram:increment(Histogram, Bytes)}
    end, Histograms, BytesPerProvider).


%%-------------------------------------------------------------------
%% @doc
%% Pad histograms with zeros since last update to specified current time.
%% If current time is smaller than last update, then left given histogram
%% intact.
%% @end
%%-------------------------------------------------------------------
-spec pad_with_zeroes(histograms(), Window :: non_neg_integer(),
    LastUpdates :: timestamps(), CurrentTime :: timestamp()) -> histograms().
pad_with_zeroes(Histograms, Window, LastUpdates, CurrentTime) ->
    maps:map(fun(Provider, Histogram) ->
        LastUpdate = maps:get(Provider, LastUpdates),
        ShiftSize = max(0, (CurrentTime div Window) - (LastUpdate div Window)),
        histogram:shift(Histogram, ShiftSize)
    end, Histograms).


%%-------------------------------------------------------------------
%% @doc
%% Erase recent n-seconds of histograms based on difference between expected
%% slots in minute speed histograms and bytes_sent histograms (it helps to
%% avoid fluctuations on charts due to synchronization between providers).
%% @end
%%-------------------------------------------------------------------
-spec trim_min_histograms(histograms(), LastUpdate :: timestamp()) ->
    {histograms(), NewTimestamp :: timestamp()}.
trim_min_histograms(Histograms, LastUpdate) ->
    SlotsToRemove = ?MIN_HIST_LENGTH - ?MIN_SPEED_HIST_LENGTH,
    TrimmedHistograms = maps:map(fun(_Provider, Histogram) ->
        {_, NewHistogram} = lists:split(SlotsToRemove, Histogram),
        NewHistogram
    end, Histograms),
    {TrimmedHistograms, trim_timestamp(LastUpdate)}.


%%-------------------------------------------------------------------
%% @doc
%% Erase recent n-seconds of histograms. To that minute histograms are required
%% as reference (to calculate bytes to remove based on difference between
%% expected slots in minute speed histograms and bytes_sent histograms.
%% Also shorten histograms length to that of equivalent speed histogram
%% (necessary before converting bytes_sent histograms to speed histograms).
%% @end
%%-------------------------------------------------------------------
-spec trim_histograms(MinHistograms, RequestedHistograms,
    TimeWindow :: non_neg_integer(), LastUpdate :: timestamp()
) -> {MinHistograms, RequestedHistograms, Timestamp :: timestamp()}
    when MinHistograms :: histograms(), RequestedHistograms :: histograms().
trim_histograms(MinHistograms, RequestedHistograms, TimeWindow, LastUpdate) ->
    NewTimestamp = trim_timestamp(LastUpdate),
    TrimFun = fun(OldMinHist, [FstSlot, SndSlot | Rest] = _OldRequestedHist) ->
        % Remove recent slots from minute histogram and calculate bytes
        % to remove from other histogram (using removed slots)
        MinSlotsToRemove = ?MIN_HIST_LENGTH - ?MIN_SPEED_HIST_LENGTH,
        {RemovedSlots, NewMinHist} = lists:split(MinSlotsToRemove, OldMinHist),
        RemovedBytes = lists:sum(RemovedSlots),
        % If bytes to remove exceed or equal number stored in first slot,
        % check whether new timestamp is still in current slot or moved back
        % and if so remove head slot.
        NewRequestedHist = case RemovedBytes >= FstSlot of
            true ->
                PreviousTimeSlot = LastUpdate div TimeWindow,
                CurrentTimeSlot = NewTimestamp div TimeWindow,
                case PreviousTimeSlot == CurrentTimeSlot of
                    true ->
                        % If we are still in the same time slot
                        % then RemovedBytes == FstSlot
                        [0, SndSlot | Rest];
                    false ->
                        [SndSlot - (RemovedBytes - FstSlot) | Rest]
                end;
            false ->
                [FstSlot - RemovedBytes, SndSlot | Rest]
        end,
        {NewMinHist, NewRequestedHist}
    end,

    {TrimmedMinHistograms, TrimmedRequestedHistograms} = maps:fold(
        fun(Provider, Hist1, {OldMinHistograms, OldRequestedHistograms}) ->
            Hist2 = maps:get(Provider, RequestedHistograms),
            {NewHist1, NewHist2} = TrimFun(Hist1, Hist2),
            {
                OldMinHistograms#{Provider => NewHist1},
                OldRequestedHistograms#{Provider => NewHist2}
            }
        end, {#{}, #{}}, MinHistograms
    ),
    {TrimmedMinHistograms, TrimmedRequestedHistograms, NewTimestamp}.


%%-------------------------------------------------------------------
%% @doc
%% Erase recent n-seconds of timestamp based on difference between expected
%% slots in minute speed histograms and bytes_sent histograms (it helps to
%% avoid fluctuations on charts due to synchronization between providers).
%% @end
%%-------------------------------------------------------------------
-spec trim_timestamp(Timestamp :: timestamp()) -> timestamp().
trim_timestamp(Timestamp) ->
    FullSlotsToSub = ?MIN_HIST_LENGTH - ?MIN_SPEED_HIST_LENGTH - 1,
    FullSlotsToSubTime = FullSlotsToSub * ?FIVE_SEC_TIME_WINDOW,
    RecentSlotDuration = (Timestamp rem ?FIVE_SEC_TIME_WINDOW) + 1,
    Timestamp - RecentSlotDuration - FullSlotsToSubTime.


%%--------------------------------------------------------------------
%% @doc
%% Converts transfer histograms to speed charts. For that number of required
%% time slots is deduced, took and convert to speed chart for each histogram.
%% If StartTime is greater then EndTime return empty charts.
%% @end
%%--------------------------------------------------------------------
-spec to_speed_charts(histograms(), StartTime :: timestamp(),
    EndTime :: timestamp(), TimeWindow :: non_neg_integer()) -> histograms().
to_speed_charts(_, StartTime, EndTime, _) when StartTime > EndTime ->
    #{};
to_speed_charts(Histograms, StartTime, EndTime, TimeWindow) ->
    MaxRequiredSlotsNum = window_to_speed_chart_len(TimeWindow),
    % If last time slot is fully filled (all n slots are fully filled ->
    % there are n+1 points on chart), the number of required slots is one less
    % than if it were only partially filled (n-1 slots are fully filled
    % and 2 only partially - on start and on end -> n+2 points on charts)
    RequiredSlotsNum = case (EndTime rem TimeWindow) + 1 of
        TimeWindow -> MaxRequiredSlotsNum - 1;
        _ -> MaxRequiredSlotsNum
    end,
    maps:map(fun(_ProviderId, Histogram) ->
        RequiredSlots = lists:sublist(Histogram, RequiredSlotsNum),
        histogram_to_speed_chart(RequiredSlots, StartTime, EndTime, TimeWindow)
    end, Histograms).


-spec type_to_time_window(type()) -> non_neg_integer().
type_to_time_window(?MINUTE_STAT_TYPE) -> ?FIVE_SEC_TIME_WINDOW;
type_to_time_window(?HOUR_STAT_TYPE) -> ?MIN_TIME_WINDOW;
type_to_time_window(?DAY_STAT_TYPE) -> ?HOUR_TIME_WINDOW;
type_to_time_window(?MONTH_STAT_TYPE) -> ?DAY_TIME_WINDOW.


-spec type_to_hist_length(type()) -> non_neg_integer().
type_to_hist_length(?MINUTE_STAT_TYPE) -> ?MIN_HIST_LENGTH;
type_to_hist_length(?HOUR_STAT_TYPE) -> ?HOUR_HIST_LENGTH;
type_to_hist_length(?DAY_STAT_TYPE) -> ?DAY_HIST_LENGTH;
type_to_hist_length(?MONTH_STAT_TYPE) -> ?MONTH_HIST_LENGTH.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
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
-spec histogram_to_speed_chart(histogram:histogram(), Start :: timestamp(),
    End :: timestamp(), Window :: non_neg_integer()) -> [non_neg_integer() | null].
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
    histogram:histogram(), Start :: timestamp(), End :: timestamp(),
    Window :: non_neg_integer(), ChartStart :: timestamp()) ->
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


-spec window_to_speed_chart_len(non_neg_integer()) -> non_neg_integer().
window_to_speed_chart_len(?FIVE_SEC_TIME_WINDOW) -> ?MIN_SPEED_HIST_LENGTH;
window_to_speed_chart_len(?MIN_TIME_WINDOW) -> ?HOUR_SPEED_HIST_LENGTH;
window_to_speed_chart_len(?HOUR_TIME_WINDOW) -> ?DAY_SPEED_HIST_LENGTH;
window_to_speed_chart_len(?DAY_TIME_WINDOW) -> ?MONTH_SPEED_HIST_LENGTH.
