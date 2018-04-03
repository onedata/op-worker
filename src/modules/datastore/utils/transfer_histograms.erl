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

-include("modules/datastore/transfer.hrl").

-type type() :: binary().
-type histograms() :: #{od_provider:id() => histogram:histogram()}.
-type timestamp() :: non_neg_integer().

%% API
-export([
    new/3, update/6,
    pad/4, trim/2,
    type_to_time_window/1, type_to_hist_length/1, window_to_speed_chart_len/1,
    histogram_to_speed_chart/4
]).


%%%===================================================================
%%% API
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% Creates a new transfer_histograms based on ProviderId, Bytes and Type.
%% @end
%%-------------------------------------------------------------------
-spec new(ProviderId :: od_provider:id(), Bytes :: non_neg_integer(),
    HistogramsType :: type()) -> histograms().
new(ProviderId, Bytes, HistogramsType) ->
    Histogram = histogram:new(type_to_hist_length(HistogramsType)),
    #{ProviderId => histogram:increment(Histogram, Bytes)}.


%%-------------------------------------------------------------------
%% @doc
%% Updates transfer_histograms for specified provider.
%% @end
%%-------------------------------------------------------------------
-spec update(ProviderId :: od_provider:id(), Bytes :: non_neg_integer(),
    Histograms, HistogramsType :: type(), LastUpdate :: timestamp(),
    CurrentTime :: timestamp()) -> Histograms when Histograms :: histograms().
update(ProviderId, Bytes, Histograms, HistogramsType, LastUpdate, CurrentTime) ->
    Histogram = case maps:find(ProviderId, Histograms) of
        {ok, OldHistogram} ->
            Window = type_to_time_window(HistogramsType),
            ShiftSize = (CurrentTime div Window) - (LastUpdate div Window),
            histogram:shift(OldHistogram, ShiftSize);
        error ->
            histogram:new(type_to_hist_length(HistogramsType))
    end,
    Histograms#{ProviderId => histogram:increment(Histogram, Bytes)}.


%%-------------------------------------------------------------------
%% @doc
%% Pad histograms with zeros since last update to current time.
%% @end
%%-------------------------------------------------------------------
-spec pad(Histograms, Window :: non_neg_integer(), CurrentTime :: timestamp(),
    LastUpdates :: #{od_provider:id() => timestamp()}
) ->
    Histograms when Histograms :: histograms().
pad(Histograms, Window, CurrentTime, LastUpdates) ->
    maps:map(fun(Provider, Histogram) ->
        LastUpdate = maps:get(Provider, LastUpdates),
        ShiftSize = (CurrentTime div Window) - (LastUpdate div Window),
        histogram:shift(Histogram, ShiftSize)
    end, Histograms).


%%-------------------------------------------------------------------
%% @doc
%% Erase recent n-seconds of histograms based on difference between expected
%% slots in speed histograms and bytes_sent histograms (it helps to avoid
%% fluctuations on charts due to synchronization between providers).
%% Also shorten histograms length to that of equivalent speed histogram
%% (necessary before converting bytes_sent histograms to speed histograms).
%% @end
%%-------------------------------------------------------------------
-spec trim(Stats, CurrentTime :: timestamp()) -> {Stats, Timestamp :: timestamp()}
    when Stats :: [{
        Histograms :: transfer_histograms:histograms(),
        Window :: non_neg_integer()
    }].
trim([{MinHistograms, ?FIVE_SEC_TIME_WINDOW}], CurrentTime) ->
    NewTimestamp = trim_timestamp(CurrentTime),
    SlotsToRemove = ?MIN_HIST_LENGTH - ?MIN_SPEED_HIST_LENGTH,
    TrimFun = fun(_Provider, Histogram) ->
        {_, NewHistogram} = lists:split(SlotsToRemove, Histogram),
        NewHistogram
    end,
    {[{maps:map(TrimFun, MinHistograms), ?FIVE_SEC_TIME_WINDOW}], NewTimestamp};

trim([{MinHistograms, _}, {RequestedHistograms, TimeWindow}], CurrentTime) ->
    NewTimestamp = trim_timestamp(CurrentTime),
    TrimFun = fun(OldHist1, [FstSlot, SndSlot | Rest]) ->
        MinSlotsToRemove = ?MIN_HIST_LENGTH - ?MIN_SPEED_HIST_LENGTH,
        {RemovedSlots, NewHist1} = lists:split(MinSlotsToRemove, OldHist1),
        RemovedBytes = lists:sum(RemovedSlots),
        OldHist2 = case RemovedBytes > FstSlot of
            true -> [0, SndSlot - (RemovedBytes - FstSlot) | Rest];
            false -> [FstSlot - RemovedBytes, SndSlot | Rest]
        end,
        NewHist2 = case (CurrentTime div TimeWindow) == (NewTimestamp div TimeWindow) of
            true -> OldHist2;
            false -> tl(OldHist2)
        end,
        SpeedChartLen = window_to_speed_chart_len(TimeWindow),
        {NewHist1, lists:sublist(NewHist2, SpeedChartLen)}
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
    TrimmedStats = [
        {TrimmedMinHistograms, ?FIVE_SEC_TIME_WINDOW},
        {TrimmedRequestedHistograms, TimeWindow}
    ],
    {TrimmedStats, NewTimestamp}.


-spec type_to_time_window(binary()) -> non_neg_integer().
type_to_time_window(?MINUTE_STAT_TYPE) -> ?FIVE_SEC_TIME_WINDOW;
type_to_time_window(?HOUR_STAT_TYPE) -> ?MIN_TIME_WINDOW;
type_to_time_window(?DAY_STAT_TYPE) -> ?HOUR_TIME_WINDOW;
type_to_time_window(?MONTH_STAT_TYPE) -> ?DAY_TIME_WINDOW.


-spec type_to_hist_length(binary()) -> non_neg_integer().
type_to_hist_length(?MINUTE_STAT_TYPE) -> ?MIN_HIST_LENGTH;
type_to_hist_length(?HOUR_STAT_TYPE) -> ?HOUR_HIST_LENGTH;
type_to_hist_length(?DAY_STAT_TYPE) -> ?DAY_HIST_LENGTH;
type_to_hist_length(?MONTH_STAT_TYPE) -> ?MONTH_HIST_LENGTH.


-spec window_to_speed_chart_len(non_neg_integer()) -> non_neg_integer().
window_to_speed_chart_len(?FIVE_SEC_TIME_WINDOW) -> ?MIN_SPEED_HIST_LENGTH;
window_to_speed_chart_len(?MIN_TIME_WINDOW) -> ?HOUR_SPEED_HIST_LENGTH;
window_to_speed_chart_len(?HOUR_TIME_WINDOW) -> ?DAY_SPEED_HIST_LENGTH;
window_to_speed_chart_len(?DAY_TIME_WINDOW) -> ?MONTH_SPEED_HIST_LENGTH.


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


%%%===================================================================
%%% Internal functions
%%%===================================================================


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


-spec trim_timestamp(Timestamp :: timestamp()) -> timestamp().
trim_timestamp(Timestamp) ->
    FullSlotsToSub = ?MIN_HIST_LENGTH - ?MIN_SPEED_HIST_LENGTH - 1,
    FullSlotsToSubTime = FullSlotsToSub * ?FIVE_SEC_TIME_WINDOW,
    RecentSlotDuration = (Timestamp rem ?FIVE_SEC_TIME_WINDOW) + 1,
    Timestamp - RecentSlotDuration - FullSlotsToSubTime.
