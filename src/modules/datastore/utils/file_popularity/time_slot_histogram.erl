%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Histogram in which each counter corresponds to a time slot. Each increment
%%% has to provide current timestamp, it shifts the histogram if the time since
%%% last update is longer than defined time window.
%%%
%%% NOTE: As clocks may warp backward, timestamps taken consecutively may not be
%%% monotonic, which could break the prepend-only histograms used by this
%%% module. For that reason, all timestamps are checked against the last update
%%% value and, if needed, rounded up artificially to ensure monotonicity. This
%%% may cause spikes in specific histogram windows, but ensures their integrity.
%%% @end
%%%--------------------------------------------------------------------
-module(time_slot_histogram).
-author("Tomasz Lichon").

-record(time_slot_histogram, {
    start_time :: timestamp(),
    last_update_time :: timestamp(),
    time_window :: window(),
    values :: values(),
    size :: non_neg_integer(),
    type :: type()
}).

-type type() :: normal | cumulative.
% Time unit can be arbitrary, depending on the module that uses the histogram.
% Note that the time window and consecutive update times must use the same unit.
-type timestamp() :: non_neg_integer().
% See the module description for details on monotonic timestamps.
-opaque monotonic_timestamp() :: {monotonic, timestamp()}.
% length of one time window
-type window() :: pos_integer().
-type histogram() :: #time_slot_histogram{}.
-type values() :: histogram:histogram() | cumulative_histogram:histogram().

-export_type([histogram/0, monotonic_timestamp/0]).

%% API
-export([
    new/2, new/3, new_cumulative/3,
    ensure_monotonic_timestamp/2,
    increment/2, increment/3,
    decrement/2, decrement/3,
    get_histogram_values/1, get_last_update/1,
    get_sum/1, get_average/1, get_size/1, reset_cumulative/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns new empty time_slot_histogram with given Size and TimeWindow.
%% LastUpdate and StartTime are set to 0.
%% @end
%%--------------------------------------------------------------------
-spec new(window(), histogram:size()) -> histogram().
new(TimeWindow, Size) ->
    new(0, 0, TimeWindow, Size, normal).

%%--------------------------------------------------------------------
%% @doc
%% Returns time_slot_histogram with given LastUpdateTime and
%% provided values or empty histogram of given size.
%% @end
%%--------------------------------------------------------------------
-spec new(LastUpdate :: timestamp(), window(), histogram:size() | values()) -> histogram().
new(LastUpdate, TimeWindow, HistogramValues) when is_list(HistogramValues) ->
    new(LastUpdate, LastUpdate, TimeWindow, HistogramValues, normal);
new(LastUpdate, TimeWindow, Size) when is_integer(Size) ->
    new(LastUpdate, TimeWindow, histogram:new(Size)).

%%--------------------------------------------------------------------
%% @doc
%% Returns a cumulative time_slot_histogram with given LastUpdateTime and
%% provided values or empty histogram of given size.
%% @end
%%--------------------------------------------------------------------
-spec new_cumulative(LastUpdate :: timestamp(), window(), histogram:size() | values()) -> histogram().
new_cumulative(LastUpdate, TimeWindow, HistogramValues) when is_list(HistogramValues) ->
    new(LastUpdate, LastUpdate, TimeWindow, HistogramValues, cumulative);
new_cumulative(LastUpdate, TimeWindow, Size) when is_integer(Size) ->
    new_cumulative(LastUpdate, TimeWindow, cumulative_histogram:new(Size)).

%% @private
-spec new(StartTime :: timestamp(), LastUpdate :: timestamp(),
    window(), histogram:size() | values(), type()) -> histogram().
new(StartTime, LastUpdate, TimeWindow, HistogramValues, Type) when is_list(HistogramValues) ->
    {monotonic, LastUpdate} = ensure_monotonic_timestamp(LastUpdate, StartTime),
    #time_slot_histogram{
        start_time = StartTime,
        last_update_time = LastUpdate,
        time_window = TimeWindow,
        values = HistogramValues,
        size = length(HistogramValues),
        type = Type
    };
new(StartTime, LastUpdate, TimeWindow, Size, Type = normal) when is_integer(Size) ->
    new(StartTime, LastUpdate, TimeWindow, histogram:new(Size), Type);
new(StartTime, LastUpdate, TimeWindow, Size, Type = cumulative) when is_integer(Size) ->
    new(StartTime, LastUpdate, TimeWindow, cumulative_histogram:new(Size), Type).

%%-------------------------------------------------------------------
%% @doc
%% Ensures that the given timestamp is not smaller than the last update.
%% Must be called before performing any modification on time slot histograms.
%% @end
%%-------------------------------------------------------------------
-spec ensure_monotonic_timestamp(histogram() | timestamp(), timestamp()) -> monotonic_timestamp().
ensure_monotonic_timestamp(#time_slot_histogram{last_update_time = LastUpdate}, Current) ->
    ensure_monotonic_timestamp(LastUpdate, Current);
ensure_monotonic_timestamp(LastUpdate, Current) ->
    {monotonic, max(LastUpdate, Current)}.

%%--------------------------------------------------------------------
%% @equiv update(Histogram, CurrentMonotonicTime, 1).
%% @end
%%--------------------------------------------------------------------
-spec increment(histogram(), CurrentMonotonicTime :: monotonic_timestamp()) -> histogram().
increment(Histogram, CurrentMonotonicTime) ->
    increment(Histogram, CurrentMonotonicTime, 1).

%%--------------------------------------------------------------------
%% @doc
%% Increments newest time window by N. The function shifts time slots
%% if the difference between provided CurrentMonotonicTime and LastUpdate
%% is greater than TimeWindow.
%% @end
%%--------------------------------------------------------------------
-spec increment(histogram(), CurrentMonotonicTime :: monotonic_timestamp(), N :: non_neg_integer()) ->
    histogram().
increment(TSH = #time_slot_histogram{type = Type}, CurrentMonotonicTime, N) ->
    ShiftSize = calc_shift_size(TSH, CurrentMonotonicTime),
    ShiftedHistogram = shift_values(TSH, ShiftSize),
    {monotonic, LastUpdate} = CurrentMonotonicTime,
    TSH#time_slot_histogram{
        last_update_time = LastUpdate,
        values = increment_by_type(ShiftedHistogram, Type, N)
    }.

%%--------------------------------------------------------------------
%% @doc
%% This function shifts cumulative histogram and fills shifted slots
%% with zeros.
%% NOTE!!! This function must be used carefully as it's unusual for
%% cumulative histogram to fill new slots with zeros instead of previous
%% values.
%% e.g. it can be used to reset cumulative histograms after restarts
%% @end
%%--------------------------------------------------------------------
-spec reset_cumulative(histogram(), CurrentMonotonicTime :: monotonic_timestamp()) ->
    histogram().
reset_cumulative(TSH = #time_slot_histogram{type = cumulative}, CurrentMonotonicTime) ->
    TSH2 = increment(TSH#time_slot_histogram{type = normal}, CurrentMonotonicTime, 0),
    TSH2#time_slot_histogram{type = cumulative}.

%%--------------------------------------------------------------------
%% @equiv decrement(Histogram, CurrentMonotonicTime, 1).
%% @end
%%--------------------------------------------------------------------
-spec decrement(histogram(), CurrentMonotonicTime :: monotonic_timestamp()) -> histogram().
decrement(Histogram, CurrentMonotonicTime) ->
    decrement(Histogram, CurrentMonotonicTime, 1).

%%--------------------------------------------------------------------
%% @doc
%% Decrements newest time window by N. The function shifts time slots
%% if the difference between provided CurrentMonotonicTime and LastUpdate
%% is greater than the TimeWindow.
%% @end
%%--------------------------------------------------------------------
-spec decrement(histogram(), CurrentMonotonicTime :: monotonic_timestamp(), N :: non_neg_integer()) ->
    histogram().
decrement(TSH = #time_slot_histogram{type = Type}, CurrentMonotonicTime, N) ->
    ShiftSize = calc_shift_size(TSH, CurrentMonotonicTime),
    ShiftedHistogram = shift_values(TSH, ShiftSize),
    {monotonic, LastUpdate} = CurrentMonotonicTime,
    TSH#time_slot_histogram{
        last_update_time = LastUpdate,
        values = decrement_by_type(ShiftedHistogram, Type, N)
    }.

-spec get_histogram_values(histogram()) -> histogram:histogram().
get_histogram_values(#time_slot_histogram{values = Histogram}) ->
    Histogram.

-spec get_last_update(histogram()) -> timestamp().
get_last_update(#time_slot_histogram{last_update_time = LastUpdate}) ->
    LastUpdate.

-spec get_size(histogram()) -> timestamp().
get_size(#time_slot_histogram{size = Size}) ->
    Size.

-spec get_sum(histogram()) -> non_neg_integer().
get_sum(TimeSlotHistogram) ->
    lists:sum(get_histogram_values(TimeSlotHistogram)).

-spec get_average(histogram()) -> number().
get_average(TimeSlotHistogram) ->
    get_sum(TimeSlotHistogram) / get_size(TimeSlotHistogram).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec calc_shift_size(histogram(), monotonic_timestamp()) -> non_neg_integer().
calc_shift_size(#time_slot_histogram{
    start_time = StartTime,
    last_update_time = LastUpdate,
    time_window = TimeWindow
}, {monotonic, CurrentTime}) ->
    max(0, (CurrentTime - StartTime) div TimeWindow - (LastUpdate - StartTime) div TimeWindow).

-spec shift_values(histogram(), non_neg_integer()) -> values().
shift_values(#time_slot_histogram{type = normal, values = Histogram}, ShiftSize) ->
    histogram:shift(Histogram, ShiftSize);
shift_values(#time_slot_histogram{type = cumulative, values = Histogram}, ShiftSize) ->
    cumulative_histogram:shift(Histogram, ShiftSize).

-spec increment_by_type(values(), type(), non_neg_integer()) -> values().
increment_by_type(HistogramValues, normal, N) ->
    histogram:increment(HistogramValues, N);
increment_by_type(HistogramValues, cumulative, N) ->
    cumulative_histogram:increment(HistogramValues, N).

-spec decrement_by_type(values(), type(), non_neg_integer()) -> values().
decrement_by_type(HistogramValues, normal, N) ->
    histogram:decrement(HistogramValues, N);
decrement_by_type(HistogramValues, cumulative, N) ->
    cumulative_histogram:decrement(HistogramValues, N).
