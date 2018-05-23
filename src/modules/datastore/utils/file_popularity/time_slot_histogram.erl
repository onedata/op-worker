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
%%% @end
%%%--------------------------------------------------------------------
-module(time_slot_histogram).
-author("Tomasz Lichon").

-record(time_slot_histogram, {
    start_time :: timestamp(),
    last_update_time :: timestamp(),
    time_window :: time(),
    values :: values(),
    size :: non_neg_integer(),
    type :: type()
}).

-type type() :: normal | cumulative.
-type timestamp() :: non_neg_integer().
-type time() :: pos_integer().
-type histogram() :: #time_slot_histogram{}.
-type values() :: histogram:histogram() | cumulative_histogram:histogram().

%% API
-export([
    new/2, new/3, new/4,
    increment/2, increment/3,
    decrement/2, decrement/3,
    get_histogram_values/1, get_last_update/1,
    get_sum/1, get_average/1, get_size/1, new_cumulative/3, reset_cumulative/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns new empty time_slot_histogram with given Size, TimeWindow.
%% LastUpdate and StartTime are set to 0.
%% @end
%%--------------------------------------------------------------------
-spec new(TimeWindow :: time(), Size :: histogram:size()) -> histogram().
new(TimeWindow, Size) ->
    new(0, 0, TimeWindow, Size, normal).

%%--------------------------------------------------------------------
%% @doc
%% Returns time_slot_histogram with given LastUpdateTime and
%% provided values or empty histogram of given size
%% @end
%%--------------------------------------------------------------------
-spec new(LastUpdate :: timestamp(), TimeWindow :: time(),
    histogram:size() | values()) -> histogram().
new(LastUpdate, TimeWindow, HistogramValues) when is_list(HistogramValues) ->
    new(LastUpdate, LastUpdate, TimeWindow, HistogramValues, normal);
new(LastUpdate, TimeWindow, Size) when is_integer(Size) ->
    new(LastUpdate, TimeWindow, histogram:new(Size)).

%%--------------------------------------------------------------------
%% @doc
%% Returns time_slot_histogram with given LastUpdateTime and
%% provided values or empty histogram of given size
%% @end
%%--------------------------------------------------------------------
-spec new(StartTime :: timestamp(), LastUpdate :: timestamp(), TimeWindow :: time(),
    histogram:size() | values()) -> histogram().
new(StartTime, LastUpdate, TimeWindow, HistogramValues) when is_list(HistogramValues) ->
    new(StartTime, LastUpdate, TimeWindow, HistogramValues, normal);
new(StartTime, LastUpdate, TimeWindow, Size) when is_integer(Size) ->
    new(StartTime, LastUpdate, TimeWindow, histogram:new(Size)).

%%--------------------------------------------------------------------
%% @doc
%% Returns time_slot_histogram with given LastUpdateTime and
%% provided values or empty histogram of given size
%% @end
%%--------------------------------------------------------------------
-spec new_cumulative(LastUpdate :: timestamp(), TimeWindow :: time(),
    histogram:size() | values()) -> histogram().
new_cumulative(LastUpdate, TimeWindow, HistogramValues) when is_list(HistogramValues) ->
    new(LastUpdate, LastUpdate, TimeWindow, HistogramValues, cumulative);
new_cumulative(LastUpdate, TimeWindow, Size) when is_integer(Size) ->
    new_cumulative(LastUpdate, TimeWindow, cumulative_histogram:new(Size)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns time_slot_histogram with provided values.
%% @end
%%--------------------------------------------------------------------
-spec new(StartTime :: timestamp(), LastUpdate :: timestamp(),
    TimeWindow :: time(), histogram:size() | values(), type()) -> histogram().
new(StartTime, LastUpdate, TimeWindow, HistogramValues, Type) when is_list(HistogramValues) ->
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

%%--------------------------------------------------------------------
%% @equiv update(Histogram, CurrentTimestamp, 1).
%% @end
%%--------------------------------------------------------------------
-spec increment(histogram(), CurrentTimestamp :: timestamp()) -> histogram().
increment(Histogram, CurrentTimestamp) ->
    increment(Histogram, CurrentTimestamp, 1).

%%--------------------------------------------------------------------
%% @doc
%% Increments newest time window by N. The function shifts time slots if
%% the difference between provided CurrentTime and LastUpdate is greater than
%% TimeWindow.
%% @end
%%--------------------------------------------------------------------
-spec increment(histogram(), CurrentTimestamp :: timestamp(), N :: non_neg_integer()) ->
    histogram().
increment(TSH = #time_slot_histogram{type = Type}, CurrentTimestamp, N) ->
    ShiftSize = shift_size(TSH, CurrentTimestamp),
    ShiftedHistogram = shift_values(TSH, ShiftSize),
    TSH#time_slot_histogram{
        last_update_time = CurrentTimestamp,
        values = increment_int(ShiftedHistogram, Type, N)
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
-spec reset_cumulative(histogram(), CurrentTimestamp :: timestamp()) ->
    histogram().
reset_cumulative(TSH = #time_slot_histogram{type = cumulative}, CurrentTimestamp) ->
    TSH2 = increment(TSH#time_slot_histogram{type = normal}, CurrentTimestamp, 0),
    TSH2#time_slot_histogram{type = cumulative}.

%%--------------------------------------------------------------------
%% @equiv decrement(Histogram, CurrentTimestamp, 1).
%% @end
%%--------------------------------------------------------------------
-spec decrement(histogram(), CurrentTimestamp :: timestamp()) -> histogram().
decrement(Histogram, CurrentTimestamp) ->
    decrement(Histogram, CurrentTimestamp, 1).

%%--------------------------------------------------------------------
%% @doc
%% Decrements newest time window by N. The function shifts time slots if
%% the difference between provided CurrentTime and LastUpdate is greater than
%% TimeWindow.
%% @end
%%--------------------------------------------------------------------
-spec decrement(histogram(), CurrentTimestamp :: timestamp(),
    N :: non_neg_integer()) -> histogram().
decrement(TSH = #time_slot_histogram{type = Type}, CurrentTimestamp, N) ->
    ShiftSize = shift_size(TSH, CurrentTimestamp),
    ShiftedHistogram = shift_values(TSH, ShiftSize),
    TSH#time_slot_histogram{
        last_update_time = CurrentTimestamp,
        values = decrement_int(ShiftedHistogram, Type, N)
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns lists with histogram values, the newest values are first.
%% @end
%%--------------------------------------------------------------------
-spec get_histogram_values(histogram()) -> histogram:histogram().
get_histogram_values(#time_slot_histogram{values = Histogram}) ->
    Histogram.

%%--------------------------------------------------------------------
%% @doc
%% Returns LastUpdate time
%% @end
%%--------------------------------------------------------------------
-spec get_last_update(histogram()) -> timestamp().
get_last_update(#time_slot_histogram{last_update_time = LastUpdate}) ->
    LastUpdate.

%%--------------------------------------------------------------------
%% @doc
%% Returns size of histogram.
%% @end
%%--------------------------------------------------------------------
-spec get_size(histogram()) -> timestamp().
get_size(#time_slot_histogram{size = Size}) ->
    Size.

%%--------------------------------------------------------------------
%% @doc
%% Returns sum of all histogram values
%% @end
%%--------------------------------------------------------------------
-spec get_sum(histogram()) -> non_neg_integer().
get_sum(TimeSlotHistogram) ->
    lists:sum(get_histogram_values(TimeSlotHistogram)).

%%--------------------------------------------------------------------
%% @doc
%% Returns average of all histogram values
%% @end
%%--------------------------------------------------------------------
-spec get_average(histogram()) -> non_neg_integer().
get_average(TimeSlotHistogram) ->
    utils:ceil(get_sum(TimeSlotHistogram)/ get_size(TimeSlotHistogram)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function counts the shift size (number of windows) between
%% StartTime and LastUpdate.
%% @end
%%-------------------------------------------------------------------
-spec shift_size(histogram(), timestamp()) -> non_neg_integer().
shift_size(#time_slot_histogram{
    start_time = StartTime,
    last_update_time = LastUpdate,
    time_window = TimeWindow
}, CurrentTimestamp) ->
    (CurrentTimestamp - StartTime) div TimeWindow - (LastUpdate - StartTime) div TimeWindow.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function shifts values in the histogram. Values are shifted
%% accordingly to the defined type.
%% @end
%%-------------------------------------------------------------------
-spec shift_values(histogram(), non_neg_integer()) -> values().
shift_values(#time_slot_histogram{
    values = Histogram,
    type = normal
}, ShiftSize) ->
    histogram:shift(Histogram, ShiftSize);
shift_values(#time_slot_histogram{
    values = Histogram,
    type = cumulative
}, ShiftSize) ->
    cumulative_histogram:shift(Histogram, ShiftSize).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Internal helper for increment/3 function. Calls increment/2 function
%% from the suitable module according to the defined type.
%% @end
%%-------------------------------------------------------------------
-spec increment_int(values(), type(), non_neg_integer()) -> values().
increment_int(HistogramValues, normal, N) ->
    histogram:increment(HistogramValues, N);
increment_int(HistogramValues, cumulative, N) ->
    cumulative_histogram:increment(HistogramValues, N).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Internal helper for decrement/3 function. Calls decrement/2 function
%% from the suitable module according to the defined type.
%% @end
%%-------------------------------------------------------------------
-spec decrement_int(values(), type(), non_neg_integer()) -> values().
decrement_int(HistogramValues, normal, N) ->
    histogram:decrement(HistogramValues, N);
decrement_int(HistogramValues, cumulative, N) ->
    cumulative_histogram:decrement(HistogramValues, N).