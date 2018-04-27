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
    values :: histogram:histogram(),
    size :: non_neg_integer()
}).

-type timestamp() :: non_neg_integer().
-type time() :: pos_integer().
-type histogram() :: #time_slot_histogram{}.

%% API
-export([
    new/2, new/3, new/4,
    increment/2, increment/3, increment_monotonic/3,
    decrement/2, decrement/3,  decrement_monotonic/3, decrement_monotonic/2,
    merge/2,
    get_histogram_values/1, get_last_update/1,
    get_sum/1, get_average/1, get_size/1]).

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
    new(0, 0, TimeWindow, Size).

%%--------------------------------------------------------------------
%% @doc
%% Returns time_slot_histogram with given LastUpdateTime and
%% provided values or empty histogram of given size
%% @end
%%--------------------------------------------------------------------
-spec new(LastUpdate :: timestamp(), TimeWindow :: time(),
    histogram:size() | histogram:histogram()) -> histogram().
new(LastUpdate, TimeWindow, HistogramValues) when is_list(HistogramValues) ->
    new(LastUpdate, LastUpdate, TimeWindow, HistogramValues);
new(LastUpdate, TimeWindow, Size) when is_integer(Size) ->
    new(LastUpdate, TimeWindow, histogram:new(Size)).

%%--------------------------------------------------------------------
%% @doc
%% Returns time_slot_histogram with provided values.
%% @end
%%--------------------------------------------------------------------
-spec new(StartTime :: timestamp(), LastUpdate :: timestamp(),
    TimeWindow :: time(), histogram:size() | histogram:histogram()) -> histogram().
new(StartTime, LastUpdate, TimeWindow, HistogramValues) when is_list(HistogramValues) ->
    #time_slot_histogram{
        start_time = StartTime,
        last_update_time = LastUpdate,
        time_window = TimeWindow,
        values = HistogramValues,
        size = length(HistogramValues)
    };
new(StartTime, LastUpdate, TimeWindow, Size) when is_integer(Size) ->
    new(StartTime, LastUpdate, TimeWindow, histogram:new(Size)).

%%--------------------------------------------------------------------
%% @equiv increment(Histogram, CurrentTimestamp, 1).
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
-spec increment(histogram(), CurrentTimestamp :: timestamp(), N :: integer()) ->
    histogram().
increment(TSH = #time_slot_histogram{
    start_time = StartTime,
    last_update_time = LastUpdate,
    time_window = TimeWindow,
    values = Histogram
}, CurrentTimestamp, N) ->
    ShiftSize = (CurrentTimestamp - StartTime) div TimeWindow - (LastUpdate - StartTime) div TimeWindow,
    ShiftedHistogram = histogram:shift(Histogram, ShiftSize),
    TSH#time_slot_histogram{
        last_update_time = CurrentTimestamp,
        values = histogram:increment(ShiftedHistogram, N)
    }.


-spec increment_monotonic(histogram(),CurrentTimestamp :: timestamp(), N :: integer()) ->
    histogram().
increment_monotonic(TSH = #time_slot_histogram{
    start_time = StartTime,
    last_update_time = LastUpdate,
    time_window = TimeWindow,
    values = Histogram = [Head | _Rest]
}, CurrentTimestamp, N) ->
    ShiftSize = (CurrentTimestamp - StartTime) div TimeWindow - (LastUpdate - StartTime) div TimeWindow,
    ShiftedHistogram = histogram:shift(Histogram, ShiftSize, Head),
    TSH#time_slot_histogram{
        last_update_time = CurrentTimestamp,
        values = histogram:increment(ShiftedHistogram, N)
    }.

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
-spec decrement(histogram(), CurrentTimestamp :: timestamp(), N :: non_neg_integer()) ->
    histogram().
decrement(TimeSlotHistogram, CurrentTimestamp, N) ->
    increment(TimeSlotHistogram, CurrentTimestamp, -N).

-spec decrement_monotonic(histogram(),CurrentTimestamp :: timestamp()) ->
    histogram().
decrement_monotonic(Histogram, CurrentTimestamp) ->
    decrement_monotonic(Histogram, CurrentTimestamp, 1).

-spec decrement_monotonic(histogram(),CurrentTimestamp :: timestamp(), N :: integer()) ->
    histogram().
decrement_monotonic(TimeSlotHistogram, CurrentTimestamp, N) ->
    increment_monotonic(TimeSlotHistogram, CurrentTimestamp, -N).

%%--------------------------------------------------------------------
%% @doc
%% Merges 2 time slot histograms of equal size and time window
%% @end
%%--------------------------------------------------------------------
-spec merge(histogram(), histogram()) -> histogram().
merge(TSH = #time_slot_histogram{
    last_update_time = LastUpdate1,
    time_window = TimeWindow,
    values = Values1,
    size = Size
}, #time_slot_histogram{
    last_update_time = LastUpdate2,
    time_window = TimeWindow,
    values = Values2,
    size = Size
}) ->
    {LastUpdate, Histogram1, Histogram2} = case LastUpdate1 > LastUpdate2 of
        true ->
            ShiftSize = (LastUpdate1 div TimeWindow) - (LastUpdate2 div TimeWindow),
            {LastUpdate1, Values1, histogram:shift(Values2, ShiftSize)};
        false ->
            ShiftSize = (LastUpdate2 div TimeWindow) - (LastUpdate1 div TimeWindow),
            {LastUpdate2, histogram:shift(Values1, ShiftSize), Values2}
    end,
    TSH#time_slot_histogram{
        last_update_time = LastUpdate,
        values = histogram:merge(Histogram1, Histogram2)
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
