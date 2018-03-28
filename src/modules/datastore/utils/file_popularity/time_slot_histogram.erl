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

-type timestamp() :: non_neg_integer().
-type time() :: pos_integer().
-type histogram() :: {LastUpdateTime :: timestamp(), TimeWindow :: time(), histogram:histogram()}.

%% API
-export([
    new/2, new/3,
    increment/2, increment/3,
    merge/2,
    get_histogram_values/1, get_last_update/1,
    get_sum/1, get_average/1
]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns new empty time_slot_histogram with given Size, TimeWindow.
%% LastUpdate is set to 0.
%% @end
%%--------------------------------------------------------------------
-spec new(TimeWindow :: time(), Size :: histogram:size()) -> histogram().
new(TimeWindow, Size) ->
    {0, TimeWindow, histogram:new(Size)}.

%%--------------------------------------------------------------------
%% @doc
%% Returns time_slot_histogram with provided values.
%% @end
%%--------------------------------------------------------------------
-spec new(LastUpdate :: timestamp(), TimeWindow :: time(), histogram:histogram()) -> term().
new(LastUpdate, TimeWindow, HistogramValues) ->
    {LastUpdate, TimeWindow, HistogramValues}.

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
-spec increment(histogram(), CurrentTimestamp :: timestamp(), N :: non_neg_integer()) ->
    histogram().
increment({LastUpdate, TimeWindow, Histogram}, CurrentTimestamp, N) ->
    ShiftSize = CurrentTimestamp div TimeWindow - LastUpdate div TimeWindow,
    ShiftedHistogram = histogram:shift(Histogram, ShiftSize),
    {CurrentTimestamp, TimeWindow, histogram:increment(ShiftedHistogram, N)}.

%%--------------------------------------------------------------------
%% @doc
%% Merges 2 time slot histograms of equal size and time window
%% @end
%%--------------------------------------------------------------------
-spec merge(histogram(), histogram()) -> histogram().
merge({LastUpdate1, TimeWindow, Values1}, {LastUpdate2, TimeWindow, Values2}) ->
    {LastUpdate, Histogram1, Histogram2} = case LastUpdate1 > LastUpdate2 of
        true ->
            ShiftSize = (LastUpdate1 div TimeWindow) - (LastUpdate2 div TimeWindow),
            {LastUpdate1, Values1, histogram:shift(Values2, ShiftSize)};
        false ->
            ShiftSize = (LastUpdate2 div TimeWindow) - (LastUpdate1 div TimeWindow),
            {LastUpdate2, histogram:shift(Values1, ShiftSize), Values2}
    end,
    {LastUpdate, TimeWindow, histogram:merge(Histogram1, Histogram2)}.

%%--------------------------------------------------------------------
%% @doc
%% Returns lists with histogram values, the newest values are first.
%% @end
%%--------------------------------------------------------------------
-spec get_histogram_values(histogram()) -> histogram:histogram().
get_histogram_values({_, _, Histogram}) ->
    Histogram.

%%--------------------------------------------------------------------
%% @doc
%% Returns LastUpdate time
%% @end
%%--------------------------------------------------------------------
-spec get_last_update(histogram()) -> timestamp().
get_last_update({LastUpdate, _, _}) ->
    LastUpdate.

%%--------------------------------------------------------------------
%% @doc
%% Returns sum of all histogram values
%% @end
%%--------------------------------------------------------------------
-spec get_sum(histogram()) -> non_neg_integer().
get_sum({_, _, Histogram}) ->
    lists:sum(Histogram).

%%--------------------------------------------------------------------
%% @doc
%% Returns average of all histogram values
%% @end
%%--------------------------------------------------------------------
-spec get_average(histogram()) -> non_neg_integer().
get_average({_, _, Histogram}) ->
    utils:ceil(lists:sum(Histogram) / length(Histogram)).
