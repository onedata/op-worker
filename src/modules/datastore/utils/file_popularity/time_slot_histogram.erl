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
-export([new/2, new/3, increment/2, get_histogram_values/1, get_last_update/1,
    get_sum/1, get_average/1]).

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
%% @doc
%% Increments newest time window. The function shifts time slots if
%% the difference between provided CurrentTime and LastUpdate is greater than
%% TimeWindow.
%% @end
%%--------------------------------------------------------------------
-spec increment(histogram(), CurrentTimestamp :: timestamp()) -> histogram().
increment({LastUpdate, TimeWindow, Histogram}, CurrentTimestamp) ->
    ShiftSize = CurrentTimestamp div TimeWindow - LastUpdate div TimeWindow,
    ShiftedHistogram = histogram:shift(Histogram, ShiftSize),
    {CurrentTimestamp, TimeWindow, histogram:increment(ShiftedHistogram)}.

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
%% Retuns sum of all histogram values
%% @end
%%--------------------------------------------------------------------
-spec get_sum(histogram()) -> non_neg_integer().
get_sum({_, _, Histogram}) ->
    lists:sum(Histogram).

%%--------------------------------------------------------------------
%% @doc
%% Retuns average of all histogram values
%% @end
%%--------------------------------------------------------------------
-spec get_average(histogram()) -> non_neg_integer().
get_average({_, _, Histogram}) ->
    ceil(lists:sum(Histogram) / length(Histogram)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Math ceil function (works on positive values).
%% @end
%%--------------------------------------------------------------------
-spec ceil(N :: number()) -> integer().
ceil(N) when trunc(N) == N -> trunc(N);
ceil(N) -> trunc(N + 1).