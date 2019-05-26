%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Cumulative histogram implemented on a list, allows for updating latest
%%% counter and for shifting the histogram values to the right.
%%% @end
%%%--------------------------------------------------------------------
-module(cumulative_histogram).
-author("Jakub Kudzia").

-type size() :: pos_integer().
-type histogram() :: [integer()].

-export_type([size/0, histogram/0]).

%% API
-export([new/1, shift/2, increment/2, decrement/2, merge/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns new histogram of given size
%% @end
%%--------------------------------------------------------------------
-spec new(size()) -> histogram().
new(Size) ->
    new(Size, 0).

%%--------------------------------------------------------------------
%% @doc
%% Shifts right all values in the histogram by given offset
%% @end
%%--------------------------------------------------------------------
-spec shift(histogram(), ShiftSize :: non_neg_integer()) -> histogram().
shift(Histogram, 0) ->
    Histogram;
shift(Histogram = [Head | _Rest], ShiftSize) when ShiftSize >= length(Histogram) ->
    new(length(Histogram), Head);
shift(Histogram = [Head | _Rest], ShiftSize) ->
    NewSlots = new(ShiftSize, Head),
    lists:sublist(NewSlots ++ Histogram, length(Histogram)).

%%--------------------------------------------------------------------
%% @doc
%% Increments first value in given histogram by adding N.
%% @end
%%--------------------------------------------------------------------
-spec increment(histogram(), non_neg_integer()) -> histogram().
increment([Head | Rest], N) ->
    [Head + N | Rest].

%%--------------------------------------------------------------------
%% @doc
%% Decrements first value in given histogram by subtracting N.
%% @end
%%--------------------------------------------------------------------
-spec decrement(histogram(), non_neg_integer()) -> histogram().
decrement([Head | Rest], N) ->
    [Head - N | Rest].

%%--------------------------------------------------------------------
%% @doc
%% Merges 2 histograms of equal size
%% @end
%%--------------------------------------------------------------------
-spec merge(histogram(), histogram()) -> histogram().
merge(Histogram1, Histogram2) ->
    lists:zipwith(fun(X, Y) -> X + Y end, Histogram1, Histogram2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns new histogram of given size with default values.
%% @end
%%--------------------------------------------------------------------
-spec new(size(), non_neg_integer()) -> histogram().
new(Size, DefaultValue) ->
    lists:duplicate(Size, DefaultValue).
