%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Basic histogram implemented on a list, allows for incrementing latest counter
%%% and for shifting the histogram values to the right.
%%% @end
%%%--------------------------------------------------------------------
-module(histogram).
-author("Tomasz Lichon").

-type size() :: pos_integer().
-type histogram() :: [integer()].

-export_type([size/0, histogram/0]).

%% API
-export([new/1, shift/2, increment/2, merge/2, reset/1, decrement/2]).

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
    lists:duplicate(Size, 0).

%%--------------------------------------------------------------------
%% @doc
%% Shifts right all values in the histogram by given offset
%% @end
%%--------------------------------------------------------------------
-spec shift(histogram(), ShiftSize :: non_neg_integer()) -> histogram().
shift(Histogram, 0) ->
    Histogram;
shift(Histogram, ShiftSize) when ShiftSize >= length(Histogram) ->
    new(length(Histogram));
shift(Histogram, ShiftSize) ->
    NewSlots = new(ShiftSize),
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

-spec reset(histogram()) -> histogram().
reset([_ | Rest]) ->
    [0 | Rest].

%%--------------------------------------------------------------------
%% @doc
%% Merges 2 histograms of equal size
%% @end
%%--------------------------------------------------------------------
-spec merge(histogram(), histogram()) -> histogram().
merge(Histogram1, Histogram2) ->
    lists:zipwith(fun(X, Y) -> X + Y end, Histogram1, Histogram2).
