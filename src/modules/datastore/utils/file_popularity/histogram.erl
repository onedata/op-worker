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

%% API
-export([new/1, shift/2, increment/2, merge/2, shift/3]).

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
%% Returns new histogram of given size with default values.
%% @end
%%--------------------------------------------------------------------
-spec new(size(), integer()) -> histogram().
new(Size, DefaultValue) ->
    lists:duplicate(Size, DefaultValue).

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

shift(Histogram, 0, _DefaultValue) ->
    Histogram;
shift(Histogram, ShiftSize, DefaultValue) when ShiftSize >= length(Histogram) ->
    new(length(Histogram), DefaultValue);
shift(Histogram, ShiftSize, DefaultValue) ->
    NewSlots = new(ShiftSize, DefaultValue),
    lists:sublist(NewSlots ++ Histogram, length(Histogram)).

%%--------------------------------------------------------------------
%% @doc
%% Increments first value in given histogram by N
%% @end
%%--------------------------------------------------------------------
-spec increment(histogram(), integer()) -> histogram().
increment([Head | Rest], N) ->
    [Head + N | Rest].

%%--------------------------------------------------------------------
%% @doc
%% Merges 2 histograms of equal size
%% @end
%%--------------------------------------------------------------------
-spec merge(histogram(), histogram()) -> histogram().
merge(Histogram1, Histogram2) ->
    lists:zipwith(fun(X, Y) -> X + Y end, Histogram1, Histogram2).
