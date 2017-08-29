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
-type histogram() :: [non_neg_integer()].

%% API
-export([new/1, shift/2, increment/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retuns new histogram of given size
%% @end
%%--------------------------------------------------------------------
-spec new(size()) -> histogram().
new(Size) ->
    [0 || _ <- lists:seq(1, Size)].

%%--------------------------------------------------------------------
%% @doc
%% Shifts right all values in the histogram by given offset
%% @end
%%--------------------------------------------------------------------
-spec shift(histogram(), ShiftSize :: non_neg_integer()) -> histogram().
shift(Histogram, ShiftSize) when ShiftSize >= Histogram ->
    new(length(Histogram));
shift(Histogram, ShiftSize) ->
    NewSlots = new(ShiftSize),
    lists:sublist(NewSlots ++ Histogram, length(Histogram)).

%%--------------------------------------------------------------------
%% @doc
%% Increments first value in given histogram
%% @end
%%--------------------------------------------------------------------
-spec increment(histogram()) -> histogram().
increment([Head | Rest]) ->
    [Head + 1 | Rest].