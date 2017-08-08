%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Tests of histogram module
%%% @end
%%%--------------------------------------------------------------------
-module(histogram_test).
-author("Tomasz Lichon").

-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/posix/errors.hrl").

new_histogram_should_be_filled_with_zeroes_test() ->
    ?assertEqual([0, 0, 0, 0, 0], histogram:new(5)).

fist_value_should_be_incremented_on_inclrement_test() ->
    Histogram = histogram:new(3),
    IncrementedHistogram1 = histogram:increment(Histogram),
    IncrementedHistogram2 = histogram:increment(IncrementedHistogram1),

    ?assertEqual([1, 0, 0], IncrementedHistogram1),
    ?assertEqual([2, 0, 0], IncrementedHistogram2).

shift_by_zero_should_do_nothing_test() ->
    Histogram = histogram:increment(histogram:new(3)),

    ?assertEqual([1, 0, 0], histogram:shift(Histogram, 0)).

shift_by_one_should_add_one_zero_to_histogram_beginning_test() ->
    Histogram = histogram:increment(histogram:new(3)),

    ?assertEqual([0, 1, 0], histogram:shift(Histogram, 1)).

shift_by_two_should_add_two_zeroes_to_histogram_beginning_test() ->
    Histogram = histogram:increment(histogram:new(3)),

    ?assertEqual([0, 0, 1], histogram:shift(Histogram, 2)).

shift_by_lenght_should_zero_histogram_test() ->
    Histogram = histogram:increment(histogram:new(5)),

    ?assertEqual([0, 0, 0, 0, 0], histogram:shift(Histogram, 5)).

shift_by_more_than_lenght_should_zero_histogram_test() ->
    Histogram = histogram:increment(histogram:new(5)),

    ?assertEqual([0, 0, 0, 0, 0], histogram:shift(Histogram, 10)).