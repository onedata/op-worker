%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Tests of time_slot_histogram module
%%% @end
%%%--------------------------------------------------------------------
-module(time_slot_histogram_test).
-author("Tomasz Lichon").

-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/posix/errors.hrl").

new_histogram_should_have_last_update_set_to_zero_test() ->
    Histogram = time_slot_histogram:new(10, 10),

    ?assertEqual(0, time_slot_histogram:get_last_update(Histogram)).

new_histogram_should_have_histograms_set_to_zero_test() ->
    Histogram = time_slot_histogram:new(10, 3),

    ?assertEqual([0, 0, 0], time_slot_histogram:get_histogram_values(Histogram)).

update_should_set_last_timestamp_test() ->
    Histogram = time_slot_histogram:new(10, 10),
    UpdatedHistogram = time_slot_histogram:increment(Histogram, 123),

    ?assertEqual(123, time_slot_histogram:get_last_update(UpdatedHistogram)).

increment_within_the_same_window_should_increment_the_same_counter_test() ->
    Histogram = time_slot_histogram:new(5, 3),
    IncrementHistogram1 = time_slot_histogram:increment(Histogram, 1),
    IncrementHistogram2 = time_slot_histogram:increment(IncrementHistogram1, 1),
    IncrementHistogram3 = time_slot_histogram:increment(IncrementHistogram2, 4),

    ?assertMatch([1, 0, 0], time_slot_histogram:get_histogram_values(IncrementHistogram1)),
    ?assertMatch([2, 0, 0], time_slot_histogram:get_histogram_values(IncrementHistogram2)),
    ?assertMatch([3, 0, 0], time_slot_histogram:get_histogram_values(IncrementHistogram3)).

increment_within_consecutive_windows_should_increment_consecutive_counters_test() ->
    Histogram = time_slot_histogram:new(5, 3),
    IncrementHistogram1 = time_slot_histogram:increment(Histogram, 1),
    IncrementHistogram2 = time_slot_histogram:increment(IncrementHistogram1, 5),
    IncrementHistogram3 = time_slot_histogram:increment(IncrementHistogram2, 12),

    ?assertMatch([1, 0, 0], time_slot_histogram:get_histogram_values(IncrementHistogram1)),
    ?assertMatch([1, 1, 0], time_slot_histogram:get_histogram_values(IncrementHistogram2)),
    ?assertMatch([1, 1, 1], time_slot_histogram:get_histogram_values(IncrementHistogram3)).

increment_within_distant_windows_should_increment_distant_counters_test() ->
    Histogram = time_slot_histogram:new(5, 6),
    IncrementHistogram1 = time_slot_histogram:increment(Histogram, 2),
    IncrementHistogram2 = time_slot_histogram:increment(IncrementHistogram1, 16),
    IncrementHistogram3 = time_slot_histogram:increment(IncrementHistogram2, 26),

    ?assertMatch([1, 0, 0, 0, 0, 0], time_slot_histogram:get_histogram_values(IncrementHistogram1)),
    ?assertMatch([1, 0, 0, 1, 0, 0], time_slot_histogram:get_histogram_values(IncrementHistogram2)),
    ?assertMatch([1, 0, 1, 0, 0, 1], time_slot_histogram:get_histogram_values(IncrementHistogram3)).

increment_after_long_time_should_strip_old_values_test() ->
    Histogram = time_slot_histogram:new(5, 6),
    IncrementHistogram1 = time_slot_histogram:increment(Histogram, 2),
    IncrementHistogram2 = time_slot_histogram:increment(IncrementHistogram1, 6),
    IncrementHistogram3 = time_slot_histogram:increment(IncrementHistogram2, 31),

    ?assertMatch([1, 0, 0, 0, 0, 0], time_slot_histogram:get_histogram_values(IncrementHistogram1)),
    ?assertMatch([1, 1, 0, 0, 0, 0], time_slot_histogram:get_histogram_values(IncrementHistogram2)),
    ?assertMatch([1, 0, 0, 0, 0, 1], time_slot_histogram:get_histogram_values(IncrementHistogram3)).