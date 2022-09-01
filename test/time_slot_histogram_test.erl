%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Tests of time_slot_histogram module.
%%% @end
%%%--------------------------------------------------------------------
-module(time_slot_histogram_test).
-author("Tomasz Lichon").

-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/errors.hrl").

new_histogram_should_have_last_update_set_to_zero_test() ->
    Histogram = time_slot_histogram:new(10, 10),
    ?assertEqual(0, time_slot_histogram:get_last_update(Histogram)).

new_histogram_should_have_histograms_set_to_zero_test() ->
    Histogram = time_slot_histogram:new(10, 3),
    ?assertEqual([0, 0, 0], time_slot_histogram:get_histogram_values(Histogram)).

update_should_set_last_timestamp_test() ->
    Histogram = time_slot_histogram:new(10, 10),
    NewHistogram = increment_histogram(Histogram, 123, rand:uniform(999)),
    ?assertEqual(123, time_slot_histogram:get_last_update(NewHistogram)).

change_within_the_same_window_should_change_the_same_counter_test() ->
    H1 = time_slot_histogram:new(5, 3),
    H2 = assert_histogram_values([2, 0, 0], increment_histogram(H1, 1, 2)),
    H3 = assert_histogram_values([1, 0, 0], decrement_histogram(H2, 1, 1)),
    _ = assert_histogram_values([4, 0, 0], increment_histogram(H3, 1, 3)).

change_within_consecutive_windows_should_change_consecutive_counters_test() ->
    H1 = time_slot_histogram:new(5, 3),
    H2 = assert_histogram_values([1, 0, 0], increment_histogram(H1, 1, 1)),
    H3 = assert_histogram_values([-2, 1, 0], decrement_histogram(H2, 5, 2)),
    _ = assert_histogram_values([3, -2, 1], increment_histogram(H3, 12, 3)).

change_with_backward_time_warp_should_change_the_same_counter_test() ->
    H1 = time_slot_histogram:new(5, 3),
    H2 = assert_histogram_values([7, 0, 0], increment_histogram(H1, 4432, 7)),
    H3 = assert_histogram_values([2, 0, 0], decrement_histogram(H2, 2504, 5)),
    _ = assert_histogram_values([6, 0, 0], increment_histogram(H3, 1, 4)).

change_within_distant_windows_should_change_distant_counters_test() ->
    H1 = time_slot_histogram:new(5, 6),
    H2 = assert_histogram_values([-7, 0, 0, 0, 0, 0], decrement_histogram(H1, 2, 7)),
    H3 = assert_histogram_values([18, 0, 0, -7, 0, 0], increment_histogram(H2, 16, 18)),
    _ = assert_histogram_values([-1, 0, 18, 0, 0, -7], decrement_histogram(H3, 26, 1)).

change_after_long_time_should_strip_old_values_test() ->
    H1 = time_slot_histogram:new(5, 6),
    H2 = assert_histogram_values([7, 0, 0, 0, 0, 0], increment_histogram(H1, 2, 7)),
    H3 = assert_histogram_values([-18, 7, 0, 0, 0, 0], decrement_histogram(H2, 6, 18)),
    _ = assert_histogram_values([-1, 0, 0, 0, 0, -18], decrement_histogram(H3, 31, 1)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% The time_slot_histogram API requires that all timestamps are monotonic
%% (this is guarded by the opaque monotonic_timestamp() type and dialyzer).
%% Above tests use the same API by calling below wrappers.

%% @private
-spec increment_histogram(time_slot_histogram:histogram(), non_neg_integer(), non_neg_integer()) ->
    time_slot_histogram:histogram().
increment_histogram(Histogram, CurrentTime, Value) ->
    MonotonicTime = time_slot_histogram:ensure_monotonic_timestamp(Histogram, CurrentTime),
    time_slot_histogram:increment(Histogram, MonotonicTime, Value).


%% @private
-spec decrement_histogram(time_slot_histogram:histogram(), non_neg_integer(), non_neg_integer()) ->
    time_slot_histogram:histogram().
decrement_histogram(Histogram, CurrentTime, Value) ->
    MonotonicTime = time_slot_histogram:ensure_monotonic_timestamp(Histogram, CurrentTime),
    time_slot_histogram:decrement(Histogram, MonotonicTime, Value).


%% @private
-spec assert_histogram_values(histogram:histogram(), time_slot_histogram:histogram()) ->
    time_slot_histogram:histogram().
assert_histogram_values(ExpectedValues, TimeSlotHistogram) ->
    ?assertEqual(ExpectedValues, time_slot_histogram:get_histogram_values(TimeSlotHistogram)),
    TimeSlotHistogram.
