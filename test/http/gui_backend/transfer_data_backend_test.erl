%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Tests of function calculating speed charts for transfers view in GUI
%%% (see transfer_data_backend:histogram_to_speed_chart/4 for clarification
%%% how the function works).
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_data_backend_test).
-author("Lukasz Opiola").

-include_lib("eunit/include/eunit.hrl").
-include("modules/datastore/transfer.hrl").

histogram_with_zero_duration_time_test() ->
    % Histogram starting in 0 and ending in 0 is considered to have one second
    % duration, because we treat each second as a slot that lasts to the
    % beginning of the next second and we always round up.
    % There should be two values for such transfer
    %   one for the starting time slot
    %   one for current point in time (a second later)
    Histogram = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    Window = ?FIVE_SEC_TIME_WINDOW,
    Start = 0,
    End = 0,
    SpeedChart = transfer_data_backend:histogram_to_speed_chart(
        Histogram, Start, End, Window
    ),
    Expected = [0, 0, null, null, null, null, null, null, null, null, null, null],
    ?assertEqual(Expected, SpeedChart).

histogram_with_zeroes_and_single_slot_test() ->
    % If the transfer lasted for a single slot, it should have two values
    % (starting and current point).
    Histogram = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    Window = ?FIVE_SEC_TIME_WINDOW,
    Start = 0,
    End = 4,
    SpeedChart = transfer_data_backend:histogram_to_speed_chart(
        Histogram, Start, End, Window
    ),
    Expected = [0, 0, null, null, null, null, null, null, null, null, null, null],
    ?assertEqual(Expected, SpeedChart).

histogram_with_zeroes_and_two_slots_test() ->
    % If the transfer lasted for two slots, it should have three values
    % (starting point, start of new slot, current point).
    Histogram = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    Window = ?FIVE_SEC_TIME_WINDOW,
    Start = 0,
    End = 7,
    SpeedChart = transfer_data_backend:histogram_to_speed_chart(
        Histogram, Start, End, Window
    ),
    Expected = [0, 0, 0, null, null, null, null, null, null, null, null, null],
    ?assertEqual(Expected, SpeedChart).

histogram_with_single_slot_test() ->
    % If the transfer lasted for a single slot, the two values (starting and
    % current point) should be the same. 10 bytes were sent here during 10
    % seconds, which should give 1B/s.
    Histogram = [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    Window = ?MIN_TIME_WINDOW,
    Start = 30,
    End = 39,
    SpeedChart = transfer_data_backend:histogram_to_speed_chart(
        Histogram, Start, End, Window
    ),
    Expected = [1, 1, null, null, null, null, null, null, null, null, null, null],
    ?assertEqual(Expected, SpeedChart).

histogram_with_two_slots_test() ->
    % If the transfer lasted for two slots, it should have three values
    % (starting point, start of new slot, current point).
    Histogram = [402, 25, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    Window = ?HR_TIME_WINDOW,
    Start = 3599,
    End = 3800,
    % First slot should last 1 second, second 201 seconds
    SpeedChart = transfer_data_backend:histogram_to_speed_chart(
        Histogram, Start, End, Window
    ),
    % The speed in the first window should be 25B/s
    ?assertMatch([_, _, 25, null | _], SpeedChart),
    [Current, Previous | _] = SpeedChart,
    ?assert(Current >= 0),
    ?assert(Previous >= 0),
    % The newest (current) measurement will be lower because there is a
    % decreasing trend in speed (first window was 25B/s, second about 2B/s)
    ?assert(Previous > Current).

histogram_with_three_slots_test() ->
    % If the transfer lasted for three slots, it should have four values
    % (starting point, start of new slot, current point).
    Histogram = [50, 3600, 500, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    Window = ?DY_TIME_WINDOW,
    Start = 60 * 60 * 24 - 100,
    End = 2 * 60 * 60 * 24 + 9,
    % First slot should last 100 seconds, second 60 * 60 * 24 seconds, third
    % 10 seconds
    SpeedChart = transfer_data_backend:histogram_to_speed_chart(
        Histogram, Start, End, Window
    ),
    % The speed in the first window should be 5B/s
    ?assertMatch([_, _, _, 5, null | _], SpeedChart),
    [Current, OneButLast, Previous | _] = SpeedChart,
    ?assert(Current >= 0),
    ?assert(OneButLast >= 0),
    ?assert(Previous >= 0),
    % The newest (current) measurement will be highest and one but last will be
    % the lowest because there is a decreasing and then increasing trend in speed
    % (first window was 25B/s, second about 1B/s, third about 5B/s)
    ?assert(Previous >= OneButLast),
    ?assert(Current >= OneButLast).

histogram_with_all_slots_test() ->
    % If the transfer lasted for more slots than fit in a histogram (in this case
    % 15 slots, while the histogram has 12), it should have as many values as
    % the histogram. All values except the newest two should be an average of
    % neighbour time slots (speed are calculated for the start time of every
    % slot, which is essentially a point between two slots).
    Histogram = [50, 50, 100, 160, 80, 200, 400, 0, 150, 300, 200, 100],
    Window = ?FIVE_SEC_TIME_WINDOW,
    Start = 17,
    End = Start + ?FIVE_SEC_TIME_WINDOW * 15,
    SpeedChart = transfer_data_backend:histogram_to_speed_chart(
        Histogram, Start, End, Window
    ),
    % Average of neighbouring values divided by time window (?FIVE_SEC_TIME_WINDOW), eg.
    % ((50 + 100) / 2) / 5 = 15
    ?assertMatch([_, _, 15, 26, 24, 28, 60, 40, 15, 45, 50, 30], SpeedChart).


