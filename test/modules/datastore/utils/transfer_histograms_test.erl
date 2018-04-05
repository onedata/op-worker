%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Tests of function calculating speed charts for transfers view in GUI
%%% (see transfer_histograms:to_speed_chart/4 for clarification
%%% how the function works).
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_histograms_test).
-author("Lukasz Opiola").

-include_lib("eunit/include/eunit.hrl").
-include("modules/datastore/transfer.hrl").

-define(PROVIDER1, <<"ProviderId1">>).
-define(PROVIDER2, <<"ProviderId2">>).

new_transfer_histograms_test() ->
    Bytes = 50,
    Histogram = histogram:increment(histogram:new(?MIN_HIST_LENGTH), Bytes),
    ExpTransferHist = #{?PROVIDER1 => Histogram},
    TransferHist = transfer_histograms:new(?PROVIDER1, Bytes, ?MINUTE_STAT_TYPE),
    ?assertEqual(ExpTransferHist, TransferHist).

update_with_nonexistent_test() ->
    Bytes = 50,
    Histogram = histogram:increment(histogram:new(?MIN_HIST_LENGTH), Bytes),
    ExpTransferHist = #{?PROVIDER1 => Histogram, ?PROVIDER2 => Histogram},
    TransferHist1 = transfer_histograms:new(?PROVIDER1, Bytes, ?MINUTE_STAT_TYPE),
    TransferHist2 = transfer_histograms:update(
        ?PROVIDER2, Bytes, TransferHist1, ?MINUTE_STAT_TYPE, 0, 0
    ),
    assertEqualMaps(ExpTransferHist, TransferHist2).

update_with_existent_test() ->
    Bytes = 50,
    Histogram1 = histogram:increment(histogram:new(?MIN_HIST_LENGTH), Bytes),
    TransferHist1 = transfer_histograms:new(?PROVIDER1, Bytes, ?MINUTE_STAT_TYPE),

    % Update within the same slot time as last_update and current_time
    % should increment only head slot.
    TransferHist2 = transfer_histograms:update(
        ?PROVIDER1, Bytes, TransferHist1, ?MINUTE_STAT_TYPE, 0, 0
    ),
    Histogram2 = histogram:increment(Histogram1, Bytes),
    ExpTransferHist2 = #{?PROVIDER1 => Histogram2},
    assertEqualMaps(ExpTransferHist2, TransferHist2),

    % Update not within the same slot time as last_update and current_time
    % should shift histogram and then increment only head slot.
    LastUpdate = 0,
    CurrentTime = 10,
    Window = ?FIVE_SEC_TIME_WINDOW,
    TransferHist3 = transfer_histograms:update(?PROVIDER1, Bytes,
        TransferHist1, ?MINUTE_STAT_TYPE, LastUpdate, CurrentTime
    ),
    ShiftSize = (CurrentTime div Window) - (LastUpdate div Window),
    Histogram3 = histogram:increment(histogram:shift(Histogram1, ShiftSize), Bytes),
    ExpTransferHist3 = #{?PROVIDER1 => Histogram3},
    assertEqualMaps(ExpTransferHist3, TransferHist3).

pad_with_zeroes_test() ->
    Bytes = 50,
    Histogram = histogram:increment(histogram:new(?MIN_HIST_LENGTH), Bytes),
    TransferHist1 = transfer_histograms:new(?PROVIDER1, Bytes, ?MINUTE_STAT_TYPE),
    TransferHist2 = transfer_histograms:update(
        ?PROVIDER2, Bytes, TransferHist1, ?MINUTE_STAT_TYPE, 0, 0
    ),
    % Histograms which last update is greater or equal to current time should
    % be left intact, otherwise they should be shifted and padded with zeroes
    Provider1LastUpdate = 80,
    Provider2LastUpdate = 110,
    CurrentTime = 100,
    Window = ?FIVE_SEC_TIME_WINDOW,
    ShiftSize = (CurrentTime div Window) - (Provider1LastUpdate div Window),
    LastUpdates = #{
        ?PROVIDER1 => Provider1LastUpdate, ?PROVIDER2 => Provider2LastUpdate
    },
    PaddedHistograms = transfer_histograms:pad_with_zeroes(
        TransferHist2, ?FIVE_SEC_TIME_WINDOW, 100, LastUpdates
    ),
    ExpPaddedHistograms = #{
        ?PROVIDER1 => histogram:shift(Histogram, ShiftSize), ?PROVIDER2 => Histogram
    },
    assertEqualMaps(ExpPaddedHistograms, PaddedHistograms).

trim_min_histograms_test() ->
    Bytes = 50,
    Histogram1 = histogram:increment(histogram:new(?MIN_HIST_LENGTH), Bytes),
    Histogram2 = histogram:shift(Histogram1, ?MIN_HIST_LENGTH - 1),
    Histogram3 = histogram:increment(Histogram2, Bytes),

    LastUpdate = 100,
    {TrimmedTransferHist, _} = transfer_histograms:trim_min_histograms(
        #{?PROVIDER1 => Histogram3}, LastUpdate
    ),
    {_, ExpHistogram} = lists:split(
        ?MIN_HIST_LENGTH - ?MIN_SPEED_HIST_LENGTH, Histogram3
    ),
    ExpTransferHist = #{?PROVIDER1 => ExpHistogram},
    assertEqualMaps(ExpTransferHist, TrimmedTransferHist).

histogram_with_zero_duration_time_test() ->
    % Histogram starting in 0 and ending in 0 is considered to have one second
    % duration, because we treat each second as a slot that lasts to the
    % beginning of the next second and we always round up.
    % There should be two values for such transfer
    %   one for the starting time slot
    %   one for current point in time (a second later)
    Window = ?FIVE_SEC_TIME_WINDOW,
    Start = 0,
    End = 0,
    Histogram = histogram_starting_with([0], Window),
    SpeedChart = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End, Window),
    ?assertEqual(?MIN_SPEED_HIST_LENGTH + 1, length(SpeedChart)),
    ?assertMatch([0, 0, null | _], SpeedChart).

histogram_with_zeroes_and_single_slot_test() ->
    % If the transfer lasted for a single slot, it should have two values
    % (starting and current point).
    Window = ?FIVE_SEC_TIME_WINDOW,
    Start = 0,
    End = 4,
    Histogram = histogram_starting_with([0], Window),
    SpeedChart = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End, Window),
    ?assertEqual(?MIN_SPEED_HIST_LENGTH + 1, length(SpeedChart)),
    ?assertMatch([0, 0, null | _], SpeedChart).

histogram_with_zeroes_and_two_slots_test() ->
    % If the transfer lasted for two slots, it should have three values
    % (starting point, start of new slot, current point).
    Window = ?FIVE_SEC_TIME_WINDOW,
    Start = 0,
    End = 7,
    Histogram = histogram_starting_with([0], Window),
    SpeedChart = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End, Window),
    ?assertEqual(?MIN_SPEED_HIST_LENGTH + 1, length(SpeedChart)),
    ?assertMatch([0, 0, 0, null | _], SpeedChart).

histogram_with_single_slot_test() ->
    % If the transfer lasted for a single slot, the two values (starting and
    % current point) should be the same. 10 bytes were sent here during 10
    % seconds, which should give 1B/s.
    Window = ?MIN_TIME_WINDOW,
    Start = 30,
    End = 39,
    Histogram = histogram_starting_with([10], Window),
    SpeedChart = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End, Window),
    ?assertEqual(?HOUR_SPEED_HIST_LENGTH + 1, length(SpeedChart)),
    ?assertMatch([1, 1, null | _], SpeedChart).

histogram_with_two_slots_test() ->
    % If the transfer lasted for two slots, it should have three values
    % (starting point, start of new slot, current point).
    Window = ?HOUR_TIME_WINDOW,
    Start = 3599,
    End = 3800,
    Histogram = histogram_starting_with([402, 25], Window),
    % First slot should last 1 second, second 201 seconds
    SpeedChart = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End, Window),
    ?assertEqual(?DAY_SPEED_HIST_LENGTH + 1, length(SpeedChart)),
    % The speed in the first window should be 25B/s
    ?assertMatch([_, _, 25, null | _], SpeedChart),
    [Current, Previous | _] = SpeedChart,
    ?assert(Current >= 0),
    ?assert(Previous >= 0),
    % The newest (current) measurement will be equal or lower because there is a
    % decreasing trend in speed (first window was 25B/s, second about 2B/s).
    % They can be the same as the newest window is much longer and the average
    % will not be too much influenced by the measurement from 1 sec window.
    ?assert(Previous >= Current).

histogram_with_three_slots_test() ->
    Day = 60 * 60 * 24,
    % If the transfer lasted for three slots, it should have four values
    % (starting point, start of new slot, current point).
    Window = ?DAY_TIME_WINDOW,
    Start = Day - 100,
    End = 2 * Day + 9,
    Histogram = histogram_starting_with([50, Day, 500], Window),
    % First slot should last 100 seconds, second 86400 seconds (a day), third
    % 10 seconds
    SpeedChart = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End, Window),
    ?assertEqual(?MONTH_SPEED_HIST_LENGTH + 1, length(SpeedChart)),
    % The speed in the first window should be 5B/s
    ?assertMatch([_, _, _, 5, null | _], SpeedChart),
    % The speed in the second window should be 1B/s - as this window was very
    % long and the neighbouring windows very short, they should not have much
    % impact on the average. Because of that, both values at the start and end
    % of second window are expected to be 1B/s (after rounding).
    ?assertMatch([_, 1, 1, 5, null | _], SpeedChart),
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
    % 15 slots, while the histogram has 13), it should have as many values as
    % the histogram. All values except the newest two should be an average of
    % neighbour time slots (speed are calculated for the start time of every
    % slot, which is essentially a point between two slots).
    Window = ?FIVE_SEC_TIME_WINDOW,
    Start = 17,
    End = Start + ?FIVE_SEC_TIME_WINDOW * 15,
    Histogram = histogram_starting_with([50, 50, 100, 160, 80, 200, 400, 0, 150, 300, 200, 100, 200], Window),
    SpeedChart = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End, Window),
    % Average of neighbouring values divided by time window (?FIVE_SEC_TIME_WINDOW), eg.
    % ((50 + 100) / 2) / 5 = 15
    %                 50  100 160 80  200 400  0  150 300 200 100
    ?assertMatch([_, _, 15, 26, 24, 28, 60, 40, 15, 45, 50, 30 | _], SpeedChart).

increasing_trend_at_the_start_of_histogram_test() ->
    % The start of the histogram should depict current trends in transfer speed
    % (if it's increasing or decreasing). The longer the newest window, the
    % smoother should be the chart.
    Window = ?FIVE_SEC_TIME_WINDOW,
    Start = 93,
    End = 140,
    Histogram = histogram_starting_with([200, 100, 0], Window),
    % The newest window has lasted only one second, the first value jump should
    % be very apparent
    [First1, Second1 | _] = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End, Window),
    ?assert(First1 > Second1),

    % Use the same histogram, but increase end time (which widens the first slot and
    % lowers the avg speed). Speed increase trend should be visible in all cases
    % (First > Second), and the leading average speed should get lower and lower
    % (as the chart is getting smoother).
    [First2, Second2 | _] = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + 1, Window),
    ?assert(First2 > Second2),
    ?assert(First1 >= First2),
    ?assert(Second1 >= Second2),

    [First3, Second3 | _] = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + 2, Window),
    ?assert(First3 > Second3),
    ?assert(First2 >= First3),
    ?assert(Second2 >= Second3),

    [First4, Second4 | _] = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + 3, Window),
    ?assert(First4 > Second4),
    ?assert(First3 >= First4),
    ?assert(Second3 >= Second4),

    [First5, Second5 | _] = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + 4, Window),
    ?assert(First5 > Second5),
    ?assert(First4 >= First5),
    ?assert(Second4 >= Second5).

decreasing_trend_at_the_start_of_histogram_test() ->
    % The start of the histogram should depict current trends in transfer speed
    % (if it's increasing or decreasing). The longer the newest window, the
    % smoother should be the chart.
    Window = ?HOUR_TIME_WINDOW,
    Start = 3425,
    End = 4 * 3600 + 9,
    Histogram = histogram_starting_with([0, 1000000, 2000000], Window),
    % The newest window has lasted only 10 seconds, the first value drop should
    % be very apparent
    [First1, Second1 | _] = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End, Window),
    ?assert(First1 < Second1),

    % Use the same histogram, but increase end time (which widens the first slot and
    % lowers the avg speed). Speed decrease trend should be visible in all cases
    % (First < Second), and the leading average speed should get lower and lower
    % (as the chart is getting smoother).
    [First2, Second2 | _] = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + 1, Window),
    ?assert(First2 < Second2),
    ?assert(First1 >= First2),
    ?assert(Second1 >= Second2),

    [First3, Second3 | _] = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + 2, Window),
    ?assert(First3 < Second3),
    ?assert(First2 >= First3),
    ?assert(Second2 >= Second3),

    [First4, Second4 | _] = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + 3, Window),
    ?assert(First4 < Second4),
    ?assert(First3 >= First4),
    ?assert(Second3 >= Second4),

    [First5, Second5 | _] = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + 4, Window),
    ?assert(First5 < Second5),
    ?assert(First4 >= First5),
    ?assert(Second4 >= Second5).

increasing_trend_at_the_end_of_histogram_test() ->
    % The end of the histogram should smoothly change towards the next measurement
    % as the time passes. If the last window fits the chart completely, the
    % last measurement should show the average speed calculated then, and as
    % the window narrows (because of passing time), the value should move towards
    % the next measurement.
    Window = ?MIN_TIME_WINDOW,
    Start = 60,
    End = 60 * 80,
    Histogram = histogram_ending_with([600, 1200, 1800], Window),
    % The newest window has lasted only one second, the first value jump should
    % be very apparent
    SpeedChart1 = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End, Window),
    [OneButLast1, Last1] = lists:nthtail(length(SpeedChart1) - 2, SpeedChart1),
    ?assert(Last1 > OneButLast1),

    % Use the same histogram, but increase end time (which narrows the last slot and
    % lowers the avg speed). Speed increase trend should be visible in all cases
    % (Last > OneButLast), and the trailing average speed should get lower and lower
    % (as the chart is getting smoother).
    SpeedChart2 = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + 12, Window),
    [OneButLast2, Last2] = lists:nthtail(length(SpeedChart2) - 2, SpeedChart2),
    ?assert(Last2 > OneButLast2),
    ?assert(Last1 >= Last2),
    ?assert(OneButLast1 >= OneButLast2),

    SpeedChart3 = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + 24, Window),
    [OneButLast3, Last3] = lists:nthtail(length(SpeedChart3) - 2, SpeedChart3),
    ?assert(Last3 > OneButLast3),
    ?assert(Last2 >= Last3),
    ?assert(OneButLast2 >= OneButLast3),

    SpeedChart4 = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + 36, Window),
    [OneButLast4, Last4] = lists:nthtail(length(SpeedChart4) - 2, SpeedChart4),
    ?assert(Last4 > OneButLast4),
    ?assert(Last3 >= Last4),
    ?assert(OneButLast3 >= OneButLast4),

    SpeedChart5 = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + 48, Window),
    [OneButLast5, Last5] = lists:nthtail(length(SpeedChart5) - 2, SpeedChart5),
    ?assert(Last5 > OneButLast5),
    ?assert(Last4 >= Last5),
    ?assert(OneButLast4 >= OneButLast5).

decreasing_trend_at_the_end_of_histogram_test() ->
    % The end of the histogram should smoothly change towards the next measurement
    % as the time passes. If the last window fits the chart completely, the
    % last measurement should show the average speed calculated then, and as
    % the window narrows (because of passing time), the value should move towards
    % the next measurement.
    Day = 60 * 60 * 24,
    Window = ?DAY_TIME_WINDOW,
    Start = Day,
    End = Day * 40,
    Histogram = histogram_ending_with([1800 * Day, 1200 * Day, 600 * Day], Window),
    % The newest window has lasted only one second, the first value jump should
    % be very apparent
    SpeedChart1 = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End, Window),
    [OneButLast1, Last1] = lists:nthtail(length(SpeedChart1) - 2, SpeedChart1),
    ?assert(Last1 < OneButLast1),

    % Use the same histogram, but increase end time (which narrows the last slot and
    % lowers the avg speed). Speed decrease trend should be visible in all cases
    % (Last < OneButLast), and the trailing average speed should get higher and higher
    % (as the chart is getting smoother).
    SpeedChart2 = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + Day div 5, Window),
    [OneButLast2, Last2] = lists:nthtail(length(SpeedChart2) - 2, SpeedChart2),
    ?assert(Last2 < OneButLast2),
    ?assert(Last1 =< Last2),
    ?assert(OneButLast1 =< OneButLast2),

    SpeedChart3 = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + Day div 4, Window),
    [OneButLast3, Last3] = lists:nthtail(length(SpeedChart3) - 2, SpeedChart3),
    ?assert(Last3 < OneButLast3),
    ?assert(Last2 =< Last3),
    ?assert(OneButLast2 =< OneButLast3),

    SpeedChart4 = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + Day div 3, Window),
    [OneButLast4, Last4] = lists:nthtail(length(SpeedChart4) - 2, SpeedChart4),
    ?assert(Last4 < OneButLast4),
    ?assert(Last3 =< Last4),
    ?assert(OneButLast3 =< OneButLast4),

    SpeedChart5 = transfer_histograms:histogram_to_speed_chart(Histogram, Start, End + Day div 2, Window),
    [OneButLast5, Last5] = lists:nthtail(length(SpeedChart5) - 2, SpeedChart5),
    ?assert(Last5 < OneButLast5),
    ?assert(Last4 =< Last5),
    ?assert(OneButLast4 =< OneButLast5).


histogram_starting_with(Beginning, Window) ->
    Length = case Window of
        ?FIVE_SEC_TIME_WINDOW -> ?MIN_SPEED_HIST_LENGTH;
        ?MIN_TIME_WINDOW -> ?HOUR_SPEED_HIST_LENGTH;
        ?HOUR_TIME_WINDOW -> ?DAY_SPEED_HIST_LENGTH;
        ?DAY_TIME_WINDOW -> ?MONTH_SPEED_HIST_LENGTH
    end,
    Beginning ++ lists:duplicate(Length - length(Beginning), 0).


% histogram_ending_with([1,2,3], W) -> [..., 1,2,3]
histogram_ending_with(Beginning, Window) ->
    lists:reverse(histogram_starting_with(lists:reverse(Beginning), Window)).


assertEqualMaps(Map1, Map2) when map_size(Map1) == map_size(Map2) ->
    maps:fold(fun(Provider, Val1, Acc) ->
        case maps:find(Provider, Map2) of
            {ok, Val2} ->
                ?assertEqual(Val1, Val2),
                Acc or true;
            error ->
                false
        end
              end, true, Map1);
assertEqualMaps(_, _) ->
    false.
