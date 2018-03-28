%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Basic utils for time_slot_histograms used as transfer histograms.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_histogram).
-author("Bartosz Walkowicz").

-include("modules/datastore/transfer.hrl").

-type timestamp() :: non_neg_integer().

%% API
-export([
    new_time_slot_histogram/2, new_time_slot_histogram/3,
    update/6,
    trim_timestamp/1,
    stats_type_to_speed_chart_len/1
]).


%%%===================================================================
%%% API
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% Creates a new time_slot_histogram based on LastUpdate time and Window.
%% The length of created histogram is based on the Window.
%% @end
%%-------------------------------------------------------------------
-spec update(oneprovider:id(), Bytes :: non_neg_integer(),
    Histograms, Window :: non_neg_integer(), LastUpdate :: timestamp(),
    CurrentTime :: timestamp()) -> Histograms
    when Histograms :: maps:map(od_provider:id(), histogram:histogram()).
update(ProviderId, Bytes, Histograms, Window, LastUpdate, CurrentTime) ->
    Histogram = case maps:find(ProviderId, Histograms) of
        error ->
            new_time_slot_histogram(LastUpdate, Window);
        {ok, Values} ->
            new_time_slot_histogram(LastUpdate, Window, Values)
    end,
    UpdatedHistogram = time_slot_histogram:increment(Histogram, CurrentTime, Bytes),
    UpdatedValues = time_slot_histogram:get_histogram_values(UpdatedHistogram),
    maps:put(ProviderId, UpdatedValues, Histograms).


%%-------------------------------------------------------------------
%% @doc
%% Creates a new time_slot_histogram based on LastUpdate time and Window.
%% The length of created histogram is based on the Window.
%% @end
%%-------------------------------------------------------------------
-spec new_time_slot_histogram(LastUpdate :: timestamp(),
    Window :: non_neg_integer()) -> time_slot_histogram:histogram().
new_time_slot_histogram(LastUpdate, ?FIVE_SEC_TIME_WINDOW) ->
    new_time_slot_histogram(LastUpdate, ?FIVE_SEC_TIME_WINDOW,
        histogram:new(?MIN_HIST_LENGTH));
new_time_slot_histogram(LastUpdate, ?MIN_TIME_WINDOW) ->
    new_time_slot_histogram(LastUpdate, ?MIN_TIME_WINDOW,
        histogram:new(?HOUR_HIST_LENGTH));
new_time_slot_histogram(LastUpdate, ?HOUR_TIME_WINDOW) ->
    new_time_slot_histogram(LastUpdate, ?HOUR_TIME_WINDOW,
        histogram:new(?DAY_HIST_LENGTH));
new_time_slot_histogram(LastUpdate, ?DAY_TIME_WINDOW) ->
    new_time_slot_histogram(LastUpdate, ?DAY_TIME_WINDOW,
        histogram:new(?MONTH_HIST_LENGTH)).


%%-------------------------------------------------------------------
%% @doc
%% Creates a new time_slot_histogram based on LastUpdate time, Window and values.
%% @end
%%-------------------------------------------------------------------
-spec new_time_slot_histogram(LastUpdate :: timestamp(),
    Window :: non_neg_integer(), histogram:histogram()) ->
    time_slot_histogram:histogram().
new_time_slot_histogram(LastUpdate, Window, Values) ->
    time_slot_histogram:new(LastUpdate, Window, Values).


%%-------------------------------------------------------------------
%% @doc
%% Trim timestamp removing recent n-seconds based on difference between expected
%% slots in speed histograms and bytes send histograms.
%% @end
%%-------------------------------------------------------------------
-spec trim_timestamp(Timestamp :: timestamp()) -> timestamp().
trim_timestamp(Timestamp) ->
    FullSecSlotsToRemove = ?MIN_HIST_LENGTH - ?MIN_SPEED_HIST_LENGTH - 1,
    (Timestamp rem ?FIVE_SEC_TIME_WINDOW) + FullSecSlotsToRemove * ?FIVE_SEC_TIME_WINDOW.


%%-------------------------------------------------------------------
%% @doc
%% Return speed chart length based on given stats type.
%% @end
%%-------------------------------------------------------------------
-spec stats_type_to_speed_chart_len(binary()) -> non_neg_integer().
stats_type_to_speed_chart_len(?MINUTE_STAT_TYPE) -> ?MIN_SPEED_HIST_LENGTH;
stats_type_to_speed_chart_len(?HOUR_STAT_TYPE) -> ?HOUR_SPEED_HIST_LENGTH;
stats_type_to_speed_chart_len(?DAY_STAT_TYPE) -> ?DAY_SPEED_HIST_LENGTH;
stats_type_to_speed_chart_len(?MONTH_STAT_TYPE) -> ?MONTH_SPEED_HIST_LENGTH.
