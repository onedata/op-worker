%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements exometer_report behaviour. It is responsible
%%% for saving subscribed metrics in ets table. It works in RRD-like
%%% style, saving up to given number of values.
%%% @end
%%%-------------------------------------------------------------------
-module(exometer_report_rrd_ets).
-author("Jakub Kudzia").

-include_lib("ctool/include/logging.hrl").
-include("modules/storage_sync/storage_sync_monitoring.hrl").

-behaviour(exometer_report).

-export([
    exometer_init/1,
    exometer_info/2,
    exometer_cast/2,
    exometer_call/3,
    exometer_report/5,
    exometer_subscribe/5,
    exometer_unsubscribe/4,
    exometer_newentry/2,
    exometer_setopts/4,
    exometer_terminate/2,
    get/1
]).


-define(DEFAULT_VALUES_TO_KEEP, 10).
-define(ETS_NAME, exometer_ets_reporter).


-record(state, {
    values_to_keep = ?DEFAULT_VALUES_TO_KEEP :: non_neg_integer()
}).

-type state() :: #state{}.


%%%===================================================================
%%% exometer_report callback API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_init/1.
%% @end
%%-------------------------------------------------------------------
-spec exometer_init([term()]) -> {ok, state()}.
exometer_init(Opts) ->
    ValuesToKeep = proplists:get_value(values_num, Opts, ?DEFAULT_VALUES_TO_KEEP),
    ?ETS_NAME = ets:new(?ETS_NAME, [public, named_table,
        {keypos, #metric_info.metric}]),
    {ok, #state{values_to_keep = ValuesToKeep}}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_subscribe/5.
%% @end
%%-------------------------------------------------------------------
-spec exometer_subscribe(exometer_report:metric(), exometer_report:datapoint(),
    exometer_report:extra(), exometer_report:interval(), state()) -> {ok, state()}.
exometer_subscribe(Metric, _DataPoint, _Extra, _Interval,
    State = #state{values_to_keep = ValuesToKeep}
) ->
    true = ets:insert(?ETS_NAME, #metric_info{
        metric = Metric,
        timestamp = calendar:local_time(),
        values = [0 || _ <- lists:seq(1, ValuesToKeep)]
    }),
    {ok, State}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_unsubscribe/4.
%% @end
%%-------------------------------------------------------------------
-spec exometer_unsubscribe(exometer_report:metric(), exometer_report:datapoint(),
    exometer_report:extra(), state()) -> {ok, state()}.
exometer_unsubscribe(Metric, _DataPoint, _Extra, State) ->
    true = ets:delete(?ETS_NAME, Metric),
    {ok, State}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_report/5.
%% @end
%%-------------------------------------------------------------------
-spec exometer_report(exometer_report:metric(), exometer_report:datapoint(),
    exometer_report:extra(), exometer:value(), state()) -> {ok, state()}.
exometer_report(Metric, _DataPoint, _Extra, Value,
    State = #state{values_to_keep = ValuesToKeep}
)->
    report(Metric, Value, ValuesToKeep),
    {ok, State}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_call/3.
%% @end
%%-------------------------------------------------------------------
-spec exometer_call(term(), pid(), state()) -> {ok, state()}.
exometer_call(_Unknown, _From, St) ->
    {ok, St}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_cast/2.
%% @end
%%-------------------------------------------------------------------
-spec exometer_cast(term(), state()) -> {ok, state()}.
exometer_cast(_Unknown, St) ->
    {ok, St}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_info/2.
%% @end
%%-------------------------------------------------------------------
-spec exometer_info(term(), state()) -> {ok, state()}.
exometer_info(_Unknown, St) ->
    {ok, St}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_newentry/2.
%% @end
%%-------------------------------------------------------------------
-spec exometer_newentry(term(), state()) -> {ok, state()}.
exometer_newentry(_Entry, St) ->
    {ok, St}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_setopts/2.
%% @end
%%-------------------------------------------------------------------
-spec exometer_setopts(exometer_report:metric(), [term()], exometer:status(),
    state()) -> {ok, state()}.
exometer_setopts(_Metric, _Options, _Status, St) ->
    {ok, St}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_terminate/2.
%% @end
%%-------------------------------------------------------------------
-spec exometer_terminate(term(), state()) -> {ok, state()}.
exometer_terminate(_, _) ->
    true = ets:delete(?ETS_NAME).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Returns saved values for given Metric.
%% @end
%%-------------------------------------------------------------------
-spec get(exometer_report:metric()) -> [#metric_info{}].
get(Metric) ->
    ets:lookup(?ETS_NAME, Metric).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Updates entry for given Metric.
%% @end
%%-------------------------------------------------------------------
-spec report(exometer_report:metric(), non_neg_integer(), non_neg_integer()) ->
    true.
report(Metric, NewValue, ValuesToKeep) ->
    [MetricInfo] = ets:lookup(?ETS_NAME, Metric),
    MetricInfo2 = update(MetricInfo, NewValue, ValuesToKeep),
    true = ets:insert(?ETS_NAME, MetricInfo2).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Updates #metric_info{} record for given Metric. Ensures that only
%% ValuesToKeep number of values will be saved.
%% @end
%%-------------------------------------------------------------------
-spec update(exometer_report:metric(), non_neg_integer(), non_neg_integer()) ->
    true.
update(MetricInfo = #metric_info{values=Values}, NewValue, ValuesToKeep) ->
    Length = length(Values) + 1,
    Values2 = lists:sublist(Values ++ [NewValue], Length - ValuesToKeep + 1, ValuesToKeep),
    MetricInfo#metric_info{
        values = Values2,
        timestamp = calendar:local_time()
    }.