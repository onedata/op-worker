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
    exometer_terminate/2
]).

-record(state, {}).
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
exometer_init(_Opts) ->
    {ok, #state{}}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_subscribe/5.
%% @end
%%-------------------------------------------------------------------
-spec exometer_subscribe(exometer_report:metric(), exometer_report:datapoint(),
    exometer_report:extra(), exometer_report:interval(), state()) -> {ok, state()}.
exometer_subscribe(Metric, _DataPoint, _Extra, _Interval, State) ->
    {ok, _} = storage_sync_histogram:new(Metric),
    {ok, State}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_unsubscribe/4.
%% @end
%%-------------------------------------------------------------------
-spec exometer_unsubscribe(exometer_report:metric(), exometer_report:datapoint(),
    exometer_report:extra(), state()) -> {ok, state()}.
exometer_unsubscribe(Metric, _DataPoint, _Extra, State) ->
    ok = storage_sync_histogram:remove(Metric),
    {ok, State}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_report/5.
%% @end
%%-------------------------------------------------------------------
-spec exometer_report(exometer_report:metric(), exometer_report:datapoint(),
    exometer_report:extra(), exometer:value(), state()) -> {ok, state()}.
exometer_report(Metric, _DataPoint, _Extra, Value, State)->
    {ok, _} = storage_sync_histogram:add(Metric, Value),
    {ok, State}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_call/3.
%% @end
%%-------------------------------------------------------------------
-spec exometer_call(term(), pid(), state()) -> {ok, state()}.
exometer_call(_Unknown, _From, State) ->
    {ok, State}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_cast/2.
%% @end
%%-------------------------------------------------------------------
-spec exometer_cast(term(), state()) -> {ok, state()}.
exometer_cast(_Unknown, State) ->
    {ok, State}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_info/2.
%% @end
%%-------------------------------------------------------------------
-spec exometer_info(term(), state()) -> {ok, state()}.
exometer_info(_Unknown, State) ->
    {ok, State}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_newentry/2.
%% @end
%%-------------------------------------------------------------------
-spec exometer_newentry(term(), state()) -> {ok, state()}.
exometer_newentry(_Entry, State) ->
    {ok, State}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_setopts/2.
%% @end
%%-------------------------------------------------------------------
-spec exometer_setopts(exometer:entry(), [term()], exometer:status(),
    state()) -> {ok, state()}.
exometer_setopts(_Metric, _Options, _Status, State) ->
    {ok, State}.

%%-------------------------------------------------------------------
%% @doc
%% {@link exometer_report} callback exometer_terminate/2.
%% @end
%%-------------------------------------------------------------------
-spec exometer_terminate(term(), state()) -> true.
exometer_terminate(_, _) ->
    true.