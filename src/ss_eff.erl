%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(ss_eff).
-author("Jakub Kudzia").

%% API
-export([init/0, insert_stat/2, insert_ls/2, stat_stats/0, ls_stats/0, delete/0, reset/0]).

-define(STAT_TABLE, ss_eff_stat).
-define(LS_TABLE, ss_eff_ls).

%%%===================================================================
%%% API functions
%%%===================================================================

init() ->
    ets:new(?STAT_TABLE, [public, named_table]),
    ets:new(?LS_TABLE, [public, named_table]).

insert_stat(Key, Time) ->
    ets:insert(?STAT_TABLE, {Key, Time}).

insert_ls(Key, Time) ->
    ets:insert(?LS_TABLE, {Key, Time}).

stat_stats() ->
    stats(?STAT_TABLE).

ls_stats() ->
    stats(?LS_TABLE).

delete() ->
    delete(?STAT_TABLE),
    delete(?LS_TABLE).

reset() ->
    delete(),
    init().

%%%===================================================================
%%% Internal functions
%%%===================================================================

delete(Table) ->
    catch ets:delete(Table).

stats(Table) ->
    Values = [V || {_K, V} <- ets:tab2list(Table)],
    Length = length(Values),
    Mean = lists:sum(Values) / Length,
    SortedList = lists:sort(Values),
    Min = hd(SortedList),
    Max = lists:last(SortedList),
    {Mean, Min, Max, median(SortedList), std_dev(Values, Mean), Length}.

median(SortedList) when length(SortedList) rem 2 =:= 1 ->
    lists:nth((length(SortedList) + 1) div 2, SortedList);
median(SortedList) when length(SortedList) rem 2 =:= 0 ->
    [M1, M2] = lists:sublist(SortedList, length(SortedList) div 2, 2),
    (M1 + M2)/2.

std_dev(Values, Mean) ->
    N = length(Values),
    math:sqrt(1/(N - 1) * lists:sum([math:pow(V - Mean, 2) || V <- Values])).