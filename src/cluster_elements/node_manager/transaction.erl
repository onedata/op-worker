%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions used to create transactions.
%%% @end
%%%-------------------------------------------------------------------
-module(transaction).
-author("Michal Wrzeszcz").

-include("cluster_elements/node_manager/task_manager.hrl").
-include_lib("ctool/include/logging.hrl").

% Fun -> {ok, Context}, {ok, stop}, {retry, Context, Reason}, {error, Reason}
-record(tranaction, {rollback_funs = [] :: [fun((term()) -> term())]}).
-define(DICT_KEY, transactions_list).

%% API
-export([]).

start() ->
    CurrentList = case get(?DICT_KEY) of
                      undefined -> [];
                      List -> List
                  end,
    put(?DICT_KEY, [#tranaction{} | CurrentList]).

commit() ->
    [_ | Tail] = get(?DICT_KEY),
    put(?DICT_KEY, Tail).

rollback_point(Fun) ->
    [Current | Tail] = get(?DICT_KEY),
    Funs = Current#tranaction.rollback_funs,
    put(?DICT_KEY, [Current#tranaction{rollback_funs = [Fun | Funs]} | Tail]).

rollback() ->
    rollback(undefined).

rollback(Context) ->
    rollback(Context, ?NODE_LEVEL).

rollback(Context, Level) ->
    [Current | _] = get(?DICT_KEY),
    Funs = Current#tranaction.rollback_funs,
    Ans = case run_rollback_funs(Funs, Context) of
        {ok, _, []} ->
            ok;
        {ok, NewContext, RepeatsList} ->
            start_rollback_task(RepeatsList, NewContext, Level);
        Error ->
            Error
    end,
    commit(),
    Ans.

rollback_asynch() ->
    rollback_asynch(undefined).

rollback_asynch(Context) ->
    rollback_asynch(Context, ?NODE_LEVEL).

rollback_asynch(Context, Level) ->
    [Current | _] = get(?DICT_KEY),
    Funs = Current#tranaction.rollback_funs,
    Ans = start_rollback_task(Funs, Context, Level),
    commit(),
    Ans.




run_rollback_funs(Funs, Context) ->
    run_rollback_funs(Funs, Context, []).

run_rollback_funs([], Context, RepeatsList) ->
    {ok, Context, lists:reverse(RepeatsList)};

run_rollback_funs([Fun | Funs], Context, RepeatsList) ->
    case Fun(Context) of
        {ok, NewContext} ->
            run_rollback_funs(Funs, NewContext, RepeatsList);
        {ok, stop} ->
            {ok, Context, []};
        {retry, NewContext, Reason} ->
            ?warning("Rollback function to be retried: ~p", [Reason]),
            run_rollback_funs(Funs, NewContext, [Fun | RepeatsList]);
        Other ->
            ?error("Rollback function failed: ~p", [Other]),
            error
    end.

start_rollback_task(Funs, Context, Level) ->
    Task = fun() ->
        case run_rollback_funs(Funs, Context) of
            {ok, _, []} ->
                ok;
            {ok, NewContext, RepeatsList} ->
                start_rollback_task(RepeatsList, NewContext, Level);
            Error ->
                Error
        end
    end,
    task_manager:start_task(Task, Level).