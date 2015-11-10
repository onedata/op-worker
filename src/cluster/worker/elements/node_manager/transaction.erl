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

-include("cluster/worker/elements/task_manager/task_manager.hrl").
-include_lib("ctool/include/logging.hrl").

%% Rollback function gets Context as only argument.
%% It should return {ok, NewContext}, {ok, stop}, {retry, Context, Reason} or {error, Reason}.
-type rollback_fun() :: fun((term()) ->
    {ok, NewContext :: term()} |
    {ok, stop} |
    {retry, NewContext:: term(), Reason :: string()} |
    {error, Reason :: string()}).

-record(tranaction, {rollback_funs = [] :: [rollback_fun()]}).
-define(DICT_KEY, transactions_list).

%% API
-export([start/0, commit/0, rollback_point/1, rollback/0, rollback/1, rollback/2,
    rollback_asynch/0, rollback_asynch/1, rollback_asynch/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts transaction.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok.
start() ->
    CurrentList = case get(?DICT_KEY) of
                      undefined -> [];
                      List -> List
                  end,
    put(?DICT_KEY, [#tranaction{} | CurrentList]),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Ends transaction.
%% @end
%%--------------------------------------------------------------------
-spec commit() -> ok.
commit() ->
    [_ | Tail] = get(?DICT_KEY),
    put(?DICT_KEY, Tail),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Sets rollback point for transaction (function to be used during rollback).
%% @end
%%--------------------------------------------------------------------
-spec rollback_point(Fun :: rollback_fun()) -> ok.
rollback_point(Fun) ->
    [Current | Tail] = get(?DICT_KEY),
    Funs = Current#tranaction.rollback_funs,
    put(?DICT_KEY, [Current#tranaction{rollback_funs = [Fun | Funs]} | Tail]),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Rollbacks transaction - if rollback fails starts rollback task.
%% @end
%%--------------------------------------------------------------------
-spec rollback() -> ok | task_sheduled | {rollback_fun_error, term()}.
rollback() ->
    rollback(undefined).

%%--------------------------------------------------------------------
%% @doc
%% Rollbacks transaction - if rollback fails starts rollback task.
%% @end
%%--------------------------------------------------------------------
-spec rollback(Context :: term()) -> ok | task_sheduled | {rollback_fun_error, term()}.
rollback(Context) ->
    rollback(Context, ?NODE_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% Rollbacks transaction - if rollback fails starts rollback task.
%% @end
%%--------------------------------------------------------------------
-spec rollback(Context :: term(), Level :: task_manager:level()) ->
    ok | task_sheduled | {rollback_fun_error, term()}.
rollback(Context, Level) ->
    [Current | _] = get(?DICT_KEY),
    Funs = Current#tranaction.rollback_funs,
    Ans = case run_rollback_funs(Funs, Context) of
        {ok, _, []} ->
            ok;
        {ok, NewContext, RepeatsList} ->
            ok = start_rollback_task(RepeatsList, NewContext, Level),
            task_sheduled;
        Error ->
            ok = start_rollback_task(Funs, Context, Level),
            Error
    end,
    commit(),
    Ans.

%%--------------------------------------------------------------------
%% @doc
%% Rollbacks transaction in asynch task.
%% @end
%%--------------------------------------------------------------------
-spec rollback_asynch() -> ok.
rollback_asynch() ->
    rollback_asynch(undefined).

%%--------------------------------------------------------------------
%% @doc
%% Rollbacks transaction in asynch task.
%% @end
%%--------------------------------------------------------------------
-spec rollback_asynch(Context :: term()) -> ok.
rollback_asynch(Context) ->
    rollback_asynch(Context, ?NODE_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% Rollbacks transaction in asynch task.
%% @end
%%--------------------------------------------------------------------
-spec rollback_asynch(Context :: term(), Level :: task_manager:level()) -> ok.
rollback_asynch(Context, Level) ->
    [Current | _] = get(?DICT_KEY),
    Funs = Current#tranaction.rollback_funs,
    start_rollback_task(Funs, Context, Level),
    commit().

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Executes list of rollback functions.
%% @end
%%--------------------------------------------------------------------
-spec run_rollback_funs(Funs :: [rollback_fun()], Context :: term()) ->
    ok | {rollback_fun_error, term()}.
run_rollback_funs(Funs, Context) ->
    run_rollback_funs(Funs, Context, []).

run_rollback_funs([], Context, RepeatsList) ->
    {ok, Context, lists:reverse(RepeatsList)};

run_rollback_funs([Fun | Funs], Context, RepeatsList) ->
    case Fun(Context) of
        {ok, stop} ->
            {ok, Context, []};
        {ok, NewContext} ->
            run_rollback_funs(Funs, NewContext, RepeatsList);
        {retry, NewContext, Reason} ->
            ?warning_stacktrace("Rollback function to be retried: ~p", [Reason]),
            run_rollback_funs(Funs, NewContext, [Fun | RepeatsList]);
        Other ->
            ?error_stacktrace("Rollback function failed: ~p", [Other]),
            {rollback_fun_error, Other}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Executes list of rollback functions in asynch task.
%% @end
%%--------------------------------------------------------------------
-spec start_rollback_task(Funs :: [rollback_fun()], Context :: term(),
    Level :: task_manager:level()) -> ok.
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
    ?warning_stacktrace("Starting rollback task"),
    task_manager:start_task(Task, Level).