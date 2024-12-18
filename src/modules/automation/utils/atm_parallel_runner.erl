%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Convenience functions for parallel code execution in automation related modules.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_parallel_runner).
-author("Lukasz Opiola").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([foreach/2, map/2]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Runs the callback for each element on the list, in parallel. The call is
%% considered successful when no process return '{error, _}', otherwise the first
%% encountered error is thrown to the calling process. The parallel processes
%% may throw an error themselves and it will be propagated.
%% @end
%%--------------------------------------------------------------------
-spec foreach(fun((X) -> ok | {error, term()}), [X]) -> ok | no_return().
foreach(Callback, Elements) ->
    map(Callback, Elements),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Maps, using callback, each element on the list, in parallel. The call is
%% considered successful when no process return '{error, _}', otherwise the first
%% encountered error is thrown to the calling process. The parallel processes
%% may throw an error themselves and it will be propagated.
%% @end
%%--------------------------------------------------------------------
-spec map(fun((X) -> {error, term()} | term()), [X]) -> term() | no_return().
map(Callback, Elements) ->
    try
        Results = lists_utils:pmap(fun(Element) ->
            try
                Callback(Element)
            catch
                throw:{error, _} = Error ->
                    Error
            end
        end, Elements),

        lists:foreach(fun
            ({error, _} = Error) -> throw(Error);
            (_) -> ok
        end, Results),

        Results
    catch
        throw:{error, _} = Error ->
            throw(Error);
        Class:Reason:Stacktrace ->
            ?error_stacktrace(
                "Unexpected error in ~w:~w - ~w:~tp",
                [?MODULE, ?FUNCTION_NAME, Class, Reason],
                Stacktrace
            ),
            throw(?ERROR_INTERNAL_SERVER_ERROR)
    end.
