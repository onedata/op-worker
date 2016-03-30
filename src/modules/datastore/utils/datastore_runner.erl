%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc Runner for datastore models
%%%--------------------------------------------------------------------
-module(datastore_runner).
-author("Tomasz Lichon").

-include_lib("ctool/include/logging.hrl").

%% API
-export([run_and_normalize_error/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Runs given function and converts any badmatch/case_clause to
%% {error, Reason :: term()}
%% @end
%%--------------------------------------------------------------------
-spec run_and_normalize_error(function(), module()) -> term().
run_and_normalize_error(Fun, Module) ->
    try Fun() of
        __Other -> __Other
    catch
        error:__Reason ->
            __Reason0 = normalize_error(__Reason),
            case __Reason0 of
                {not_found, _} ->
                    ?debug_stacktrace("~p error: ~p", [Module, __Reason0]);
                _ ->
                    ?error_stacktrace("~p error: ~p", [Module, __Reason0])
            end,
            {error, __Reason0}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns just error reason for given error tuple
%% @end
%%--------------------------------------------------------------------
-spec normalize_error(term()) -> term().
normalize_error({badmatch, Reason}) ->
    normalize_error(Reason);
normalize_error({case_clause, Reason}) ->
    normalize_error(Reason);
normalize_error({error, Reason}) ->
    normalize_error(Reason);
normalize_error({ok, Inv}) ->
    normalize_error({invalid_response, normalize_error(Inv)});
normalize_error(Reason) ->
    Reason.
