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

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([run_and_normalize_error/2]).
-export([extract_ok/1, extract_key/1]).

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
        Other -> Other
    catch
        error:Reason ->
            Reason2 = normalize_error(Reason),
            case Reason2 of
                not_found ->
                    ?debug_stacktrace("~p error: ~p", [Module, Reason2]);
                _ ->
                    ?error_stacktrace("~p error: ~p", [Module, Reason2])
            end,
            {error, Reason2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Converts successful datastore call result to ok.
%% @end
%%--------------------------------------------------------------------
-spec extract_ok(term()) -> ok | term().
extract_ok({ok, _}) -> ok;
extract_ok([{ok, _}]) -> ok;
extract_ok(Result) -> Result.

%%--------------------------------------------------------------------
%% @doc
%% Extracts document key from successful datastore call result.
%% @end
%%--------------------------------------------------------------------
-spec extract_key(term()) -> {ok, datastore:key()} | term().
extract_key({ok, #document{key = Key}}) -> {ok, Key};
extract_key(Result) -> Result.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
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
