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
-include_lib("ctool/include/errors.hrl").

%% API
-export([run_and_normalize_error/1]).
-export([extract_ok/1, extract_key/1, ok_if_not_found/1, ok_if_exists/1, ok_if_no_change/1]).
-export([get_field/3]).
-export([normalize_error/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Runs given function and converts any badmatch/case_clause to
%% {error, Reason :: term()}
%% @end
%%--------------------------------------------------------------------
-spec run_and_normalize_error(function()) -> term().
run_and_normalize_error(Fun) ->
    try Fun() of
        Other -> Other
    catch
        error:Reason:Stacktrace ->
            Reason2 = normalize_error(Reason),
            case Reason2 of
                not_found ->
                    ?debug_exception(error, Reason2, Stacktrace);
                _ ->
                    ?error_exception(error, Reason2, Stacktrace)
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

%%--------------------------------------------------------------------
%% @doc
%% Marks datastore call to non existing document as ok.
%% @end
%%--------------------------------------------------------------------
-spec ok_if_not_found(T) -> ok | T.
ok_if_not_found({error, not_found}) -> ok;
ok_if_not_found(Result) -> Result.

%%--------------------------------------------------------------------
%% @doc
%% Marks datastore call to already existing document as ok.
%% @end
%%--------------------------------------------------------------------
-spec ok_if_exists(T) -> ok | T.
ok_if_exists({error, already_exists}) -> ok;
ok_if_exists(Result) -> Result.


-spec ok_if_no_change(T) -> ok | T.
ok_if_no_change({error, no_change}) -> ok;
ok_if_no_change(Result) -> Result.


-spec get_field(datastore:key(), datastore_model:model(),
    fun((datastore:doc()) -> {ok, FieldValue :: term()})) ->
    {ok, FieldValue :: term()} | {error, term()}.
get_field(Key, Model, GetterFun) ->
    case Model:get(Key) of
        {ok, Doc} -> GetterFun(Doc);
        {error, _} = Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns just error reason for given error tuple
%% @end
%%--------------------------------------------------------------------
-spec normalize_error(term()) -> term().
normalize_error({badmatch, {error, _} = Error}) ->
    normalize_error(Error);
normalize_error({case_clause, {error, _} = Error}) ->
    normalize_error(Error);
normalize_error({error, Reason}) ->
    normalize_error(Reason);
normalize_error({ok, Inv}) ->
    normalize_error({invalid_response, normalize_error(Inv)});
normalize_error(Reason) ->
    Reason.
