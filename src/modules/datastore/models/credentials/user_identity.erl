%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Cache that maps credentials to users' identities
%%% @end
%%%-------------------------------------------------------------------
-module(user_identity).
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([get/1, fetch/1, get_or_fetch/1, delete/1, get_user_id/1,
    get_or_fetch_user_id/1]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type record() :: #user_identity{}.
-type doc() :: datastore_doc:doc(record()).
-type credentials() :: #token_auth{} | binary().
-export_type([credentials/0]).

-define(CTX, #{
    model => ?MODULE,
    routing => local,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns user's identity.
%% @end
%%--------------------------------------------------------------------
-spec get(credentials()) -> {ok, doc()} | {error, term()}.
get(Credentials) ->
    datastore_model:get(?CTX, term_to_binary(to_auth(Credentials))).

%%--------------------------------------------------------------------
%% @doc
%% Fetches user's identity from onezone and stores it in cache.
%% @end
%%--------------------------------------------------------------------
-spec fetch(credentials()) -> {ok, doc()} | {error, term()}.
fetch(Credentials) ->
    Auth = to_auth(Credentials),
    try
        case user_logic:get_by_auth(Auth) of
            {ok, #document{key = UserId}} ->
                case provider_logic:has_eff_user(UserId) of
                    false ->
                        ?ERROR_FORBIDDEN;
                    true ->
                        NewDoc = #document{
                            key = term_to_binary(Auth),
                            value = #user_identity{user_id = UserId}
                        },
                        case datastore_model:create(?CTX, NewDoc) of
                            {ok, _} -> ok;
                            {error, already_exists} -> ok
                        end,
                        {ok, NewDoc}
                end;
            {error, _} = Error ->
                Error
        end
    catch
        _:Reason ->
            ?error_stacktrace("Cannot establish onedata user identity due to: ~p", [Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets user's identity from cache, or fetches it from onezone
%% and stores in cache.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(credentials()) -> {ok, doc()} | {error, term()}.
get_or_fetch(Credentials) ->
    Auth = to_auth(Credentials),
    case datastore_model:get(?CTX, term_to_binary(Auth)) of
        {ok, Doc} -> {ok, Doc};
        {error, not_found} -> fetch(Auth);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes user's identity from cache.
%% @end
%%--------------------------------------------------------------------
-spec delete(credentials()) -> ok | {error, term()}.
delete(Credentials) ->
    datastore_model:delete(?CTX, term_to_binary(to_auth(Credentials))).


-spec get_user_id(credentials()) -> {ok, od_user:id()} | {error, term()}.
get_user_id(Credentials) ->
    case user_identity:get(Credentials) of
        {ok, #document{value = #user_identity{user_id = UserId}}} ->
            {ok, UserId};
        Error ->
            Error
    end.

-spec get_or_fetch_user_id(credentials()) -> {ok, od_user:id()} | {error, term()}.
get_or_fetch_user_id(Credentials) ->
    case get_or_fetch(Credentials) of
        {ok, #document{value = #user_identity{user_id = UserId}}} ->
            {ok, UserId};
        Error ->
            Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec to_auth(credentials()) -> #token_auth{}.
to_auth(Auth = #token_auth{}) ->
    Auth;
to_auth(Token) when is_binary(Token) ->
    #token_auth{token = Token}.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.