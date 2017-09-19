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

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_users.hrl").

%% API
-export([get/1, fetch/1, get_or_fetch/1, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type record() :: #user_identity{}.
-type doc() :: datastore_doc:doc(record()).
-type credentials() :: #macaroon_auth{} | #token_auth{} | #basic_auth{} |
                       #certificate_auth{}.
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
get(Auth) ->
    datastore_model:get(?CTX, term_to_binary(Auth)).

%%--------------------------------------------------------------------
%% @doc
%% Fetches user's identity from onezone and stores it in cache.
%% @end
%%--------------------------------------------------------------------
-spec fetch(credentials()) -> {ok, doc()} | {error, term()}.
fetch(#certificate_auth{}) ->
    {error, not_supported};
fetch(Auth) ->
    try
        case oz_users:get_details(Auth) of
            {ok, #user_details{id = UserId}} ->
                {ok, #document{key = Id}} = od_user:get_or_fetch(Auth, UserId),
                NewDoc = #document{
                    key = term_to_binary(Auth),
                    value = #user_identity{user_id = Id}
                },
                case datastore_model:create(?CTX, NewDoc) of
                    {ok, _} -> ok;
                    {error, already_exists} -> ok
                end,
                {ok, NewDoc};
            {error, {_Code, _Reason, _}} = Error ->
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
get_or_fetch(Auth) ->
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
delete(Auth) ->
    datastore_model:delete(?CTX, term_to_binary(Auth)).

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