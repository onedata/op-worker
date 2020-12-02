%%%-------------------------------------------------------------------
%%% @Clientor Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Model caching idp access tokens.
%%% @end
%%%-------------------------------------------------------------------
-module(idp_access_token).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([acquire/3, delete/2]).

%% datastore_model callbacks
-export([get_ctx/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

-type id() :: binary().
-type record() :: #idp_access_token{}.
-type doc() :: datastore_doc:doc(record()).
-type error() :: {error, term()}.
-type token() :: binary().
-type ttl() :: time:seconds().
-type expiration_time() :: time:seconds().
-type idp() :: binary().

-export_type([token/0, expiration_time/0]).

-define(REFRESH_THRESHOLD, application:get_env(?APP_NAME,
    idp_access_token_refresh_threshold, 300)).

-define(ID_SEPARATOR, <<"##">>).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec acquire(od_user:id(), gs_client_worker:client(), idp()) ->
    {ok, {token(), expiration_time()}} | error().
acquire(UserId, Client, IdP) ->
    Id = id(UserId, IdP),
    case datastore_model:get(?CTX, Id) of
        {ok, Doc = #document{value = #idp_access_token{token = Token}}} ->
            case should_refresh(Doc) of
                true ->
                    fetch_and_cache(UserId, Client, IdP);
                false ->
                    {ok, {Token, get_current_ttl(Doc)}}
            end;
        {error, not_found} ->
            fetch_and_cache(UserId, Client, IdP)
    end.

delete(UserId, Idp) ->
    datastore_model:delete(?CTX, id(UserId, Idp)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec id(od_user:id(), idp()) -> id().
id(UserId, IdP) ->
    <<(UserId)/binary, (?ID_SEPARATOR)/binary, (IdP)/binary>>.

-spec fetch_and_cache(od_user:id(), gs_client_worker:client(), idp()) ->
    {ok, {token(), expiration_time()}} | error().
fetch_and_cache(UserId, Client, IdP) ->
    % todo VFS-5298 maybe add critical section?
    case user_logic:fetch_idp_access_token(Client, UserId, IdP) of
        {ok, {Token, TTL}} ->
            Id = id(UserId, IdP),
            case cache(Id, Token, TTL) of
                ok ->
                    {ok, {Token, TTL}};
                Error ->
                    Error
            end;
        Error2 ->
            Error2
    end.

-spec cache(id(), binary(), ttl()) ->
    ok | error().
cache(Id, Token, TTL) ->
    ?extract_ok(datastore_model:save(?CTX, #document{
        key = Id,
        value = #idp_access_token{
            token = Token,
            expiration_time = global_clock:timestamp_seconds() + TTL
        }
    })).

-spec should_refresh(doc()) -> boolean().
should_refresh(Doc) ->
    ?REFRESH_THRESHOLD > get_current_ttl(Doc).

-spec get_current_ttl(doc() | record()) -> ttl().
get_current_ttl(#document{value = IdPAccessToken}) ->
    get_current_ttl(IdPAccessToken);
get_current_ttl(#idp_access_token{expiration_time = ExpirationTime}) ->
    ExpirationTime - global_clock:timestamp_seconds().

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
