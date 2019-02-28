%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
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
-export([get/2, get/3, delete/1, delete/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

%% exported for CT
-export([acquire/3]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined,
    generated_key => false
}).

-type key() :: binary().
-type record() :: #idp_access_token{}.
-type doc() :: datastore_doc:doc(record()).
-type error() :: {error, term()}.

-type onedata_token() :: binary().
-type auth() :: session:id() | onedata_token().
-type idp() :: binary().


-define(REFRESH_THRESHOLD, application:get_env(?APP_NAME,
    idp_access_token_refresh_threshold, 300)).

-define(KEY_SEPARATOR, <<"##">>).
-define(EMPTY_USER_ID, <<"">>).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec get(onedata_token(), idp()) -> {ok, {binary(), non_neg_integer()}} | error().
get(OnedataAccessToken, IdP) ->
    get(?EMPTY_USER_ID, OnedataAccessToken, IdP).

-spec get(od_user:id(), auth(), idp()) -> {ok, {binary(), non_neg_integer()}} | error().
get(UserId, Auth, IdP) ->
    Key = key(UserId, IdP),
    case datastore_model:get(?CTX, Key) of
        {ok, Doc} ->
            CurrentTime = time_utils:system_time_seconds(),
            case should_refresh(Doc, CurrentTime) of
                true ->
                    acquire_and_cache(UserId, Auth, IdP);
                false ->
                    {ok, {get_token(Doc), get_current_ttl(Doc, CurrentTime)}}
            end;
        {error, not_found} ->
            acquire_and_cache(UserId, Auth, IdP)
    end.

delete(IdP) ->
    delete(?EMPTY_USER_ID, IdP).

delete(UserId, Idp) ->
    datastore_model:delete(?CTX, key(UserId, Idp)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec key(od_user:id(), idp()) -> key().
key(UserId, IdP) ->
    <<(UserId)/binary, (?KEY_SEPARATOR)/binary, (IdP)/binary>>.

-spec acquire_and_cache(od_user:id(), auth(), idp()) ->
    {ok, {binary(), non_neg_integer()}} | error().
acquire_and_cache(UserId, Auth, IdP) ->
    CurrentTime = time_utils:system_time_seconds(),
    case idp_access_token:acquire(UserId, Auth, IdP) of
        {ok, {Token, TTL}} ->
            Key = key(UserId, IdP),
            case cache(Key, Token, TTL, CurrentTime) of
                ok ->
                    {ok, {Token, TTL}};
                Error->
                    Error
            end;
        Error2 ->
            Error2
    end.

-spec acquire(od_user:id(), auth(), idp()) -> 
    {ok, {binary(), non_neg_integer()}} | error().
acquire(?EMPTY_USER_ID, OnedataAccessToken, IdP) ->
    user_logic:acquire_idp_access_token(OnedataAccessToken, IdP);
acquire(UserId, SessionId, IdP) ->
    user_logic:acquire_idp_access_token(SessionId, UserId, IdP).

-spec cache(key(), binary(), non_neg_integer(), non_neg_integer()) ->
    ok | error().
cache(Key, Token, TTL, CurrentTime) ->
    ?extract_ok(save(#document{
        key = Key,
        value = #idp_access_token{
            token = Token,
            expiration_time = CurrentTime + TTL
        }
    })).

-spec save(doc()) -> {ok, doc()} | error().
save(Doc) ->
    datastore_model:save(?CTX, Doc).

-spec should_refresh(doc() | record(), non_neg_integer()) -> boolean().
should_refresh(#document{value = IdPAccessToken}, CurrentTime) ->
    should_refresh(IdPAccessToken, CurrentTime);
should_refresh(#idp_access_token{expiration_time = ExpirationTime}, CurrentTime) ->
    CurrentTime > (ExpirationTime - ?REFRESH_THRESHOLD).

-spec get_token(doc() | record()) -> binary().
get_token(#document{value = IdPAccessToken}) ->
    get_token(IdPAccessToken);
get_token(#idp_access_token{token = Token})  ->
    Token.

-spec get_current_ttl(doc() | record(), non_neg_integer()) -> non_neg_integer().
get_current_ttl(#document{value = IdPAccessToken}, CurrentTime) ->
    get_current_ttl(IdPAccessToken, CurrentTime);
get_current_ttl(#idp_access_token{expiration_time = ExpirationTime }, CurrentTime)  ->
    ExpirationTime - CurrentTime.

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

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {token, string},
        {expiration_time, integer}
    ]}.
