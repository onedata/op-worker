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
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([get_or_fetch/1, delete/1, get_or_fetch_user_id/1]).

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
%% Gets user's identity from cache, or fetches it from onezone
%% and stores in cache.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(credentials()) -> {ok, doc()} | {error, term()}.
get_or_fetch(Credentials) ->
    Auth = to_auth(Credentials),
    case datastore_model:get(?CTX, id(Auth)) of
        {ok, Doc} -> {ok, Doc};
        {error, not_found} -> fetch(Auth);
        {error, Reason} -> {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Gets user's is based on token auth.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch_user_id(credentials()) -> {ok, od_user:id()} | {error, term()}.
get_or_fetch_user_id(Credentials) ->
    case get_or_fetch(Credentials) of
        {ok, #document{value = #user_identity{user_id = UserId}}} ->
            {ok, UserId};
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Deletes user's identity from cache.
%% @end
%%--------------------------------------------------------------------
-spec delete(credentials()) -> ok | {error, term()}.
delete(Credentials) ->
    datastore_model:delete(?CTX, id(to_auth(Credentials))).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Fetches user's identity from onezone and stores it in cache.
%% @end
%%--------------------------------------------------------------------
-spec fetch(credentials()) -> {ok, doc()} | {error, term()}.
fetch(Credentials) ->
    Auth = to_auth(Credentials),
    try
        case user_logic:preauthorize(Auth) of
            {ok, #auth{subject = ?SUB(user, UserId), caveats = _Caveats}} ->
                %% @TODO VFS-5719 use the caveats in op_logic and user_ctx
                case provider_logic:has_eff_user(UserId) of
                    false ->
                        ?ERROR_FORBIDDEN;
                    true ->
                        % Fetch the user doc to trigger user setup (od_user:run_after/3)
                        {ok, _} = user_logic:get(Auth, UserId),
                        NewDoc = #document{
                            key = id(Auth),
                            value = #user_identity{user_id = UserId}
                        },
                        %% @TODO VFS-5719 the auth should be cached no longer
                        %% than token's TTL
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


-spec to_auth(credentials()) -> #token_auth{}.
to_auth(Auth = #token_auth{}) ->
    Auth;
to_auth(Token) when is_binary(Token) ->
    #token_auth{token = Token}.


-spec id(#token_auth{}) -> binary().
id(#token_auth{token = Token}) ->
    Token.

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