%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper function for user authentication in REST/CDMI
%%% @end
%%%-------------------------------------------------------------------
-module(rest_auth).
-author("Tomasz Lichon").

-include("op_logic.hrl").
-include("http/rest.hrl").
-include("http/http_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([is_authorized/2, authenticate/1]).

% opaque type of auth token that is necessary to perform operations on files.
-type auth() :: any().

-export_type([auth/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function authorizes user and inserts 'auth' field to
%% request's State
%% @end
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | stop, req(), maps:map()}.
is_authorized(Req, State) ->
    case authenticate(Req) of
        {ok, ?USER(UserId, SessionId)} ->
            {true, Req, State#{
                user_id => UserId,
                auth => SessionId
            }};
        {ok, ?NOBODY} ->
            NewReq = cowboy_req:reply(?HTTP_401_UNAUTHORIZED, Req),
            {stop, NewReq, State};
        {error, not_found} ->
            NewReq = cowboy_req:reply(?HTTP_401_UNAUTHORIZED, Req),
            {stop, NewReq, State};
        {error, Error} ->
            ?debug("Authentication error ~p", [Error]),
            {{false, <<"authentication_error">>}, Req, State}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Authenticates user based on request headers.
%% @end
%%--------------------------------------------------------------------
-spec authenticate(req()) -> op_logic:client() | {error, term()}.
authenticate(Req) ->
    case resolve_auth(Req) of
        {error, not_found} ->
            {ok, ?NOBODY};
        Auth ->
            case user_identity:get_or_fetch(Auth) of
                {ok, #document{value = #user_identity{user_id = UserId} = Iden}} ->
                    case session_manager:reuse_or_create_rest_session(Iden, Auth) of
                        {ok, SessId} ->
                            {ok, ?USER(UserId, SessId)};
                        {error, {invalid_identity, _}} ->
                            user_identity:delete(Auth),
                            authenticate(Req)
                    end;
                Error ->
                    Error
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolves authorization carried by request (if any).
%% @end
%%--------------------------------------------------------------------
-spec resolve_auth(req()) -> user_identity:credentials() | {error, not_found}.
resolve_auth(Req) ->
    case parse_macaroon_from_header(Req) of
        undefined -> {error, not_found};
        Macaroon -> #macaroon_auth{macaroon = Macaroon}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Parses macaroon from request headers, accepted headers:
%%  * Macaroon
%%  * X-Auth-Token
%%  * Bearer Authentication
%% @end
%%--------------------------------------------------------------------
-spec parse_macaroon_from_header(req()) -> undefined | binary().
parse_macaroon_from_header(Req) ->
    case cowboy_req:header(<<"authorization">>, Req) of
        <<"Bearer ", Macaroon/binary>> ->
            Macaroon;
        _ ->
            case cowboy_req:header(<<"macaroon">>, Req) of
                undefined ->
                    cowboy_req:header(<<"x-auth-token">>, Req);
                Value ->
                    Value
            end
    end.
