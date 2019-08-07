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
-module(http_auth).
-author("Tomasz Lichon").

-include("op_logic.hrl").
-include("http/rest.hrl").
-include("http/http_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([is_authorized/2, authenticate/2]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% This function authorizes user and inserts 'auth' field to
%% request's State
%% @end
%%--------------------------------------------------------------------
-spec is_authorized(cowboy_req:req(), map()) ->
    {true | {false, binary()} | stop, cowboy_req:req(), map()}.
is_authorized(Req, State) ->
    case authenticate(Req, rest) of
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
-spec authenticate(#macaroon_auth{} | cowboy_req:req(), rest | gui) ->
    aai:auth() | {error, term()}.
authenticate(#macaroon_auth{} = Credentials, Type) ->
    case user_identity:get_or_fetch(Credentials) of
        {ok, #document{value = #user_identity{user_id = UserId} = Iden}} ->
            Result = case Type of
                rest ->
                    session_manager:reuse_or_create_rest_session(Iden, Credentials);
                gui ->
                    session_manager:reuse_or_create_gui_session(Iden, Credentials)
            end,
            case Result of
                {ok, SessionId} ->
                    {ok, ?USER(UserId, SessionId)};
                {error, {invalid_identity, _}} ->
                    user_identity:delete(Credentials),
                    authenticate(Credentials, Type)
            end;
        Error ->
            Error
    end;
authenticate(Req, Type) ->
    case tokens:parse_access_token_header(Req) of
        undefined ->
            {ok, ?NOBODY};
        AccessToken ->
            authenticate(#macaroon_auth{macaroon = AccessToken}, Type)
    end.
