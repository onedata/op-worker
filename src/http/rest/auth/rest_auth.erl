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

-include("http/http_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([is_authorized/2, authenticate/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function authorizes user and inserts 'auth' field to
%% request's State
%% @end
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | halt, req(), maps:map()}.
is_authorized(Req, State) ->
    case authenticate(Req) of
        {ok, Auth} ->
            {true, Req, State#{auth => Auth}};
        {error, {not_found, _}} ->
            GrUrl = oz_plugin:get_oz_url(),
            ProviderId = oneprovider:get_provider_id(),
            {_, NewReq2} = cowboy_req:host(Req),
            {<<"http://", Url/binary>>, NewReq3} = cowboy_req:url(NewReq2),

            {ok, NewReq4} = cowboy_req:reply(
                307,
                [
                    {<<"location">>, <<(list_to_binary(GrUrl))/binary,
                        "/user/providers/", ProviderId/binary, "/auth_proxy?ref=https://", Url/binary>>}
                ],
                <<"">>,
                NewReq3
            ),
            {halt, NewReq4, State};
        {error, Error} ->
            ?debug("Authentication error ~p", [Error]),
            {{false, <<"authentication_error">>}, Req, State}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Authenticates user based on request headers or certificate.
%% @end
%%--------------------------------------------------------------------
-spec authenticate(Req :: req()) -> {ok, session:id()} | {error, term()}.
authenticate(Req) ->
    case resolve_auth(Req) of
        {error, Reason} ->
            {error, Reason};
        Auth ->
            case user_identity:get_or_fetch(Auth) of
                {ok, #document{value = Iden}} ->
                    {ok, SessId} = session_manager:reuse_or_create_rest_session(Iden, Auth),
                    {ok, SessId};
                Error ->
                    Error
            end
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Resolves authorization carried by request (if any). Types of authorization
%% supported:
%% - macaroon
%% - token
%% - basic auth
%% - certificate
%% @end
%%--------------------------------------------------------------------
-spec resolve_auth(Req :: req()) -> user_identity:credentials() | {error, term()}.
resolve_auth(Req) ->
    resolve_auth(macaroon, Req).


%%--------------------------------------------------------------------
%% @doc
%% Resolves authorization carried by request (if any). Tries all possible types
%% of authorization, starting with macaroon.
%% Types of authorization supported:
%% - macaroon
%% - token
%% - basic auth
%% - certificate
%% @end
%%--------------------------------------------------------------------
-spec resolve_auth(Type :: macaroon | token | basic | certificate, Req :: req()) ->
    user_identity:credentials() | {error, term()}.
resolve_auth(macaroon, Req) ->
    case cowboy_req:header(<<"macaroon">>, Req) of
        {undefined, _} ->
            resolve_auth(token, Req);
        {Macaroon, _} ->
            #macaroon_auth{macaroon = Macaroon}
    end;
resolve_auth(token, Req) ->
    case cowboy_req:header(<<"x-auth-token">>, Req) of
        {undefined, _} ->
            resolve_auth(basic, Req);
        {Token, _} ->
            #token_auth{token = Token}
    end;
resolve_auth(basic, Req) ->
    case cowboy_req:header(<<"authorization">>, Req) of
        {undefined, _} ->
            resolve_auth(certificate, Req);
        {BasicAuthHeader, _} ->
            #basic_auth{credentials = BasicAuthHeader}
    end;
resolve_auth(certificate, Req) ->
    Socket = cowboy_req:get(socket, Req),
    case ssl:peercert(Socket) of
        {ok, Der} ->
            Certificate = public_key:pkix_decode_cert(Der, otp),
            #certificate_auth{otp_cert = Certificate};
        {error, Reason} ->
            {error, Reason}
    end.
