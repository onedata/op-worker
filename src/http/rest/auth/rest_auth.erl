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
-spec is_authorized(req(), #{}) -> {true | {false, binary()} | halt, req(), #{}}.
is_authorized(Req, State) ->
    case authenticate(Req) of
        {{ok, Auth}, NewReq} ->
            {true, NewReq, State#{auth => Auth}};
        {{error, {not_found, _}}, NewReq} ->
            GrUrl = oz_plugin:get_oz_url(),
            ProviderId = oneprovider:get_provider_id(),
            {_, NewReq2} = cowboy_req:host(NewReq),
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
        {{error, Error}, NewReq} ->
            ?debug("Authentication error ~p", [Error]),
            {{false, <<"authentication_error">>}, NewReq, State}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Authenticates user basing on request headers
%% @end
%%--------------------------------------------------------------------
-spec authenticate(Req :: req()) -> {{ok, session:id()} | {error, term()}, req()}.
authenticate(Req) ->
    case cowboy_req:header(<<"x-auth-token">>, Req) of
        {undefined, Req2} ->
            authenticate_using_cert(Req2);
        {Token, Req2} ->
            authenticate_using_token(Req2, Token)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Authenticates user basing on provided token.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_token(req(), Token :: binary()) ->
    {{ok, session:id()} | {error, term()}, req()}.
authenticate_using_token(Req, Token) ->
    case macaroon:deserialize(Token) of
        {ok, Macaroon} ->
            Auth = #auth{macaroon = Macaroon},
            case identity:get_or_fetch(Auth) of
                {ok, #document{value = Iden}} ->
                    {ok, SessId} = session_manager:reuse_or_create_rest_session(Iden, Auth),
                    {{ok, SessId}, Req};
                Error ->
                    {Error, Req}
            end;
        Error ->
            {Error, Req}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Authenticates user basing on onedata-internal certificate headers.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_cert(req()) ->
    {{ok, session:id()} | {error, term()}, req()}.
authenticate_using_cert(Req) ->
    Socket = cowboy_req:get(socket, Req),
    case ssl2:peercert(Socket) of
        {ok, Der} ->
            Certificate = public_key:pkix_decode_cert(Der, otp),
            case identity:get_or_fetch(Certificate) of
                {ok, #document{value = Iden}} ->
                    {ok, SessId} = session_manager:reuse_or_create_rest_session(Iden),
                    {{ok, SessId}, Req};
                Error ->
                    {Error, Req}
            end;
        Error ->
            {Error, Req}
    end.
