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

-include("modules/http_worker/http_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([is_authorized/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function authorizes user and inserts 'identity' field to
%% request's State
%% @end
%%--------------------------------------------------------------------
-spec is_authorized(req(), #{}) -> {boolean(), req(), #{}}.
is_authorized(Req, State) ->
    case authenticate(Req) of
        {{ok, Iden}, NewReq} ->
            {true, NewReq, State#{identity => Iden}};
        {{error, {not_found, _}}, NewReq} ->
            GrUrl = gr_plugin:get_gr_url(),
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Authenticates user basing on request headers
%% @end
%%--------------------------------------------------------------------
-spec authenticate(Req :: req()) -> {{ok, #identity{}} | {error, term()}, req()}.
authenticate(Req) ->
    case cowboy_req:header(<<"X-Auth-Token">>, Req) of
        {undefined, NewReq} ->
            case cowboy_req:header(<<"x-auth-token">>, NewReq) of
                {undefined, NewReq2} ->
                    authenticate_using_cert(NewReq2);
                {Token, NewReq2}->
                    authenticate_using_token(NewReq2, Token)
            end;
        {Token, NewReq} ->
            authenticate_using_token(NewReq, Token)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Athenticates user basing on provided token
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_token(req(), Token :: binary()) -> {{ok, #identity{}} | {error, term()}, req()}.
authenticate_using_token(Req, Token) ->
    case identity:get_or_fetch(#auth{macaroon = Token}) of
        {ok, #document{value = Iden}} ->
            {{ok, Iden}, Req};
        Error ->
            {Error, Req}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Athenticates user basing on onedata-internal certificate headers
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_cert(req()) -> {{ok, #identity{}} | {error, term()}, req()}.
authenticate_using_cert(Req) ->
    Socket = cowboy_req:get(socket, Req),
    {ok, Der} = ssl2:peercert(Socket),
    Certificate = public_key:pkix_decode_cert(Der, otp),
    case identity:get_or_fetch(Certificate) of
        {ok, #document{value = Iden}} ->
            {{ok, Iden}, Req};
        Error ->
            {Error, Req}
    end.

