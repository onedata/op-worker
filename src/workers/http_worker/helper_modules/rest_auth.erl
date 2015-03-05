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

-include("cluster_elements/oneproxy/oneproxy.hrl").
-include("workers/http_worker/http_common.hrl").
-include("workers/datastore/datastore_models.hrl").
-include("proto_internal/oneclient/handshake_messages.hrl").
-include("proto_internal/oneproxy/oneproxy_messages.hrl").

%% API
-export([authenticate/1]).

%%%===================================================================
%%% API
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Athenticates user basing on provided token
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_token(req(), Token :: binary()) -> {{ok, #identity{}} | {error, term()}, req()}.
authenticate_using_token(Req, Token) ->
    case identity:get_or_fetch(#token{value = Token}) of
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
    {SessionId, Req2} = cowboy_req:header(<<"onedata-internal-client-session-id">>, Req),
    {SubjectDn, Req3} = cowboy_req:header(<<"onedata-internal-client-subject-dn">>, Req2),
    CertInfo = #certificate_info{client_session_id = SessionId, client_subject_dn = SubjectDn},
    case identity:get_or_fetch(CertInfo) of
        {ok, #document{value = Iden}} ->
            {{ok, Iden}, Req3};
        Error ->
            {Error, Req3}
    end.
