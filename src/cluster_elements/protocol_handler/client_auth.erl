%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Client authentication
%%% @end
%%%-------------------------------------------------------------------
-module(client_auth).
-author("Tomasz Lichon").

-include("cluster_elements/protocol_handler/credentials.hrl").
-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/server_messages.hrl").
-include("proto_internal/oneclient/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([handle_handshake/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handle first client message, which describes authentication method
%% (cert/token)
%% @end
%%--------------------------------------------------------------------
-spec handle_handshake(Message :: #client_message{}, CertInfo :: #certificate_info{}) ->
    {ok, {Cred :: #credentials{}, #server_message{}}} | {error, term()}.
handle_handshake(#client_message{client_message = #handshake_request{
    session_id = _Id, token = Token = #token{}}}, _) ->
    #credentials{session_id = SessionId} = Cred = authenticate_using_token(Token),
    {ok, {Cred, #server_message{server_message = #handshake_response{session_id = SessionId}}}};

handle_handshake(#client_message{client_message = #handshake_request{session_id = _Id}}, CertInfo) ->
    #credentials{session_id = SessionId} = Cred = authenticate_using_certificate(CertInfo),
    {ok, {Cred, #server_message{server_message = #handshake_response{session_id = SessionId}}}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Authenticate client using given token, returns client credentials.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_token(Token :: #token{}) -> #credentials{}.
authenticate_using_token(_Token) ->
    ?dump(_Token),
    #credentials{}.

%%--------------------------------------------------------------------
%% @doc
%% Authenticate client using given certificate. Returns client credentials.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_certificate(CertInfo :: #certificate_info{}) ->
    #credentials{}.
authenticate_using_certificate(_CertInfo) ->
    ?dump(_CertInfo),
    #credentials{}.

