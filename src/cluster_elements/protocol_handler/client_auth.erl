%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Client authentication library
%%% @end
%%%-------------------------------------------------------------------
-module(client_auth).
-author("Tomasz Lichon").

-include("workers/datastore/datastore_models.hrl").
-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/server_messages.hrl").
-include("proto_internal/oneclient/handshake_messages.hrl").
-include("proto_internal/oneproxy/oneproxy_messages.hrl").
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
-spec handle_handshake(#client_message{}, #certificate_info{}) ->
    {ok, #server_message{}} | no_return().
handle_handshake(#client_message{client_message = #handshake_request{
    session_id = IdToReuse, token = Token = #token{}}}, _) ->
    Cred = authenticate_using_token(Token),
    {ok, SessionId} = session:create_or_reuse_session(Cred, self(), IdToReuse),
    {ok, #server_message{server_message = #handshake_response{session_id = SessionId}}};

handle_handshake(#client_message{client_message = #handshake_request{
    session_id = IdToReuse}}, CertInfo) ->
    Cred = authenticate_using_certificate(CertInfo),
    {ok, SessionId} = session:create_or_reuse_session(Cred, self(), IdToReuse),
    {ok, #server_message{server_message = #handshake_response{session_id = SessionId}}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Authenticate client using given token, returns client credentials.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_token(#token{}) -> #credentials{}.
authenticate_using_token(_Token) ->
    ?dump(_Token),
    #credentials{}. %todo

%%--------------------------------------------------------------------
%% @doc
%% Authenticate client using given certificate. Returns client credentials.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_certificate(#certificate_info{}) ->
    #credentials{}.
authenticate_using_certificate(_CertInfo) ->
    ?dump(_CertInfo),
    #credentials{}. %todo
