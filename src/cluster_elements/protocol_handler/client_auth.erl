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
-include("proto_internal/oneclient/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([handle_handshake/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handle first client message, which describes authentication method
%% (cert/token)
%% @end
%%--------------------------------------------------------------------
-spec handle_handshake(Message :: binary()) ->
    {ok, Cred :: #credentials{}} | {error, term()}.
handle_handshake(#client_message{client_message =
#handshake_request{session_id = _Id, auth_method = #token{value = Val}}}) ->
    authenticate_using_token(Val);
handle_handshake(#client_message{client_message =
#handshake_request{session_id = _Id, auth_method = #certificate{value = Val}}}) ->
    authenticate_using_certificate(Val).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Authenticate client using given token, returns client credentials.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_token(Token :: binary()) ->
    {ok, Cred :: #credentials{}} | {error, term()}.
authenticate_using_token(_Token) ->
    ?dump(_Token),
    {ok, #credentials{}}.

%%--------------------------------------------------------------------
%% @doc
%% Authenticate client using given certificate. Returns client credentials.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_certificate(Token :: binary()) ->
    {ok, Cred :: #credentials{}} | {error, term()}.
authenticate_using_certificate(_Certificate) ->
    ?dump(_Certificate),
    {ok, #credentials{}}.

