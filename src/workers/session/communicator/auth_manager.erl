%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Client authentication library.
%%% @end
%%%-------------------------------------------------------------------
-module(auth_manager).
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
handle_handshake(#client_message{message_body = #handshake_request{
    session_id = IdToReuse, token = Token = #token{}}}, _) when is_binary(IdToReuse) ->
    {ok, Iden} = authenticate_using_token(Token),
    {ok, _} = session_manager:reuse_or_create_session(IdToReuse, Iden, self()),
    {ok, #server_message{message_body = #handshake_response{session_id = IdToReuse}}};

handle_handshake(#client_message{message_body = #handshake_request{
    session_id = IdToReuse}}, CertInfo) when is_binary(IdToReuse) ->
    {ok, Iden} = authenticate_using_certificate(CertInfo),
    {ok, _} = session_manager:reuse_or_create_session(IdToReuse, Iden, self()),
    {ok, #server_message{message_body = #handshake_response{session_id = IdToReuse}}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Authenticate client using given token, returns client identity.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_token(#token{}) -> {ok, #identity{}} | {error, term()}.
authenticate_using_token(Token) ->
    {ok, #document{value = Iden}} = identity:get_or_fetch(Token),
    {ok, Iden}.

%%--------------------------------------------------------------------
%% @doc
%% Authenticate client using given certificate. Returns client identity.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_certificate(#certificate_info{}) -> {ok, #identity{}}.
authenticate_using_certificate(_CertInfo) ->
    %identity:get_or_fetch(_CertInfo). todo integrate with identity model
    {ok, #identity{}}.
