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
-module(fuse_auth_manager).
-author("Tomasz Lichon").

-include("modules/datastore/datastore.hrl").
-include("modules/events/definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/handshake_messages.hrl").
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
-spec handle_handshake(#client_message{}, #'OTPCertificate'{}) ->
    {ok, #server_message{}} | no_return().
handle_handshake(#client_message{message_body = #handshake_request{
    session_id = SessId, auth = Auth = #auth{}}}, _) when is_binary(SessId) ->
    {ok, Iden} = authenticate_using_token(Auth),
    case session_manager:reuse_or_create_fuse_session(SessId, Iden, Auth, self()) of
        {ok, created} ->
            handle_initial_handshake(SessId);
        {ok, reused} ->
            {ok, #server_message{message_body = #handshake_response{session_id = SessId}}}
    end;

handle_handshake(#client_message{message_body = #handshake_request{
    session_id = SessId}}, OtpCert) when is_binary(SessId) ->
    {ok, Iden} = authenticate_using_certificate(OtpCert),
    case session_manager:reuse_or_create_fuse_session(SessId, Iden, self()) of
        {ok, created} ->
            handle_initial_handshake(SessId);
        {ok, reused} ->
            {ok, #server_message{message_body = #handshake_response{session_id = SessId}}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles initial handshake by starting event streams.
%% @end
%%--------------------------------------------------------------------
-spec handle_initial_handshake(SessId :: session:id()) -> {ok, #server_message{}}.
handle_initial_handshake(SessId) ->
    {ok, Docs} = subscription:list(),
    Subs = lists:filtermap(fun
        (#document{value = #subscription{object = undefined}}) -> false;
        (#document{value = #subscription{} = Sub}) -> {true, Sub}
    end, Docs),
    {ok, #server_message{message_body = #handshake_response{
        session_id = SessId, subscriptions = Subs
    }}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Authenticate client using given token, returns client identity.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_token(#auth{}) -> {ok, #identity{}} | {error, term()}.
authenticate_using_token(Auth) ->
    {ok, #document{value = Iden}} = identity:get_or_fetch(Auth),
    {ok, Iden}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Authenticate client using given certificate. Returns client identity.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_certificate(#'OTPCertificate'{}) -> {ok, #identity{}}.
authenticate_using_certificate(_OtpCert) ->
    %identity:get_or_fetch(_CertInfo). todo integrate with identity model
    {ok, #identity{}}.
