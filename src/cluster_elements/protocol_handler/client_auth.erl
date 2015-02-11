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
-spec handle_handshake(Message :: #client_message{}) ->
    {ok, {Cred :: #credentials{}, #server_message{}}}.
handle_handshake(#client_message{client_message = #handshake_request{
    session_id = _Id, auth_method = #token{value = Val}}}) ->
    #credentials{session_id = SessionId} = Cred = authenticate_using_token(Val),
    {ok, {Cred, #server_message{server_message = #handshake_response{session_id = SessionId}}}};
handle_handshake(#client_message{client_message = #handshake_request{
    session_id = _Id, auth_method =
    #certificate{client_session_id = Id, client_subject_dn = Dn}}}) ->
    #credentials{session_id = SessionId} = Cred = authenticate_using_certificate(Id, Dn),
    {ok, {Cred, #server_message{server_message = #handshake_response{session_id = SessionId}}}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Authenticate client using given token, returns client credentials.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_token(Token :: binary()) -> #credentials{}.
authenticate_using_token(_Token) ->
    ?dump(_Token),
    #credentials{}.

%%--------------------------------------------------------------------
%% @doc
%% Authenticate client using given certificate. Returns client credentials.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_certificate(OneproxySessionId :: binary(), Dn :: binary()) ->
    #credentials{}.
authenticate_using_certificate(_OneproxySessionId, _Dn) ->
    ?dump([_OneproxySessionId, _Dn]),
    #credentials{}.

