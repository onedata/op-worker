%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Provider's authentication helper functions.
%%% @end
%%%-------------------------------------------------------------------
-module(provider_auth_manager).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([handle_handshake/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles provider handshake request
%% @end
%%--------------------------------------------------------------------
-spec handle_handshake(#provider_handshake_request{}) ->
    {od_provider:id(), session:id()} | no_return().
handle_handshake(#provider_handshake_request{provider_id = ProviderId, macaroon = TokenAuth})
    when is_binary(ProviderId) andalso (is_record(TokenAuth, token_auth)) ->

    case provider_logic:verify_provider_identity(ProviderId, TokenAuth#token_auth.token) of
        ok ->
            ok;
        Error ->
            ?debug("Discarding provider connection as its identity cannot be verified: ~p", [
                Error
            ]),
            throw(invalid_token)
    end,
    Identity = #user_identity{provider_id = ProviderId},
    SessionId = session_manager:get_provider_session_id(incoming, ProviderId),
    {ok, _} = session_manager:reuse_or_create_provider_session(SessionId, provider_incoming, Identity, self()),
    {ProviderId, SessionId}.
