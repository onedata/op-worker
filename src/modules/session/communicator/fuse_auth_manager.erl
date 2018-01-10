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

-include("modules/datastore/datastore_models.hrl").
-include("modules/events/definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([handle_handshake/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles client handshake request
%% @end
%%--------------------------------------------------------------------
-spec handle_handshake(#client_handshake_request{}) -> {od_user:id(), session:id()} | no_return().
handle_handshake(#client_handshake_request{session_id = SessId, auth = Auth})
    when is_binary(SessId) andalso (is_record(Auth, macaroon_auth) orelse is_record(Auth, token_auth)) ->

    {ok, #document{
        value = Iden = #user_identity{user_id = UserId}
    }} = user_identity:get_or_fetch(Auth),
    {ok, _} = session_manager:reuse_or_create_fuse_session(SessId, Iden, Auth, self()),
    {UserId, SessId}.
