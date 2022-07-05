%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides helper methods for processing requests that were
%%% rerouted to other provider.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_remote).
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_provider_to_route/1, route/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get provider from list to reroute msg.
%% @end
%%--------------------------------------------------------------------
-spec get_provider_to_route([od_provider:id()]) -> od_provider:id().
get_provider_to_route([ProviderId | _]) ->
    ProviderId.

%%--------------------------------------------------------------------
%% @doc
%% Reroute given request to given provider.
%% @end
%%--------------------------------------------------------------------
-spec route(user_ctx:ctx(), oneprovider:id(), fslogic_worker:request()) ->
    fslogic_worker:response().
route(UserCtx, ProviderId, Request) ->
    ?debug("Rerouting ~p ~p", [ProviderId, Request]),
    
    EffSessionId = user_ctx:get_session_id(UserCtx),
    Credentials = user_ctx:get_credentials(UserCtx),
    Msg = #client_message{
        message_body = Request,
        effective_session_id = EffSessionId,
        effective_client_tokens = auth_manager:get_client_tokens(Credentials),
        effective_session_mode = user_ctx:get_session_mode(UserCtx)
    },
    SessionId = session_utils:get_provider_session_id(outgoing, ProviderId),
    {ok, #server_message{
        message_body = MsgBody
    }} = communicator:communicate_with_provider(SessionId, Msg),
    MsgBody.
