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

-include("global_definitions.hrl").
-include("modules/events/definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_provider_to_reroute/1, reroute/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get provider from list to reroute msg.
%% @end
%%--------------------------------------------------------------------
-spec get_provider_to_reroute([od_provider:id()]) -> od_provider:id().
get_provider_to_reroute([ProviderId | _]) ->
    ProviderId.

%%--------------------------------------------------------------------
%% @doc
%% Reroute given request to given provider.
%% @end
%%--------------------------------------------------------------------
-spec reroute(fslogic_context:ctx(), oneprovider:id(), fslogic_worker:request()) ->
    {ok, term()}.
reroute(Ctx, ProviderId, Request) ->
    ?debug("Rerouting ~p ~p", [ProviderId, Request]),
    SessId = fslogic_context:get_session_id(Ctx),
    Auth = fslogic_context:get_auth(Ctx),
    {ok, #server_message{message_body = MsgBody}} =
        provider_communicator:communicate(#client_message{
            message_body = Request,
            proxy_session_id = SessId,
            proxy_session_auth = Auth
        }, session_manager:get_provider_session_id(outgoing, ProviderId)),
    MsgBody.