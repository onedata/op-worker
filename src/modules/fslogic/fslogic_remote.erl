%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module provides helper methods for processing requests that were
%%%      rerouted to other provider.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_remote).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("modules/events/definitions.hrl").
-include("proto/common/credentials.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([reroute/3, postrouting/3, prerouting/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Reroute given request to given provider.
%% @end
%%--------------------------------------------------------------------
-spec reroute(fslogic_worker:ctx(), oneprovider:id(), term()) ->
    term().
reroute(#fslogic_ctx{session_id = SessionId}, ProviderId, Request) ->
    ?debug("Rerouting ~p ~p", [ProviderId, Request]),
    {ok, #document{value = #session{auth = Auth}}} = session:get(SessionId),

    MessageStream =
        case Request of
            #fuse_request{fuse_request = #fsync{uuid = FileGUID}} ->

                #message_stream{stream_id = sequencer:term_to_stream_id(fslogic_uuid:file_guid_to_uuid(FileGUID))};
            _ ->
                undefined
        end,

    {ok, #server_message{message_body = MsgBody}} =
        provider_communicator:communicate(#client_message{
            message_body = Request,
            proxy_session_id = SessionId,
            proxy_session_auth = Auth,
            message_stream = MessageStream
        }, session_manager:get_provider_session_id(outgoing, ProviderId)),
    MsgBody.




%%--------------------------------------------------------------------
%% @doc This function is called for each request that should be rerouted to remote provider and allows to choose
%%      the provider ({ok, {reroute, ProviderId}}), stop rerouting while giving response to the request ({ok, {response, Response}})
%%      or stop rerouting due to error.
%% @end
%%--------------------------------------------------------------------
-spec prerouting(fslogic_worker:ctx(), Request :: term(), [ProviderId :: binary()]) ->
    {ok, {response, Response :: term()}} | {ok, {reroute, ProviderId :: binary(), NewRequest :: term()}} | {error, Reason :: any()}.
prerouting(_, _, []) ->
    {error, no_providers};
prerouting(_CTX, RequestBody, [RerouteTo | _Providers]) ->
    {ok, {reroute, RerouteTo, RequestBody}}.



%%--------------------------------------------------------------------
%% @doc This function is called for each response from remote provider and allows altering this response
%%      (i.e. show empty directory instead of error in some cases).
%%      'undefined' return value means, that response is invalid and the whole rerouting process shall fail.
%% @end
%%--------------------------------------------------------------------
-spec postrouting(#fslogic_ctx{}, {ok | error, ResponseOrReason :: term()}, Request :: term()) -> Result :: undefined | term().
postrouting(_CTX, {ok, Response}, _Request) ->
    Response;
postrouting(_CTX, UnkResult, Request) ->
    ?error("Unknown result ~p for request ~p", [UnkResult, Request]),
    undefined.
