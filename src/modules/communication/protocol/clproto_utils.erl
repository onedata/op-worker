%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for clproto messages.
%%% @end
%%%-------------------------------------------------------------------
-module(clproto_utils).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/stream_messages.hrl").
-include("proto/common/handshake_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneprovider/dbsync_messages.hrl").
-include("proto/oneprovider/dbsync_messages2.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("proto/oneprovider/remote_driver_messages.hrl").
-include("proto/oneprovider/rtransfer_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([msg_to_string/1]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Converts client/server msg to string. Depending on whether
%% `log_whole_messages_on_errors` env variable is set or not only
%% relevant fields are kept.
%% @end
%%--------------------------------------------------------------------
-spec msg_to_string(#client_message{} | #server_message{}) -> string().
msg_to_string(Request) ->
    StringifyWholeMsg = application:get_env(?APP_NAME,
        log_whole_messages_on_errors, false
    ),
    case StringifyWholeMsg of
        true -> lager:pr(Request, ?MODULE);
        _ -> stringify_only_relevant_info(Request)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


stringify_only_relevant_info(#server_message{
    message_id = MsgId,
    message_stream = MsgStream,
    message_body = MsgBody,
    effective_session_id = EffSessionId
}) ->
    str_utils:format(
        "ServerMessage{
            message_id = ~p,
            effective_session_id = ~p,
            message_stream = ~p,
            message_type = ~p
        }", [MsgId, EffSessionId, MsgStream, element(1, MsgBody)]
    );
stringify_only_relevant_info(#client_message{
    message_id = MsgId,
    session_id = SessionId,
    effective_session_id = EffSessionId,
    message_stream = MsgStream,
    message_body = MsgBody
}) ->
    str_utils:format(
        "ClientMessage{
            message_id = ~p,
            session_id = ~p,
            effective_session_id = ~p,
            message_stream = ~p,
            message_type = ~p
        }", [MsgId, SessionId, EffSessionId, MsgStream, element(1, MsgBody)]
    ).
