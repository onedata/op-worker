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
-include("proto/oneclient/server_messages.hrl").

%% API
-export([msg_to_string/1]).

-type message() :: #client_message{} | #server_message{}.

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Converts client/server msg to string. If `log_whole_messages_on_errors`
%% env variable is set whole msg is printed. Otherwise only relevant fields.
%% @end
%%--------------------------------------------------------------------
-spec msg_to_string(message()) -> binary().
msg_to_string(Msg) ->
    case op_worker:get_env(log_whole_messages_on_errors, false) of
        true -> str_utils:format_bin("~tp", [Msg]);
        _ -> stringify_only_relevant_info(Msg)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec stringify_only_relevant_info(#client_message{} | #server_message{}) ->
    binary().
stringify_only_relevant_info(#server_message{
    message_id = MsgId,
    message_stream = MsgStream,
    message_body = MsgBody,
    session_id = EffSessionId
}) ->
    str_utils:format_bin(
        "ServerMessage{id = ~w, eff_sess_id = ~ts, stream = ~w, body = ~ts#{...}}",
        [MsgId, EffSessionId, MsgStream, element(1, MsgBody)]
    );
stringify_only_relevant_info(#client_message{
    message_id = MsgId,
    session_id = SessionId,
    message_stream = MsgStream,
    message_body = MsgBody
}) ->
    str_utils:format_bin(
        "ClientMessage{id = ~w, sess_id = ~ts, stream = ~w, body = ~ts#{...}}",
        [MsgId, SessionId, MsgStream, element(1, MsgBody)]
    ).
