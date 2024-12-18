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
-export([
    fill_effective_session_info/2,
    msg_to_string/1
]).

-type message() :: #client_message{} | #server_message{}.

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Sets effective session field in message to specified session id if
%% this session is proxied by some other session.
%% @end
%%--------------------------------------------------------------------
% TODO VFS-5412
-spec fill_effective_session_info(message(), session:id()) -> message().
fill_effective_session_info(Msg, SessionId) ->
    case session:get(SessionId) of
        {ok, #document{value = #session{proxy_via = PV}}} when is_binary(PV) ->
            case Msg of
                #server_message{effective_session_id = undefined} ->
                    Msg#server_message{effective_session_id = SessionId};
                #client_message{effective_session_id = undefined} ->
                    Msg#client_message{effective_session_id = SessionId};
                _ ->
                    Msg
            end;
        _ ->
            Msg
    end.


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
    effective_session_id = EffSessionId
}) ->
    str_utils:format_bin(
        "ServerMessage{id = ~w, eff_sess_id = ~ts, stream = ~w, body = ~ts#{...}}",
        [MsgId, EffSessionId, MsgStream, element(1, MsgBody)]
    );
stringify_only_relevant_info(#client_message{
    message_id = MsgId,
    session_id = SessionId,
    effective_session_id = EffSessionId,
    message_stream = MsgStream,
    message_body = MsgBody
}) ->
    str_utils:format_bin(
        "ClientMessage{id = ~w, sess_id = ~ts, eff_sess_id = ~ts, stream = ~w, body = ~ts#{...}}",
        [MsgId, SessionId, EffSessionId, MsgStream, element(1, MsgBody)]
    ).
