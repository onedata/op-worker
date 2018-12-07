%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module decides where to send incoming stream messages.
%%% @end
%%%-------------------------------------------------------------------
-module(stream_router).
-author("Michal Wrzeszcz").

-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").

%% API
-export([is_stream_message/1, route_message/1, make_message_direct/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if message is sequential, if so - proxy it throught sequencer
%% @end
%%--------------------------------------------------------------------
-spec is_stream_message(Msg :: #client_message{} | #server_message{}) ->
    boolean() | ignore.
is_stream_message(#client_message{message_body = #message_request{}}) ->
    true;
is_stream_message(#client_message{message_body = #message_acknowledgement{}}) ->
    true;
is_stream_message(#client_message{message_body = #end_of_message_stream{}}) ->
    true;
is_stream_message(#client_message{message_body = #message_stream_reset{}}) ->
    true;
is_stream_message(#server_message{message_body = #message_request{}}) ->
    true;
is_stream_message(#server_message{message_body = #message_acknowledgement{}}) ->
    true;
is_stream_message(#server_message{message_body = #end_of_message_stream{}}) ->
    true;
is_stream_message(#server_message{message_body = #message_stream_reset{}}) ->
    true;
is_stream_message(#client_message{message_stream = undefined}) ->
    false;
is_stream_message(#client_message{} = Msg) ->
    SessId = router:effective_session_id(Msg),
    case session_utils:is_provider_session_id(SessId) of
        true ->
            ignore;
        false ->
            true
    end;
is_stream_message(#server_message{message_stream = undefined}) ->
    false;
is_stream_message(#server_message{proxy_session_id = SessId}) ->
    case session_utils:is_provider_session_id(SessId) of
        true ->
            ignore;
        false ->
            true
    end.

%%--------------------------------------------------------------------
%% @doc
%% Forwards message to the sequencer stream for incoming messages.
%% @end
%%--------------------------------------------------------------------
-spec route_message(Msg :: term()) -> ok | {error, Reason :: term()}.
route_message(#client_message{session_id = From, proxy_session_id = ProxySessionId} = Msg) ->
    case {session_utils:is_provider_session_id(From), is_binary(ProxySessionId)} of
        {true, true} ->
            ProviderId = session_utils:session_id_to_provider_id(From),
            SequencerSessionId = session_utils:get_provider_session_id(outgoing, ProviderId),
            communicator:ensure_connected(SequencerSessionId),
            sequencer:communicate_with_sequencer_manager(Msg, SequencerSessionId);
        {true, false} ->
            ok;
        {false, _} ->
            sequencer:communicate_with_sequencer_manager(Msg, From)
    end;
route_message(#server_message{proxy_session_id = Ref} = Msg) ->
    sequencer:communicate_with_sequencer_manager(Msg, Ref).

make_message_direct(Msg) ->
    Msg#client_message{message_stream = undefined}.