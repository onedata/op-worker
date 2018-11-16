%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module decides where to send incoming client messages.
%%% @end
%%%-------------------------------------------------------------------
-module(stream_router).
-author("Tomasz Lichon").

-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").

%% API
-export([route_message/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if message is sequential, if so - proxy it throught sequencer
%% @end
%%--------------------------------------------------------------------
-spec route_message(Msg :: #client_message{} | #server_message{},
    SessId :: session:id()) -> ok | direct_message | {error, term()}.
route_message(#client_message{message_body = #message_request{}} = Msg, SessId) ->
    sequencer:route_message(Msg, SessId);
route_message(#client_message{message_body = #message_acknowledgement{}} = Msg,
    SessId) ->
    sequencer:route_message(Msg, SessId);
route_message(#client_message{message_body = #end_of_message_stream{}} = Msg,
    SessId) ->
    sequencer:route_message(Msg, SessId);
route_message(#client_message{message_body = #message_stream_reset{}} = Msg,
    SessId) ->
    sequencer:route_message(Msg, SessId);
route_message(#server_message{message_body = #message_request{}} = Msg,
    SessId) ->
    sequencer:route_message(Msg, SessId);
route_message(#server_message{message_body = #message_acknowledgement{}} = Msg,
    SessId) ->
    sequencer:route_message(Msg, SessId);
route_message(#server_message{message_body = #end_of_message_stream{}} = Msg,
    SessId) ->
    sequencer:route_message(Msg, SessId);
route_message(#server_message{message_body = #message_stream_reset{}} = Msg,
    SessId) ->
    sequencer:route_message(Msg, SessId);
route_message(#client_message{message_stream = undefined}, _SessId) ->
    direct_message;
route_message(#client_message{message_body = #subscription{}} = Msg, SessId) ->
    case session_manager:is_provider_session_id(SessId) of
        true ->
            ok;
        false ->
            sequencer:route_message(Msg, SessId)
    end;
route_message(#client_message{message_body = #subscription_cancellation{}} = Msg,
    SessId) ->
    case session_manager:is_provider_session_id(SessId) of
        true ->
            ok;
        false ->
            sequencer:route_message(Msg, SessId)
    end;
route_message(#server_message{message_stream = undefined}, _SessId) ->
    direct_message;
route_message(Msg, SessId) ->
    case session_manager:is_provider_session_id(SessId) of
        true ->
            ok;
        false ->
            sequencer:route_message(Msg, SessId)
    end.
