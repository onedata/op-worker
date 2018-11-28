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
-export([route_message/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if message is sequential, if so - proxy it throught sequencer
%% @end
%%--------------------------------------------------------------------
-spec route_message(Msg :: #client_message{} | #server_message{}) ->
    ok | direct_message | {error, term()}.
route_message(#client_message{message_body = #message_request{}} = Msg) ->
    sequencer:route_message(Msg);
route_message(#client_message{message_body = #message_acknowledgement{}} = Msg) ->
    sequencer:route_message(Msg);
route_message(#client_message{message_body = #end_of_message_stream{}} = Msg) ->
    sequencer:route_message(Msg);
route_message(#client_message{message_body = #message_stream_reset{}} = Msg) ->
    sequencer:route_message(Msg);
route_message(#server_message{message_body = #message_request{}} = Msg) ->
    sequencer:route_message(Msg);
route_message(#server_message{message_body = #message_acknowledgement{}} = Msg) ->
    sequencer:route_message(Msg);
route_message(#server_message{message_body = #end_of_message_stream{}} = Msg) ->
    sequencer:route_message(Msg);
route_message(#server_message{message_body = #message_stream_reset{}} = Msg) ->
    sequencer:route_message(Msg);
route_message(#client_message{message_stream = undefined}) ->
    direct_message;
route_message(#client_message{} = Msg) ->
    SessId = router:effective_session_id(Msg),
    case session_manager:is_provider_session_id(SessId) of
        true ->
            ok;
        false ->
            sequencer:route_message(Msg)
    end;
route_message(#server_message{message_stream = undefined}) ->
    direct_message;
route_message(#server_message{proxy_session_id = SessId} = Msg) ->
    case session_manager:is_provider_session_id(SessId) of
        true ->
            ok;
        false ->
            sequencer:route_message(Msg)
    end.
