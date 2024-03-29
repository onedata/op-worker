%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module decides where to send incoming event messages.
%%% @end
%%%-------------------------------------------------------------------
-module(event_router).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([route_message/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv route_message(Msg, router:effective_session_id(Msg))
%% @end
%%--------------------------------------------------------------------
-spec route_message(#client_message{}) -> ok.
route_message(Msg) ->
    route_message(Msg, router:effective_session_id(Msg)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Route message to adequate worker and return ok
%% @end
%%--------------------------------------------------------------------
-spec route_message(#client_message{}, session:id()) -> ok.
route_message(#client_message{message_body = #event{} = Evt}, SessionID) ->
    event:emit(Evt, SessionID),
    ok;
route_message(#client_message{message_body = #events{events = Evts}}, SessionID) ->
    lists:foreach(fun(#event{} = Evt) ->
        event:emit(Evt, SessionID) end, Evts),
    ok;
route_message(#client_message{message_body = #subscription{} = Sub, message_id = undefined}, SessionID) ->
    case session_utils:is_provider_session_id(SessionID) of
        true ->
            ok; %% Do not route subscriptions from other providers (route only subscriptions from users)
        false ->
            event:subscribe(Sub, SessionID),
            ok
    end;
route_message(#client_message{
    message_body = #subscription{},
    message_id = MsgId,
    session_id = OriginSessId
} = ClientMessage, SessionID) ->
    % Spawn because send or subscription delegation can last long time and block sequencer_in_stream
    spawn(fun() ->
        route_message(ClientMessage#client_message{message_id = undefined}, SessionID),
        communicator:send_to_oneclient(OriginSessId, #server_message{
            message_id = MsgId,
            message_body = #status{code = ?OK}
        })
    end),
    ok;
route_message(#client_message{message_body = #subscription_cancellation{} = SubCan}, SessionID) ->
    case session_utils:is_provider_session_id(SessionID) of
        true ->
            ok; %% Do not route subscription_cancellations from other providers
        false ->
            event:unsubscribe(SubCan, SessionID),
            ok
    end;
route_message(#client_message{
    session_id = OriginSessId,
    % TODO VFS-5326
    message_id = MsgId,
    message_body = FlushMsg = #flush_events{}
}, SessionID) ->
    event:flush(FlushMsg#flush_events{notify =
    fun(Result) ->
        % Spawn because send can wait and block event_stream
        % Probably not needed after migration to asynchronous connections
        spawn(fun() ->
            communicator:send_to_oneclient(OriginSessId, Result#server_message{message_id = MsgId})
        end)
    end
    }, SessionID),
    ok.
