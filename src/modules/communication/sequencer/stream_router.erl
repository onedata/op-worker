%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
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
-export([is_stream_message/2, route_message/1, make_message_direct/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec is_stream_message(Msg :: #client_message{} | #server_message{}, session:type()) ->
    boolean() | ignore.
is_stream_message(#client_message{message_body = #subscription{}, message_id = undefined} = Msg, fuse) ->
    % Current version of oneclient is not able to prevent async subscriptions to be sent but hangs
    % if such subscription is processed before sync one - ignore async subscriptions from client until its fixed.
    % NOTE: use env to allow async subscriptions testing as its valid functionality from oneprovider's point of view.
    case op_worker:get_env(ignore_async_subscriptions, true) of
        true -> ignore;
        false -> is_stream_message(Msg)
    end;
is_stream_message(Msg, _) ->
    is_stream_message(Msg).

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
is_stream_message(#server_message{effective_session_id = SessId}) ->
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
route_message(#client_message{session_id = From, effective_session_id = EffSessionId} = Msg) ->
    case {session_utils:is_provider_session_id(From), is_binary(EffSessionId)} of
        {true, true} ->
            ProviderId = session_utils:session_id_to_provider_id(From),
            SequencerSessionId = session_utils:get_provider_session_id(outgoing, ProviderId),
            sequencer:communicate_with_sequencer_manager(Msg, SequencerSessionId, true);
        {true, false} ->
            ok;
        {false, _} ->
            sequencer:communicate_with_sequencer_manager(Msg, From)
    end;
route_message(#server_message{effective_session_id = Ref} = Msg) ->
    sequencer:communicate_with_sequencer_manager(Msg, Ref).

%%--------------------------------------------------------------------
%% @doc
%% Marks that message has been processed by sequencer.
%% @end
%%--------------------------------------------------------------------
-spec make_message_direct(Msg :: term()) -> DirectMsg :: term().
make_message_direct(Msg) ->
    Msg#client_message{message_stream = undefined}.