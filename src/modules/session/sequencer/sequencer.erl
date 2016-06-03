%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides API for message sequencing.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer).
-author("Krzysztof Trzepla").

-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/stream_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([open_stream/1, close_stream/2, send_message/3, route_message/2]).
-export([term_to_stream_id/1]).

-export_type([stream_id/0, sequence_number/0]).

-type stream_id() :: non_neg_integer().
-type sequence_number() :: non_neg_integer().
-type sequencer_manager_ref() :: pid() | session:id().

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Opens sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec open_stream(Ref :: sequencer_manager_ref()) ->
    {ok, StmId :: stream_id()} | {error, Reason :: term()}.
open_stream(Ref) ->
    communicate_with_sequencer_manager(open_stream, Ref).

%%--------------------------------------------------------------------
%% @doc
%% Closes sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec close_stream(StmId :: stream_id(), Ref :: sequencer_manager_ref()) ->
    ok | {error, Reason :: term()}.
close_stream(StmId, Ref) ->
    send_to_sequencer_manager({close_stream, StmId}, Ref).

%%--------------------------------------------------------------------
%% @doc
%% Forwards message to the sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec send_message(Msg :: term(), StmId :: stream_id(), Ref :: sequencer_manager_ref()) ->
    ok | {error, Reason :: term()}.
send_message(#server_message{} = Msg, StmId, Ref) ->
    send_to_sequencer_manager(Msg#server_message{
        message_stream = #message_stream{stream_id = StmId}
    }, Ref);
send_message(#client_message{} = Msg, StmId, Ref) ->
    send_to_sequencer_manager(Msg#client_message{
        message_stream = #message_stream{stream_id = StmId}
    }, Ref);

send_message(Msg, StmId, Ref) ->
    send_message(#server_message{message_body = Msg}, StmId, Ref).

%%--------------------------------------------------------------------
%% @doc
%% Forwards message to the sequencer stream for incoming messages.
%% @end
%%--------------------------------------------------------------------
-spec route_message(Msg :: term(), Ref :: sequencer_manager_ref()) ->
    ok | {error, Reason :: term()}.
route_message(#client_message{session_id = From, proxy_session_id = ProxySessionId, message_body = MsgBody} = Msg, Ref) ->
    case {session_manager:is_provider_session_id(From), is_binary(ProxySessionId)} of
        {true, true} ->
            ProviderId = session_manager:session_id_to_provider_id(From),
            SequencerSessionId = session_manager:get_provider_session_id(outgoing, ProviderId),
            provider_communicator:ensure_connected(SequencerSessionId),
            send_to_sequencer_manager(Msg, SequencerSessionId);
        {true, false} ->
            ok;
        {false, _} ->
            send_to_sequencer_manager(Msg, From)
    end;
route_message(Msg, Ref) ->
    send_to_sequencer_manager(Msg, Ref).


%%--------------------------------------------------------------------
%% @doc
%% Generates stream id based on
%% @end
%%--------------------------------------------------------------------
-spec term_to_stream_id(term()) -> stream_id().
term_to_stream_id(Term) ->
    Binary = term_to_binary(Term),
    PHash1 = erlang:phash(Binary, 4294967296) bsl 32,
    PHash2 = erlang:phash2(Binary, 4294967296),
    PHash1 + PHash2.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends a message to the sequencer manager referenced by pid or session ID.
%% @end
%%--------------------------------------------------------------------
-spec send_to_sequencer_manager(Msg :: term(), Ref :: sequencer_manager_ref()) ->
    ok | {error, Reason :: term()}.
send_to_sequencer_manager(Msg, Ref) when is_pid(Ref) ->
    gen_server:cast(Ref, Msg);

send_to_sequencer_manager(Msg, Ref) ->

    case session:get_sequencer_manager(Ref) of
        {ok, ManPid} -> send_to_sequencer_manager(Msg, ManPid);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Communicates with the sequencer manager referenced by pid or session ID.
%% @end
%%--------------------------------------------------------------------
-spec communicate_with_sequencer_manager(Msg :: term(),
    Ref :: sequencer_manager_ref()) -> Reply :: term().
communicate_with_sequencer_manager(Msg, Ref) when is_pid(Ref) ->
    gen_server:call(Ref, Msg);

communicate_with_sequencer_manager(Msg, Ref) ->
    case session:get_sequencer_manager(Ref) of
        {ok, ManPid} -> communicate_with_sequencer_manager(Msg, ManPid);
        {error, Reason} -> {error, Reason}
    end.
