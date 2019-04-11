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
-export([open_stream/1, close_stream/2, send_message/3]).
-export([communicate_with_sequencer_manager/2,
    communicate_with_sequencer_manager/3, term_to_stream_id/1]).

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
    communicate_with_sequencer_manager({close_stream, StmId}, Ref).

%%--------------------------------------------------------------------
%% @doc
%% Forwards message to the sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec send_message(Msg :: term(), StmId :: stream_id(), Ref :: sequencer_manager_ref()) ->
    ok | {error, Reason :: term()}.
send_message(#server_message{} = Msg, StmId, Ref) ->
    communicate_with_sequencer_manager(Msg#server_message{
        message_stream = #message_stream{stream_id = StmId}
    }, Ref);
send_message(#client_message{} = Msg, StmId, Ref) ->
    communicate_with_sequencer_manager(Msg#client_message{
        message_stream = #message_stream{stream_id = StmId}
    }, Ref);

send_message(Msg, StmId, Ref) ->
    send_message(#server_message{message_body = Msg}, StmId, Ref).

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

%%--------------------------------------------------------------------
%% @doc
%% @equiv communicate_with_sequencer_manager(Msg, Ref, false)
%% @end
%%--------------------------------------------------------------------
-spec communicate_with_sequencer_manager(Msg :: term(),
    Ref :: sequencer_manager_ref()) -> Reply :: term().
communicate_with_sequencer_manager(Msg, Ref) ->
    communicate_with_sequencer_manager(Msg, Ref, false).

%%--------------------------------------------------------------------
%% @doc
%% Communicates with the sequencer manager referenced by pid or session ID.
%% @end
%%--------------------------------------------------------------------
-spec communicate_with_sequencer_manager(Msg :: term(),
    Ref :: sequencer_manager_ref(), EnsureConnected :: boolean()) ->
    Reply :: term().
communicate_with_sequencer_manager(Msg, Ref, _) when is_pid(Ref) ->
    sequencer_manager:send(Ref, Msg);

communicate_with_sequencer_manager(Msg, Ref, EnsureConnected) ->
    case {session:get_sequencer_manager(Ref), EnsureConnected} of
        {{ok, ManPid}, _} -> communicate_with_sequencer_manager(Msg, ManPid);
        {{error, not_found}, true} ->
            communicate_with_sequencer_manager(Msg, Ref);
        {{error, Reason}, _} -> {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================