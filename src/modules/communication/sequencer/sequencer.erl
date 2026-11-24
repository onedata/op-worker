%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015-2026 Onedata (onedata.org)
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
-export([communicate_with_sequencer_manager/2]).

%% Test API
-export([open_stream/1, close_stream/2]).

-export_type([stream_id/0, sequence_number/0]).

-type stream_id() :: non_neg_integer().
-type sequence_number() :: non_neg_integer().
-type sequencer_manager_ref() :: pid() | session:id().


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Communicates with the sequencer manager referenced by pid or session ID.
%% @end
%%--------------------------------------------------------------------
-spec communicate_with_sequencer_manager(Msg :: term(),
    Ref :: sequencer_manager_ref()) -> Reply :: term().
communicate_with_sequencer_manager(Msg, Ref) when is_pid(Ref) ->
    sequencer_manager:handle(Ref, Msg);

communicate_with_sequencer_manager(Msg, Ref) ->
    case session:get_sequencer_manager(Ref) of
        {ok, ManPid} -> communicate_with_sequencer_manager(Msg, ManPid);
        {error, Reason} -> {error, Reason}
    end.

%%%===================================================================
%%% Test API
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

%%%===================================================================
%%% Internal functions
%%%===================================================================