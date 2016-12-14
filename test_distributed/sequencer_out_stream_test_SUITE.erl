%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains event stream tests.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_out_stream_test_SUITE).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("proto/oneclient/server_messages.hrl").
-include_lib("proto/oneclient/stream_messages.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    sequencer_out_stream_should_register_with_sequencer_manager_on_init/1,
    sequencer_out_stream_should_unregister_from_sequencer_manager_on_terminate/1,
    sequencer_out_stream_should_forward_messages/1,
    sequencer_out_stream_should_increment_sequence_number/1,
    sequencer_out_stream_should_resend_all_messages_on_message_stream_reset/1,
    sequencer_out_stream_should_remove_messages_on_message_acknowledgement/1,
    sequencer_out_stream_should_recompute_sequence_numbers_on_message_stream_reset/1,
    sequencer_out_stream_should_resend_messages_on_message_request/1,
    sequencer_out_stream_should_ignore_message_request_for_acknowledged_messages/1,
    sequencer_out_stream_should_ignore_message_request_for_unsent_messages/1,
    sequencer_out_stream_should_unregister_from_sequencer_manager_on_acknowledged_end_of_stream/1
]).

all() ->
    ?ALL([
        sequencer_out_stream_should_register_with_sequencer_manager_on_init,
        sequencer_out_stream_should_unregister_from_sequencer_manager_on_terminate,
        sequencer_out_stream_should_forward_messages,
        sequencer_out_stream_should_increment_sequence_number,
        sequencer_out_stream_should_resend_all_messages_on_message_stream_reset,
        sequencer_out_stream_should_remove_messages_on_message_acknowledgement,
        sequencer_out_stream_should_recompute_sequence_numbers_on_message_stream_reset,
        sequencer_out_stream_should_resend_messages_on_message_request,
        sequencer_out_stream_should_ignore_message_request_for_acknowledged_messages,
        sequencer_out_stream_should_ignore_message_request_for_unsent_messages,
        sequencer_out_stream_should_unregister_from_sequencer_manager_on_acknowledged_end_of_stream
    ]).

-define(TIMEOUT, timer:seconds(5)).

%%%===================================================================
%%% Test functions
%%%===================================================================

sequencer_out_stream_should_register_with_sequencer_manager_on_init(_) ->
    ?assertReceivedMatch({'$gen_cast', {register_out_stream, 1, _}}, ?TIMEOUT).

sequencer_out_stream_should_unregister_from_sequencer_manager_on_terminate(Config) ->
    stop_sequencer_out_stream(?config(sequencer_out_stream, Config)),
    ?assertReceivedMatch({'$gen_cast', {unregister_out_stream, 1}}, ?TIMEOUT).

sequencer_out_stream_should_forward_messages(Config) ->
    send_message(?config(sequencer_out_stream, Config), server_message()),
    ?assertReceivedMatch(#server_message{message_stream = #message_stream{
        sequence_number = 0
    }}, ?TIMEOUT).

sequencer_out_stream_should_increment_sequence_number(Config) ->
    send_message(?config(sequencer_out_stream, Config), server_message()),
    initializer:remove_pending_messages(?TIMEOUT),
    send_message(?config(sequencer_out_stream, Config), server_message()),
    ?assertReceivedMatch(#server_message{message_stream = #message_stream{
        sequence_number = 1
    }}, ?TIMEOUT).

sequencer_out_stream_should_resend_all_messages_on_message_stream_reset(Config) ->
    SeqStm = ?config(sequencer_out_stream, Config),
    send_message(SeqStm, server_message()),
    initializer:remove_pending_messages(?TIMEOUT),
    send_message(SeqStm, #message_stream_reset{}),
    ?assertReceivedMatch(#server_message{message_stream = #message_stream{
        sequence_number = 0
    }}, ?TIMEOUT).

sequencer_out_stream_should_remove_messages_on_message_acknowledgement(Config) ->
    SeqStm = ?config(sequencer_out_stream, Config),
    send_message(SeqStm, server_message()),
    send_message(SeqStm, #message_acknowledgement{sequence_number = 0}),
    initializer:remove_pending_messages(?TIMEOUT),
    send_message(SeqStm, #message_stream_reset{}),
    ?assertNotReceivedMatch(#server_message{}, ?TIMEOUT).

sequencer_out_stream_should_recompute_sequence_numbers_on_message_stream_reset(Config) ->
    SeqStm = ?config(sequencer_out_stream, Config),
    send_message(SeqStm, server_message()),
    send_message(SeqStm, #message_acknowledgement{sequence_number = 0}),
    send_message(SeqStm, server_message()),
    initializer:remove_pending_messages(?TIMEOUT),
    send_message(SeqStm, #message_stream_reset{}),
    ?assertReceivedMatch(#server_message{message_stream = #message_stream{
        sequence_number = 0
    }}, ?TIMEOUT).

sequencer_out_stream_should_resend_messages_on_message_request(Config) ->
    SeqStm = ?config(sequencer_out_stream, Config),
    send_message(SeqStm, server_message()),
    send_message(SeqStm, server_message()),
    send_message(SeqStm, server_message()),
    initializer:remove_pending_messages(?TIMEOUT),
    send_message(SeqStm, #message_request{
        lower_sequence_number = 1, upper_sequence_number = 1
    }),
    ?assertReceivedMatch(#server_message{message_stream = #message_stream{
        sequence_number = 1
    }}, ?TIMEOUT).

sequencer_out_stream_should_ignore_message_request_for_acknowledged_messages(Config) ->
    SeqStm = ?config(sequencer_out_stream, Config),
    send_message(SeqStm, server_message()),
    send_message(SeqStm, #message_acknowledgement{sequence_number = 0}),
    initializer:remove_pending_messages(?TIMEOUT),
    send_message(SeqStm, #message_request{
        lower_sequence_number = 0, upper_sequence_number = 0
    }),
    ?assertNotReceivedMatch(#server_message{}, ?TIMEOUT).

sequencer_out_stream_should_ignore_message_request_for_unsent_messages(Config) ->
    send_message(?config(sequencer_out_stream, Config), #message_request{
        lower_sequence_number = 0, upper_sequence_number = 0
    }),
    ?assertNotReceivedMatch(#server_message{}, ?TIMEOUT).

sequencer_out_stream_should_unregister_from_sequencer_manager_on_acknowledged_end_of_stream(Config) ->
    SeqStm = ?config(sequencer_out_stream, Config),
    send_message(SeqStm, server_message(#end_of_message_stream{})),
    send_message(SeqStm, #message_acknowledgement{sequence_number = 0}),
    ?assertReceivedMatch({'$gen_cast', {unregister_out_stream, 1}}, ?TIMEOUT).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer]} | Config].

init_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    initializer:remove_pending_messages(),
    mock_communicator(Worker),
    mock_session(Worker),
    {ok, SeqStm} = start_sequencer_out_stream(Worker),
    [{sequencer_out_stream, SeqStm} | Config].

end_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    stop_sequencer_out_stream(?config(sequencer_out_stream, Config)),
    test_utils:mock_validate_and_unload(Worker, [communicator, session]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec start_sequencer_out_stream(Worker :: node()) -> {ok, SeqStm :: pid()}.
start_sequencer_out_stream(Worker) ->
    SeqMan = self(),
    ?assertMatch({ok, _}, rpc:call(Worker, gen_server, start, [
        sequencer_out_stream, [SeqMan, 1, <<"session_id">>], []
    ])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec stop_sequencer_out_stream(SeqStm :: pid()) -> true.
stop_sequencer_out_stream(SeqStm) ->
    exit(SeqStm, shutdown).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends message to the sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec send_message(SeqStm :: pid(), Msg :: term()) -> ok.
send_message(SeqStm, Msg) ->
    gen_server:cast(SeqStm, Msg).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv server_message(undefined)
%% @end
%%--------------------------------------------------------------------
-spec server_message() -> Msg :: #server_message{}.
server_message() ->
    server_message(undefined).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns server message as a part of a message stream. Sets message body.
%% @end
%%--------------------------------------------------------------------
-spec server_message(Body :: term()) -> Msg :: #server_message{}.
server_message(Body) ->
    #server_message{message_stream = #message_stream{
        stream_id = 1
    }, message_body = Body}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks communicator, so that on send it forwards all messages to this process.
%% @end
%%--------------------------------------------------------------------
-spec mock_communicator(Worker :: node()) -> ok.
mock_communicator(Worker) ->
    Self = self(),
    test_utils:mock_new(Worker, communicator),
    test_utils:mock_expect(Worker, communicator, send, fun
        (Msg, _, _) -> Self ! Msg, ok
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks session, so that it returns this process as random connection.
%% @end
%%--------------------------------------------------------------------
-spec mock_session(Worker :: node()) -> ok.
mock_session(Worker) ->
    Self = self(),
    test_utils:mock_new(Worker, session),
    test_utils:mock_expect(Worker, session, get_random_connection, fun
        (_) -> {ok, Self}
    end).
