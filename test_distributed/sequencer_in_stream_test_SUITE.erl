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
-module(sequencer_in_stream_test_SUITE).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("proto/oneclient/client_messages.hrl").
-include_lib("proto/oneclient/stream_messages.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    sequencer_in_stream_should_register_with_sequencer_manager_on_init/1,
    sequencer_in_stream_should_unregister_from_sequencer_manager_on_terminate/1,
    sequencer_in_stream_should_send_message_stream_reset_on_init/1,
    sequencer_in_stream_should_send_message_acknowledgement_on_terminate/1,
    sequencer_in_stream_should_send_message_request_when_receiving_timout_exceeded/1,
    sequencer_in_stream_should_send_message_request_when_missing_message/1,
    sequencer_in_stream_should_send_message_request_when_requesting_timeout_exceeded/1,
    sequencer_in_stream_should_forward_messages_in_ascending_sequence_number_order/1,
    sequencer_in_stream_should_ignore_duplicated_messages/1,
    sequencer_in_stream_should_send_message_acknowledgement_when_threshold_exceeded/1,
    sequencer_in_stream_should_send_message_request_for_awaited_message/1,
    sequecner_in_stream_should_send_message_acknowledgement_when_end_of_stream/1,
    sequencer_in_stream_should_unregister_from_sequencer_manager_when_end_of_stream/1
]).

all() ->
    ?ALL([
        sequencer_in_stream_should_register_with_sequencer_manager_on_init,
        sequencer_in_stream_should_unregister_from_sequencer_manager_on_terminate,
        sequencer_in_stream_should_send_message_stream_reset_on_init,
        sequencer_in_stream_should_send_message_acknowledgement_on_terminate,
        sequencer_in_stream_should_send_message_request_when_receiving_timout_exceeded,
        sequencer_in_stream_should_send_message_request_when_missing_message,
        sequencer_in_stream_should_send_message_request_when_requesting_timeout_exceeded,
        sequencer_in_stream_should_forward_messages_in_ascending_sequence_number_order,
        sequencer_in_stream_should_ignore_duplicated_messages,
        sequencer_in_stream_should_send_message_acknowledgement_when_threshold_exceeded,
        sequencer_in_stream_should_send_message_request_for_awaited_message,
        sequecner_in_stream_should_send_message_acknowledgement_when_end_of_stream,
        sequencer_in_stream_should_unregister_from_sequencer_manager_when_end_of_stream
    ]).

-define(TIMEOUT, timer:seconds(15)).

%%%===================================================================
%%% Test functions
%%%===================================================================

sequencer_in_stream_should_register_with_sequencer_manager_on_init(_) ->
    ?assertReceivedMatch({'$gen_cast', {register_in_stream, 1, _}}, ?TIMEOUT).

sequencer_in_stream_should_unregister_from_sequencer_manager_on_terminate(Config) ->
    stop_sequencer_in_stream(?config(sequencer_in_stream, Config)),
    ?assertReceivedMatch({'$gen_cast', {unregister_in_stream, 1}}, ?TIMEOUT).

sequencer_in_stream_should_send_message_stream_reset_on_init(_) ->
    ?assertReceivedMatch(#message_stream_reset{}, ?TIMEOUT).

sequencer_in_stream_should_send_message_acknowledgement_on_terminate(Config) ->
    SeqStm = ?config(sequencer_in_stream, Config),
    route_message(SeqStm, client_message(0)),
    stop_sequencer_in_stream(SeqStm),
    ?assertReceivedMatch(#message_acknowledgement{}, ?TIMEOUT).

sequencer_in_stream_should_send_message_request_when_receiving_timout_exceeded(_) ->
    ?assertReceivedMatch(#message_request{}, ?TIMEOUT).

sequencer_in_stream_should_send_message_request_when_missing_message(Config) ->
    route_message(?config(sequencer_in_stream, Config), client_message(10)),
    ?assertReceivedMatch(#message_request{
        stream_id = 1, lower_sequence_number = 0, upper_sequence_number = 9
    }, ?TIMEOUT).

sequencer_in_stream_should_send_message_request_when_requesting_timeout_exceeded(Config) ->
    route_message(?config(sequencer_in_stream, Config), client_message(10)),
    ?assertReceivedMatch(#message_request{
        stream_id = 1, lower_sequence_number = 0, upper_sequence_number = 0
    }, ?TIMEOUT).

sequencer_in_stream_should_forward_messages_in_ascending_sequence_number_order(Config) ->
    SeqStm = ?config(sequencer_in_stream, Config),
    MsgCtr = 100,
    lists:foreach(fun(SeqNum) ->
        route_message(SeqStm, client_message(SeqNum))
    end, lists:seq(MsgCtr - 1, 0, -1)),
    lists:foreach(fun(SeqNum) ->
        #client_message{message_stream = #message_stream{
            sequence_number = MsgSeqNum
        }} = ?assertReceivedMatch(#client_message{}, ?TIMEOUT),
        ?assertEqual(SeqNum, MsgSeqNum)
    end, lists:seq(0, MsgCtr - 1)).

sequencer_in_stream_should_ignore_duplicated_messages(Config) ->
    SeqStm = ?config(sequencer_in_stream, Config),
    route_message(SeqStm, client_message(0)),
    route_message(SeqStm, client_message(0)),
    ?assertReceivedMatch(#client_message{}, ?TIMEOUT),
    ?assertNotReceivedMatch(#client_message{}).

sequencer_in_stream_should_send_message_acknowledgement_when_threshold_exceeded(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SeqStm = ?config(sequencer_in_stream, Config),
    LastSeqNum = rpc:call(Worker, application, get_env, [
        ?APP_NAME, sequencer_stream_msg_ack_threshold, 1
    ]) - 1,
    lists:foreach(fun(SeqNum) ->
        route_message(SeqStm, client_message(SeqNum))
    end, lists:seq(0, LastSeqNum)),
    ?assertReceivedMatch(#message_acknowledgement{
        sequence_number = LastSeqNum
    }, ?TIMEOUT).

sequencer_in_stream_should_send_message_request_for_awaited_message(Config) ->
    SeqStm = ?config(sequencer_in_stream, Config),
    route_message(SeqStm, client_message(1)),
    route_message(SeqStm, client_message(0)),
    ?assertReceivedMatch(#message_request{
        stream_id = 1, lower_sequence_number = 2, upper_sequence_number = 2
    }, ?TIMEOUT).

sequecner_in_stream_should_send_message_acknowledgement_when_end_of_stream(Config) ->
    route_message(?config(sequencer_in_stream, Config),
        client_message(0, #end_of_message_stream{})),
    ?assertReceivedMatch(#message_acknowledgement{sequence_number = 0}, ?TIMEOUT).

sequencer_in_stream_should_unregister_from_sequencer_manager_when_end_of_stream(Config) ->
    route_message(?config(sequencer_in_stream, Config),
        client_message(0, #end_of_message_stream{})),
    ?assertReceivedMatch({'$gen_cast', {unregister_in_stream, 1}}, ?TIMEOUT).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer]} | Config].

init_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    initializer:remove_pending_messages(),
    mock_communicator(Worker),
    mock_router(Worker),
    {ok, _} = rpc:call(Worker, session, save, [#document{key = <<"session_id">>, value =
        #session{type = fuse}}]),
    set_sequencer_in_stream_timeouts(Worker),
    {ok, SeqStm} = start_sequencer_in_stream(Worker),
    [{sequencer_in_stream, SeqStm} | Config].

end_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    rpc:call(Worker, session, delete, [<<"session_id">>]),
    stop_sequencer_in_stream(?config(sequencer_in_stream, Config)),
    test_utils:mock_validate_and_unload(Worker, [communicator, router]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts sequencer stream for incoming messages.
%% @end
%%--------------------------------------------------------------------
-spec start_sequencer_in_stream(Worker :: node()) -> {ok, SeqStm :: pid()}.
start_sequencer_in_stream(Worker) ->
    SeqMan = self(),
    ?assertMatch({ok, _}, rpc:call(Worker, gen_fsm, start, [
        sequencer_in_stream, [SeqMan, 1, <<"session_id">>], []
    ])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops sequencer stream for incoming messages.
%% @end
%%--------------------------------------------------------------------
-spec stop_sequencer_in_stream(SeqStm :: pid()) -> true.
stop_sequencer_in_stream(SeqStm) ->
    exit(SeqStm, shutdown).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends message to the sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec route_message(SeqStm :: pid(), Msg :: #client_message{}) -> ok.
route_message(SeqStm, Msg) ->
    gen_fsm:send_event(SeqStm, Msg).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv client_massage(SeqNum, undefined)
%% @end
%%--------------------------------------------------------------------
-spec client_message(SeqNum :: sequencer:sequence_number()) ->
    Msg :: #client_message{}.
client_message(SeqNum) ->
    client_message(SeqNum, undefined).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns client message as a part of a message stream. Sets sequence number
%% and message body.
%% @end
%%--------------------------------------------------------------------
-spec client_message(SeqNum :: sequencer:sequence_number(), Body :: term()) ->
    Msg :: #client_message{}.
client_message(SeqNum, Body) ->
    #client_message{message_stream = #message_stream{
        stream_id = 1, sequence_number = SeqNum
    }, message_body = Body}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets lower timeouts for sequence stream states.
%% @end
%%--------------------------------------------------------------------
-spec set_sequencer_in_stream_timeouts(Worker :: node()) -> ok.
set_sequencer_in_stream_timeouts(Worker) ->
    rpc:call(Worker, application, set_env, [
        ?APP_NAME, sequencer_stream_msg_req_short_timeout_seconds, 1
    ]),
    rpc:call(Worker, application, set_env, [
        ?APP_NAME, sequencer_stream_msg_req_long_timeout_seconds, 1
    ]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks communicator, so that on send it forwards all messages to this process.
%% @end
%%--------------------------------------------------------------------
-spec mock_communicator(Worker :: node()) -> ok.
mock_communicator(Worker) ->
    Self = self(),
    test_utils:mock_new(Worker, [communicator]),
    test_utils:mock_expect(Worker, communicator, send, fun
        (Msg, _, _) -> Self ! Msg, ok
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks communicator, so that on send it forwards all messages to this process.
%% @end
%%--------------------------------------------------------------------
-spec mock_router(Worker :: node()) -> ok.
mock_router(Worker) ->
    Self = self(),
    test_utils:mock_new(Worker, [router]),
    test_utils:mock_expect(Worker, router, route_message, fun
        (Msg) -> Self ! Msg, ok
    end).


