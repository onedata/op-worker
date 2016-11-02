%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains sequencer API tests.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_test_SUITE).
-author("Krzysztof Trzepla").


-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("proto/oneclient/client_messages.hrl").
-include_lib("proto/oneclient/server_messages.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    open_stream_should_return_stream_id/1,
    open_stream_should_return_different_stream_ids/1,
    close_stream_should_notify_sequencer_manager/1,
    send_message_should_forward_message/1,
    send_message_should_inject_stream_id_into_message/1,
    route_message_should_forward_message/1,
    route_message_should_forward_messages_to_the_same_stream/1,
    route_message_should_forward_messages_to_different_streams/1
]).

all() ->
    ?ALL([
        open_stream_should_return_stream_id,
        open_stream_should_return_different_stream_ids,
        close_stream_should_notify_sequencer_manager,
        send_message_should_forward_message,
        send_message_should_inject_stream_id_into_message,
        route_message_should_forward_message,
        route_message_should_forward_messages_to_the_same_stream,
        route_message_should_forward_messages_to_different_streams
    ]).

-define(TIMEOUT, timer:seconds(15)).

%%%===================================================================
%%% Test functions
%%%===================================================================

open_stream_should_return_stream_id(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, StmId} = open_stream(Worker, ?config(session_id, Config)),
    ?assert(is_integer(StmId)).

open_stream_should_return_different_stream_ids(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, StmId1} = open_stream(Worker, ?config(session_id, Config)),
    {ok, StmId2} = open_stream(Worker, ?config(session_id, Config)),
    ?assert(StmId1 =/= StmId2).

close_stream_should_notify_sequencer_manager(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, StmId} = open_stream(Worker, ?config(session_id, Config)),
    close_stream(Worker, ?config(session_id, Config), StmId).

send_message_should_forward_message(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StmId = ?config(stream_id, Config),
    send_message(Worker, ?config(session_id, Config), StmId, msg),
    ?assertReceivedMatch(
        #server_message{message_stream = #message_stream{}}, ?TIMEOUT
    ).

send_message_should_inject_stream_id_into_message(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StmId = ?config(stream_id, Config),
    send_message(Worker, ?config(session_id, Config), StmId, msg),
    ?assertReceivedMatch(#server_message{message_stream = #message_stream{
        stream_id = StmId
    }}, ?TIMEOUT).

route_message_should_forward_message(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Msg = client_message(?config(session_id, Config), 1, 0),
    route_message(Worker, ?config(session_id, Config), Msg),
    ?assertReceivedMatch(Msg, ?TIMEOUT).

route_message_should_forward_messages_to_the_same_stream(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Msgs = lists:map(fun(SeqNum) ->
        Msg = client_message(SessId, 1, SeqNum),
        route_message(Worker, SessId, Msg),
        Msg
    end, lists:seq(9, 0, -1)),
    lists:foreach(fun(Msg) ->
        ?assertReceivedNextMatch(Msg, ?TIMEOUT)
    end, lists:reverse(Msgs)).

route_message_should_forward_messages_to_different_streams(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Msgs = lists:map(fun(StmId) ->
        Msg = client_message(SessId, StmId, 0),
        route_message(Worker, SessId, Msg),
        Msg
    end, lists:seq(1, 10)),
    lists:foreach(fun(Msg) ->
        ?assertReceivedMatch(Msg, ?TIMEOUT)
    end, Msgs).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    NewConfig = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    [Worker | _] = ?config(op_worker_nodes, NewConfig),
    initializer:clear_models(Worker, [subscription]),
    NewConfig.

end_per_suite(Config) ->
    ?TEST_STOP(Config).

init_per_testcase(Case, Config) when
    Case =:= send_message_should_forward_message;
    Case =:= send_message_should_inject_stream_id_into_message ->
    ?CASE_START(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    mock_communicator(Worker),
    {ok, SessId} = session_setup(Worker),
    {ok, StmId} = open_stream(Worker, SessId),
    [{session_id, SessId}, {stream_id, StmId} | Config];

init_per_testcase(Case, Config) when
    Case =:= open_stream_should_return_stream_id;
    Case =:= open_stream_should_return_different_stream_ids;
    Case =:= close_stream_should_notify_sequencer_manager;
    Case =:= route_message_should_forward_message;
    Case =:= route_message_should_forward_messages_to_the_same_stream;
    Case =:= route_message_should_forward_messages_to_different_streams ->
    ?CASE_START(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    mock_router(Worker),
    mock_communicator(Worker, fun(_, _, _) -> ok end),
    {ok, SessId} = session_setup(Worker),
    [{session_id, SessId} | Config].

end_per_testcase(Case, Config) when
    Case =:= send_message_should_forward_message;
    Case =:= send_message_should_inject_stream_id_into_message ->
    ?CASE_STOP(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    close_stream(Worker, SessId, ?config(stream_id, Config)),
    session_teardown(Worker, SessId),
    test_utils:mock_validate_and_unload(Worker, communicator);

end_per_testcase(Case, Config) when
    Case =:= open_stream_should_return_stream_id;
    Case =:= open_stream_should_return_different_stream_ids;
    Case =:= close_stream_should_notify_sequencer_manager;
    Case =:= route_message_should_forward_message;
    Case =:= route_message_should_forward_messages_to_the_same_stream;
    Case =:= route_message_should_forward_messages_to_different_streams ->
    ?CASE_STOP(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    session_teardown(Worker, SessId),
    test_utils:mock_validate_and_unload(Worker, [communicator, router]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv session_setup(Worker, <<"session_id">>
%% @end
%%--------------------------------------------------------------------
-spec session_setup(Worker :: node()) -> {ok, SessId :: session:id()}.
session_setup(Worker) ->
    session_setup(Worker, <<"session_id">>).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session document in datastore.
%% @end
%%--------------------------------------------------------------------
-spec session_setup(Worker :: node(), SessId :: session:id()) ->
    {ok, SessId :: session:id()}.
session_setup(Worker, SessId) ->
    Self = self(),
    Iden = #user_identity{user_id = <<"user_id">>},
    ?assertMatch({ok, _}, rpc:call(Worker, session_manager,
        reuse_or_create_fuse_session, [SessId, Iden, Self]
    )),
    {ok, SessId}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes session document from datastore.
%% @end
%%--------------------------------------------------------------------
-spec session_teardown(Worker :: node(), SessId :: session:id()) -> ok.
session_teardown(Worker, SessId) ->
    rpc:call(Worker, session_manager, remove_session, [SessId]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec open_stream(Worker :: node(), SessId :: session:id()) ->
    {ok, StmId :: sequencer:stream_id()}.
open_stream(Worker, SessId) ->
    ?assertMatch({ok, _}, rpc:call(Worker, sequencer, open_stream, [SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Closes sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec close_stream(Worker :: node(), SessId :: session:id(),
    StmId :: sequencer:stream_id()) -> ok.
close_stream(Worker, SessId, StmId) ->
    ?assertEqual(ok, rpc:call(Worker, sequencer, close_stream, [StmId, SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends message to sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec send_message(Worker :: node(), SessId :: session:id(),
    StmId :: sequencer:stream_id(), Msg :: term()) -> ok.
send_message(Worker, SessId, StmId, Msg) ->
    ?assertEqual(ok, rpc:call(Worker, sequencer, send_message, [Msg, StmId, SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends message to sequencer stream for incoming messages.
%% @end
%%--------------------------------------------------------------------
-spec route_message(Worker :: node(), SessId :: session:id(), Msg :: term()) -> ok.
route_message(Worker, SessId, Msg) ->
    ?assertEqual(ok, rpc:call(Worker, sequencer, route_message, [Msg, SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns client message as part of messages stream.
%% @end
%%--------------------------------------------------------------------
-spec client_message(SessId :: session:id(), StmId :: sequencer:stream_id(),
    SeqNum :: sequencer:sequence_number()) -> Msg :: #client_message{}.
client_message(SessId, StmId, SeqNum) ->
    #client_message{session_id = SessId, message_stream = #message_stream{
        stream_id = StmId, sequence_number = SeqNum
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks communicator, so that on send it forwards all messages to this process.
%% @end
%%--------------------------------------------------------------------
-spec mock_communicator(Worker :: node()) -> ok.
mock_communicator(Worker) ->
    Self = self(),
    mock_communicator(Worker, fun(Msg, _, _) -> Self ! Msg, ok end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks communicator, so that send function calls provided mock function.
%% @end
%%--------------------------------------------------------------------
-spec mock_communicator(Worker :: node(), MockFun :: fun()) -> ok.
mock_communicator(Worker, MockFun) ->
    test_utils:mock_new(Worker, [communicator]),
    test_utils:mock_expect(Worker, communicator, send, MockFun).

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
        (Msg) -> Self ! Msg
    end).

