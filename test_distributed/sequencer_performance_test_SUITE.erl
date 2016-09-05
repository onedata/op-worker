%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains sequencer API performance tests.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_performance_test_SUITE).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
  route_message_should_forward_messages_in_right_order/1,
  route_message_should_work_for_multiple_streams/1,
  route_message_should_forward_messages_in_right_order_base/1,
  route_message_should_work_for_multiple_streams_base/1]).

-define(TEST_CASES, [
    route_message_should_forward_messages_in_right_order,
    route_message_should_work_for_multiple_streams
]).

all() -> ?ALL(?TEST_CASES, ?TEST_CASES).

-define(TIMEOUT, timer:seconds(15)).
-define(MSG_NUM(Value), [
    {name, msg_num}, {value, Value}, {description, "Number of messages."}
]).
-define(STM_NUM(Value), [
    {name, stm_num}, {value, Value}, {description, "Number of streams."}
]).
-define(MSG_ORD(Value), [
    {name, msg_ord}, {value, Value}, {description, "Order of messages to be "
    "processed by the sequencer."}
]).
-define(SEQ_CFG(CfgName, Descr, Num, Ord), {config,
    [
        {name, CfgName},
        {description, Descr},
        {parameters, [?MSG_NUM(Num), ?MSG_ORD(Ord)]}
    ]
}).

%%%===================================================================
%%% Test functions
%%%===================================================================

route_message_should_forward_messages_in_right_order(Config) ->
  ?PERFORMANCE(Config, [
    {repeats, 10},
    {success_rate, 90},
    {parameters, [?MSG_NUM(10), ?MSG_ORD(reverse)]},
    {description, "Check whether sequencer stream forwards messages in right order."},
    ?SEQ_CFG(small_msg_norm_ord, "Small number of messages in the right order", 100, normal),
    ?SEQ_CFG(medium_msg_norm_ord, "Medium number of messages in the right order", 1000, normal),
    ?SEQ_CFG(large_msg_norm_ord, "Large number of messages in the right order", 10000, normal),
    ?SEQ_CFG(small_msg_rev_ord, "Small number of messages in the reverse order", 100, reverse),
    ?SEQ_CFG(medium_msg_rev_ord, "Medium number of messages in the reverse order", 1000, reverse),
    ?SEQ_CFG(large_msg_rev_ord, "Large number of messages in the reverse order", 10000, reverse),
    ?SEQ_CFG(small_msg_rnd_ord, "Small number of messages in the random order", 100, random),
    ?SEQ_CFG(medium_msg_rnd_ord, "Medium number of messages in the random order", 1000, random),
    ?SEQ_CFG(large_msg_rnd_ord, "Large number of messages in the random order", 10000, random)
  ]).
route_message_should_forward_messages_in_right_order_base(Config) ->
  [Worker | _] = ?config(op_worker_nodes, Config),
  StmId = 1,
  MsgNum = ?config(msg_num, Config),
  MsgOrd = ?config(msg_ord, Config),

  SeqNums = case MsgOrd of
              normal -> lists:seq(0, MsgNum - 1);
              reverse -> lists:seq(MsgNum - 1, 0, -1);
              random -> utils:random_shuffle(lists:seq(0, MsgNum - 1))
            end,

  initializer:remove_pending_messages(),
  {ok, SessId} = session_setup(Worker),

  % Send 'MsgNum' messages in 'MsgOrd' order.
  {_, SendUs, SendTime, SendUnit} = utils:duration(fun() ->
    lists:foreach(fun(SeqNum) ->
      route_message(Worker, SessId, client_message(SessId, StmId, SeqNum))
    end, SeqNums)
  end),

  % Check whether messages were forwarded in right order.
  {Msgs, RecvUs, RecvTime, RecvUnit} = utils:duration(fun() ->
    lists:map(fun(_) ->
      ?assertReceivedMatch(#client_message{}, ?TIMEOUT)
    end, SeqNums)
  end),

  lists:foreach(fun({SeqNum, #client_message{message_stream = MsgStm}}) ->
    ?assertEqual(SeqNum, MsgStm#message_stream.sequence_number)
  end, lists:zip(lists:seq(0, MsgNum - 1), Msgs)),

  session_teardown(Worker, SessId),

  [send_time(SendTime, SendUnit), recv_time(RecvTime, RecvUnit),
    msg_per_sec(MsgNum, SendUs + RecvUs)].

route_message_should_work_for_multiple_streams(Config) ->
  ?PERFORMANCE(Config, [
    {repeats, 10},
    {success_rate, 90},
    {parameters, [?MSG_NUM(10), ?STM_NUM(5)]},
    {description, "Check whether messages are forwarded in right order for each "
    "stream despite of worker routing the message."},
    {config, [{name, small_stm_num},
      {description, "Small number of streams."},
      {parameters, [?MSG_NUM(100), ?STM_NUM(10)]}
    ]},
    {config, [{name, medium_stm_num},
      {description, "Medium number of streams."},
      {parameters, [?MSG_NUM(100), ?STM_NUM(50)]}
    ]},
    {config, [{name, large_stm_num},
      {description, "Large number of streams."},
      {parameters, [?MSG_NUM(100), ?STM_NUM(100)]}
    ]}
  ]).
route_message_should_work_for_multiple_streams_base(Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    MsgNum = ?config(msg_num, Config),
    StmNum = ?config(stm_num, Config),

    Msgs = [#message_stream{sequence_number = SeqNum} ||
      SeqNum <- lists:seq(0, MsgNum - 1)],
    RevSeqNums = lists:seq(MsgNum - 1, 0, -1),

    initializer:remove_pending_messages(),
    {ok, SessId} = session_setup(Worker),

    % Production of 'MsgNum' messages in random order belonging to 'StmsCount'
    % streams. Requests are routed through random workers.
    {_, SendUs, SendTime, SendUnit} = utils:duration(fun() ->
      utils:pforeach(fun(StmId) ->
        lists:foreach(fun(Msg) ->
          [Wrk | _] = utils:random_shuffle(Workers),
          route_message(Wrk, SessId, #client_message{session_id = SessId,
            message_stream = Msg#message_stream{stream_id = StmId}
          })
        end, utils:random_shuffle(Msgs))
      end, lists:seq(1, StmNum))
    end),

    InitialMsgsMap = lists:foldl(fun(StmId, Map) ->
      maps:put(StmId, [], Map)
    end, #{}, lists:seq(1, StmNum)),

    % Check whether 'MsgsCount' messages have been forwarded in a right order
    % from each stream.
    {MsgsMap, RecvUs, RecvTime, RecvUnit} = utils:duration(fun() ->
      lists:foldl(fun(_, Map) ->
        #client_message{session_id = SessId, message_stream = #message_stream{
          stream_id = StmId, sequence_number = SeqNum}
        } = ?assertReceivedMatch(#client_message{}, ?TIMEOUT),
        StmMsgs = maps:get(StmId, Map),
        maps:update(StmId, [SeqNum | StmMsgs], Map)
      end, InitialMsgsMap, lists:seq(0, MsgNum * StmNum - 1))
    end),

    lists:foreach(fun(StmId) ->
      ?assertEqual(RevSeqNums, maps:get(StmId, MsgsMap))
    end, lists:seq(1, StmNum)),

    session_teardown(Worker, SessId),

    [send_time(SendTime, SendUnit), recv_time(RecvTime, RecvUnit),
      msg_per_sec(MsgNum * StmNum, SendUs + RecvUs)].

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    mock_router(Workers),
    mock_communicator(Workers),
    Config.

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, [communicator, router]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session document in datastore with random ID.
%% @end
%%--------------------------------------------------------------------
-spec session_setup(Worker :: node()) -> {ok, SessId :: session:id()}.
session_setup(Worker) ->
    SessId = base64:encode(crypto:rand_bytes(20)),
    session_setup(Worker, SessId).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session document in datastore with given ID.
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
-spec mock_communicator(Workers :: [node()]) -> ok.
mock_communicator(Workers) ->
    Self = self(),
    test_utils:mock_new(Workers, [communicator]),
    test_utils:mock_expect(Workers, communicator, send, fun
        (Msg, _, _) -> Self ! Msg, ok
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks communicator, so that on send it forwards all messages to this process.
%% @end
%%--------------------------------------------------------------------
-spec mock_router(Workers :: [node()]) -> ok.
mock_router(Workers) ->
    Self = self(),
    test_utils:mock_new(Workers, [router]),
    test_utils:mock_expect(Workers, router, route_message, fun
        (Msg) -> Self ! Msg
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns summary messages send time parameter.
%% @end
%%--------------------------------------------------------------------
-spec send_time(Value :: integer() | float(), Unit :: string()) -> #parameter{}.
send_time(Value, Unit) ->
    #parameter{name = send_time, description = "Summary messages send time.",
        value = Value, unit = Unit}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns summary messages receive time parameter.
%% @end
%%--------------------------------------------------------------------
-spec recv_time(Value :: integer() | float(), Unit :: string()) -> #parameter{}.
recv_time(Value, Unit) ->
    #parameter{name = aggr_time, description = "Summary messages receive time.",
        value = Value, unit = Unit}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns number of messages per second parameter.
%% @end
%%--------------------------------------------------------------------
-spec msg_per_sec(MsgNum :: integer(), Time :: integer()) -> #parameter{}.
msg_per_sec(MsgNum, Time) ->
    #parameter{name = msgps, unit = "msg/s", description = "Number of messages "
    "per second.", value = 1000000 * MsgNum / Time}.