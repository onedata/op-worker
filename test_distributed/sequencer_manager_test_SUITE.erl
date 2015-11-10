%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of sequencer manager API.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_manager_test_SUITE).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("cluster/worker/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/stream_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("annotations/include/annotations.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    sequencer_stream_reset_stream_message_test/1,
    sequencer_stream_messages_ordering_test/1,
    sequencer_stream_request_messages_test/1,
    sequencer_stream_messages_acknowledgement_test/1,
    sequencer_stream_end_of_stream_test/1,
    sequencer_stream_periodic_ack_test/1,
    sequencer_stream_duplication_test/1,
    sequencer_stream_crash_test/1,
    sequencer_manager_multiple_streams_messages_ordering_test/1
]).

-performance({test_cases, [
    sequencer_stream_messages_ordering_test,
    sequencer_manager_multiple_streams_messages_ordering_test
]}).
all() -> [
    sequencer_stream_reset_stream_message_test,
    sequencer_stream_messages_ordering_test,
    sequencer_stream_request_messages_test,
    sequencer_stream_messages_acknowledgement_test,
    sequencer_stream_end_of_stream_test,
    sequencer_stream_periodic_ack_test,
    sequencer_stream_duplication_test,
    sequencer_stream_crash_test,
    sequencer_manager_multiple_streams_messages_ordering_test
].

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

%% Check whether sequencer manager sends reset streams message at the start.
sequencer_stream_reset_stream_message_test(_) ->
    % Check whether reset stream message was sent.
    ?assertMatch({ok, _}, test_utils:receive_msg(
        #message_stream_reset{}, ?TIMEOUT)),

    ok.

-performance([
    {repeats, 10},
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
sequencer_stream_messages_ordering_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},
    StmId = 1,
    MsgNum = ?config(msg_num, Config),
    MsgOrd = ?config(msg_ord, Config),

    communicator_echo_mock_setup(Worker, SessId),
    session_setup(Worker, SessId, Iden, Config),

    SeqNums = case MsgOrd of
                  normal -> lists:seq(0, MsgNum - 1);
                  reverse -> lists:seq(MsgNum - 1, 0, -1);
                  random -> utils:random_shuffle(lists:seq(0, MsgNum - 1))
              end,

    % Send 'MsgNum' messages in 'MsgOrd' order.
    {_, SendUs, SendTime, SendUnit} = utils:duration(fun() ->
        lists:foreach(fun(SeqNum) ->
            ?assertEqual(ok, rpc:call(Worker, sequencer_manager, route_message, [
                #client_message{message_stream = #message_stream{
                    stream_id = StmId, sequence_number = SeqNum
                }}, SessId
            ]))
        end, SeqNums)
    end),

    % Check whether messages were forwarded in right order.
    {_, RecvUs, RecvTime, RecvUnit} = utils:duration(fun() ->
        lists:foreach(fun(SeqNum) ->
            ?assertReceived(#client_message{
                message_stream = #message_stream{
                    stream_id = StmId, sequence_number = SeqNum
                }}, ?TIMEOUT)
        end, lists:seq(0, MsgNum - 1))
    end),

    session_teardown(Worker, [{session_id, SessId}]),
    mocks_teardown(Worker, [communicator]),
    remove_pending_messages(),

    [send_time(SendTime, SendUnit), recv_time(RecvTime, RecvUnit),
        msg_per_sec(MsgNum, SendUs + RecvUs)].

%% Check whether sequencer stream sends requests for messages when they arrive
%% in wrong order.
sequencer_stream_request_messages_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    StmId = 1,
    {ok, MsgsCount} = test_utils:get_env(Worker, ?APP_NAME,
        sequencer_stream_messages_ack_window),

    % Send 'MsgsCount' messages in reverse order.
    lists:foreach(fun(SeqNum) ->
        ?assertEqual(ok, rpc:call(Worker, sequencer_manager, route_message, [
            #client_message{message_stream = #message_stream{
                stream_id = StmId, sequence_number = SeqNum
            }}, SessId
        ]))
    end, lists:seq(MsgsCount - 1, 0, -1)),

    % Check whether 'MsgsCount' - 1 request messages were sent.
    lists:foreach(fun(SeqNum) ->
        ?assertMatch({ok, _}, test_utils:receive_msg(#message_request{
            stream_id = StmId, lower_sequence_number = 0, upper_sequence_number = SeqNum
        }, ?TIMEOUT))
    end, lists:seq(MsgsCount - 2, 0, -1)),

    ok.

%% Check whether sequencer stream sends acknowledgement message when more than
%% 'sequencer_stream_messages_ack_window' messages have been forwarded.
sequencer_stream_messages_acknowledgement_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    StmId = 1,
    {ok, MsgsCount} = test_utils:get_env(Worker, ?APP_NAME,
        sequencer_stream_messages_ack_window),

    % Send 'MsgsCount' messages in reverse order.
    lists:foreach(fun(SeqNum) ->
        ?assertEqual(ok, rpc:call(Worker, sequencer_manager, route_message, [
            #client_message{message_stream = #message_stream{
                stream_id = StmId, sequence_number = SeqNum
            }}, SessId
        ]))
    end, lists:seq(MsgsCount - 1, 0, -1)),

    % Check whether messages acknowledgement was sent.
    ?assertMatch({ok, _}, test_utils:receive_msg(#message_acknowledgement{
        stream_id = StmId, sequence_number = MsgsCount - 1
    }, ?TIMEOUT)),

    ok.

%% Check whether sequencer stream forward last message in the stream, sends
%% acknowledgement message and finally closes.
sequencer_stream_end_of_stream_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    StmId = 1,
    SeqNum = 0,

    % Send last message.
    ?assertEqual(ok, rpc:call(Worker, sequencer_manager, route_message, [
        #client_message{message_stream = #message_stream{
            stream_id = StmId, sequence_number = SeqNum
        }, message_body = #end_of_message_stream{}}, SessId
    ])),

    % Check whether last message was sent.
    ?assertMatch({ok, _}, test_utils:receive_msg(
        #client_message{message_stream = #message_stream{
            stream_id = StmId, sequence_number = SeqNum
        }, message_body = #end_of_message_stream{}}, ?TIMEOUT
    )),

    % Check whether last message acknowledgement was sent.
    ?assertMatch({ok, _}, test_utils:receive_msg(#message_acknowledgement{
        stream_id = StmId, sequence_number = SeqNum
    }, ?TIMEOUT)),

    % Check whether sequencer stream process has terminated normaly.
    {ok, {SessSup, _}} = rpc:call(Worker, session,
        get_session_supervisor_and_node, [SessId]),
    {ok, SeqManSup} = get_child(SessSup, sequencer_manager_sup),
    {ok, SeqStmSup} = get_child(SeqManSup, sequencer_stream_sup),
    ?assertEqual([], supervisor:which_children(SeqStmSup)),

    ok.

%% Check whether sequencer stream emits periodic acknowledgement messages.
sequencer_stream_periodic_ack_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    {ok, MsgsAckWin} = test_utils:get_env(Worker, ?APP_NAME,
        sequencer_stream_messages_ack_window),
    SecsAckWin = 1,
    ok = test_utils:set_env(Worker, ?APP_NAME,
        sequencer_stream_seconds_ack_window, SecsAckWin),
    MsgsCount = min(MsgsAckWin, 5),

    % Check whether reset stream message was sent.
    ?assertMatch({ok, _}, test_utils:receive_msg(
        #message_stream_reset{}, ?TIMEOUT)),

    % Send messages in right order and wait for periodic acknowledgement.
    lists:foreach(fun(SeqNum) ->
        Msg = #client_message{message_stream = #message_stream{
            stream_id = 1, sequence_number = SeqNum
        }},
        ?assertEqual(ok, rpc:call(Worker, sequencer_manager, route_message,
            [Msg, SessId]
        )),
        ?assertReceived(Msg, (?TIMEOUT)),
        ?assertReceived(#message_acknowledgement{
            stream_id = 1, sequence_number = SeqNum
        }, ?TIMEOUT + SecsAckWin)
    end, lists:seq(0, MsgsCount - 1)),

    ?assertReceived(#write_event_subscription{}, ?TIMEOUT),
    ?assertEqual({error, timeout}, test_utils:receive_any()),

    ok.

%% Check whether sequencer stream does not forward the same message twice.
sequencer_stream_duplication_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    MsgsCount = 20,
    Msgs = [#client_message{message_stream = #message_stream{
        stream_id = 1, sequence_number = SeqNum
    }} || SeqNum <- lists:seq(0, MsgsCount - 1)],
    RandomMsgs = utils:random_shuffle(Msgs ++ Msgs ++ Msgs ++ Msgs ++ Msgs),

    % Send duplicated messages messages in random order.
    lists:foreach(fun(Msg) ->
        ?assertEqual(ok, rpc:call(Worker, sequencer_manager, route_message,
            [Msg, SessId]
        ))
    end, RandomMsgs),

    % Check whether messages were not duplicated and forwarded in right order.
    lists:foreach(fun(Msg) ->
        ?assertEqual({ok, Msg}, test_utils:receive_any(?TIMEOUT))
    end, Msgs),

    ?assertEqual({error, timeout}, test_utils:receive_any()),

    ok.

%% Check whether sequencer stream is reinitialized in previous state in case of crash.
sequencer_stream_crash_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    MsgsCount = 100,
    MsgsHalf = round(MsgsCount / 2),
    Msgs = [#client_message{message_stream = #message_stream{
        stream_id = 1, sequence_number = SeqNum
    }} || SeqNum <- lists:seq(0, MsgsCount - 1)],
    RandomMsgs = utils:random_shuffle(Msgs),
    MsgsPart1 = lists:sublist(RandomMsgs, MsgsHalf),
    MsgsPart2 = lists:sublist(RandomMsgs, MsgsHalf + 1, MsgsHalf),

    % Send first part of messages.
    lists:foreach(fun(Msg) ->
        ?assertEqual(ok, rpc:call(Worker, sequencer_manager, route_message,
            [Msg, SessId]
        ))
    end, MsgsPart1),

    % Get sequencer stream pid.
    {ok, {SessSup, _}} = rpc:call(Worker, session,
        get_session_supervisor_and_node, [SessId]),
    {ok, SeqManSup} = get_child(SessSup, sequencer_manager_sup),
    {ok, SeqStmSup} = get_child(SeqManSup, sequencer_stream_sup),
    {ok, SeqStm} = get_child(SeqStmSup, undefined),

    % Send crash message and wait for event stream recovery.
    gen_server:cast(SeqStm, kill),
    timer:sleep(?TIMEOUT),

    % Send second part of messages.
    lists:foreach(fun(Msg) ->
        ?assertEqual(ok, rpc:call(Worker, sequencer_manager, route_message,
            [Msg, SessId]
        ))
    end, MsgsPart2),

    % Check whether messages were not lost and forwarded in right order.
    lists:foreach(fun(Msg) ->
        ?assertEqual({ok, Msg}, test_utils:receive_any(?TIMEOUT))
    end, Msgs),

    ?assertEqual({error, timeout}, test_utils:receive_any()),

    ok.

-performance([
    {repeats, 10},
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
sequencer_manager_multiple_streams_messages_ordering_test(Config) ->
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},
    MsgNum = ?config(msg_num, Config),
    StmNum = ?config(stm_num, Config),

    session_setup(Worker, SessId, Iden, Config),

    Msgs = [#message_stream{sequence_number = SeqNum} ||
        SeqNum <- lists:seq(0, MsgNum - 1)],
    RevSeqNums = lists:seq(MsgNum - 1, 0, -1),

    % Production of 'MsgNum' messages in random order belonging to 'StmsCount'
    % streams. Requests are routed through random workers.
    {_, SendUs, SendTime, SendUnit} = utils:duration(fun() ->
        utils:pforeach(fun(StmId) ->
            lists:foreach(fun(Msg) ->
                [Wrk | _] = utils:random_shuffle(Workers),
                ?assertEqual(ok, rpc:call(Wrk, sequencer_manager, route_message, [
                    #client_message{message_stream = Msg#message_stream{stream_id = StmId}},
                    SessId
                ]))
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
            Msg = test_utils:receive_any(?TIMEOUT),
            ?assertMatch({ok, #client_message{}}, Msg),
            {ok, #client_message{message_stream = #message_stream{stream_id = StmId,
                sequence_number = SeqNum}}} = Msg,
            StmMsgs = maps:get(StmId, Map),
            maps:update(StmId, [SeqNum | StmMsgs], Map)
        end, InitialMsgsMap, lists:seq(0, MsgNum * StmNum - 1))
    end),

    lists:foreach(fun(StmId) ->
        ?assertEqual(RevSeqNums, maps:get(StmId, MsgsMap))
    end, lists:seq(1, StmNum)),

    ?assertEqual({error, timeout}, test_utils:receive_any()),

    session_teardown(Worker, [{session_id, SessId}]),

    [send_time(SendTime, SendUnit), recv_time(RecvTime, RecvUnit),
        msg_per_sec(MsgNum * StmNum, SendUs + RecvUs)].

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(sequencer_stream_crash_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},
    router_echo_mock_setup(Worker),
    communicator_retransmission_mock_setup(Worker),
    logger_crash_mock_setup(Worker),
    session_setup(Worker, SessId, Iden, Config);

init_per_testcase(sequencer_stream_duplication_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},
    router_echo_mock_setup(Worker),
    communicator_retransmission_mock_setup(Worker),
    session_setup(Worker, SessId, Iden, Config);

init_per_testcase(sequencer_stream_messages_ordering_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    router_echo_mock_setup(Worker),
    Config;

init_per_testcase(sequencer_manager_multiple_streams_messages_ordering_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    router_echo_mock_setup(Worker),
    communicator_retransmission_mock_setup(Worker),
    Config;

init_per_testcase(Case, Config) when
    Case =:= sequencer_stream_reset_stream_message_test;
    Case =:= sequencer_stream_request_messages_test;
    Case =:= sequencer_stream_messages_acknowledgement_test;
    Case =:= sequencer_stream_end_of_stream_test;
    Case =:= sequencer_stream_periodic_ack_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},
    router_echo_mock_setup(Worker),
    communicator_echo_mock_setup(Worker, SessId),
    session_setup(Worker, SessId, Iden, Config).

end_per_testcase(sequencer_stream_crash_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    remove_pending_messages(),
    NewConfig = session_teardown(Worker, Config),
    mocks_teardown(Worker, [router, communicator, logger]),
    NewConfig;

end_per_testcase(sequencer_stream_messages_ordering_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    remove_pending_messages(),
    mocks_teardown(Worker, [router]),
    Config;

end_per_testcase(sequencer_manager_multiple_streams_messages_ordering_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    remove_pending_messages(),
    mocks_teardown(Worker, [router, communicator]),
    Config;

end_per_testcase(Case, Config) when
    Case =:= sequencer_stream_reset_stream_message_test;
    Case =:= sequencer_stream_request_messages_test;
    Case =:= sequencer_stream_messages_acknowledgement_test;
    Case =:= sequencer_stream_end_of_stream_test;
    Case =:= sequencer_stream_periodic_ack_test;
    Case =:= sequencer_stream_duplication_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    remove_pending_messages(),
    NewConfig = session_teardown(Worker, Config),
    mocks_teardown(Worker, [router, communicator]),
    NewConfig.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new test session.
%% @end
%%--------------------------------------------------------------------
-spec session_setup(Worker :: node(), SessId :: session:id(),
    Iden :: session:identity(), Config :: term()) -> NewConfig :: term().
session_setup(Worker, SessId, Iden, Config) ->
    Self = self(),
    ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, Iden, Self])),
    [{session_id, SessId}, {identity, Iden} | Config].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes existing test session.
%% @end
%%--------------------------------------------------------------------
-spec session_teardown(Worker :: node(), Config :: term()) -> NewConfig :: term().
session_teardown(Worker, Config) ->
    SessId = ?config(session_id, Config),
    ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId])),
    proplists:delete(session_id, proplists:delete(identity, Config)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks router module, so that all messages are sent back to test process.
%% @end
%%--------------------------------------------------------------------
-spec router_echo_mock_setup(Workers :: node() | [node()]) -> ok.
router_echo_mock_setup(Workers) ->
    Self = self(),
    test_utils:mock_new(Workers, router),
    test_utils:mock_expect(Workers, router, route_message, fun(Msg) ->
        Self ! Msg, ok
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks communicator module, so that all messages are sent back to test process.
%% @end
%%--------------------------------------------------------------------
-spec communicator_echo_mock_setup(Workers :: node() | [node()],
    SessId :: session:id()) -> ok.
communicator_echo_mock_setup(Workers, SessId) ->
    Self = self(),
    test_utils:mock_new(Workers, communicator),
    test_utils:mock_expect(Workers, communicator, send,
        fun(Msg, Id) when Id =:= SessId ->
            Self ! Msg, ok
        end
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks communicator module, so that for messages of type 'message_request'
%% proper answer is sent back.
%% @end
%%--------------------------------------------------------------------
-spec communicator_retransmission_mock_setup(Workers :: node() | [node()]) -> ok.
communicator_retransmission_mock_setup(Workers) ->
    test_utils:mock_new(Workers, communicator),
    test_utils:mock_expect(Workers, communicator, send, fun
        (#message_request{lower_sequence_number = LSeqNum, upper_sequence_number = USeqNum,
            stream_id = StmId}, Id) ->
            lists:foreach(fun(SeqNum) ->
                sequencer_manager:route_message(#client_message{
                    message_stream = #message_stream{
                        stream_id = StmId, sequence_number = SeqNum
                    }
                }, Id)
            end, lists:seq(LSeqNum, USeqNum));
        (_, _) -> ok
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks logger module, so that execution of 'log_bad_request' macro with 'kill'
%% argument throws an exception.
%% @end
%%--------------------------------------------------------------------
-spec logger_crash_mock_setup(Workers :: node() | [node()]) -> ok.
logger_crash_mock_setup(Workers) ->
    test_utils:mock_new(Workers, logger),
    test_utils:mock_expect(Workers, logger, dispatch_log, fun
        (_, _, _, [_, _, kill], _) -> meck:exception(throw, crash);
        (A, B, C, D, E) -> meck:passthrough([A, B, C, D, E])
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Validates and unloads mocks.
%% @end
%%--------------------------------------------------------------------
-spec mocks_teardown(Workers :: node() | [node()],
    Modules :: module() | [module()]) -> ok.
mocks_teardown(Workers, Modules) ->
    test_utils:mock_validate(Workers, Modules),
    test_utils:mock_unload(Workers, Modules).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns supervisor child.
%% @end
%%--------------------------------------------------------------------
-spec get_child(Sup :: pid(), ChildId :: term()) ->
    {ok, Child :: pid()} | {error, not_found}.
get_child(Sup, ChildId) ->
    Children = supervisor:which_children(Sup),
    case lists:keyfind(ChildId, 1, Children) of
        {ChildId, Child, _, _} -> {ok, Child};
        false -> {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes messages for process messages queue.
%% @end
%%--------------------------------------------------------------------
-spec remove_pending_messages() -> ok.
remove_pending_messages() ->
    case test_utils:receive_any() of
        {error, timeout} -> ok;
        _ -> remove_pending_messages()
    end.

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
