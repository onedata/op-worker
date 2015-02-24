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
-include("workers/datastore/datastore_models.hrl").
-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/stream_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    sequencer_stream_test/1,
    sequencer_stream_periodic_ack_test/1,
    sequencer_stream_duplication_test/1,
    sequencer_stream_crash_test/1,
    sequencer_manager_test/1
]).

all() -> [
    sequencer_stream_test,
    sequencer_stream_periodic_ack_test,
    sequencer_stream_duplication_test,
    sequencer_stream_crash_test,
    sequencer_manager_test
].

-define(TIMEOUT, timer:seconds(5)).

%%%====================================================================
%%% Test function
%%%====================================================================

%% Test sequencer stream behaviour.
sequencer_stream_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    StmId = 1,
    {ok, MsgsCount} = test_utils:get_env(Worker, ?APP_NAME,
        sequencer_stream_messages_ack_window),

    % Check whether reset stream message was sent.
    ?assertMatch({ok, _}, test_utils:receive_msg(
        #message_stream_reset{}, ?TIMEOUT)),

    % Send 'MsgsCount' messages in reverse order.
    lists:foreach(fun(SeqNum) ->
        ?assertEqual(ok, rpc:call(Worker, sequencer_manager, route_message, [
            #client_message{message_stream = #message_stream{
                stm_id = StmId, seq_num = SeqNum, eos = false
            }}, SessId
        ]))
    end, lists:seq(MsgsCount - 1, 0, -1)),

    % Check whether 'MsgsCount' - 1 request messages were sent.
    lists:foreach(fun(SeqNum) ->
        ?assertEqual({ok, #message_request{
            stm_id = StmId, lower_seq_num = 0, upper_seq_num = SeqNum
        }}, test_utils:receive_any(?TIMEOUT))
    end, lists:seq(MsgsCount - 2, 0, -1)),

    % Check whether messages were forwarded in right order.
    lists:foreach(fun(SeqNum) ->
        ?assertEqual({ok, #client_message{message_stream = #message_stream{
            stm_id = StmId, seq_num = SeqNum, eos = false
        }}}, test_utils:receive_any(?TIMEOUT))
    end, lists:seq(0, MsgsCount - 1)),

    % Check whether messages acknowledgement was sent.
    ?assertEqual({ok, #message_acknowledgement{
        stm_id = StmId, seq_num = MsgsCount - 1
    }}, test_utils:receive_any(?TIMEOUT)),

    % Send last message.
    ?assertEqual(ok, rpc:call(Worker, sequencer_manager, route_message, [
        #client_message{message_stream = #message_stream{
            stm_id = StmId, seq_num = MsgsCount, eos = true
        }}, SessId])
    ),

    % Check whether last message was sent.
    ?assertMatch({ok, _}, test_utils:receive_msg(
        #client_message{message_stream = #message_stream{
            stm_id = StmId, seq_num = MsgsCount, eos = true
        }}, ?TIMEOUT
    )),

    % Check whether last message acknowledgement was sent.
    ?assertMatch({ok, _}, test_utils:receive_msg(#message_acknowledgement{
        stm_id = StmId, seq_num = MsgsCount}, ?TIMEOUT)),

    ?assertEqual({error, timeout}, test_utils:receive_any()),

    ok.

%% Test periodic emission of acknowledgement messages.
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
            stm_id = 1, seq_num = SeqNum, eos = false
        }},
        ?assertEqual(ok, rpc:call(Worker, sequencer_manager, route_message,
            [Msg, SessId]
        )),
        ?assertEqual({ok, Msg}, test_utils:receive_any(?TIMEOUT)),
        ?assertEqual({ok, #message_acknowledgement{
            stm_id = 1, seq_num = SeqNum
        }}, test_utils:receive_any(?TIMEOUT + SecsAckWin))
    end, lists:seq(0, MsgsCount - 1)),

    ?assertEqual({error, timeout}, test_utils:receive_any()),

    ok.

%% Test sequencer stream does not forward the same message twice.
sequencer_stream_duplication_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    MsgsCount = 20,
    Msgs = [#client_message{message_stream = #message_stream{
        stm_id = 1, seq_num = SeqNum, eos = false
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

%% Test sequencer stream reinitialization in case of crash.
sequencer_stream_crash_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    MsgsCount = 100,
    MsgsHalf = round(MsgsCount / 2),
    Msgs = [#client_message{message_stream = #message_stream{
        stm_id = 1, seq_num = SeqNum, eos = false
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

%% Test sequencer stream behaviour.
sequencer_manager_test(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    MsgsCount = 100,
    StmsCount = 10,
    Msgs = [#message_stream{seq_num = SeqNum, eos = false} ||
        SeqNum <- lists:seq(0, MsgsCount - 1)],
    RevSeqNums = lists:seq(MsgsCount - 1, 0, -1),

    % Production of 'MsgsCount' messages in random order belonging to 'StmsCount'
    % streams. Requests are routed through random workers.
    utils:pforeach(fun(StmId) ->
        lists:foreach(fun(Msg) ->
            [Worker | _] = utils:random_shuffle(Workers),
            ?assertEqual(ok, rpc:call(Worker, sequencer_manager, route_message, [
                #client_message{message_stream = Msg#message_stream{stm_id = StmId}},
                SessId
            ]))
        end, utils:random_shuffle(Msgs))
    end, lists:seq(1, StmsCount)),

    InitialMsgsMap = lists:foldl(fun(StmId, Map) ->
        maps:put(StmId, [], Map)
    end, #{}, lists:seq(0, MsgsCount - 1)),

    % Check whether 'MsgsCount' messages have been forwarded in a right order
    % from each stream.
    MsgsMap = lists:foldl(fun(_, Map) ->
        Msg = test_utils:receive_any(?TIMEOUT),
        ?assertMatch({ok, #client_message{}}, Msg),
        {ok, #client_message{message_stream = #message_stream{stm_id = StmId,
            seq_num = SeqNum}}} = Msg,
        StmMsgs = maps:get(StmId, Map),
        maps:update(StmId, [SeqNum | StmMsgs], Map)
    end, InitialMsgsMap, lists:seq(0, MsgsCount * StmsCount - 1)),

    lists:foreach(fun(StmId) ->
        ?assertEqual(RevSeqNums, maps:get(StmId, MsgsMap))
    end, lists:seq(1, StmsCount)),

    ?assertEqual({error, timeout}, test_utils:receive_any()),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(Case, Config) when
    Case =:= sequencer_stream_test;
    Case =:= sequencer_stream_periodic_ack_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},
    router_echo_mock_setup(Worker),
    communicator_echo_mock_setup(Worker, SessId),
    session_setup(Worker, SessId, Iden, Config);

init_per_testcase(sequencer_stream_crash_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},
    router_echo_mock_setup(Worker),
    communicator_retransmission_mock_setup(Worker),
    logger_crash_mock_setup(Worker),
    session_setup(Worker, SessId, Iden, Config);

init_per_testcase(Case, Config) when
    Case =:= sequencer_stream_duplication_test;
    Case =:= sequencer_manager_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},
    router_echo_mock_setup(Worker),
    communicator_retransmission_mock_setup(Worker),
    session_setup(Worker, SessId, Iden, Config).

end_per_testcase(Case, Config) when
    Case =:= sequencer_stream_test;
    Case =:= sequencer_stream_periodic_ack_test;
    Case =:= sequencer_stream_duplication_test;
    Case =:= sequencer_manager_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    NewConfig = session_teardown(Worker, Config),
    mocks_teardown(Worker, [router, communicator]),
    NewConfig;

end_per_testcase(sequencer_stream_crash_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    NewConfig = session_teardown(Worker, Config),
    mocks_teardown(Worker, [router, communicator, logger]),
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
        (#message_request{lower_seq_num = LSeqNum, upper_seq_num = USeqNum,
            stm_id = StmId}, Id) ->
            lists:foreach(fun(SeqNum) ->
                sequencer_manager:route_message(#client_message{
                    message_stream = #message_stream{
                        stm_id = StmId, seq_num = SeqNum, eos = false
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