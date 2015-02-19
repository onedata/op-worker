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

-include("workers/datastore/models/session.hrl").
-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/server_messages.hrl").
-include("proto_internal/oneclient/stream_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([sequencer_stream_test/1]).

all() -> [sequencer_stream_test].

-define(TIMEOUT, timer:seconds(5)).

%%%====================================================================
%%% Test function
%%%====================================================================

%% Test sequencer stream behaviour.
sequencer_stream_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    SessionId = ?config(session_id, Config),
    StmId = 1,
    MsgCount = 100,

    %% Check whether reset stream message was sent
    ?assertMatch({ok, _}, test_utils:receive_msg(
        #message_stream_reset{}, ?TIMEOUT)),

    %% Send 'MsgCount' messages in reverse order
    lists:foreach(fun(SeqNum) ->
        ?assertEqual(ok, rpc:call(Worker, sequencer_manager, route_message, [
            #client_message{message_stream = #message_stream{
                stm_id = StmId, seq_num = SeqNum, eos = false
            }}, SessionId])
        )
    end, lists:seq(MsgCount, 1, -1)),

    %% Check whether 'MsgCount' - 1 request messages were sent
    lists:foreach(fun(SeqNum) ->
        ?assertMatch({ok, _}, test_utils:receive_msg(#server_message{
            message_body = #message_request{
                stm_id = StmId, lower_seq_num = 1, upper_seq_num = SeqNum - 1
            }
        }, ?TIMEOUT))
    end, lists:seq(MsgCount, 2, -1)),

    %% Check whether messages were forwarded in right order
    lists:foreach(fun(SeqNum) ->
        ?assertMatch({ok, _}, test_utils:receive_msg(
            #client_message{message_stream = #message_stream{
                stm_id = StmId, seq_num = SeqNum, eos = false
            }}, ?TIMEOUT
        ))
    end, lists:seq(1, MsgCount)),

    %% Check whether messages acknowledgement was sent
    ?assertMatch({ok, _}, test_utils:receive_msg(
        #server_message{message_body = #message_acknowledgement{
            stm_id = StmId, seq_num = MsgCount
        }}, ?TIMEOUT
    )),

    %% Send last message
    ?assertEqual(ok, rpc:call(Worker, sequencer_manager, route_message, [
        #client_message{message_stream = #message_stream{
            stm_id = StmId, seq_num = MsgCount + 1, eos = true
        }}, SessionId])
    ),

    %% Check whether last message was sent
    ?assertMatch({ok, _}, test_utils:receive_msg(
        #client_message{message_stream = #message_stream{
            stm_id = StmId, seq_num = MsgCount + 1, eos = true
        }}, ?TIMEOUT
    )),

    %% Check whether last message acknowledgement was sent
    ?assertMatch({ok, _}, test_utils:receive_msg(
        #server_message{message_body = #message_acknowledgement{
            stm_id = StmId, seq_num = MsgCount + 1
        }}, ?TIMEOUT
    )),

    ?assertEqual({error, timeout}, test_utils:receive_any()),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(sequencer_stream_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId = <<"session_id">>,
    Cred = #credentials{user_id = <<"user_id">>},

    test_utils:mock_new(Worker, [communicator, router]),
    test_utils:mock_expect(Worker, router, route_message, fun(Msg) ->
        Self ! Msg, ok
    end),
    test_utils:mock_expect(Worker, communicator, send,
        fun(Msg, Id) when Id =:= SessId ->
            Self ! Msg, ok
        end
    ),

    ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, Cred, Self])),

    [{session_id, SessId} | Config].

end_per_testcase(sequencer_stream_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),

    ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId])),

    test_utils:mock_validate(Worker, [communicator, router]),
    test_utils:mock_unload(Worker, [communicator, router]),

    [_ | NewConfig] = Config,
    NewConfig.

%%%===================================================================
%%% Internal functions
%%%===================================================================