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

-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/server_messages.hrl").
-include("proto_internal/oneclient/communication_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([sequencer_manager_test/1, sequencer_stream_test/1]).

all() -> [sequencer_manager_test, sequencer_stream_test].

-define(TIMEOUT, timer:seconds(5)).

%%%====================================================================
%%% Test function
%%%====================================================================

%% Test creation and removal of sequencer dispatcher using sequencer manager.
sequencer_manager_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Self = self(),
    SessionId1 = <<"session_id_1">>,
    SessionId2 = <<"session_id_2">>,

    % Check whether sequencer worker returns the same sequencer dispatcher
    % for given session dispite of node on which request is processed.
    [SeqDisp1, SeqDisp2] = lists:map(fun({SessionId, Workers}) ->
        CreateOrGetSeqDispAnswers = utils:pmap(fun(Worker) ->
            rpc:call(Worker, sequencer_manager,
                get_or_create_sequencer_dispatcher, [SessionId, Self])
        end, Workers),

        lists:foreach(fun(CreateOrGetSeqDispAnswer) ->
            ?assertMatch({ok, _}, CreateOrGetSeqDispAnswer)
        end, CreateOrGetSeqDispAnswers),

        {_, [FirstSeqDisp | SeqDisps]} = lists:unzip(CreateOrGetSeqDispAnswers),

        lists:foreach(fun(SeqDisp) ->
            ?assertEqual(FirstSeqDisp, SeqDisp)
        end, SeqDisps),

        FirstSeqDisp
    end, [
        {SessionId1, lists:duplicate(2, Worker1) ++ lists:duplicate(2, Worker2)},
        {SessionId2, lists:duplicate(2, Worker2) ++ lists:duplicate(2, Worker1)}
    ]),

    % Check whether sequencer worker returns different sequencer dispatchers for
    % different sessions.
    ?assertNotEqual(SeqDisp1, SeqDisp2),

    % Check whether sequencer worker can remove sequencer manager
    % for given session dispite of node on which request is processed.
    utils:pforeach(fun({SessionId, Worker}) ->
        ProcessesBeforeRemoval = processes([Worker1, Worker2]),
        ?assertMatch([_], lists:filter(fun(P) ->
            P =:= SeqDisp1
        end, ProcessesBeforeRemoval)),

        RemoveSeqDispAnswer1 = rpc:call(Worker, sequencer_manager,
            remove_sequencer_dispatcher, [SessionId]),
        ?assertMatch(ok, RemoveSeqDispAnswer1),
        RemoveSeqDispAnswer2 = rpc:call(Worker, sequencer_manager,
            remove_sequencer_dispatcher, [SessionId]),
        ?assertMatch({error, _}, RemoveSeqDispAnswer2),

        ProcessesAfterRemoval = processes([Worker1, Worker2]),
        ?assertMatch([], lists:filter(fun(Proces) ->
            Proces =:= SeqDisp1
        end, ProcessesAfterRemoval))
    end, [
        {SessionId1, Worker2},
        {SessionId2, Worker1}
    ]),

    % Check whether sequencer dispatcher were deleted
    ?assertMatch({error, {not_found, _}}, rpc:call(Worker1,
        sequencer_dispatcher_data, get, [SessionId1])),
    ?assertMatch({error, {not_found, _}}, rpc:call(Worker2,
        sequencer_dispatcher_data, get, [SessionId2])),

    ok.

%% Test sequencer stream behaviour.
sequencer_stream_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessionId = <<"session_id">>,
    MsgId = 1,
    MsgCount = 100,
    ClientMsg = #client_message{message_id = MsgId, last_message = false},
    MsgReq = #message_request{message_id = MsgId},
    MsgAck = #message_acknowledgement{message_id = MsgId, seq_num = MsgCount},

    test_utils:mock_new(Worker, [router, protocol_handler]),
    test_utils:mock_expect(Worker, router, route_message, fun(Msg) ->
        Self ! Msg
    end),
    test_utils:mock_expect(Worker, protocol_handler, cast, fun(Connection, Msg) ->
        Connection ! Msg
    end),

    {ok, SeqDisp} = rpc:call(Worker, sequencer_manager,
        get_or_create_sequencer_dispatcher, [SessionId, Self]),

    %% Send 'MsgCount' messages in reverse order
    lists:foreach(fun(SeqNum) ->
        gen_server:cast(SeqDisp, ClientMsg#client_message{seq_num = SeqNum})
    end, lists:seq(MsgCount, 1, -1)),

    %% Check whether 'MsgCount' - 1 request messages were sent
    lists:foreach(fun(SeqNum) ->
        ?assertMatch({ok, _}, test_utils:receive_msg(#server_message{
            server_message = MsgReq#message_request{
                lower_seq_num = 1, upper_seq_num = SeqNum - 1
            }
        }, ?TIMEOUT))
    end, lists:seq(MsgCount, 2, -1)),

    %% Check whether messages were forwarded in right order
    lists:foreach(fun(SeqNum) ->
        ?assertMatch({ok, _}, test_utils:receive_msg(ClientMsg#client_message{
            seq_num = SeqNum}, ?TIMEOUT))
    end, lists:seq(1, MsgCount)),

    %% Check whether messages acknowledgement was sent
    ?assertMatch({ok, _}, test_utils:receive_msg(
        #server_message{server_message = MsgAck}, ?TIMEOUT)),

    %% Send last message
    gen_server:cast(SeqDisp, ClientMsg#client_message{
        seq_num = MsgCount + 1, last_message = true}),

    %% Check whether last message was sent
    ?assertMatch({ok, _}, test_utils:receive_msg(ClientMsg#client_message{
        seq_num = MsgCount + 1, last_message = true}, ?TIMEOUT)),

    %% Check whether last message acknowledgement was sent
    ?assertMatch({ok, _}, test_utils:receive_msg(
        #server_message{server_message = MsgAck#message_acknowledgement{
            seq_num = MsgCount + 1}}, ?TIMEOUT)),

    ?assertEqual({error, timeout}, test_utils:receive_any()),

    ok = rpc:call(Worker, sequencer_manager,
        remove_sequencer_dispatcher, [SessionId]),

    test_utils:mock_validate(Worker, [router, protocol_handler]),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns list of processes running on given nodes.
%% @end
%%--------------------------------------------------------------------
-spec processes(Nodes :: [node()]) -> [Pid :: pid()].
processes(Nodes) ->
    lists:foldl(fun(Node, Processes) ->
        Processes ++ rpc:call(Node, erlang, processes, [])
    end, [], Nodes).