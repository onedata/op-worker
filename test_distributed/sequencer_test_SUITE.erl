%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests sequencer API.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_test_SUITE).
-author("Krzysztof Trzepla").

-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/server_messages.hrl").
-include("proto_internal/oneclient/communication_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([sequencer_worker_test/1, sequencer_test/1]).

all() -> [sequencer_worker_test, sequencer_test].

-define(TIMEOUT, timer:seconds(5)).

%%%===================================================================
%%% Test function
%%%====================================================================

%% Test creation and removal of sequencer managers using sequencer worker.
sequencer_worker_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Self = self(),
    FuseId1 = <<"fuse_id_1">>,
    FuseId2 = <<"fuse_id_2">>,

    % Check whether sequencer worker returns the same sequencer manager
    % for given session dispite of node on which request is processed.
    [SeqMan1, SeqMan2] = lists:map(fun({FuseId, Workers}) ->
        CreateOrGetSeqManAnswers = utils:pmap(fun(Worker) ->
            rpc:call(Worker, worker_proxy, call, [sequencer_worker,
                {get_or_create_sequencer_manager, FuseId, Self},
                ?TIMEOUT, prefer_local])
        end, Workers),

        lists:foreach(fun(CreateOrGetSeqManAnswer) ->
            ?assertMatch({ok, _}, CreateOrGetSeqManAnswer)
        end, CreateOrGetSeqManAnswers),

        {_, [FirstSeqMan | SeqMans]} = lists:unzip(CreateOrGetSeqManAnswers),

        lists:foreach(fun(SeqMan) ->
            ?assertEqual(FirstSeqMan, SeqMan)
        end, SeqMans),

        FirstSeqMan
    end, [
        {FuseId1, lists:duplicate(2, Worker1) ++ lists:duplicate(2, Worker2)},
        {FuseId2, lists:duplicate(2, Worker2) ++ lists:duplicate(2, Worker1)}
    ]),

    % Check whether sequencer worker returns different sequencer manager for
    % different session.
    ?assertNotEqual(SeqMan1, SeqMan2),

    % Check whether sequencer worker can remove sequencer manager
    % for given session dispite of node on which request is processed.
    utils:pforeach(fun({FuseId, Worker}) ->
        ProcessesBeforeRemoval = processes([Worker1, Worker2]),
        ?assertMatch([_], lists:filter(fun(P) ->
            P =:= SeqMan1
        end, ProcessesBeforeRemoval)),

        RemoveSeqManAnswer1 = rpc:call(Worker, worker_proxy, call, [
            sequencer_worker, {remove_sequencer_manager, FuseId},
            ?TIMEOUT, prefer_local]
        ),
        ?assertMatch(ok, RemoveSeqManAnswer1),
        RemoveSeqManAnswer2 = rpc:call(Worker, worker_proxy, call, [
            sequencer_worker, {remove_sequencer_manager, FuseId},
            ?TIMEOUT, prefer_local]
        ),
        ?assertMatch({error, _}, RemoveSeqManAnswer2),

        ProcessesAfterRemoval = processes([Worker1, Worker2]),
        ?assertMatch([], lists:filter(fun(P) ->
            P =:= SeqMan1
        end, ProcessesAfterRemoval))
    end, [
        {FuseId1, Worker2},
        {FuseId2, Worker1}
    ]),

    ok.

%% Test sequencer behaviour.
sequencer_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    Self = self(),
    FuseId = <<"fuse_id">>,
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

    {ok, SeqMan} = rpc:call(Worker, worker_proxy, call, [sequencer_worker,
        {get_or_create_sequencer_manager, FuseId, Self},
        ?TIMEOUT, prefer_local
    ]),

    lists:foreach(fun(SeqNum) ->
        gen_server:cast(SeqMan, ClientMsg#client_message{seq_num = SeqNum})
    end, lists:seq(MsgCount, 1, -1)),

    lists:foreach(fun(SeqNum) ->
        ReceiveAnswer = test_utils:receive_msg(#server_message{
            server_message = MsgReq#message_request{
                lower_seq_num = 1, upper_seq_num = SeqNum - 1
            }
        }, ?TIMEOUT),
        ?assertEqual(ok, ReceiveAnswer)
    end, lists:seq(MsgCount, 2, -1)),

    lists:foreach(fun(SeqNum) ->
        ReceiveAnswer = test_utils:receive_msg(ClientMsg#client_message{
            seq_num = SeqNum}, ?TIMEOUT),
        ?assertEqual(ok, ReceiveAnswer)
    end, lists:seq(1, MsgCount)),

    ReceiveAnswer = test_utils:receive_msg(
        #server_message{server_message = MsgAck}, ?TIMEOUT),
    ?assertEqual(ok, ReceiveAnswer),

    ?assertEqual({error, timeout}, test_utils:receive_any()),

    ok = rpc:call(Worker, worker_proxy, call, [
        sequencer_worker, {remove_sequencer_manager, FuseId}
    ]),

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