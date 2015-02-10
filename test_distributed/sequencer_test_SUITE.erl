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

-include("test_utils.hrl").
-include("registered_names.hrl").
-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/server_messages.hrl").
-include("proto_internal/oneclient/communication_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([sequencer_worker_test/1, sequencer_test/1]).

all() -> [sequencer_worker_test, sequencer_test].

%%%===================================================================
%%% Test function
%% ====================================================================

sequencer_worker_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Self = self(),
    FuseId1 = <<"fuse_id_1">>,
    FuseId2 = <<"fuse_id_2">>,

    [SeqMan1, SeqMan2] = lists:map(fun(FuseId) ->
        CreateOrGetSeqManAnswers = utils:pmap(fun(Worker) ->
            rpc:call(Worker, worker_proxy, call, [sequencer_worker,
                {get_or_create_sequencer_manager, FuseId, Self}])
        end, lists:duplicate(2, Worker1) ++ lists:duplicate(2, Worker2)),

        lists:foreach(fun(CreateOrGetSeqManAnswer) ->
            ?assertMatch({ok, _}, CreateOrGetSeqManAnswer)
        end, CreateOrGetSeqManAnswers),

        {_, [FirstSeqMan | SeqMans]} = lists:unzip(CreateOrGetSeqManAnswers),

        lists:foreach(fun(SeqMan) ->
            ?assertEqual(FirstSeqMan, SeqMan)
        end, SeqMans),

        FirstSeqMan
    end, [FuseId1, FuseId2]),

    ?assertNotEqual(SeqMan1, SeqMan2),

    utils:pforeach(fun(FuseId) ->
        ProcessesBeforeRemoval = processes([Worker1, Worker2]),
        ?assertMatch([_], lists:filter(fun(P) ->
            P =:= SeqMan1
        end, ProcessesBeforeRemoval)),

        RemoveSeqManAnswer1 = rpc:call(Worker1, worker_proxy, call, [
            sequencer_worker, {remove_sequencer_manager, FuseId}]),
        ?assertMatch(ok, RemoveSeqManAnswer1),
        RemoveSeqManAnswer2 = rpc:call(Worker1, worker_proxy, call, [
            sequencer_worker, {remove_sequencer_manager, FuseId}]),
        ?assertMatch({error, _}, RemoveSeqManAnswer2),

        ProcessesAfterRemoval = processes([Worker1, Worker2]),
        ?assertMatch([], lists:filter(fun(P) ->
            P =:= SeqMan1
        end, ProcessesAfterRemoval))
    end, [FuseId1, FuseId2]),

    ok.

sequencer_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),

    Answer = rpc:call(Worker, erlang, apply, [fun() ->
        Self = self(),
        FuseId = <<"fuse_id">>,
        MsgId = 1,
        MsgCount = 100,
        ClientMsg = #client_message{message_id = MsgId, last_message = false},
        MsgReq = #message_request{message_id = MsgId},
        MsgAck = #message_acknowledgement{message_id = MsgId, seq_num = MsgCount},

        {ok, SeqMan} = worker_proxy:call(sequencer_worker,
            {get_or_create_sequencer_manager, FuseId, Self}),
        ok = meck:new(router, []),
        ok = meck:expect(router, route_message, fun(Msg) -> Self ! Msg end),
        ok = meck:new(protocol_handler, []),
        ok = meck:expect(protocol_handler, cast, fun(Connection, Msg) ->
            Connection ! Msg
        end),

        lists:foreach(fun(SeqNum) ->
            gen_server:cast(SeqMan, ClientMsg#client_message{seq_num = SeqNum})
        end, lists:seq(MsgCount, 1, -1)),

        lists:foreach(fun(SeqNum) ->
            ReceiveMsg = receive_msg(#server_message{
                server_message = MsgReq#message_request{
                    lower_seq_num = 1, upper_seq_num = SeqNum - 1
                }
            }),
            ?assertEqual(ok, ReceiveMsg)
        end, lists:seq(MsgCount, 2, -1)),

        lists:foreach(fun(SeqNum) ->
            ReceiveMsg = receive_msg(ClientMsg#client_message{seq_num = SeqNum}),
            ?assertEqual(ok, ReceiveMsg)
        end, lists:seq(1, MsgCount)),

        ReceiveMsg = receive_msg(#server_message{server_message = MsgAck}),
        ?assertEqual(ok, ReceiveMsg),

        true = meck:validate(router),
        true = meck:validate(protocol_handler),
        ok = worker_proxy:call(sequencer_worker, {remove_sequencer_manager, FuseId})
    end, []]),

    ?assertEqual(ok, Answer),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    try
        test_node_starter:prepare_test_environment(Config,
            ?TEST_FILE(Config, "env_desc.json"), ?MODULE)
    catch
        A:B -> ct:print("~p:~p~n~p", [A, B, erlang:get_stacktrace()])
    end.

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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Waits for given message or returns timeout.
%% @end
%%--------------------------------------------------------------------
-spec receive_msg(Msg :: term()) -> ok | {error, {timeout, Msg :: term()}}.
receive_msg(Msg) ->
    receive
        Msg -> ok
    after
        timer:seconds(1) -> {error, {timeout, Msg}}
    end.