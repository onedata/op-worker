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
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([sequencer_dispatcher_test/1, sequencer_test/1]).

-record(client_message, {message_id, seq_num, last_message, client_message}).

all() -> [sequencer_dispatcher_test, sequencer_test].

%%%===================================================================
%%% Test function
%% ====================================================================

sequencer_dispatcher_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Self = self(),
    FuseId1 = <<"fuse_id_1">>,
    FuseId2 = <<"fuse_id_2">>,

    [SeqMan1, SeqMan2] = lists:map(fun(FuseId) ->
        CreateOrGetSeqManAnswers = utils:pmap(fun(Worker) ->
            rpc:call(Worker, sequencer_dispatcher,
                create_or_get_sequencer_manager, [FuseId, Self])
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

        RemoveSeqManAnswer1 = rpc:call(Worker1, sequencer_dispatcher,
            remove_sequencer_manager, [FuseId]),
        ?assertMatch(ok, RemoveSeqManAnswer1),
        RemoveSeqManAnswer2 = rpc:call(Worker1, sequencer_dispatcher,
            remove_sequencer_manager, [FuseId]),
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
        ClientMsg = #client_message{message_id = 1, last_message = false},

        {ok, SeqMan} = sequencer_dispatcher:create_or_get_sequencer_manager(FuseId, Self),
        ok = meck:new(router, [non_strict]),
        ok = meck:expect(router, route, fun(Msg) -> Self ! Msg end),

        lists:foreach(fun(SeqNum) ->
            gen_server:cast(SeqMan, ClientMsg#client_message{seq_num = SeqNum})
        end, utils:random_shuffle(lists:seq(1, 100))),

        lists:foreach(fun(SeqNum) ->
            Msg = ClientMsg#client_message{seq_num = SeqNum},
            ReceiveAnswer = receive
                                Msg -> ok
                            after
                                timer:seconds(1) -> {error, {timeout, SeqNum}}
                            end,
            ?assertEqual(ok, ReceiveAnswer)
        end, lists:seq(1, 100)),

        ReceiveAnswer = receive
                            ack -> ok
                        after
                            timer:seconds(1) -> {error, timeout}
                        end,
        ?assertEqual(ok, ReceiveAnswer),

        true = meck:validate(router),
        ok = sequencer_dispatcher:remove_sequencer_manager(FuseId)
    end, []]),

    ?assertEqual(ok, Answer),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TRY_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

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