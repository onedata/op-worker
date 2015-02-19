%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of event manager API.
%%% @end
%%%-------------------------------------------------------------------
-module(event_manager_test_SUITE).
-author("Krzysztof Trzepla").

-include("workers/datastore/models/session.hrl").
-include("workers/session/event_manager/read_event.hrl").
-include("workers/session/event_manager/write_event.hrl").
-include("workers/session/event_manager/event_stream.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).

%% tests
-export([event_manager_subscription_and_emission_test/1]).

all() -> [event_manager_subscription_and_emission_test].

-define(TIMEOUT, timer:seconds(5)).

%%%====================================================================
%%% Test function
%%%====================================================================

event_manager_subscription_and_emission_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Self = self(),
    SessId1 = <<"session_id_1">>,
    SessId2 = <<"session_id_2">>,
    Cred1 = #credentials{user_id = <<"user_id_1">>},
    Cred2 = #credentials{user_id = <<"user_id_2">>},

    Sub = #write_event_subscription{
        producer = gui,
        event_stream = #event_stream{
            metadata = 0,
            admission_rule = fun(#write_event{}) -> true; (_) -> false end,
            aggregation_rule = fun
                (#write_event{file_id = FileId} = Evt1, #write_event{file_id = FileId} = Evt2) ->
                    {ok, #write_event{
                        file_id = Evt1#write_event.file_id,
                        counter = Evt1#write_event.counter + Evt2#write_event.counter,
                        size = Evt1#write_event.size + Evt2#write_event.size,
                        file_size = Evt2#write_event.file_size,
                        blocks = Evt1#write_event.blocks ++ Evt2#write_event.blocks
                    }};
                (_, _) -> {error, different}
            end,
            transition_rule = fun(Meta, #write_event{counter = Counter}) ->
                Meta + Counter
            end,
            emission_rule = fun(Meta) -> Meta >= 6 end,
            handlers = [fun(Evts) -> Self ! {handler, Evts} end]
        }
    },

    ?assertEqual({ok, created}, rpc:call(Worker1, session_manager,
        reuse_or_create_session, [SessId1, Cred1, Self])),

    SubAnswer = rpc:call(Worker2, event_manager, subscribe, [Sub]),
    ?assertMatch({ok, _}, SubAnswer),
    {ok, SubId} = SubAnswer,

    ?assertEqual({ok, created}, rpc:call(Worker2, session_manager,
        reuse_or_create_session, [SessId2, Cred2, Self])),

    lists:foldl(fun(Evt, N) ->
        EmitAns = rpc:call(Worker1, event_manager, emit, [Evt#write_event{blocks = [N]}, SessId1]),
        ?assertEqual(ok, EmitAns),
        N + 1
    end, 1, lists:duplicate(6, #write_event{counter = 1, size = 1, file_size = 1})),

    ?assertMatch({ok, _}, test_utils:receive_msg({handler, [#write_event{counter = 6,
        size = 6, file_size = 1, blocks = [1, 2, 3, 4, 5, 6]}]}, ?TIMEOUT)),
    ?assertMatch({error, timeout}, test_utils:receive_any()),

    FileId1 = <<"file_id_1">>,
    FileId2 = <<"file_id_2">>,

    lists:foreach(fun(FileId) ->
        lists:foreach(fun(Evt) ->
            EmitAns = rpc:call(Worker1, event_manager, emit, [Evt, SessId2]),
            ?assertEqual(ok, EmitAns)
        end, lists:duplicate(3, #write_event{file_id = FileId, counter = 1, size = 1, file_size = 1}))
    end, [FileId1, FileId2]),

    ?assertMatch({ok, _}, test_utils:receive_msg({handler, [
        #write_event{file_id = FileId2, counter = 3, size = 3, file_size = 1},
        #write_event{file_id = FileId1, counter = 3, size = 3, file_size = 1}
    ]}, ?TIMEOUT)),
    ?assertMatch({error, timeout}, test_utils:receive_any()),

    UnsubAnswer = rpc:call(Worker1, event_manager, unsubscribe, [SubId]),
    ?assertEqual(ok, UnsubAnswer),

    ?assertEqual(ok, rpc:call(Worker1, session_manager, remove_session, [SessId2])),
    ?assertEqual(ok, rpc:call(Worker2, session_manager, remove_session, [SessId1])),

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
