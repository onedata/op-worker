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

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    event_stream_the_same_file_id_aggregation_test/1,
    event_stream_different_file_id_aggregation_test/1,
    event_stream_emission_rule_test/1,
    event_stream_emission_time_test/1,
    event_stream_crash_test/1,
    event_manager_subscription_creation_and_cancellation_test/1,
    event_manager_multiple_subscription_test/1,
    event_manager_multiple_handlers_test/1
]).

-performance({test_cases, []}).
all() -> [
    event_stream_the_same_file_id_aggregation_test,
    event_stream_different_file_id_aggregation_test,
    event_stream_emission_rule_test,
    event_stream_emission_time_test,
    event_stream_crash_test,
    event_manager_subscription_creation_and_cancellation_test,
    event_manager_multiple_subscription_test,
    event_manager_multiple_handlers_test
].

-define(TIMEOUT, timer:seconds(5)).

%%%====================================================================
%%% Test function
%%%====================================================================

%% Check whether events for the same file are properly aggregated.
event_stream_the_same_file_id_aggregation_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    FileId = <<"file_id">>,
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},

    session_setup(Worker, SessId, Iden, Self),

    {ok, SubId} = subscribe(Worker,
        all,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(Meta) -> Meta >= 6 end,
        [fun(Evts) -> Self ! {handler, Evts} end]
    ),

    % Emit events.
    lists:foreach(fun(N) ->
        emit(Worker, #write_event{file_id = FileId, size = 1, counter = 1,
            file_size = N + 1, blocks = [#file_block{offset = N, size = 1}]}, SessId)
    end, lists:seq(0, 5)),

    % Check whether events have been aggregated and handler has been executed.
    ?assertMatch({ok, _}, test_utils:receive_msg({handler, [#write_event{
        file_id = FileId, counter = 6, size = 6, file_size = 6,
        blocks = [#file_block{offset = 0, size = 6}]
    }]}, ?TIMEOUT)),

    unsubscribe(Worker, SubId),
    session_teardown(Worker, SessId),

    ok.

%% Check whether events for different files are properly aggregated.
event_stream_different_file_id_aggregation_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},

    session_setup(Worker, SessId, Iden, Self),

    {ok, SubId} = subscribe(Worker,
        all,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(Meta) -> Meta >= 6 end,
        [fun(Evts) -> Self ! {handler, Evts} end]
    ),

    FileId1 = <<"file_id_1">>,
    FileId2 = <<"file_id_2">>,

    % Emit events for different files.
    lists:foreach(fun(FileId) ->
        lists:foreach(fun(Evt) ->
            emit(Worker, Evt, SessId)
        end, lists:duplicate(3, #write_event{
            file_id = FileId, counter = 1, size = 1, file_size = 1
        }))
    end, [FileId1, FileId2]),

    % Check whether events have been aggregated in terms of the same file ID
    % and handler has been executed.
    ?assertMatch({ok, _}, test_utils:receive_msg({handler, [
        #write_event{file_id = FileId2, counter = 3, size = 3, file_size = 1},
        #write_event{file_id = FileId1, counter = 3, size = 3, file_size = 1}
    ]}, ?TIMEOUT)),

    unsubscribe(Worker, SubId),
    session_teardown(Worker, SessId),

    ok.

%% Check whether event stream executes handlers when emission rule is satisfied.
event_stream_emission_rule_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    FileId = <<"file_id">>,
    SessId = ?config(session_id, Config),

    {ok, SubId} = subscribe(Worker,
        all,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(Meta) -> Meta >= 10 end,
        [fun(Evts) -> Self ! {handler, Evts} end]
    ),

    % Emit events.
    lists:foreach(fun(N) ->
        emit(Worker, #write_event{file_id = FileId, size = 1, counter = 1,
            file_size = N + 1, blocks = [#file_block{offset = N, size = 1}]}, SessId)
    end, lists:seq(0, 99)),

    % Check whether events have been aggregated and handler has been executed
    % when emission rule has been satisfied.
    lists:foreach(fun(N) ->
        ?assertMatch({ok, _}, test_utils:receive_msg({handler, [#write_event{
            file_id = FileId, counter = 10, size = 10, file_size = (N + 1) * 10,
            blocks = [#file_block{offset = N * 10, size = 10}]
        }]}, ?TIMEOUT))
    end, lists:seq(0, 9)),

    unsubscribe(Worker, SubId),

    ok.

%% Check whether event stream executes handlers when emission time expires.
event_stream_emission_time_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Self = self(),
    EmTime = timer:seconds(2),
    EvtsCount = 100,

    {ok, SubId} = subscribe(Worker,
        gui,
        EmTime,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(_) -> false end,
        [fun(Evts) -> Self ! {handler, Evts} end]
    ),

    % Emit events.
    lists:foreach(fun(N) ->
        emit(Worker, #write_event{size = 1, counter = 1, file_size = N + 1,
            blocks = [#file_block{offset = N, size = 1}]}, SessId)
    end, lists:seq(0, EvtsCount - 1)),

    % Check whether event handlers have been executed.
    ?assertMatch({ok, _}, test_utils:receive_msg({handler, [#write_event{
        size = EvtsCount, counter = EvtsCount, file_size = EvtsCount,
        blocks = [#file_block{offset = 0, size = EvtsCount}]}]}, ?TIMEOUT + EmTime)),
    ?assertEqual({error, timeout}, test_utils:receive_any(EmTime)),

    unsubscribe(Worker, SubId),

    ok.

%% Check whether event stream is reinitialized in previous state in case of crash.
event_stream_crash_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Self = self(),
    EvtsCount = 100,
    HalfEvtsCount = round(EvtsCount / 2),

    {ok, SubId} = subscribe(Worker,
        gui,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(Meta) -> Meta >= EvtsCount end,
        [fun(Evts) -> Self ! {handler, Evts} end]
    ),

    % Emit first part of events.
    lists:foreach(fun(N) ->
        emit(Worker, #write_event{size = 1, counter = 1, file_size = N + 1,
            blocks = [#file_block{offset = N, size = 1}]}, SessId)
    end, lists:seq(0, HalfEvtsCount - 1)),

    % Get event stream pid.
    {ok, {SessSup, _}} = rpc:call(Worker, session,
        get_session_supervisor_and_node, [SessId]),
    {ok, EvtManSup} = get_child(SessSup, event_manager_sup),
    {ok, EvtStmSup} = get_child(EvtManSup, event_stream_sup),
    {ok, EvtStm} = get_child(EvtStmSup, undefined),

    % Send crash message and wait for event stream recovery.
    gen_server:cast(EvtStm, kill),
    timer:sleep(?TIMEOUT),

    % Emit second part of events.
    lists:foreach(fun(N) ->
        emit(Worker, #write_event{size = 1, counter = 1, file_size = N + 1,
            blocks = [#file_block{offset = N, size = 1}]}, SessId)
    end, lists:seq(HalfEvtsCount, EvtsCount - 1)),

    % Check whether event handlers have been executed.
    ?assertMatch({ok, _}, test_utils:receive_msg({handler, [#write_event{
        size = EvtsCount, counter = EvtsCount, file_size = EvtsCount,
        blocks = [#file_block{offset = 0, size = EvtsCount}]}]}, ?TIMEOUT)),
    ?assertEqual({error, timeout}, test_utils:receive_any()),

    unsubscribe(Worker, SubId),

    ok.

%% Check whether subscription can be created and cancelled.
event_manager_subscription_creation_and_cancellation_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId1 = <<"session_id_1">>,
    SessId2 = <<"session_id_2">>,
    Iden1 = #identity{user_id = <<"user_id_1">>},
    Iden2 = #identity{user_id = <<"user_id_2">>},

    session_setup(Worker1, SessId1, Iden1, Self),

    {ok, SubId} = subscribe(Worker2,
        all,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(Meta) -> Meta >= 6 end,
        [fun(Evts) -> Self ! {handler, Evts} end]
    ),

    session_setup(Worker2, SessId2, Iden2, Self),

    % Check whether subscription message has been sent to clients.
    ?assertMatch({ok, #write_event_subscription{}}, test_utils:receive_any(?TIMEOUT)),
    ?assertMatch({ok, #write_event_subscription{}}, test_utils:receive_any(?TIMEOUT)),
    ?assertEqual({error, timeout}, test_utils:receive_any()),

    % Check subscription has been added to distributed cache.
    ?assertMatch({ok, [_]}, rpc:call(Worker1, subscription, list, [])),

    % Unsubscribe and check subscription cancellation message has been sent to
    % clients
    unsubscribe(Worker1, SubId),
    ?assertEqual({ok, #event_subscription_cancellation{id = SubId}},
        test_utils:receive_any(?TIMEOUT)),
    ?assertEqual({ok, #event_subscription_cancellation{id = SubId}},
        test_utils:receive_any(?TIMEOUT)),
    ?assertEqual({error, timeout}, test_utils:receive_any()),

    % Check subscription has been removed from distributed cache.
    ?assertEqual({ok, []}, rpc:call(Worker1, subscription, list, [])),

    session_teardown(Worker1, SessId2),
    session_teardown(Worker2, SessId1),

    ok.

%% Check whether multiple subscriptions are properly processed.
event_manager_multiple_subscription_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Self = self(),
    SubsCount = 10,
    EvtsCount = 100,

    % Create subscriptions for events associated with different files.
    {SubIds, FileIds} = lists:unzip(lists:map(fun(N) ->
        FileId = <<"file_id_", (integer_to_binary(N))/binary>>,
        {ok, SubId} = subscribe(Worker,
            gui,
            fun(#write_event{file_id = Id}) -> Id =:= FileId; (_) -> false end,
            fun(Meta) -> Meta >= EvtsCount end,
            [fun(Evts) -> Self ! {handler, Evts} end]
        ),
        {SubId, FileId}
    end, lists:seq(1, SubsCount))),

    % Emit events.
    utils:pforeach(fun(FileId) ->
        lists:foreach(fun(N) ->
            emit(Worker, #write_event{file_id = FileId, size = 1, counter = 1,
                file_size = N + 1, blocks = [#file_block{offset = N, size = 1}]},
                SessId)
        end, lists:seq(0, EvtsCount - 1))
    end, FileIds),

    % Check whether event handlers have been executed.
    lists:foreach(fun(FileId) ->
        ?assertMatch({ok, _}, test_utils:receive_msg({handler, [#write_event{
            file_id = FileId, size = EvtsCount, counter = EvtsCount,
            file_size = EvtsCount, blocks = [#file_block{
                offset = 0, size = EvtsCount
            }]
        }]}, ?TIMEOUT))
    end, FileIds),
    ?assertEqual({error, timeout}, test_utils:receive_any()),

    lists:foreach(fun(SubId) ->
        unsubscribe(Worker, SubId)
    end, SubIds),

    ok.

%% Check whether multiple handlers are executed in terms of one event stream.
event_manager_multiple_handlers_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    FileId = <<"file_id">>,
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},

    session_setup(Worker, SessId, Iden, Self),

    {ok, SubId} = subscribe(Worker,
        all,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(Meta) -> Meta >= 10 end,
        [
            fun(Evts) -> Self ! {handler1, Evts} end,
            fun(Evts) -> Self ! {handler2, Evts} end,
            fun(Evts) -> Self ! {handler3, Evts} end
        ]
    ),

    % Emit events.
    lists:foreach(fun(N) ->
        emit(Worker, #write_event{file_id = FileId, size = 1, counter = 1,
            file_size = N + 1, blocks = [#file_block{offset = N, size = 1}]}, SessId)
    end, lists:seq(0, 9)),

    % Check whether events have been aggregated and each handler has been executed.
    lists:foreach(fun(Handler) ->
        ?assertMatch({ok, _}, test_utils:receive_msg({Handler, [#write_event{
            file_id = FileId, counter = 10, size = 10, file_size = 10,
            blocks = [#file_block{offset = 0, size = 10}]
        }]}, ?TIMEOUT))
    end, [handler1, handler2, handler3]),

    unsubscribe(Worker, SubId),
    session_teardown(Worker, SessId),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(event_stream_crash_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},
    test_utils:mock_new(Worker, [communicator, logger]),
    test_utils:mock_expect(Worker, communicator, send, fun
        (_, _) -> ok
    end),
    test_utils:mock_expect(Worker, logger, dispatch_log, fun
        (_, _, _, [_, _, kill], _) -> meck:exception(throw, crash);
        (A, B, C, D, E) -> meck:passthrough([A, B, C, D, E])
    end),
    session_setup(Worker, SessId, Iden, Self),
    [{session_id, SessId} | Config];

init_per_testcase(Case, Config) when
    Case =:= event_manager_subscription_creation_and_cancellation_test;
    Case =:= event_stream_the_same_file_id_aggregation_test;
    Case =:= event_stream_different_file_id_aggregation_test;
    Case =:= event_manager_multiple_handlers_test ->
    Self = self(),
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, communicator),
    test_utils:mock_expect(Workers, communicator, send, fun
        (#write_event_subscription{} = Msg, _) -> Self ! Msg, ok;
        (#event_subscription_cancellation{} = Msg, _) -> Self ! Msg, ok;
        (_, _) -> ok
    end),
    Config;

init_per_testcase(Case, Config) when
    Case =:= event_stream_emission_rule_test;
    Case =:= event_stream_emission_time_test;
    Case =:= event_manager_multiple_subscription_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},
    test_utils:mock_new(Worker, communicator),
    test_utils:mock_expect(Worker, communicator, send, fun
        (_, _) -> ok
    end),
    session_setup(Worker, SessId, Iden, Self),
    [{session_id, SessId} | Config].

end_per_testcase(event_stream_crash_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    remove_pending_messages(),
    session_teardown(Worker, SessId),
    test_utils:mock_validate(Worker, [communicator, logger]),
    test_utils:mock_unload(Worker, [communicator, logger]),
    proplists:delete(session_id, Config);

end_per_testcase(Case, Config) when
    Case =:= event_manager_subscription_creation_and_cancellation_test;
    Case =:= event_stream_the_same_file_id_aggregation_test;
    Case =:= event_stream_different_file_id_aggregation_test;
    Case =:= event_manager_multiple_handlers_test ->
    Workers = ?config(op_worker_nodes, Config),
    remove_pending_messages(),
    test_utils:mock_validate(Workers, communicator),
    test_utils:mock_unload(Workers, communicator),
    Config;

end_per_testcase(Case, Config) when
    Case =:= event_stream_emission_rule_test;
    Case =:= event_stream_emission_time_test;
    Case =:= event_manager_multiple_subscription_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    remove_pending_messages(),
    session_teardown(Worker, SessId),
    test_utils:mock_validate(Worker, communicator),
    test_utils:mock_unload(Worker, communicator),
    proplists:delete(session_id, Config).

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
    Iden :: session:identity(), Con :: pid()) -> ok.
session_setup(Worker, SessId, Iden, Con) ->
    ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, Iden, Con])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Remove existing test session.
%% @end
%%--------------------------------------------------------------------
-spec session_teardown(Worker :: node(), SessId :: session:id()) -> ok.
session_teardown(Worker, SessId) ->
    ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Emits an event.
%% @end
%%--------------------------------------------------------------------
-spec emit(Worker :: node(), Evt :: event_manager:event(), SessId :: session:id()) ->
    ok.
emit(Worker, Evt, SessId) ->
    ?assertEqual(ok, rpc:call(Worker, event_manager, emit, [Evt, SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv subscribe(Worker, Producer, infinity, AdmRule, EmRule, Handlers)
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), Producer :: event_manager:producer(),
    AdmRule :: event_stream:admission_rule(),
    EmRule :: event_stream:emission_rule(),
    Handlers :: [event_stream:event_handler()]) ->
    {ok, SubId :: event_manager:subscription_id()}.
subscribe(Worker, Producer, AdmRule, EmRule, Handlers) ->
    subscribe(Worker, Producer, infinity, AdmRule, EmRule, Handlers).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates event subscription.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), Producer :: event_manager:producer(),
    EmTime :: timeout(), AdmRule :: event_stream:admission_rule(),
    EmRule :: event_stream:emission_rule(),
    Handlers :: [event_stream:event_handler()]) ->
    {ok, SubId :: event_manager:subscription_id()}.
subscribe(Worker, Producer, EmTime, AdmRule, EmRule, Handlers) ->
    Sub = #write_event_subscription{
        producer = Producer,
        event_stream = ?WRITE_EVENT_STREAM#event_stream{
            metadata = 0,
            admission_rule = AdmRule,
            emission_rule = EmRule,
            emission_time = EmTime,
            handlers = Handlers
        }
    },
    SubAnswer = rpc:call(Worker, event_manager, subscribe, [Sub]),
    ?assertMatch({ok, _}, SubAnswer),
    SubAnswer.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes event subscription.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(Worker :: node(), SubId :: event_manager:subscription_id()) ->
    ok.
unsubscribe(Worker, SubId) ->
    ?assertEqual(ok, rpc:call(Worker, event_manager, unsubscribe, [SubId])).

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