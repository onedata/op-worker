%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains event manager tests.
%%% @end
%%%-------------------------------------------------------------------
-module(event_manager_test_SUITE).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore.hrl").
-include("modules/events/definitions.hrl").
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
    event_manager_should_update_session_on_init/1,
    event_manager_should_update_session_on_terminate/1,
    event_manager_should_start_event_streams_on_init/1,
    event_manager_should_register_event_stream/1,
    event_manager_should_unregister_event_stream/1,
    event_manager_should_forward_events_to_event_streams/1,
    event_manager_should_start_event_stream_on_subscription/1,
    event_manager_should_terminate_event_stream_on_subscription_cancellation/1
]).

-performance({test_cases, []}).
all() -> [
    event_manager_should_update_session_on_init,
    event_manager_should_update_session_on_terminate,
    event_manager_should_start_event_streams_on_init,
    event_manager_should_register_event_stream,
    event_manager_should_unregister_event_stream,
    event_manager_should_forward_events_to_event_streams,
    event_manager_should_start_event_stream_on_subscription,
    event_manager_should_terminate_event_stream_on_subscription_cancellation
].

-define(TIMEOUT, timer:seconds(5)).

%%%===================================================================
%%% Test functions
%%%===================================================================

event_manager_should_update_session_on_init(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertMatch({ok, _}, rpc:call(Worker, session, get_event_manager,
        [?config(session_id, Config)])).

event_manager_should_update_session_on_terminate(Config) ->
    stop_event_manager(?config(event_manager, Config)),
    [Worker | _] = ?config(op_worker_nodes, Config),
    repeat(fun() -> rpc:call(
        Worker, session, get_event_manager, [?config(session_id, Config)]
    ) end, {error, {not_found, missing}}, 10, 500).

event_manager_should_start_event_streams_on_init(_) ->
    ?assertReceived({start_event_stream, #subscription{id = 1}}, ?TIMEOUT),
    ?assertReceived({start_event_stream, #subscription{id = 2}}, ?TIMEOUT).

event_manager_should_register_event_stream(Config) ->
    EvtMan = ?config(event_manager, Config),
    gen_server:cast(EvtMan, {register_stream, 1, self()}),
    gen_server:cast(EvtMan, #event{}),
    ?assertReceived({'$gen_cast', #event{}}, ?TIMEOUT).

event_manager_should_unregister_event_stream(Config) ->
    EvtMan = ?config(event_manager, Config),
    gen_server:cast(EvtMan, {unregister_stream, 1}),
    gen_server:cast(EvtMan, {unregister_stream, 2}),
    gen_server:cast(EvtMan, #event{}),
    ?assertNotReceived({'$gen_cast', #event{}}, ?TIMEOUT).

event_manager_should_forward_events_to_event_streams(Config) ->
    EvtMan = ?config(event_manager, Config),
    gen_server:cast(EvtMan, #event{}),
    ?assertReceived({'$gen_cast', #event{}}, ?TIMEOUT),
    ?assertReceived({'$gen_cast', #event{}}, ?TIMEOUT).

event_manager_should_start_event_stream_on_subscription(Config) ->
    EvtMan = ?config(event_manager, Config),
    gen_server:cast(EvtMan, #subscription{id = 1}),
    ?assertReceived({start_event_stream, #subscription{id = 1}}, ?TIMEOUT).

event_manager_should_terminate_event_stream_on_subscription_cancellation(Config) ->
    EvtMan = ?config(event_manager, Config),
    gen_server:cast(EvtMan, #subscription_cancellation{id = 1}),
    ?assertReceived({'DOWN', _, _, _, _}, ?TIMEOUT).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(event_manager_should_start_event_stream_on_subscription, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    mock_event_stream_sup(Worker),
    init_per_testcase(event_manager_should_update_session_on_init, Config);

init_per_testcase(Case, Config) when
    Case =:= event_manager_should_start_event_streams_on_init;
    Case =:= event_manager_should_unregister_event_stream;
    Case =:= event_manager_should_forward_events_to_event_streams;
    Case =:= event_manager_should_terminate_event_stream_on_subscription_cancellation ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    mock_subscription(Worker),
    mock_event_stream_sup(Worker),
    init_per_testcase(event_manager_should_update_session_on_init, Config);

init_per_testcase(_, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, SessId} = create_session(Worker),
    mock_event_manager_sup(Worker),
    {ok, EvtMan} = start_event_manager(Worker, SessId),
    [{event_manager, EvtMan}, {session_id, SessId} | Config].

end_per_testcase(event_manager_should_start_event_stream_on_subscription, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    end_per_testcase(event_manager_should_update_session_on_init, Config),
    validate_and_unload_mocks(Worker, [event_stream_sup]);

end_per_testcase(Case, Config) when
    Case =:= event_manager_should_start_event_streams_on_init;
    Case =:= event_manager_should_unregister_event_stream;
    Case =:= event_manager_should_forward_events_to_event_streams;
    Case =:= event_manager_should_terminate_event_stream_on_subscription_cancellation ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    end_per_testcase(event_manager_should_update_session_on_init, Config),
    validate_and_unload_mocks(Worker, [subscription, event_stream_sup]);

end_per_testcase(_, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    stop_event_manager(?config(event_manager, Config)),
    validate_and_unload_mocks(Worker, [event_manager_sup]),
    remove_session(Worker, ?config(session_id, Config)),
    remove_pending_messages(),
    proplists:delete(session_id, proplists:delete(event_manager, Config)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts event manager for given session.
%% @end
%%--------------------------------------------------------------------
-spec start_event_manager(Worker :: node(), SessId :: session:id()) ->
    {ok, EvtMan :: pid()}.
start_event_manager(Worker, SessId) ->
    EvtManSup = self(),
    ?assertMatch({ok, _}, rpc:call(Worker, gen_server, start, [
        event_manager, [EvtManSup, SessId], []
    ])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops event stream.
%% @end
%%--------------------------------------------------------------------
-spec stop_event_manager(EvtMan :: pid()) -> true.
stop_event_manager(EvtMan) ->
    exit(EvtMan, shutdown).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session document in datastore.
%% @end
%%--------------------------------------------------------------------
-spec create_session(Worker :: node()) -> {ok, SessId :: session:id()}.
create_session(Worker) ->
    ?assertMatch({ok, _}, rpc:call(Worker, session, create, [#document{
        key = <<"session_id">>, value = #session{}
    }])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes session document from datastore.
%% @end
%%--------------------------------------------------------------------
-spec remove_session(Worker :: node(), SessId :: session:id()) -> ok.
remove_session(Worker, SessId) ->
    ?assertEqual(ok, rpc:call(Worker, session, delete, [SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks event manager supervisor, so that it returns this process as event
%% stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec mock_event_manager_sup(Worker :: node()) -> ok.
mock_event_manager_sup(Worker) ->
    EvtStmSup = self(),
    test_utils:mock_new(Worker, [event_manager_sup]),
    test_utils:mock_expect(Worker, event_manager_sup, get_event_stream_sup, fun
        (_) -> {ok, EvtStmSup}
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks event stream supervisor, so that it notifies this process when event
%% stream is started. Moreover, started fake event stream will forward all
%% received messages to this process.
%% @end
%%--------------------------------------------------------------------
-spec mock_event_stream_sup(Worker :: node()) -> ok.
mock_event_stream_sup(Worker) ->
    Self = self(),
    Loop = fun(Fun) -> receive Msg -> Self ! Msg, Fun(Fun) end end,
    {EvtStm, _} = spawn_monitor(fun() -> Loop(Loop) end),
    test_utils:mock_new(Worker, [event_stream_sup]),
    test_utils:mock_expect(Worker, event_stream_sup, start_event_stream, fun
        (_, _, Sub, _) -> Self ! {start_event_stream, Sub}, {ok, EvtStm}
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks subscription model, so that it returns defined list of subscriptions.
%% @end
%%--------------------------------------------------------------------
-spec mock_subscription(Worker :: node()) -> ok.
mock_subscription(Worker) ->
    test_utils:mock_new(Worker, [subscription]),
    test_utils:mock_expect(Worker, subscription, list, fun
        () -> {ok, [#subscription{id = 1}, #subscription{id = 2}]}
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Validates and unloads mocks.
%% @end
%%--------------------------------------------------------------------
-spec validate_and_unload_mocks(Worker :: node(), Mocks :: [atom()]) -> ok.
validate_and_unload_mocks(Worker, Mocks) ->
    test_utils:mock_validate(Worker, Mocks),
    test_utils:mock_unload(Worker, Mocks).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Repeatedly executes provided function until it returns expected value or
%% attempts limit exceeds.
%% @end
%%--------------------------------------------------------------------
-spec repeat(Fun :: fun(), Expected :: term(), Attempts :: non_neg_integer(),
    Delay :: timeout()) -> ok.
repeat(Fun, Expected, 0, _) ->
    ?assertMatch(Expected, Fun());

repeat(Fun, Expected, Attempts, Delay) ->
    case Fun() of
        Expected -> ok;
        _ ->
            timer:sleep(Delay),
            repeat(Fun, Expected, Attempts - 1, Delay)
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