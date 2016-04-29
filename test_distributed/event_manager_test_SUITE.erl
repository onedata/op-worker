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

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/events/definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

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

all() ->
    ?ALL([
        event_manager_should_update_session_on_init,
        event_manager_should_update_session_on_terminate,
        event_manager_should_start_event_streams_on_init,
        event_manager_should_register_event_stream,
        event_manager_should_unregister_event_stream,
        event_manager_should_forward_events_to_event_streams,
        event_manager_should_start_event_stream_on_subscription,
        event_manager_should_terminate_event_stream_on_subscription_cancellation
    ]).

-define(TIMEOUT, timer:seconds(15)).

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
    ?assertEqual({error, {not_found, missing}}, rpc:call(
        Worker, session, get_event_manager, [?config(session_id, Config)]
    ), 10).

event_manager_should_start_event_streams_on_init(_) ->
    ?assertReceivedMatch({start_event_stream, #subscription{id = 1}}, ?TIMEOUT),
    ?assertReceivedMatch({start_event_stream, #subscription{id = 2}}, ?TIMEOUT).

event_manager_should_register_event_stream(Config) ->
    EvtMan = ?config(event_manager, Config),
    gen_server:cast(EvtMan, {register_stream, 1, self()}),
    gen_server:cast(EvtMan, #event{}),
    ?assertReceivedMatch({'$gen_cast', #event{}}, ?TIMEOUT).

event_manager_should_unregister_event_stream(Config) ->
    EvtMan = ?config(event_manager, Config),
    gen_server:cast(EvtMan, {unregister_stream, 1}),
    gen_server:cast(EvtMan, {unregister_stream, 2}),
    gen_server:cast(EvtMan, #event{}),
    ?assertNotReceivedMatch({'$gen_cast', #event{}}, ?TIMEOUT).

event_manager_should_forward_events_to_event_streams(Config) ->
    EvtMan = ?config(event_manager, Config),
    gen_server:cast(EvtMan, #event{}),
    ?assertReceivedMatch({'$gen_cast', #event{}}, ?TIMEOUT),
    ?assertReceivedMatch({'$gen_cast', #event{}}, ?TIMEOUT).

event_manager_should_start_event_stream_on_subscription(Config) ->
    EvtMan = ?config(event_manager, Config),
    gen_server:cast(EvtMan, #subscription{id = 1}),
    ?assertReceivedMatch({start_event_stream, #subscription{id = 1}}, ?TIMEOUT).

event_manager_should_terminate_event_stream_on_subscription_cancellation(Config) ->
    EvtMan = ?config(event_manager, Config),
    gen_server:cast(EvtMan, #subscription_cancellation{id = 1}),
    ?assertReceivedMatch({'DOWN', _, _, _, _}, ?TIMEOUT).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(event_manager_should_start_event_stream_on_subscription, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    mock_event_stream_sup(Worker),
    init_per_testcase(default, Config);

init_per_testcase(Case, Config) when
    Case =:= event_manager_should_start_event_streams_on_init;
    Case =:= event_manager_should_unregister_event_stream;
    Case =:= event_manager_should_forward_events_to_event_streams;
    Case =:= event_manager_should_terminate_event_stream_on_subscription_cancellation ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, SessId} = session_setup(Worker),
    initializer:remove_pending_messages(),
    mock_subscription(Worker),
    mock_event_stream_sup(Worker),
    mock_event_manager_sup(Worker),
    {ok, EvtMan} = start_event_manager(Worker, SessId),
    [{event_manager, EvtMan}, {session_id, SessId} | Config];

init_per_testcase(_, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, SessId} = session_setup(Worker),
    initializer:remove_pending_messages(),
    mock_subscription(Worker, []),
    mock_event_manager_sup(Worker),
    {ok, EvtMan} = start_event_manager(Worker, SessId),
    [{event_manager, EvtMan}, {session_id, SessId} | Config].

end_per_testcase(event_manager_should_start_event_stream_on_subscription, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    end_per_testcase(default, Config),
    test_utils:mock_validate_and_unload(Worker, event_stream_sup);

end_per_testcase(Case, Config) when
    Case =:= event_manager_should_start_event_streams_on_init;
    Case =:= event_manager_should_unregister_event_stream;
    Case =:= event_manager_should_forward_events_to_event_streams;
    Case =:= event_manager_should_terminate_event_stream_on_subscription_cancellation ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    end_per_testcase(default, Config),
    test_utils:mock_validate_and_unload(Worker, event_stream_sup);

end_per_testcase(_, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    stop_event_manager(?config(event_manager, Config)),
    test_utils:mock_validate_and_unload(Worker, [event_manager_sup, subscription]),
    session_teardown(Worker, ?config(session_id, Config)).

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
-spec session_setup(Worker :: node()) -> {ok, SessId :: session:id()}.
session_setup(Worker) ->
    ?assertMatch({ok, _}, rpc:call(Worker, session, create, [#document{
        key = <<"session_id">>, value = #session{identity = #identity{}}
    }])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes session document from datastore.
%% @end
%%--------------------------------------------------------------------
-spec session_teardown(Worker :: node(), SessId :: session:id()) -> ok.
session_teardown(Worker, SessId) ->
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
    mock_subscription(Worker, [
        #document{key = 1, value = #subscription{id = 1}},
        #document{key = 2, value = #subscription{id = 2}}
    ]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks subscription model, so that it returns custom list of subscriptions.
%% @end
%%--------------------------------------------------------------------
-spec mock_subscription(Worker :: node(), Subs :: [#document{}]) -> ok.
mock_subscription(Worker, Docs) ->
    test_utils:mock_new(Worker, [subscription]),
    test_utils:mock_expect(Worker, subscription, list, fun
        () -> {ok, Docs}
    end).