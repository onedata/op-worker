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
-include("modules/datastore/datastore_models.hrl").
-include("modules/events/definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    event_manager_should_update_session_on_init/1,
    event_manager_should_update_session_on_terminate/1,
    event_manager_should_start_streams_on_init/1,
    event_manager_should_register_event_stream/1,
    event_manager_should_unregister_event_stream/1,
    event_manager_should_forward_events_to_event_streams/1,
    event_manager_should_start_stream_on_subscription/1,
    event_manager_should_terminate_event_stream_on_subscription_cancellation/1
]).

all() ->
    ?ALL([
        event_manager_should_update_session_on_init,
        event_manager_should_update_session_on_terminate,
        event_manager_should_start_streams_on_init,
        event_manager_should_register_event_stream,
        event_manager_should_unregister_event_stream,
        event_manager_should_forward_events_to_event_streams,
        event_manager_should_start_stream_on_subscription,
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
    ?assertEqual({error, not_found}, rpc:call(
        Worker, session, get_event_manager, [?config(session_id, Config)]
    ), 10).

event_manager_should_start_streams_on_init(_) ->
    ?assertReceivedMatch({start_stream, #subscription{type = stream_1}}, ?TIMEOUT),
    ?assertReceivedMatch({start_stream, #subscription{type = stream_2}}, ?TIMEOUT).

event_manager_should_register_event_stream(Config) ->
    Mgr = ?config(event_manager, Config),
    gen_server:cast(Mgr, {register_stream, stream_1, self()}),
    gen_server:cast(Mgr, #event{type = stream_1}),
    ?assertReceivedMatch({'$gen_cast', #event{}}, ?TIMEOUT).

event_manager_should_unregister_event_stream(Config) ->
    Mgr = ?config(event_manager, Config),
    gen_server:cast(Mgr, {unregister_stream, stream_1}),
    gen_server:cast(Mgr, {unregister_stream, stream_2}),
    gen_server:cast(Mgr, #event{type = stream_1}),
    ?assertNotReceivedMatch({'$gen_cast', #event{}}, ?TIMEOUT).

event_manager_should_forward_events_to_event_streams(Config) ->
    Mgr = ?config(event_manager, Config),
    gen_server:cast(Mgr, #event{type = stream_1}),
    gen_server:cast(Mgr, #event{type = stream_2}),
    ?assertReceivedMatch({'$gen_cast', #event{}}, ?TIMEOUT),
    ?assertReceivedMatch({'$gen_cast', #event{}}, ?TIMEOUT),
    ?assertNotReceivedMatch({'$gen_cast', #event{}}, ?TIMEOUT).

event_manager_should_start_stream_on_subscription(Config) ->
    Mgr = ?config(event_manager, Config),
    gen_server:cast(Mgr, #subscription{type = test}),
    ?assertReceivedMatch({start_stream, #subscription{type = test}}, ?TIMEOUT).

event_manager_should_terminate_event_stream_on_subscription_cancellation(Config) ->
    Mgr = ?config(event_manager, Config),
    gen_server:cast(Mgr, #subscription_cancellation{id = 1}),
    ?assertReceivedMatch({'DOWN', _, _, _, _}, ?TIMEOUT).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer]} | Config].


init_per_testcase(event_manager_should_start_stream_on_subscription = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    mock_event_stream_sup(Worker),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case, Config) when
    Case =:= event_manager_should_start_streams_on_init;
    Case =:= event_manager_should_unregister_event_stream;
    Case =:= event_manager_should_forward_events_to_event_streams;
    Case =:= event_manager_should_terminate_event_stream_on_subscription_cancellation ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, SessId} = session_setup(Worker),
    initializer:remove_pending_messages(),
    mock_subscription(Worker),
    mock_types(Worker),
    mock_event_stream_sup(Worker),
    mock_event_manager_sup(Worker),
    {ok, Mgr} = start_event_manager(Worker, SessId),
    [{event_manager, Mgr}, {session_id, SessId} | Config];

init_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, SessId} = session_setup(Worker),
    initializer:remove_pending_messages(),
    mock_subscription(Worker, []),
    mock_types(Worker),
    mock_event_manager_sup(Worker),
    {ok, Mgr} = start_event_manager(Worker, SessId),
    [{event_manager, Mgr}, {session_id, SessId} | Config].

end_per_testcase(event_manager_should_start_stream_on_subscription = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config),
    test_utils:mock_validate_and_unload(Worker, event_stream_sup);

end_per_testcase(Case, Config) when
    Case =:= event_manager_should_start_streams_on_init;
    Case =:= event_manager_should_unregister_event_stream;
    Case =:= event_manager_should_forward_events_to_event_streams;
    Case =:= event_manager_should_terminate_event_stream_on_subscription_cancellation ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config),
    test_utils:mock_validate_and_unload(Worker, event_stream_sup);

end_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    stop_event_manager(?config(event_manager, Config)),
    test_utils:mock_validate_and_unload(Worker,
        [event_manager_sup, subscription, event_type, subscription_type]),
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
    {ok, Mgr :: pid()}.
start_event_manager(Worker, SessId) ->
    MgrSup = self(),
    ?assertMatch({ok, _}, rpc:call(Worker, gen_server, start, [
        event_manager, [MgrSup, SessId], []
    ])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops event stream.
%% @end
%%--------------------------------------------------------------------
-spec stop_event_manager(Mgr :: pid()) -> true.
stop_event_manager(Mgr) ->
    exit(Mgr, shutdown).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session document in datastore.
%% @end
%%--------------------------------------------------------------------
-spec session_setup(Worker :: node()) -> {ok, SessId :: session:id()}.
session_setup(Worker) ->
    ?assertMatch({ok, _}, rpc:call(Worker, session, create, [#document{
        key = <<"session_id">>, value = #session{identity = #user_identity{}}
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
    StmsSup = self(),
    test_utils:mock_new(Worker, [event_manager_sup]),
    test_utils:mock_expect(Worker, event_manager_sup, get_event_stream_sup, fun
        (_) -> {ok, StmsSup}
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
    Loop = fun Fun() ->
        receive
            {'$gen_cast', {remove_subscription, _}} -> ok;
            Msg -> Self ! Msg, Fun()
        end
    end,
    {Stm, _} = spawn_monitor(Loop),
    test_utils:mock_new(Worker, [event_stream_sup]),
    test_utils:mock_expect(Worker, event_stream_sup, start_stream, fun
        (_, _, Sub, _) -> Self ! {start_stream, Sub}, {ok, Stm}
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
        #document{key = 1, value = #subscription{id = 1, type = stream_1}},
        #document{key = 2, value = #subscription{id = 2, type = stream_2}}
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks event type, so that it returns expected stream key.
%% @end
%%--------------------------------------------------------------------
-spec mock_types(Worker :: node()) -> ok.
mock_types(Worker) ->
    test_utils:mock_new(Worker, [event_type, subscription_type]),
    test_utils:mock_expect(Worker, event_type, get_stream_key, fun
        (#event{type = Type}) -> Type
    end),
    test_utils:mock_expect(Worker, subscription_type, get_stream_key, fun
        (#subscription{type = Type}) -> Type
    end).