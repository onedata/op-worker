%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains event stream tests.
%%% @end
%%%-------------------------------------------------------------------
-module(session_watcher_test_SUITE).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    incoming_session_watcher_should_not_remove_session_with_connections/1,
    incoming_session_watcher_should_remove_session_without_connections/1,
    incoming_session_watcher_should_remove_inactive_session/1,
    incoming_session_watcher_should_remove_session_on_error/1,
    incoming_session_watcher_should_retry_session_removal/1,
    session_create_or_reuse_session_should_update_session_access_time/1,
    session_update_should_update_session_access_time/1,
    session_save_should_update_session_access_time/1,
    session_create_should_set_session_access_time/1
]).

all() ->
    ?ALL([
        incoming_session_watcher_should_not_remove_session_with_connections,
        incoming_session_watcher_should_remove_session_without_connections,
        incoming_session_watcher_should_remove_inactive_session,
        incoming_session_watcher_should_remove_session_on_error,
        incoming_session_watcher_should_retry_session_removal,
        session_create_or_reuse_session_should_update_session_access_time,
        session_update_should_update_session_access_time,
        session_save_should_update_session_access_time,
        session_create_should_set_session_access_time
    ]).

-define(TIMEOUT, timer:seconds(20)).

-define(call(N, F, A), ?call(N, session, F, A)).
-define(call(N, M, F, A), rpc:call(N, M, F, A)).

%%%===================================================================
%%% Test functions
%%%===================================================================

incoming_session_watcher_should_not_remove_session_with_connections(_Config) ->
    ?assertNotReceivedMatch({remove_session, _}, ?TIMEOUT).

incoming_session_watcher_should_remove_session_without_connections(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Self = self(),
    ?call(Worker, session_connections, deregister, [SessId, Self]),
    ?assertReceivedMatch({remove_session, _}, ?TIMEOUT).

incoming_session_watcher_should_remove_inactive_session(Config) ->
    set_session_status(Config, inactive),
    ?assertReceivedMatch({remove_session, _}, ?TIMEOUT).

incoming_session_watcher_should_remove_session_on_error(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    ?assertEqual(ok, ?call(Worker, delete, [SessId])),
    ?assertReceivedMatch({remove_session, _}, ?TIMEOUT).

incoming_session_watcher_should_retry_session_removal(Config) ->
    set_session_status(Config, inactive),
    ?assertReceivedMatch({remove_session, _}, ?TIMEOUT),
    ?assertReceivedMatch({remove_session, _}, ?TIMEOUT).

session_create_or_reuse_session_should_update_session_access_time(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Nonce = ?config(session_nonce, Config),
    Accessed1 = get_session_access_time(Config),
    timer:sleep(timer:seconds(1)),
    ?assertMatch(
        {ok, SessId},
        fuse_test_utils:reuse_or_create_fuse_session(
            Worker, Nonce, undefined, undefined, self()
        )
    ),
    Accessed2 = get_session_access_time(Config),
    ?assert(Accessed2 - Accessed1 > 0).

session_update_should_update_session_access_time(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Accessed1 = get_session_access_time(Config),
    timer:sleep(timer:seconds(1)),
    ?assertMatch(
        {ok, #document{key = SessId}},
        ?call(Worker, update_doc_and_time, [SessId, fun(Sess) -> {ok, Sess} end])
    ),
    Accessed2 = get_session_access_time(Config),
    ?assert(Accessed2 - Accessed1 > 0).

session_save_should_update_session_access_time(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Accessed1 = get_session_access_time(Config),
    timer:sleep(timer:seconds(1)),
    ?assertMatch(
        {ok, SessId},
        ?call(Worker, save, [get_session_doc(Config)])
    ),
    Accessed2 = get_session_access_time(Config),
    ?assert(Accessed2 - Accessed1 > 0).

session_create_should_set_session_access_time(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = base64:encode(crypto:strong_rand_bytes(20)),
    Accessed1 = rpc:call(Worker, global_clock, timestamp_seconds, []),
    timer:sleep(timer:seconds(1)),
    ?call(Worker, create, [#document{key = SessId, value = #session{}}]),
    Accessed2 = get_session_access_time([{session_id, SessId} | Config]),
    ?call(Worker, delete, [SessId]),
    ?assert(Accessed2 - Accessed1 > 0).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer, fuse_test_utils]} | Config].

init_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Nonce = crypto:strong_rand_bytes(20),
    SessId = datastore_key:new_from_digest([<<"fuse">>, Nonce]),
    initializer:remove_pending_messages(),
    mock_session_manager(Worker),
    {ok, Pid} = start_incoming_session_watcher(Worker, SessId),
    [{incoming_session_watcher, Pid}, {session_id, SessId}, {session_nonce, Nonce} | Config].

end_per_suite(_Config) ->
    ok.

end_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Pid = ?config(incoming_session_watcher, Config),
    stop_incoming_session_watcher(Worker, Pid, SessId),
    test_utils:mock_validate_and_unload(Worker, [session_manager]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec start_incoming_session_watcher(Worker :: node(), SessId :: session:id()) ->
    {ok, Pid :: pid()}.
start_incoming_session_watcher(Worker, SessId) ->
    Self = self(),
    ?call(Worker, application, set_env, [
        ?APP_NAME, fuse_session_grace_period_seconds, 2
    ]),
    ?assertMatch({ok, _}, ?call(Worker, save, [#document{
        key = SessId,
        value = #session{status = active, type = fuse, connections = [Self]}
    }])),
    ?assertMatch({ok, _}, ?call(Worker, gen_server, start, [
        incoming_session_watcher, [SessId, fuse], []
    ])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec stop_incoming_session_watcher(Worker :: node(), Pid :: pid(),
    SessId :: session:id()) -> true.
stop_incoming_session_watcher(Worker, Pid, SessId) ->
    ?call(Worker, delete, [SessId]),
    exit(Pid, shutdown).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks session_manager, so that it sends messages on session removal.
%% @end
%%--------------------------------------------------------------------
-spec mock_session_manager(Worker :: node()) -> ok.
mock_session_manager(Worker) ->
    Self = self(),
    test_utils:mock_new(Worker, session_manager),
    test_utils:mock_expect(Worker, session_manager, terminate_session, fun
        (SessID) -> Self ! {remove_session, SessID}, ok
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets session status.
%% @end
%%--------------------------------------------------------------------
-spec set_session_status(Config :: term(), Status :: session:status()) -> ok.
set_session_status(Config, Status) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    ?call(Worker, update, [SessId, fun(Sess = #session{}) ->
        {ok, Sess#session{status = Status}}
    end]),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns session.
%% @end
%%--------------------------------------------------------------------
-spec get_session_doc(Config :: term()) -> Session :: #session{}.
get_session_doc(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    {ok, #document{} = Doc} = ?assertMatch(
        {ok, _},
        ?call(Worker, get, [SessId])
    ),
    Doc.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns session access time.
%% @end
%%--------------------------------------------------------------------
-spec get_session_access_time(Config :: term()) -> time:seconds().
get_session_access_time(Config) ->
    #document{value = #session{accessed = Accessed}} = get_session_doc(Config),
    Accessed.
