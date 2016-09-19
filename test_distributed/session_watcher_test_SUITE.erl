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
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    session_watcher_should_not_remove_session_with_connections/1,
    session_watcher_should_remove_session_without_connections/1,
    session_watcher_should_remove_inactive_session/1,
    session_watcher_should_remove_session_on_error/1,
    session_watcher_should_retry_session_removal/1,
    session_create_or_reuse_session_should_update_session_access_time/1,
    session_update_should_update_session_access_time/1,
    session_save_should_update_session_access_time/1,
    session_create_should_set_session_access_time/1
]).

all() ->
    ?ALL([
        session_watcher_should_not_remove_session_with_connections,
        session_watcher_should_remove_session_without_connections,
        session_watcher_should_remove_inactive_session,
        session_watcher_should_remove_session_on_error,
        session_watcher_should_retry_session_removal,
        session_create_or_reuse_session_should_update_session_access_time,
        session_update_should_update_session_access_time,
        session_save_should_update_session_access_time,
        session_create_should_set_session_access_time
    ]).

-define(TIMEOUT, timer:seconds(10)).

-define(call(N, F, A), ?call(N, session, F, A)).
-define(call(N, M, F, A), rpc:call(N, M, F, A)).

%%%===================================================================
%%% Test functions
%%%===================================================================

session_watcher_should_not_remove_session_with_connections(_Config) ->
    ?assertNotReceivedMatch({remove_session, _}, ?TIMEOUT).

session_watcher_should_remove_session_without_connections(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Self = self(),
    ?call(Worker, remove_connection, [SessId, Self]),
    ?assertReceivedMatch({remove_session, _}, ?TIMEOUT).

session_watcher_should_remove_inactive_session(Config) ->
    set_session_status(Config, inactive),
    ?assertReceivedMatch({remove_session, _}, ?TIMEOUT).

session_watcher_should_remove_session_on_error(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    ?call(Worker, delete, [SessId]),
    ?assertReceivedMatch({remove_session, _}, ?TIMEOUT).

session_watcher_should_retry_session_removal(Config) ->
    set_session_status(Config, inactive),
    ?assertReceivedMatch({remove_session, _}, ?TIMEOUT),
    ?assertReceivedMatch({remove_session, _}, ?TIMEOUT).

session_create_or_reuse_session_should_update_session_access_time(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Accessed1 = get_session_access_time(Config),
    rpc:call(Worker, session_manager, reuse_or_create_fuse_session,
        [SessId, undefined, self()]),
    ?call(Worker, get, [SessId]),
    Accessed2 = get_session_access_time(Config),
    ?assert(Accessed2 - Accessed1 >= 0).

session_update_should_update_session_access_time(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Accessed1 = get_session_access_time(Config),
    ?call(Worker, update, [SessId, #{}]),
    Accessed2 = get_session_access_time(Config),
    ?assert(Accessed2 - Accessed1 >= 0).

session_save_should_update_session_access_time(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Accessed1 = get_session_access_time(Config),
    ?call(Worker, save, [#document{value = get_session(Config)}]),
    Accessed2 = get_session_access_time(Config),
    ?assert(Accessed2 - Accessed1 >= 0).

session_create_should_set_session_access_time(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = base64:encode(crypto:rand_bytes(20)),
    Accessed1 = erlang:system_time(seconds),
    ?call(Worker, create, [#document{key = SessId, value = #session{}}]),
    Accessed2 = get_session_access_time([{session_id, SessId} | Config]),
    ?call(Worker, delete, [SessId]),
    ?assert(Accessed2 - Accessed1 >= 0).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    ?TEST_STOP(Config).

init_per_testcase(Case, Config) ->
    ?CASE_START(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = <<"session_id">>,
    initializer:remove_pending_messages(),
    mock_worker_proxy(Worker),
    {ok, Pid} = start_session_watcher(Worker, SessId),
    [{session_watcher, Pid}, {session_id, SessId} | Config].

end_per_testcase(Case, Config) ->
    ?CASE_STOP(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Pid = ?config(session_watcher, Config),
    stop_session_watcher(Worker, Pid, SessId),
    test_utils:mock_validate_and_unload(Worker, [worker_proxy]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec start_session_watcher(Worker :: node(), SessId :: session:id()) ->
    {ok, Pid :: pid()}.
start_session_watcher(Worker, SessId) ->
    Self = self(),
    ?call(Worker, application, set_env,
        [?APP_NAME, fuse_session_ttl_seconds, 1]),
    ?assertMatch({ok, _}, ?call(Worker, save, [#document{
        key = SessId, value = #session{status = active, type = fuse, connections = [Self]}
    }])),
    ?assertMatch({ok, _}, ?call(Worker, gen_server, start, [
        session_watcher, [SessId, fuse], []
    ])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec stop_session_watcher(Worker :: node(), Pid :: pid(),
    SessId :: session:id()) -> true.
stop_session_watcher(Worker, Pid, SessId) ->
    ?call(Worker, delete, [SessId]),
    exit(Pid, shutdown).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks worker proxy, so that on cast it forwards all messages to this process.
%% @end
%%--------------------------------------------------------------------
-spec mock_worker_proxy(Worker :: node()) -> ok.
mock_worker_proxy(Worker) ->
    Self = self(),
    test_utils:mock_new(Worker, worker_proxy),
    test_utils:mock_expect(Worker, worker_proxy, cast, fun
        (_, Msg) -> Self ! Msg, ok
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
    ?call(Worker, update, [SessId, #{status => Status}]),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns session.
%% @end
%%--------------------------------------------------------------------
-spec get_session(Config :: term()) -> Session :: #session{}.
get_session(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    {ok, #document{value = Session}} =
        ?assertMatch({ok, _}, ?call(Worker, get, [SessId])),
    Session.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns session access time.
%% @end
%%--------------------------------------------------------------------
-spec get_session_access_time(Config :: term()) -> Accessed :: erlang:timestamp().
get_session_access_time(Config) ->
    #session{accessed = Accessed} = get_session(Config),
    Accessed.
