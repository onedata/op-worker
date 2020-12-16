%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains incoming_session_watcher tests.
%%% @end
%%%-------------------------------------------------------------------
-module(session_watcher_test_SUITE).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

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
    incoming_session_watcher_should_work_properly_with_forward_time_warps/1,
    incoming_session_watcher_should_work_properly_with_backward_time_warps/1,
    session_create_or_reuse_session_should_update_session_access_time/1,
    session_update_should_update_session_access_time/1,
    session_save_should_update_session_access_time/1,
    session_create_should_set_session_access_time/1
]).

all() -> [
    incoming_session_watcher_should_not_remove_session_with_connections,
    incoming_session_watcher_should_remove_session_without_connections,
    incoming_session_watcher_should_remove_inactive_session,
    incoming_session_watcher_should_remove_session_on_error,
    incoming_session_watcher_should_retry_session_removal,
    incoming_session_watcher_should_work_properly_with_forward_time_warps,
    incoming_session_watcher_should_work_properly_with_backward_time_warps,
    session_create_or_reuse_session_should_update_session_access_time,
    session_update_should_update_session_access_time,
    session_save_should_update_session_access_time,
    session_create_should_set_session_access_time
].

-define(USER_ID, <<"user1">>).

-define(SHORT_GRACE_PERIOD, 2).
-define(LONG_GRACE_PERIOD, 1000).

-define(TIMEOUT, timer:seconds(5)).
-define(ATTEMPTS, 10).

-define(call(N, F, A), ?call(N, session, F, A)).
-define(call(N, M, F, A), rpc:call(N, M, F, A)).


%%%===================================================================
%%% Test functions
%%%===================================================================


incoming_session_watcher_should_not_remove_session_with_connections(Config) ->
    SessId = ?config(session_id, Config),

    ?assertNotReceivedMatch({termination_request, SessId}, ?TIMEOUT).


incoming_session_watcher_should_remove_session_without_connections(Config) ->
    SessId = ?config(session_id, Config),

    close_connections(Config),
    ?assertReceivedMatch({termination_request, SessId}, ?TIMEOUT).


incoming_session_watcher_should_remove_inactive_session(Config) ->
    SessId = ?config(session_id, Config),

    set_session_status(Config, inactive),
    ?assertReceivedMatch({termination_request, SessId}, ?TIMEOUT).


incoming_session_watcher_should_remove_session_on_error(Config) ->
    SessId = ?config(session_id, Config),
    [Worker] = ?config(op_worker_nodes, Config),

    ?assertEqual(ok, ?call(Worker, delete, [SessId])),
    ?assertReceivedMatch({termination_request, SessId}, ?TIMEOUT).


incoming_session_watcher_should_retry_session_removal(Config) ->
    SessId = ?config(session_id, Config),

    set_session_status(Config, inactive),
    ?assertReceivedMatch({termination_request, SessId}, ?TIMEOUT),
    % Longer timeout including session removal retry delay
    ?assertReceivedMatch({termination_request, SessId}, timer:seconds(20)).


incoming_session_watcher_should_work_properly_with_forward_time_warps(Config) ->
    SessId = ?config(session_id, Config),

    close_connections(Config),
    CurrTime = time_test_utils:get_frozen_time_seconds(),
    ?assertEqual(CurrTime, get_session_access_time(Config)),

    % Session shouldn't be removed until grace period has passed
    force_session_activity_check(Config),
    ?assertNotReceivedMatch({termination_request, SessId}, ?TIMEOUT),

    time_test_utils:set_current_time_seconds(CurrTime + ?LONG_GRACE_PERIOD div 2),
    force_session_activity_check(Config),
    ?assertEqual(CurrTime, get_session_access_time(Config)),
    ?assertNotReceivedMatch({termination_request, SessId}, ?TIMEOUT),

    % And should be removed immediately after that period passes
    time_test_utils:set_current_time_seconds(CurrTime + ?LONG_GRACE_PERIOD + 1),
    force_session_activity_check(Config),
    ?assertReceivedMatch({termination_request, SessId}, ?TIMEOUT).


incoming_session_watcher_should_work_properly_with_backward_time_warps(Config) ->
    SessId = ?config(session_id, Config),

    close_connections(Config),
    CurrTime = time_test_utils:get_frozen_time_seconds(),
    ?assertEqual(CurrTime, get_session_access_time(Config)),

    % Session shouldn't be removed until grace period has passed
    force_session_activity_check(Config),
    ?assertNotReceivedMatch({termination_request, SessId}, ?TIMEOUT),

    PastTime = CurrTime - ?LONG_GRACE_PERIOD div 2,
    time_test_utils:set_current_time_seconds(PastTime),
    force_session_activity_check(Config),

    % In case of backward time warp session last access time is set to the new
    % "current" time (past to the previous time). With this the passage of grace
    % period will be checked against this new access time
    ?assertEqual(PastTime, get_session_access_time(Config), ?ATTEMPTS),
    ?assertNotReceivedMatch({termination_request, SessId}, ?TIMEOUT),

    % And immediately after that period passes (starting from PastTime)
    % session should be removed
    time_test_utils:set_current_time_seconds(PastTime + ?LONG_GRACE_PERIOD - 1),
    force_session_activity_check(Config),
    ?assertEqual(PastTime, get_session_access_time(Config)),
    ?assertNotReceivedMatch({termination_request, SessId}, ?TIMEOUT),

    time_test_utils:set_current_time_seconds(PastTime + ?LONG_GRACE_PERIOD + 1),
    force_session_activity_check(Config),
    ?assertEqual(PastTime, get_session_access_time(Config)),
    ?assertReceivedMatch({termination_request, SessId}, ?TIMEOUT).


session_create_or_reuse_session_should_update_session_access_time(Config) ->
    SessId = ?config(session_id, Config),
    Nonce = ?config(session_nonce, Config),
    [Worker] = ?config(op_worker_nodes, Config),

    Accessed1 = get_session_access_time(Config),
    time_test_utils:simulate_seconds_passing(100),
    ?assertMatch(
        {ok, SessId},
        fuse_test_utils:reuse_or_create_fuse_session(
            Worker, Nonce, ?SUB(user, ?USER_ID), undefined, self()
        )
    ),
    Accessed2 = get_session_access_time(Config),
    ?assertEqual(100, Accessed2 - Accessed1).


session_update_should_update_session_access_time(Config) ->
    SessId = ?config(session_id, Config),
    [Worker] = ?config(op_worker_nodes, Config),

    Accessed1 = get_session_access_time(Config),
    time_test_utils:simulate_seconds_passing(432),
    ?assertMatch(
        {ok, #document{key = SessId}},
        ?call(Worker, update_doc_and_time, [SessId, fun(Sess) -> {ok, Sess} end])
    ),
    Accessed2 = get_session_access_time(Config),
    ?assertEqual(432, Accessed2 - Accessed1).


session_save_should_update_session_access_time(Config) ->
    SessId = ?config(session_id, Config),
    [Worker] = ?config(op_worker_nodes, Config),

    Accessed1 = get_session_access_time(Config),
    time_test_utils:simulate_seconds_passing(123),
    ?assertMatch({ok, SessId}, ?call(Worker, save, [get_session_doc(Config)])),
    Accessed2 = get_session_access_time(Config),
    ?assertEqual(123, Accessed2 - Accessed1).


session_create_should_set_session_access_time(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),

    SessId = base64:encode(crypto:strong_rand_bytes(20)),
    CurrTime = time_test_utils:simulate_seconds_passing(1),

    ?call(Worker, create, [#document{key = SessId, value = #session{}}]),
    ?assertEqual(CurrTime, get_session_access_time([{session_id, SessId} | Config])),
    ?call(Worker, delete, [SessId]).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    ssl:start(),
    hackney:start(),

    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        NewConfig3 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig2, "env_desc.json"),
            NewConfig2
        ),
        NewConfig3
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, fuse_test_utils]} | Config].


end_per_suite(Config) ->
    hackney:stop(),
    ssl:stop(),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:teardown_storage(Config).


init_per_testcase(Case, Config) when
    Case =:= incoming_session_watcher_should_work_properly_with_forward_time_warps;
    Case =:= incoming_session_watcher_should_work_properly_with_backward_time_warps;
    Case =:= session_create_or_reuse_session_should_update_session_access_time;
    Case =:= session_update_should_update_session_access_time;
    Case =:= session_save_should_update_session_access_time;
    Case =:= session_create_should_set_session_access_time
->
    ok = time_test_utils:freeze_time(Config),
    init_per_testcase(?DEFAULT_CASE(Case), [{grace_period, ?LONG_GRACE_PERIOD} | Config]);

init_per_testcase(_Case, Config) ->
    initializer:remove_pending_messages(),
    mock_session_manager(Config),
    set_fuse_session_grace_period_seconds(Config),

    Nonce = crypto:strong_rand_bytes(20),
    AccessToken = initializer:create_access_token(?USER_ID),
    [Worker] = ?config(op_worker_nodes, Config),
    {ok, {Sock, SessId}} = fuse_test_utils:connect_via_token(
        Worker, [{active, true}], Nonce, AccessToken
    ),

    [
        {session_id, SessId},
        {session_nonce, Nonce},
        {session_token, AccessToken},
        {socket, Sock}
        | Config
    ].


end_per_testcase(Case, Config) when
    Case =:= incoming_session_watcher_should_work_properly_with_forward_time_warps;
    Case =:= incoming_session_watcher_should_work_properly_with_backward_time_warps;
    Case =:= session_create_or_reuse_session_should_update_session_access_time;
    Case =:= session_update_should_update_session_access_time;
    Case =:= session_save_should_update_session_access_time;
    Case =:= session_create_should_set_session_access_time
->
    ok = time_test_utils:unfreeze_time(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    unmock_session_manager(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec mock_session_manager(test_config:config()) -> ok.
mock_session_manager(Config) ->
    Self = self(),
    [Worker] = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Worker, session_manager),
    test_utils:mock_expect(Worker, session_manager, terminate_session, fun
        (SessID) -> Self ! {termination_request, SessID}, ok
    end).


%% @private
-spec unmock_session_manager(test_config:config()) -> ok.
unmock_session_manager(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Worker, [session_manager]).


%% @private
-spec set_fuse_session_grace_period_seconds(test_config:config()) -> ok.
set_fuse_session_grace_period_seconds(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    Period = test_config:get_custom(Config, grace_period, ?SHORT_GRACE_PERIOD),

    ?call(Worker, application, set_env, [
        ?APP_NAME, fuse_session_grace_period_seconds, Period
    ]).


%% @private
-spec set_session_status(test_config:config(), session:status()) -> ok.
set_session_status(Config, Status) ->
    SessId = ?config(session_id, Config),
    [Worker] = ?config(op_worker_nodes, Config),
    ?call(Worker, update, [SessId, fun(Sess = #session{}) ->
        {ok, Sess#session{status = Status}}
    end]),
    ok.


%% @private
-spec get_session_access_time(test_config:config()) -> time:seconds().
get_session_access_time(Config) ->
    #document{value = #session{accessed = Accessed}} = get_session_doc(Config),
    Accessed.


%% @private
-spec get_session_doc(test_config:config()) -> session:doc().
get_session_doc(Config) ->
    SessId = ?config(session_id, Config),
    [Worker] = ?config(op_worker_nodes, Config),
    {ok, Doc} = ?assertMatch({ok, _}, ?call(Worker, get, [SessId]), ?ATTEMPTS),
    Doc.


%% @private
-spec close_connections(test_config:config()) -> ok.
close_connections(Config) ->
    Sock = ?config(socket, Config),
    ssl:close(Sock),
    ok.


%% @private
-spec force_session_activity_check(test_config:config()) -> ok.
force_session_activity_check(Config) ->
    #document{value = #session{watcher = IncomingSessionWatcher}} = get_session_doc(Config),
    IncomingSessionWatcher ! check_session_activity,
    ok.
