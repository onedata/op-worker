%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning offline access management.
%%% @end
%%%-------------------------------------------------------------------
-module(auth_offline_access_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_file_test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    offline_session_creation_for_root_should_fail_test/1,
    offline_session_creation_for_guest_should_fail_test/1,
    offline_session_should_work_as_any_other_session_test/1,
    offline_token_should_be_refreshed_if_needed_test/1,
    offline_session_should_properly_react_to_time_warps_test/1
]).

all() -> [
    offline_session_creation_for_root_should_fail_test,
    offline_session_creation_for_guest_should_fail_test,
    offline_session_should_work_as_any_other_session_test,
    offline_token_should_be_refreshed_if_needed_test,
    offline_session_should_properly_react_to_time_warps_test
].


-define(HOUR, 3600).
-define(DAY, 24 * ?HOUR).

-define(OFFLINE_ACCESS_TOKEN_TTL, 7 * ?DAY).

-define(NODE, hd(oct_background:get_provider_nodes(krakow))).
-define(ATTEMPTS, 30).


%%%===================================================================
%%% Test functions
%%%===================================================================


offline_session_creation_for_root_should_fail_test(_Config) ->
    JobId = str_utils:rand_hex(10),
    ?assertMatch(?ERROR_TOKEN_SUBJECT_INVALID, init_offline_session(JobId, ?ROOT_CREDENTIALS)).


offline_session_creation_for_guest_should_fail_test(_Config) ->
    JobId = str_utils:rand_hex(10),
    ?assertMatch(?ERROR_TOKEN_SUBJECT_INVALID, init_offline_session(JobId, ?GUEST_CREDENTIALS)).


offline_session_should_work_as_any_other_session_test(_Config) ->
    JobId = str_utils:rand_hex(10),
    UserId = oct_background:get_user_id(user1),
    UserCredentials = get_user_credentials(),

    {ok, SessionId} = ?assertMatch({ok, _}, init_offline_session(JobId, UserCredentials)),
    SpaceKrkId = oct_background:get_space_id(space_krk),
    SpaceKrkGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceKrkId),
    UserRootDirGuid = fslogic_uuid:user_root_dir_guid(UserId),

    ?assertMatch(
        {ok, [{SpaceKrkGuid, _}]},
        lfm_proxy:get_children(?NODE, SessionId, {guid, UserRootDirGuid}, 0, 100)
    ),

    clear_auth_cache(),

    ?assertMatch(
        {ok, #file_attr{guid = SpaceKrkGuid}},
        lfm_proxy:stat(?NODE, SessionId, {guid, SpaceKrkGuid})
    ).


offline_token_should_be_refreshed_if_needed_test(_Config) ->
    JobId = str_utils:rand_hex(10),
    UserCredentials = get_user_credentials(),

    {ok, SessionId} = ?assertMatch({ok, _}, init_offline_session(JobId, UserCredentials)),
    OfflineCredentials1 = get_session_access_token(SessionId),

    % Credentials are refreshed on call to `offline_access_manager:get_session_id`
    % but only if at least 1/3 of token TTL has passed
    time_test_utils:simulate_seconds_passing(?HOUR),
    ?assertMatch({ok, SessionId}, get_offline_session_id(JobId)),
    ?assertEqual(OfflineCredentials1, get_session_access_token(SessionId)),
    force_session_validity_check(SessionId),
    ?assertEqual(true, session_exists(SessionId)),

    % After that time token refresh should be attempted. In case of failure
    % (e.g. lost connection to oz) old credentials should be returned (they
    % would be still valid for some time).
    time_test_utils:simulate_seconds_passing(4 * ?DAY),

    mock_acquire_offline_user_access_token_failure(),
    ?assertMatch({ok, SessionId}, get_offline_session_id(JobId)),
    ?assertEqual(OfflineCredentials1, get_session_access_token(SessionId)),
    unmock_acquire_offline_user_access_token_failure(),

    % If there are no errors token should be properly refreshed
    ?assertMatch({ok, SessionId}, get_offline_session_id(JobId)),
    ?assertNotEqual(OfflineCredentials1, get_session_access_token(SessionId)),
    force_session_validity_check(SessionId),
    ?assertEqual(true, session_exists(SessionId)),

    ok.


offline_session_should_properly_react_to_time_warps_test(_Config) ->
    JobId = str_utils:rand_hex(10),
    UserCredentials = get_user_credentials(),

    {ok, SessionId} = ?assertMatch({ok, _}, init_offline_session(JobId, UserCredentials)),
    OfflineCredentials1 = get_session_access_token(SessionId),

    % Offline session/token shouldn't react to backward time warp as it will
    % just extend the token validity period.
    time_test_utils:simulate_seconds_passing(-6 * ?DAY),
    ?assertMatch({ok, SessionId}, get_offline_session_id(JobId)),
    ?assertEqual(OfflineCredentials1, get_session_access_token(SessionId)),

    % With previous backward time warp session should still exist even after 8 day passage
    % (offline_access_token_ttl is set to 7 days by default). Also no token refresh should
    % be performed until 1/3 of token TTL has elapsed after original acquirement timestamp
    time_test_utils:simulate_seconds_passing(8 * ?DAY),
    ?assertMatch({ok, SessionId}, get_offline_session_id(JobId)),
    ?assertEqual(OfflineCredentials1, get_session_access_token(SessionId)),

    % In case of forward time warp token may expire and session may terminate (after some
    % time of inertia) but offline credentials docs are not automatically removed - it is
    % responsibility of offline job to do so by calling `offline_access_manager:close_session`.
    time_test_utils:simulate_seconds_passing(7 * ?DAY),
    ?assertMatch(?ERROR_TOKEN_INVALID, get_offline_session_id(JobId)),
    force_session_validity_check(SessionId),
    ?assertEqual(false, session_exists(SessionId), ?ATTEMPTS),
    ?assert(offline_credentials_exist(JobId)),

    % close_session hasn't been called yet, it is still possible to recreate session
    % if backward time warp happens
    time_test_utils:simulate_seconds_passing(-3 * ?DAY),
    ?assertMatch({ok, SessionId}, get_offline_session_id(JobId), ?ATTEMPTS),
    ?assertEqual(true, session_exists(SessionId)),
    ?assertEqual(true, offline_credentials_exist(JobId)),

    % If `close_session` is called the session is terminated and credentials removed even
    % if token has not expired yet
    close_offline_session(JobId),
    ?assertEqual(false, session_exists(SessionId), ?ATTEMPTS),
    ?assertEqual(false, offline_credentials_exist(JobId)),

    % After offline session credentials are deleted it should be impossible to
    % recreate offline session
    ?assertMatch(?ERROR_NOT_FOUND, get_offline_session_id(JobId)),
    ?assertEqual(false, session_exists(SessionId)),
    ?assertEqual(false, offline_credentials_exist(JobId)),

    ok.


%%%===================================================================
%%% Helper functions
%%%===================================================================


%% @private
-spec get_user_credentials() -> auth_manager:credentials().
get_user_credentials() ->
    auth_manager:build_token_credentials(
        oct_background:get_user_access_token(user1), undefined,
        initializer:local_ip_v4(), oneclient, allow_data_access_caveats
    ).


%% @private
-spec init_offline_session(offline_access_manager:offline_job_id(), auth_manager:credentials()) ->
    {ok, session:id()} | {error, term()}.
init_offline_session(JobId, UserCredentials) ->
    rpc:call(?NODE, offline_access_manager, init_session, [JobId, UserCredentials]).


%% @private
-spec get_offline_session_id(offline_access_manager:offline_job_id()) ->
    {ok, session:id()} | {error, term()}.
get_offline_session_id(JobId) ->
    rpc:call(?NODE, offline_access_manager, get_session_id, [JobId]).


%% @private
-spec close_offline_session(offline_access_manager:offline_job_id()) -> ok.
close_offline_session(JobId) ->
    rpc:call(?NODE, offline_access_manager, close_session, [JobId]).


%% @private
-spec get_session_access_token(session:id()) -> tokens:serialized().
get_session_access_token(SessionId) ->
    {ok, #document{value = #session{credentials = Credentials}}} = get_session_doc(SessionId),
    auth_manager:get_access_token(Credentials).


%% @private
-spec force_session_validity_check(session:id()) -> ok.
force_session_validity_check(SessionId) ->
    {ok, #document{value = #session{watcher = Watcher}}} = get_session_doc(SessionId),
    Watcher ! check_session_validity,
    ok.


%% @private
-spec session_exists(session:id()) -> boolean().
session_exists(SessionId) ->
    rpc:call(?NODE, session, exists, [SessionId]).


%% @private
-spec get_session_doc(session:id()) -> {ok, session:doc()} | {error, term()}.
get_session_doc(SessionId) ->
    rpc:call(?NODE, session, get, [SessionId]).


%% @private
-spec offline_credentials_exist(offline_access_credentials:id()) -> boolean().
offline_credentials_exist(JobId) ->
    case rpc:call(?NODE, offline_access_credentials, get, [JobId]) of
        {ok, _} -> true;
        ?ERROR_NOT_FOUND -> false
    end.


%% @private
-spec clear_auth_cache() -> ok.
clear_auth_cache() ->
    rpc:call(?NODE, ets, delete_all_objects, [auth_cache]).


%% @private
-spec mock_acquire_offline_user_access_token_failure() -> ok.
mock_acquire_offline_user_access_token_failure() ->
    test_utils:mock_new(?NODE, auth_manager, [passthrough]),
    test_utils:mock_expect(?NODE, auth_manager, acquire_offline_user_access_token, fun(_, _) ->
        ?ERROR_NO_CONNECTION_TO_ONEZONE
    end).


%% @private
-spec unmock_acquire_offline_user_access_token_failure() -> ok.
unmock_acquire_offline_user_access_token_failure() ->
    test_utils:mock_unload(?NODE, auth_manager).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    ssl:start(),
    hackney:start(),
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "auth_tests",
        envs = [
            {oz_worker, oz_worker, [{offline_access_token_ttl, ?OFFLINE_ACCESS_TOKEN_TTL}]},
            {op_worker, op_worker, [{fuse_session_grace_period_seconds, ?DAY}]}
        ]
    }).


end_per_suite(_Config) ->
    hackney:stop(),
    ssl:stop().


init_per_testcase(_Case, Config) ->
    unmock_acquire_offline_user_access_token_failure(),
    ok = time_test_utils:freeze_time(Config),
    ct:timetrap({minutes, 20}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    ok = time_test_utils:unfreeze_time(Config),
    lfm_proxy:teardown(Config).
