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
-module(session_offline_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_file_test_utils.hrl").
-include("modules/auth/offline_access_manager.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    offline_session_creation_for_root_should_fail_test/1,
    offline_session_creation_for_guest_should_fail_test/1,
    offline_session_should_work_as_any_other_session_test/1,
    offline_token_should_be_renewed_if_needed_test/1,
    offline_session_should_properly_react_to_time_warps_test/1
]).

all() -> [
    offline_session_creation_for_root_should_fail_test,
    offline_session_creation_for_guest_should_fail_test,
    offline_session_should_work_as_any_other_session_test,
    offline_token_should_be_renewed_if_needed_test,
    offline_session_should_properly_react_to_time_warps_test
].


-define(HOUR, 3600).
-define(DAY, 24 * ?HOUR).

-define(PROVIDER_TOKEN_TTL, 3).
-define(OFFLINE_ACCESS_TOKEN_TTL, 7 * ?DAY).

-define(RAND_JOB_ID(), str_utils:rand_hex(10)).

-define(NODE, hd(oct_background:get_provider_nodes(krakow))).
-define(ATTEMPTS, 30).


%%%===================================================================
%%% Test functions
%%%===================================================================


offline_session_creation_for_root_should_fail_test(_Config) ->
    ?assertMatch(?ERROR_TOKEN_SUBJECT_INVALID, init_offline_session(?RAND_JOB_ID(), ?ROOT_CREDENTIALS)).


offline_session_creation_for_guest_should_fail_test(_Config) ->
    ?assertMatch(?ERROR_TOKEN_SUBJECT_INVALID, init_offline_session(?RAND_JOB_ID(), ?GUEST_CREDENTIALS)).


offline_session_should_work_as_any_other_session_test(_Config) ->
    JobId = ?RAND_JOB_ID(),
    UserId = oct_background:get_user_id(user1),
    UserCredentials = get_user_credentials(),

    {ok, SessionId} = ?assertMatch({ok, _}, init_offline_session(JobId, UserCredentials)),
    SpaceKrkId = oct_background:get_space_id(space_krk),
    SpaceKrkGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceKrkId),
    UserRootDirGuid = fslogic_file_id:user_root_dir_guid(UserId),

    {ok, ListedSpaces} = ?assertMatch(
        {ok, [_ | _]},
        lfm_proxy:get_children(?NODE, SessionId, ?FILE_REF(UserRootDirGuid), 0, 100)
    ),
    {ListedSpacesGuids, _} = lists:unzip(ListedSpaces),
    ?assert(lists:member(SpaceKrkGuid, ListedSpacesGuids)),

    % Check that even in case of various environment situations everything is resolved
    % internally and session works properly (possibly after some time though)
    lists:foreach(fun(MessUpSthFun) ->
        MessUpSthFun(),

        ?assertMatch(
            {ok, #file_attr{guid = SpaceKrkGuid}},
            lfm_proxy:stat(?NODE, SessionId, ?FILE_REF(SpaceKrkGuid)),
            ?ATTEMPTS
        )
    end, [
        fun clear_auth_cache/0,
        fun force_oz_connection_restart/0,
        fun() -> ozw_test_rpc:simulate_downtime(5) end
    ]).


offline_token_should_be_renewed_if_needed_test(_Config) ->
    JobId = ?RAND_JOB_ID(),
    UserCredentials = get_user_credentials(),

    {ok, SessionId} = ?assertMatch({ok, _}, init_offline_session(JobId, UserCredentials)),
    OfflineCredentials1 = get_session_credentials(SessionId),

    % Credentials renew is attempted on call to `offline_access_manager:get_session_id`
    % but only if at least 1/3 of token TTL has passed
    time_test_utils:simulate_seconds_passing(?HOUR),
    ?assertMatch({ok, SessionId}, get_offline_session_id(JobId)),
    ?assertEqual(OfflineCredentials1, get_session_credentials(SessionId)),
    force_session_validity_check(SessionId),
    ?assertEqual(true, session_exists(SessionId)),

    % After that time token renewal should be attempted. In case of failure
    % (e.g. lost connection to oz) old credentials should be returned (they
    % would be still valid for some time).
    time_test_utils:simulate_seconds_passing(4 * ?DAY),

    mock_acquire_offline_user_access_token_failure(),

    AssertRenewalFailureFun = fun() ->
        ?assertMatch({ok, SessionId}, get_offline_session_id(JobId)),
        ?assertEqual(OfflineCredentials1, get_session_credentials(SessionId))
    end,

    lists:foreach(fun(BackoffInterval) ->
        AssertRenewalFailureFun(),
        ?assertEqual(1, count_offline_token_acquisition_attempts()),

        % Credentials renewal failure sets backoff so that next attempt wouldn't be
        % tried immediately
        time_test_utils:simulate_seconds_passing(BackoffInterval - 1),
        lists:foreach(fun(_) -> AssertRenewalFailureFun() end, lists:seq(1, 100)),
        ?assertEqual(0, count_offline_token_acquisition_attempts()),

        time_test_utils:simulate_seconds_passing(2)
    end, get_offline_token_renewal_backoff_Intervals(
        ?MIN_OFFLINE_TOKEN_RENEWAL_INTERVAL_SEC, []
    )),
    unmock_acquire_offline_user_access_token_failure(),

    % If there are no such failures and backoff interval has passed,
    % then token should be properly renewed
    time_test_utils:simulate_seconds_passing(?MAX_OFFLINE_TOKEN_RENEWAL_INTERVAL_SEC + 1),
    ?assertMatch({ok, SessionId}, get_offline_session_id(JobId)),
    ?assertNotEqual(OfflineCredentials1, get_session_credentials(SessionId)),
    force_session_validity_check(SessionId),
    ?assertEqual(true, session_exists(SessionId)),

    ok.


offline_session_should_properly_react_to_time_warps_test(_Config) ->
    JobId = ?RAND_JOB_ID(),
    UserCredentials = get_user_credentials(),

    {ok, SessionId} = ?assertMatch({ok, _}, init_offline_session(JobId, UserCredentials)),
    OfflineCredentials1 = get_session_credentials(SessionId),

    % Offline session/token shouldn't react to backward time warp as it will
    % just extend the token validity period.
    time_test_utils:simulate_seconds_passing(-6 * ?DAY),
    ?assertMatch({ok, SessionId}, get_offline_session_id(JobId)),
    ?assertEqual(OfflineCredentials1, get_session_credentials(SessionId)),

    % With previous backward time warp session should still exist even after 8 day passage
    % (offline_access_token_ttl is set to 7 days by default). Also no token renewal should
    % be performed until 1/3 of token TTL has elapsed after original acquirement timestamp
    time_test_utils:simulate_seconds_passing(8 * ?DAY),
    ?assertMatch({ok, SessionId}, get_offline_session_id(JobId)),
    ?assertEqual(OfflineCredentials1, get_session_credentials(SessionId)),

    % In case of forward time warp token may expire and session may terminate (after some
    % time of inertia) but offline credentials docs are not automatically removed - it is
    % responsibility of offline job to do so by calling `offline_access_manager:close_session`.
    time_test_utils:simulate_seconds_passing(7 * ?DAY),
    ?assertMatch(?ERROR_TOKEN_CAVEAT_UNVERIFIED(#cv_time{}), get_offline_session_id(JobId)),
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
-spec get_session_credentials(session:id()) -> auth_manager:credentials().
get_session_credentials(SessionId) ->
    {ok, #document{value = #session{credentials = Credentials}}} = get_session_doc(SessionId),
    Credentials.


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
-spec force_oz_connection_restart() -> ok.
force_oz_connection_restart() ->
    rpc:call(?NODE, gs_channel_service, force_restart_connection, []).


%% @private
-spec mock_verify_token_to_inform_about_consumer_token_used(tokens:serialized()) -> ok.
mock_verify_token_to_inform_about_consumer_token_used(OfflineAccessToken) ->
    Self = self(),

    test_utils:mock_new(?NODE, token_logic, [passthrough]),
    test_utils:mock_expect(?NODE, token_logic, verify_access_token,
        fun(AccessToken, ConsumerToken, PeerIp, Interface, DataAccessCaveatsPolicy) ->
            case AccessToken == OfflineAccessToken of
                true -> Self ! {consumer_token, ConsumerToken};
                false -> ok
            end,
            meck:passthrough([AccessToken, ConsumerToken, PeerIp, Interface, DataAccessCaveatsPolicy])
        end
    ).


%% @private
-spec unmock_verify_token_to_inform_about_consumer_token_used() -> ok.
unmock_verify_token_to_inform_about_consumer_token_used() ->
    test_utils:mock_unload(?NODE, token_logic).


%% @private
-spec mock_acquire_offline_user_access_token_failure() -> ok.
mock_acquire_offline_user_access_token_failure() ->
    Self = self(),

    test_utils:mock_new(?NODE, auth_manager, [passthrough]),
    test_utils:mock_expect(?NODE, auth_manager, acquire_offline_user_access_token, fun(_) ->
        Self ! acquire_offline_access_token,
        ?ERROR_NO_CONNECTION_TO_ONEZONE
    end).


%% @private
-spec unmock_acquire_offline_user_access_token_failure() -> ok.
unmock_acquire_offline_user_access_token_failure() ->
    test_utils:mock_unload(?NODE, auth_manager).


%% @private
-spec count_offline_token_acquisition_attempts() -> non_neg_integer().
count_offline_token_acquisition_attempts() ->
    count_offline_token_acquisition_attempts(0).


%% @private
-spec count_offline_token_acquisition_attempts(non_neg_integer()) -> non_neg_integer().
count_offline_token_acquisition_attempts(Num) ->
    receive acquire_offline_access_token ->
        count_offline_token_acquisition_attempts(Num + 1)
    after 100 ->
        Num
    end.


%% @private
-spec get_offline_token_renewal_backoff_Intervals(time:seconds(), [time:seconds()]) ->
    [time:seconds()].
get_offline_token_renewal_backoff_Intervals(?MAX_OFFLINE_TOKEN_RENEWAL_INTERVAL_SEC, Intervals) ->
    lists:reverse([
        % Test 3 times maximum backoff to assert that it will not be increased
        % infinitely but will stop at tha value
        ?MAX_OFFLINE_TOKEN_RENEWAL_INTERVAL_SEC,
        ?MAX_OFFLINE_TOKEN_RENEWAL_INTERVAL_SEC,
        ?MAX_OFFLINE_TOKEN_RENEWAL_INTERVAL_SEC
        | Intervals]);
get_offline_token_renewal_backoff_Intervals(Interval, Intervals) ->
    get_offline_token_renewal_backoff_Intervals(
        min(
            Interval * ?OFFLINE_TOKEN_RENEWAL_BACKOFF_RATE,
            ?MAX_OFFLINE_TOKEN_RENEWAL_INTERVAL_SEC
        ),
        [Interval | Intervals]
    ).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
        envs = [
            {oz_worker, oz_worker, [{offline_access_token_ttl, ?OFFLINE_ACCESS_TOKEN_TTL}]},
            {op_worker, op_worker, [
                {provider_token_ttl_sec, ?PROVIDER_TOKEN_TTL},
                {fuse_session_grace_period_seconds, ?DAY}
            ]}
        ]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(Case, Config) when
    Case == offline_token_should_be_renewed_if_needed_test;
    Case == offline_session_should_properly_react_to_time_warps_test
->
    ok = time_test_utils:freeze_time(Config),
    CurrTime = time_test_utils:get_frozen_time_seconds(),
    init_per_testcase(?DEFAULT_CASE(Case), [{frozen_time, CurrTime} | Config]);

init_per_testcase(_Case, Config) ->
    unmock_acquire_offline_user_access_token_failure(),
    ct:timetrap({minutes, 20}),
    lfm_proxy:init(Config).


end_per_testcase(offline_session_should_work_as_any_other_session_test = Case, Config) ->
    % Await renewal of oz connection (it is teardown as part of test).
    ?assertMatch(true, rpc:call(?NODE, gs_channel_service, is_connected, []), ?ATTEMPTS),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) when
    Case == offline_token_should_be_renewed_if_needed_test;
    Case == offline_session_should_properly_react_to_time_warps_test
->
    OriginalTime = ?config(frozen_time, Config),
    time_test_utils:set_current_time_seconds(OriginalTime),
    ok = time_test_utils:unfreeze_time(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
