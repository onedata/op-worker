%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of misc authorization mechanism
%%% functionalities.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_misc_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/logical_file_manager/lfm.hrl").
-include("permissions_test.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

% Export for use in rpc
-export([check_perms/3]).

-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    test_expired_session/1,
    test_multi_provider_posix_permission_cache/1,
    test_multi_provider_acl_permission_cache/1
]).

groups() -> [
    {all_tests, [parallel], [
        test_expired_session,
        test_multi_provider_posix_permission_cache,
        test_multi_provider_acl_permission_cache
    ]}
].

all() -> [
    {group, all_tests}
].

-define(ATTEMPTS, 30).


%%%===================================================================
%%% Test functions
%%%===================================================================


test_expired_session(_Config) ->
    Node = oct_background:get_random_provider_node(?RAND_ELEMENT([krakow, paris])),

    UserId = oct_background:get_user_id(user2),
    AccessToken = provider_onenv_test_utils:create_oz_temp_access_token(UserId),

    SessionId = provider_onenv_test_utils:create_session(Node, UserId, AccessToken),

    SpaceName = oct_background:get_space_name(space1),
    FilePath = filepath_utils:join([<<"/">>, SpaceName, ?RAND_STR()]),
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SessionId, FilePath, 8#770)),

    ok = rpc:call(Node, session, delete, [SessionId]),

    ?assertMatch({error, ?EACCES}, lfm_proxy:open(Node, SessionId, ?FILE_REF(FileGuid), read)).


test_multi_provider_posix_permission_cache(_Config) ->
    NodeKrakow = oct_background:get_random_provider_node(krakow),
    AssertionNodes = oct_background:get_all_providers_nodes(),

    UserSelector = user2,
    {Guid, AllPerms} = prepare_multi_provider_permission_cache_test_env(NodeKrakow, UserSelector),

    % Set random posix permissions for file/dir and assert they are properly propagated to other
    % nodes/providers (that includes permissions cache - obsolete entries should be overridden)
    utils:repeat(5, fun(_IterationNum) ->
        PosixPerms = lists_utils:random_sublist(?ALL_POSIX_PERMS),
        PosixMode = lists:foldl(fun(Perm, Acc) ->
            Acc bor authz_test_utils:posix_perm_to_mode(Perm, owner)
        end, 0, PosixPerms),
        authz_test_utils:set_modes(NodeKrakow, #{Guid => PosixMode}),

        {AllowedPerms, DeniedPerms} = lists:foldl(fun(Perm, {AllowedPermsAcc, DeniedPermsAcc}) ->
            case authz_test_utils:perm_to_posix_perms(Perm) -- [owner, owner_if_parent_sticky | PosixPerms] of
                [] -> {[Perm | AllowedPermsAcc], DeniedPermsAcc};
                _ -> {AllowedPermsAcc, [Perm | DeniedPermsAcc]}
            end
        end, {[], []}, AllPerms),

        run_multi_provider_perm_test(
            AssertionNodes, UserSelector, Guid, PosixPerms, DeniedPerms,
            {error, ?EACCES}, <<"denied posix perm">>
        ),
        run_multi_provider_perm_test(
            AssertionNodes, UserSelector, Guid, PosixPerms, AllowedPerms,
            ok, <<"allowed posix perm">>
        )
    end).


test_multi_provider_acl_permission_cache(_Config) ->
    NodeKrakow = oct_background:get_random_provider_node(krakow),
    AssertionNodes = oct_background:get_all_providers_nodes(),

    UserSelector = user2,
    {Guid, AllPerms} = prepare_multi_provider_permission_cache_test_env(NodeKrakow, UserSelector),

    % Set random acl permissions for file/dir and assert they are properly propagated to other
    % nodes/providers (that includes permissions cache - obsolete entries should be overridden)
    utils:repeat(10, fun(_IterationNum) ->
        SetPerms = lists_utils:random_sublist(AllPerms),
        authz_test_utils:set_acls(NodeKrakow, #{Guid => SetPerms}, #{}, ?everyone, ?no_flags_mask),

        ComplementaryPerms = authz_test_utils:complementary_perms(NodeKrakow, Guid, SetPerms),

        run_multi_provider_perm_test(
            AssertionNodes, UserSelector, Guid, SetPerms, ComplementaryPerms,
            {error, ?EACCES}, <<"denied acl perm">>
        ),
        run_multi_provider_perm_test(
            AssertionNodes, UserSelector, Guid, SetPerms, SetPerms,
            ok, <<"allowed acl perm">>
        )
    end).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
prepare_multi_provider_permission_cache_test_env(Node, UserSelector) ->
    SessionIdKrakow = oct_background:get_user_session_id(UserSelector, krakow),

    SpaceName = oct_background:get_space_name(space1),
    Path = filepath_utils:join([<<"/">>, SpaceName, ?RAND_STR()]),
    case rand:uniform(2) of
        1 ->
            {ok, FileGuid} = lfm_proxy:create(Node, SessionIdKrakow, Path, 8#777),
            {FileGuid, ?ALL_FILE_PERMS};
        2 ->
            {ok, DirGuid} = lfm_proxy:mkdir(Node, SessionIdKrakow, Path, 8#777),
            {DirGuid, ?ALL_DIR_PERMS}
    end.


%% @private
run_multi_provider_perm_test(AssertNodes, User, Guid, PermsSet, TestedPerms, ExpResult, Scenario) ->
    lists:foreach(fun(TestedPerm) ->
        lists:foreach(fun(Node) ->
            try
                ?assertEqual(ExpResult, check_perms(Node, User, Guid, [TestedPerm]), ?ATTEMPTS)
            catch Class:Reason:Stacktrace ->
                ?ct_pal_exception(
                    "Multi provider permission cache test case failure ~ts",
                    [?autoformat(Scenario, Node, PermsSet, TestedPerm)],
                    Class, Reason, Stacktrace
                ),
                error(assert_multi_provider_perms_cache_failed)
            end
        end, AssertNodes)
    end, TestedPerms).


%% @private
check_perms(Node, UserSelector, Guid, Perms) ->
    ProviderId = opw_test_rpc:get_provider_id(Node),

    SessionId = oct_background:get_user_session_id(UserSelector, ProviderId),
    UserCtx = rpc:call(Node, user_ctx, new, [SessionId]),

    rpc:call(Node, ?MODULE, check_perms, [
        UserCtx, file_ctx:new_by_guid(Guid),
        [?OPERATIONS(authz_test_utils:perms_to_bitmask(Perms))]
    ]).


%% @private
check_perms(UserCtx, FileCtx, Perms) ->
    try
        fslogic_authz:ensure_authorized(UserCtx, FileCtx, Perms),
        ok
    catch _Type:Reason ->
        {error, Reason}
    end.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op-2nodes",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_GroupName, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_GroupName, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(TestCase, Config) when
    TestCase =:= test_multi_provider_posix_permission_cache,
    TestCase =:= test_multi_provider_acl_permission_cache
->
    ct:timetrap({minutes, 15}),
    init_per_testcase(?DEFAULT_CASE(TestCase), Config);

init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
