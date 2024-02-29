%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of posix and acl
%%% permissions with corresponding lfm (logical_file_manager) functions
%%% TODO VFS-11759 rewrite permissions cache test to onenv
%%% @end
%%%-------------------------------------------------------------------
-module(permissions_test_base).
-author("Bartosz Walkowicz").

-include("modules/logical_file_manager/lfm.hrl").
-include("permissions_test.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    multi_provider_permission_cache_test/1,
    expired_session_test/1
]).
% Export for use in rpc
-export([check_perms/3]).

-define(ATTEMPTS, 35).


%%%===================================================================
%%% Test functions
%%%===================================================================


multi_provider_permission_cache_test(Config) ->
    [P1W1, P1W2, P2] = ?config(op_worker_nodes, Config),
    Nodes = [P1W2, P1W1, P2],

    User = <<"user1">>,

    Path = <<"/space1/multi_provider_permission_cache_test">>,
    P1W2SessId = ?config({session_id, {User, ?GET_DOMAIN(P1W2)}}, Config),

    {Guid, AllPerms} = case rand:uniform(2) of
        1 ->
            {_, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(P1W2, P1W2SessId, Path, 8#777)),
            {FileGuid, ?ALL_FILE_PERMS};
        2 ->
            {_, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(P1W2, P1W2SessId, Path, 8#777)),
            {DirGuid, ?ALL_DIR_PERMS}
    end,

    % Set random posix permissions for file/dir and assert they are properly propagated to other
    % nodes/providers (that includes permissions cache - obsolete entries should be overridden)
    lists:foreach(fun(_IterationNum) ->
        PosixPerms = lists_utils:random_sublist(?ALL_POSIX_PERMS),
        Mode = lists:foldl(fun(Perm, Acc) ->
            Acc bor permissions_test_utils:posix_perm_to_mode(Perm, owner)
        end, 0, PosixPerms),
        permissions_test_utils:set_modes(P1W2, #{Guid => Mode}),

        {AllowedPerms, DeniedPerms} = lists:foldl(fun(Perm, {AllowedPermsAcc, DeniedPermsAcc}) ->
            case permissions_test_utils:perm_to_posix_perms(Perm) -- [owner, owner_if_parent_sticky | PosixPerms] of
                [] -> {[Perm | AllowedPermsAcc], DeniedPermsAcc};
                _ -> {AllowedPermsAcc, [Perm | DeniedPermsAcc]}
            end
        end, {[], []}, AllPerms),

        run_multi_provider_perm_test(
            Nodes, User, Guid, PosixPerms, DeniedPerms,
            {error, ?EACCES}, <<"denied posix perm">>, Config
        ),
        run_multi_provider_perm_test(
            Nodes, User, Guid, PosixPerms, AllowedPerms,
            ok, <<"allowed posix perm">>, Config
        )
    end, lists:seq(1, 5)),

    % Set random acl permissions for file/dir and assert they are properly propagated to other
    % nodes/providers (that includes permissions cache - obsolete entries should be overridden)
    lists:foreach(fun(_IterationNum) ->
        SetPerms = lists_utils:random_sublist(AllPerms),
        permissions_test_utils:set_acls(P1W2, #{Guid => SetPerms}, #{}, ?everyone, ?no_flags_mask),

        run_multi_provider_perm_test(
            Nodes, User, Guid, SetPerms, permissions_test_utils:complementary_perms(P1W2, Guid, SetPerms),
            {error, ?EACCES}, <<"denied acl perm">>, Config
        ),
        run_multi_provider_perm_test(
            Nodes, User, Guid, SetPerms, SetPerms,
            ok, <<"allowed acl perm">>, Config
        )
    end, lists:seq(1, 10)).


run_multi_provider_perm_test(Nodes, User, Guid, PermsSet, TestedPerms, ExpResult, Scenario, Config) ->
    lists:foreach(fun(TestedPerm) ->
        lists:foreach(fun(Node) ->
            try
                ?assertMatch(
                    ExpResult,
                    check_perms(Node, User, Guid, [TestedPerm], Config),
                    ?ATTEMPTS
                )
            catch _:Reason ->
                ct:pal(
                    "PERMISSIONS TESTS FAILURE~n"
                    "   Scenario: multi_provider_permission_cache_test ~p~n"
                    "   Node: ~p~n"
                    "   Perms set: ~p~n"
                    "   Tested perm: ~p~n"
                    "   Reason: ~p~n",
                    [
                        Scenario, Node, PermsSet, TestedPerm, Reason
                    ]
                ),
                erlang:error(perms_test_failed)
            end
        end, Nodes)
    end, TestedPerms).


expired_session_test(Config) ->
    % Setup
    [_, _, W] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    {_, GUID} = ?assertMatch(
        {ok, _},
        lfm_proxy:create(W, SessId1, <<"/space1/es_file">>, 8#770)
    ),

    ok = rpc:call(W, session, delete, [SessId1]),

    % Verification
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:open(W, SessId1, ?FILE_REF(GUID), write)
    ).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = initializer:setup_storage(NewConfig),
        NewConfig2 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig1, "env_desc.json"),
            [{spaces_owners, [<<"owner">>]} | NewConfig1]
        ),
        initializer:mock_auth_manager(NewConfig2),

        % Increase permissions_cache size during cache check procedure to prevent cache cleaning during tests
        % (cache is cleaned only when it exceeds size)
        Workers = ?config(op_worker_nodes, NewConfig),
        test_utils:mock_new(Workers, bounded_cache, [passthrough]),
        test_utils:mock_expect(Workers, bounded_cache, check_cache_size, fun
            (#{name := permissions_cache} = Options) -> meck:passthrough([Options#{size := 1000000000}]);
            (Options) -> meck:passthrough([Options])
        end),
        lists:foreach(fun({SpaceId, _}) ->
            lists:foreach(fun(W) ->
                ?assertEqual(ok, rpc:call(W, dir_stats_service_state, enable, [SpaceId])),
                ?assertEqual(enabled, rpc:call(W, dir_stats_service_state, get_extended_status, [SpaceId]), ?ATTEMPTS)
            end, initializer:get_different_domain_workers(NewConfig2))
        end, ?config(spaces, NewConfig2)),
        NewConfig2
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, ?MODULE]} | Config].


end_per_suite(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, bounded_cache),

    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:teardown_storage(Config).


init_per_testcase(multi_provider_permission_cache_test, Config) ->
    ct:timetrap({minutes, 15}),
    init_per_testcase(default, Config);

init_per_testcase(_Case, Config) ->
    initializer:mock_share_logic(Config),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
check_perms(Node, User, Guid, Perms, Config) ->
    SessId = ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config),
    UserCtx = rpc:call(Node, user_ctx, new, [SessId]),

    rpc:call(Node, ?MODULE, check_perms, [
        UserCtx, file_ctx:new_by_guid(Guid),
        [?OPERATIONS(permissions_test_utils:perms_to_bitmask(Perms))]
    ]).


%% @private
check_perms(UserCtx, FileCtx, Perms) ->
    try
        fslogic_authz:ensure_authorized(UserCtx, FileCtx, Perms),
        ok
    catch _Type:Reason ->
        {error, Reason}
    end.
