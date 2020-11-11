%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Runs various permissions test scenarios (e.g. space privs, posix mode, acl)
%%% using specified perms_test_spec().
%%% Scenarios asserts that combinations not having all required perms
%%% (specified in perms_test_spec()) fails and that combination
%%% consisting of only required perms succeeds.
%%% @end
%%%-------------------------------------------------------------------
-module(permissions_test_runner).
-author("Bartosz Walkowicz").

-include("../storage_files_test_SUITE.hrl").
-include("permissions_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

-export([run_scenarios/2]).


-type ct_config() :: proplists:proplist().
-type perms_test_spec() :: #perms_test_spec{}.

-type perms_per_file() :: #{file_id:file_guid() => [FilePerm :: binary()]}.
-type posix_perms_per_file() :: #{file_id:file_guid() => [PosixPerm :: atom()]}.

-record(scenario_ctx, {
    meta_spec :: perms_test_spec(),
    scenario_name :: binary(),
    scenario_root_dir_path :: file_meta:path(),
    files_owner_session_id :: session:id(),
    executioner_session_id :: session:id(),
    required_space_privs = [] :: owner | [privileges:space_privilege()],
    required_perms_per_file = #{} :: perms_per_file(),
    extra_data = #{} :: map()
}).
-type scenario_ctx() :: #scenario_ctx{}.


-define(assertMatchWithPerms(__Expect, __Expression, __ScenarioName, __Node, __FileOwnerSessId, __PermsPerGuid),
    (fun() ->
        try
            ?assertMatch(__Expect, __Expression)
        catch _:__Reason ->
            ct:pal(
                "PERMISSIONS TESTS FAILURE~n"
                "   Scenario: ~p~n"
                "   Perms per file: ~p~n"
                "   Reason: ~p~n",
                [
                    __ScenarioName,
                    maps:fold(fun(G, SetPerms, Acc) ->
                        Acc#{get_file_path(__Node, __FileOwnerSessId, G) => SetPerms}
                    end, #{}, __PermsPerGuid),
                    __Reason
                ]
            ),
            erlang:error(perms_test_failed)
        end
    end)()
).

-define(SCENARIO_NAME(__PREFIX, __TYPE),
    <<__PREFIX, (atom_to_binary(__TYPE, utf8))/binary>>
).

-define(SCENARIO_DIR(__ROOT_DIR, __SCENARIO_NAME),
    <<
        __ROOT_DIR/binary,
        "/",
        __SCENARIO_NAME/binary
    >>
).


%%%===================================================================
%%% TEST MECHANISM
%%%===================================================================


-spec run_scenarios(perms_test_spec(), ct_config()) -> ok | no_return().
run_scenarios(TestSpec, Config) ->
    ScenariosRootDirPath = create_all_scenarios_root_dir(TestSpec, Config),

    run_space_privs_scenarios(ScenariosRootDirPath, TestSpec, Config),
    run_data_access_caveats_scenarios(ScenariosRootDirPath, TestSpec, Config),
    run_share_test_scenarios(ScenariosRootDirPath, TestSpec, Config),
    run_posix_perms_scenarios(ScenariosRootDirPath, TestSpec, Config),
    run_acl_perms_scenarios(ScenariosRootDirPath, TestSpec, Config),
    run_space_owner_test_scenarios(ScenariosRootDirPath, TestSpec, Config).


%%%===================================================================
%%% SPACE PRIVILEGES TESTS SCENARIOS MECHANISM
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tests space permissions needed to perform #perms_test_spec.operation.
%% It will setup environment, add full posix or acl permissions and
%% assert that without space privs operation cannot be performed and
%% with it it succeeds.
%% @end
%%--------------------------------------------------------------------
-spec run_space_privs_scenarios(file_meta:path(), perms_test_spec(), ct_config()) ->
    ok | no_return().
run_space_privs_scenarios(ScenariosRootDirPath, #perms_test_spec{
    test_node = Node,
    space_id = SpaceId,
    owner_user = FileOwner,
    space_user = SpaceUserId,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    posix_requires_space_privs = PosixSpacePrivs,
    acl_requires_space_privs = AclSpacePrivs,
    files = Files
} = TestSpec, Config) ->
    FileOwnerUserSessId = ?config({session_id, {FileOwner, ?GET_DOMAIN(Node)}}, Config),

    lists:foreach(fun({ScenarioType, RequiredPrivs}) ->
        ScenarioName = ?SCENARIO_NAME("space_privs_", ScenarioType),
        ScenarioRootDirPath = ?SCENARIO_DIR(ScenariosRootDirPath, ScenarioName),

        % Create necessary file hierarchy
        {PermsPerFile, ExtraData} = create_files(
            Node, FileOwnerUserSessId, ScenariosRootDirPath, #dir{
                name = ScenarioName,
                perms = case RequiresTraverseAncestors of
                    true -> [?traverse_container];
                    false -> []
                end,
                children = Files
            }
        ),

        % Set all posix or acl (depending on scenario) perms to files
        set_full_perms(ScenarioType, Node, maps:keys(PermsPerFile)),

        % Assert that even with all perms set operation cannot be performed
        % without space privileges
        ScenarioCtx = #scenario_ctx{
            meta_spec = TestSpec,
            scenario_name = ScenarioName,
            scenario_root_dir_path = ScenarioRootDirPath,
            files_owner_session_id = FileOwnerUserSessId,
            executioner_session_id = ?config({session_id, {SpaceUserId, ?GET_DOMAIN(Node)}}, Config),
            required_space_privs = RequiredPrivs,
            extra_data = ExtraData
        },

        try
            space_privs_test(ScenarioCtx)
        catch _:Reason ->
            ct:pal("SPACE PRIVS TEST FAILURE~n"
                   "   Scenario: ~p~n"
                   "   File owner: ~p~n"
                   "   Test User: ~p~n"
                   "   Root path: ~p~n"
                   "   Required space priv: ~p~n"
                   "   Reason: ~p~n", [
                ScenarioName, FileOwner, SpaceUserId, ScenarioRootDirPath, RequiredPrivs, Reason
            ]),
            erlang:error(space_privs_test_failed)
        after
            initializer:testmaster_mock_space_user_privileges(
                [Node], SpaceId, FileOwner, privileges:space_admin()
            ),
            initializer:testmaster_mock_space_user_privileges(
                [Node], SpaceId, SpaceUserId, privileges:space_admin()
            )
        end
    end, [
        {posix, PosixSpacePrivs},
        {acl, AclSpacePrivs}
    ]).


%% @private
-spec space_privs_test(scenario_ctx()) -> ok | no_return().
space_privs_test(ScenarioCtx = #scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        space_id = SpaceId,
        owner_user = FileOwnerId,
        space_user = UserId,
        operation = Operation
    },
    scenario_root_dir_path = ScenarioRootDirPath,
    files_owner_session_id = FileOwnerSessId,
    executioner_session_id = UserSessId,
    required_space_privs = owner,
    extra_data = ExtraData
}) ->
    % invalidate permission cache as it is not done due to initializer mocks
    invalidate_perms_cache(Node),

    initializer:testmaster_mock_space_user_privileges([Node], SpaceId, UserId, privileges:space_admin()),
    ?assertMatch({error, ?EACCES}, Operation(UserSessId, ScenarioRootDirPath, ExtraData)),

    initializer:testmaster_mock_space_user_privileges([Node], SpaceId, FileOwnerId, []),
    ?assertMatch(ok, Operation(FileOwnerSessId, ScenarioRootDirPath, ExtraData)),
    run_final_ownership_check(ScenarioCtx#scenario_ctx{executioner_session_id = FileOwnerSessId});

space_privs_test(ScenarioCtx = #scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        space_id = SpaceId,
        space_user = UserId,
        operation = Operation
    },
    scenario_root_dir_path = ScenarioRootDirPath,
    executioner_session_id = ExecutionerSessId,
    required_space_privs = RequiredPrivs,
    extra_data = ExtraData
}) ->
    AllSpacePrivs = privileges:space_admin(),

    % If operation requires space privileges it should fail without any of them
    lists:foreach(fun(SomeOfRequiredPrivs) ->
        initializer:testmaster_mock_space_user_privileges(
            [Node], SpaceId, UserId, AllSpacePrivs -- SomeOfRequiredPrivs
        ),
        ?assertMatch({error, ?EACCES}, Operation(ExecutionerSessId, ScenarioRootDirPath, ExtraData))
    end, combinations(RequiredPrivs) -- [[]]),

    % invalidate permission cache as it is not done due to initializer mocks
    invalidate_perms_cache(Node),

    initializer:testmaster_mock_space_user_privileges([Node], SpaceId, UserId, RequiredPrivs),
    ?assertMatch(ok, Operation(ExecutionerSessId, ScenarioRootDirPath, ExtraData)),
    run_final_ownership_check(ScenarioCtx).


%%%===================================================================
%%% CAVEATS TESTS SCENARIOS MECHANISM
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tests data caveats. For that it will setup environment,
%% add full acl permissions and assert that even with full perms set
%% operations can be performed only when caveats (data constraints)
%% allow it.
%% @end
%%--------------------------------------------------------------------
-spec run_data_access_caveats_scenarios(file_meta:path(), perms_test_spec(), ct_config()) ->
    ok | no_return().
run_data_access_caveats_scenarios(ScenariosRootDirPath, #perms_test_spec{
    test_node = Node,
    space_id = SpaceId,
    owner_user = FileOwner,
    space_user = User,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    files = Files
} = TestSpec, Config) ->
    initializer:testmaster_mock_space_user_privileges(
        [Node], SpaceId, User, privileges:space_admin()
    ),
    FileOwnerUserSessId = ?config({session_id, {FileOwner, ?GET_DOMAIN(Node)}}, Config),
    MainToken = initializer:create_access_token(User),

    lists:foreach(fun(ScenarioType) ->
        ScenarioName = ?SCENARIO_NAME("cv_", ScenarioType),
        ScenarioRootDirPath = ?SCENARIO_DIR(ScenariosRootDirPath, ScenarioName),

        % Create necessary file hierarchy
        {PermsPerFile, ExtraData} = create_files(
            Node, FileOwnerUserSessId, ScenariosRootDirPath, #dir{
                name = ScenarioName,
                perms = case RequiresTraverseAncestors of
                    true -> [?traverse_container];
                    false -> []
                end,
                children = Files
            }
        ),

        set_full_perms(acl, Node, maps:keys(PermsPerFile)),

        % Assert that even with all perms set operation can be performed
        % only when caveats allows it
        ScenarioCtx = #scenario_ctx{
            meta_spec = TestSpec,
            scenario_name = ScenarioName,
            scenario_root_dir_path = ScenarioRootDirPath,
            files_owner_session_id = FileOwnerUserSessId,
            % special user session will be made using prepared token later during test
            executioner_session_id = <<"undefined">>,
            extra_data = ExtraData
        },
        run_caveats_scenario(ScenarioCtx, MainToken)

    end, [data_path, data_objectid, data_readonly]).


%% @private
-spec run_caveats_scenario(scenario_ctx(), tokens:serialized()) ->
    ok | no_return().
run_caveats_scenario(ScenarioCtx = #scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        space_user = UserId,
        operation = Operation
    },
    scenario_name = <<"cv_data_path">>,
    scenario_root_dir_path = ScenarioRootDirPath,
    extra_data = ExtraData
}, MainToken) ->
    Token1 = tokens:confine(MainToken, #cv_data_path{whitelist = [<<"i_am_nowhere">>]}),
    SessId1 = permissions_test_utils:create_session(Node, UserId, Token1),
    ?assertMatch({error, ?EACCES}, Operation(SessId1, ScenarioRootDirPath, ExtraData)),

    Token2 = tokens:confine(MainToken, #cv_data_path{whitelist = [ScenarioRootDirPath]}),
    SessId2 = permissions_test_utils:create_session(Node, UserId, Token2),
    ?assertMatch(ok, Operation(SessId2, ScenarioRootDirPath, ExtraData)),
    run_final_ownership_check(ScenarioCtx#scenario_ctx{executioner_session_id = SessId2});

run_caveats_scenario(ScenarioCtx = #scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        space_user = UserId,
        operation = Operation
    },
    scenario_name = <<"cv_data_objectid">>,
    scenario_root_dir_path = ScenarioRootDirPath,
    extra_data = ExtraData
}, MainToken) ->
    {guid, ScenarioRootDirGuid} = maps:get(ScenarioRootDirPath, ExtraData),
    {ok, ScenarioRootDirObjectId} = file_id:guid_to_objectid(ScenarioRootDirGuid),

    DummyGuid = <<"Z3VpZCNfaGFzaF9mNmQyOGY4OTNjOTkxMmVh">>,
    {ok, DummyObjectId} = file_id:guid_to_objectid(DummyGuid),

    Token1 = tokens:confine(MainToken, #cv_data_objectid{whitelist = [DummyObjectId]}),
    SessId1 = permissions_test_utils:create_session(Node, UserId, Token1),
    ?assertMatch({error, ?EACCES}, Operation(SessId1, ScenarioRootDirPath, ExtraData)),

    Token2 = tokens:confine(MainToken, #cv_data_objectid{whitelist = [ScenarioRootDirObjectId]}),
    SessId2 = permissions_test_utils:create_session(Node, UserId, Token2),
    ?assertMatch(ok, Operation(SessId2, ScenarioRootDirPath, ExtraData), 100),
    run_final_ownership_check(ScenarioCtx#scenario_ctx{executioner_session_id = SessId2});

run_caveats_scenario(ScenarioCtx = #scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        space_user = UserId,
        available_in_readonly_mode = AvailableInReadonlyMode,
        operation = Operation
    },
    scenario_name = <<"cv_data_readonly">>,
    scenario_root_dir_path = ScenarioRootDirPath,
    extra_data = ExtraData
}, MainToken) ->
    Token = tokens:confine(MainToken, #cv_data_readonly{}),
    SessId = permissions_test_utils:create_session(Node, UserId, Token),

    case AvailableInReadonlyMode of
        true ->
            ?assertMatch(ok, Operation(SessId, ScenarioRootDirPath, ExtraData)),
            run_final_ownership_check(ScenarioCtx#scenario_ctx{executioner_session_id = SessId});
        false ->
            ?assertMatch({error, ?EACCES}, Operation(SessId, ScenarioRootDirPath, ExtraData))
    end.


%%%===================================================================
%%% SHARE TESTS SCENARIOS MECHANISM
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tests permissions needed to perform operation in share mode.
%% If operation is not nat available in share mode then checks
%% that even with all perms set ?EACCES should be returned.
%% @end
%%--------------------------------------------------------------------
-spec run_share_test_scenarios(file_meta:path(), perms_test_spec(), ct_config()) ->
    ok | no_return().
run_share_test_scenarios(_ScenariosRootDirPath, #perms_test_spec{
    available_in_share_mode = inapplicable
}, _Config) ->
    ok;
run_share_test_scenarios(ScenariosRootDirPath, #perms_test_spec{
    test_node = Node,
    owner_user = FileOwner,
    space_user = SpaceUser,
    other_user = OtherUser,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    files = Files
} = TestSpec, Config) ->
    FileOwnerUserSessId = ?config({session_id, {FileOwner, ?GET_DOMAIN(Node)}}, Config),
    SpaceUserSessId = ?config({session_id, {SpaceUser, ?GET_DOMAIN(Node)}}, Config),
    OtherUserSessId = ?config({session_id, {OtherUser, ?GET_DOMAIN(Node)}}, Config),

    lists:foreach(fun({ExecutionerSessId, PermsType, ScenarioName}) ->
        ScenarioRootDirPath = ?SCENARIO_DIR(ScenariosRootDirPath, ScenarioName),

        % Create necessary file hierarchy
        {PermsPerFile, ExtraData0} = create_files(
            Node, FileOwnerUserSessId, ScenariosRootDirPath, #dir{
                name = ScenarioName,
                perms = case RequiresTraverseAncestors of
                    true -> [?traverse_container];
                    false -> []
                end,
                children = Files
            }
        ),

        TestCaseRootDirKey = maps:get(ScenarioRootDirPath, ExtraData0),
        {ok, ShareId} = lfm_proxy:create_share(
            Node, FileOwnerUserSessId, TestCaseRootDirKey, ScenarioName
        ),
        ExtraData1 = maps:map(fun
            (_, {guid, FileGuid}) ->
                {guid, file_id:guid_to_share_guid(FileGuid, ShareId)};
            (_, Val) ->
                Val
        end, ExtraData0),

        ScenarioCtx = #scenario_ctx{
            meta_spec = TestSpec,
            scenario_name = ScenarioName,
            scenario_root_dir_path = ScenarioRootDirPath,
            files_owner_session_id = FileOwnerUserSessId,
            executioner_session_id = ExecutionerSessId,
            required_perms_per_file = PermsPerFile,
            extra_data = ExtraData1
        },
        run_share_test_scenario(ScenarioCtx, PermsType)
    end, [
        {FileOwnerUserSessId, posix, <<"owner_posix_share">>},
        {SpaceUserSessId, posix, <<"space_user_posix_share">>},
        {OtherUserSessId, posix, <<"other_user_posix_share">>},
        {?GUEST_SESS_ID, posix, <<"guest_posix_share">>},
        {FileOwnerUserSessId, {acl, allow}, <<"owner_acl_allow_share">>},
        {FileOwnerUserSessId, {acl, deny}, <<"owner_acl_deny_share">>},
        {SpaceUserSessId, {acl, allow}, <<"space_user_acl_allow_share">>},
        {SpaceUserSessId, {acl, deny}, <<"space_user_acl_deny_share">>},
        {OtherUserSessId, {acl, allow}, <<"other_user_acl_allow_share">>},
        {OtherUserSessId, {acl, deny}, <<"other_user_acl_deny_share">>},
        {?GUEST_SESS_ID, {acl, allow}, <<"guest_acl_allow_share">>},
        {?GUEST_SESS_ID, {acl, deny}, <<"guest_acl_deny_share">>}
    ]).


%% @private
-spec run_share_test_scenario(scenario_ctx(), posix | {acl, allow | deny}) ->
    ok | no_return().
run_share_test_scenario(#scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        available_in_share_mode = false,
        operation = Operation
    },
    scenario_name = ScenarioName,
    scenario_root_dir_path = ScenarioRootDirPath,
    files_owner_session_id = FilesOwnerSessId,
    executioner_session_id = ExecutionerSessId,
    required_perms_per_file = PermsPerFile,
    extra_data = ExtraData
}, PermsType0) ->
    % Set all posix or acl (depending on scenario) perms to files
    PermsType1 = case PermsType0 of
        posix -> posix;
        {acl, _} -> acl
    end,
    set_full_perms(PermsType1, Node, maps:keys(PermsPerFile)),

    % Even with all perms set operation should fail
    ?assertMatchWithPerms(
        {error, ?EACCES},
        Operation(ExecutionerSessId, ScenarioRootDirPath, ExtraData),
        ScenarioName, Node, FilesOwnerSessId,
        maps:map(fun(_, _) -> <<"all">> end, PermsPerFile)
    );

run_share_test_scenario(ScenarioCtx = #scenario_ctx{required_perms_per_file = PermsPerFile}, posix) ->
    {ComplementaryPosixPermsPerFile, RequiredPosixPerms} = get_complementary_posix_perms(
        maps:map(fun(_, Perms) -> perms_to_posix_perms(Perms) end, PermsPerFile)
    ),
    run_standard_posix_tests(ScenarioCtx, ComplementaryPosixPermsPerFile, RequiredPosixPerms, other);

run_share_test_scenario(ScenarioCtx, {acl, AllowOrDeny}) ->
    run_acl_perms_scenario(ScenarioCtx, AllowOrDeny, ?everyone, ?no_flags_mask).


%%%===================================================================
%%% POSIX TESTS SCENARIOS MECHANISM
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tests posix permissions needed to perform #perms_test_spec.operation.
%% For each bits group (`owner`, `group`, `other`) it will setup
%% environment and test combination of posix perms. It will assert
%% that combinations not having all required perms fails and that
%% combination consisting of only required perms succeeds.
%% @end
%%--------------------------------------------------------------------
-spec run_posix_perms_scenarios(file_meta:path(), perms_test_spec(), ct_config()) ->
    ok | no_return().
run_posix_perms_scenarios(ScenariosRootDirPath, #perms_test_spec{
    test_node = Node,
    owner_user = FileOwner,
    space_user = User,
    other_user = OtherUser,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    files = Files
} = TestSpec, Config) ->
    FileOwnerUserSessId = ?config({session_id, {FileOwner, ?GET_DOMAIN(Node)}}, Config),
    GroupUserSessId = ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config),
    OtherUserSessId = ?config({session_id, {OtherUser, ?GET_DOMAIN(Node)}}, Config),

    lists:foreach(fun({ScenarioType, ExecutionerSessId}) ->
        ScenarioName = ?SCENARIO_NAME("posix_", ScenarioType),
        ScenarioRootDirPath = ?SCENARIO_DIR(ScenariosRootDirPath, ScenarioName),

        % Create necessary file hierarchy
        {PermsPerFile, ExtraData} = create_files(
            Node, FileOwnerUserSessId, ScenariosRootDirPath, #dir{
                name = ScenarioName,
                perms = case RequiresTraverseAncestors of
                    true -> [?traverse_container];
                    false -> []
                end,
                children = Files
            }
        ),
        PosixPermsPerFile = maps:map(fun(_, Perms) ->
            perms_to_posix_perms(Perms)
        end, PermsPerFile),

        ScenarioCtx = #scenario_ctx{
            meta_spec = TestSpec,
            scenario_name = ScenarioName,
            scenario_root_dir_path = ScenarioRootDirPath,
            files_owner_session_id = FileOwnerUserSessId,
            executioner_session_id = ExecutionerSessId,
            extra_data = ExtraData
        },
        run_posix_perms_scenario(ScenarioCtx, PosixPermsPerFile, ScenarioType)
    end, [
        {owner, FileOwnerUserSessId},
        {group, GroupUserSessId},
        {other, OtherUserSessId}
    ]).


%% @private
-spec run_posix_perms_scenario(scenario_ctx(), posix_perms_per_file(), owner | group | other) ->
    ok | no_return().
run_posix_perms_scenario(ScenarioCtx = #scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node
    },
    scenario_root_dir_path = ScenarioRootDirPath,
    files_owner_session_id = FilesOwnerSessId
}, PosixPermsPerFile, Type) ->
    {ComplementaryPosixPermsPerFile, RequiredPosixPerms} = get_complementary_posix_perms(
        PosixPermsPerFile
    ),

    try
        run_posix_tests(ScenarioCtx, ComplementaryPosixPermsPerFile, RequiredPosixPerms, Type)
    catch _:Reason ->
        FilePathsToRequiredPerms = maps:fold(fun(Guid, RequiredPerms, Acc) ->
            Acc#{get_file_path(Node, FilesOwnerSessId, Guid) => RequiredPerms}
        end, #{}, PosixPermsPerFile),

        ct:pal(
            "POSIX TESTS FAILURE~n"
            "   Type: ~p~n"
            "   Root path: ~p~n"
            "   Required Perms: ~p~n"
            "   Reason: ~p~n",
            [
                Type, ScenarioRootDirPath,
                FilePathsToRequiredPerms,
                Reason
            ]
        ),
        erlang:error(posix_perms_test_failed)
    end.


%% @private
-spec run_posix_tests(
    scenario_ctx(),
    ComplementaryPosixPermsPerFile :: posix_perms_per_file(),
    RequiredPosixPerms :: [{file_id:file_guid(), PosixPerm :: atom()}],
    owner | group | other
) ->
    ok | no_return().
run_posix_tests(ScenarioCtx = #scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        operation = Operation
    },
    scenario_root_dir_path = ScenarioRootDirPath,
    executioner_session_id = ExecutionerSessId,
    extra_data = ExtraData
}, ComplementaryPosixPermsPerFile, AllRequiredPosixPerms, owner) ->
    RequiredPosixPermsWithoutFileOwnership = lists:filter(fun({_, Perm}) ->
        Perm == read orelse Perm == write orelse Perm == exec
    end, AllRequiredPosixPerms),

    case RequiredPosixPermsWithoutFileOwnership of
        [] ->
            % If operation requires only ownership then it should succeed
            % even if all files modes are set to 0
            permissions_test_utils:set_modes(Node, maps:map(fun(_, _) -> 0 end, ComplementaryPosixPermsPerFile)),
            ?assertMatch(ok, Operation(ExecutionerSessId, ScenarioRootDirPath, ExtraData)),
            run_final_ownership_check(ScenarioCtx);
        _ ->
            run_standard_posix_tests(
                ScenarioCtx, ComplementaryPosixPermsPerFile,
                RequiredPosixPermsWithoutFileOwnership, owner
            )
    end;

run_posix_tests(ScenarioCtx = #scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        operation = Operation
    },
    scenario_root_dir_path = ScenarioRootDirPath,
    executioner_session_id = ExecutionerSessId,
    extra_data = ExtraData
}, ComplementaryPosixPermsPerFile, AllRequiredPosixPerms, group) ->
    OperationRequiresFileOwnership = lists:any(fun({_, Perm}) ->
        Perm == owner
    end, AllRequiredPosixPerms),

    case OperationRequiresFileOwnership of
        true ->
            % If operation requires ownership then for group member it should fail
            % even if all files modes are set to 777
            permissions_test_utils:set_modes(
                Node, maps:map(fun(_, _) -> 8#777 end, ComplementaryPosixPermsPerFile)
            ),
            ?assertMatch({error, ?EACCES}, Operation(ExecutionerSessId, ScenarioRootDirPath, ExtraData));
        false ->
            RequiredNormalPosixPerms = lists:filter(fun({_, Perm}) ->
                Perm == read orelse Perm == write orelse Perm == exec
            end, AllRequiredPosixPerms),

            run_standard_posix_tests(
                ScenarioCtx, ComplementaryPosixPermsPerFile, RequiredNormalPosixPerms, group
            )
    end;

% Users not belonging to space or unauthorized should not be able to conduct any operation
run_posix_tests(#scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        operation = Operation
    },
    scenario_root_dir_path = ScenarioRootDirPath,
    executioner_session_id = ExecutionerSessId,
    extra_data = ExtraData
}, ComplementaryPosixPermsPerFile, _AllRequiredPosixPerms, other) ->
    permissions_test_utils:set_modes(Node, maps:map(fun(_, _) -> 8#777 end, ComplementaryPosixPermsPerFile)),
    ?assertMatch({error, ?ENOENT}, Operation(ExecutionerSessId, ScenarioRootDirPath, ExtraData)),

    % Some operations cannot be performed with special session (either root or guest)
    % and result in eagain error instead of enoent
    ?assertMatch({error, _}, Operation(?GUEST_SESS_ID, ScenarioRootDirPath, ExtraData)).


%% @private
-spec run_standard_posix_tests(
    scenario_ctx(),
    posix_perms_per_file(),
    [{file_id:file_guid(), PosixPerm :: atom()}],
    owner | group | other
) ->
    ok | no_return().
run_standard_posix_tests(ScenarioCtx = #scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        operation = Operation
    },
    scenario_name = ScenarioName,
    scenario_root_dir_path = ScenarioRootDirPath,
    files_owner_session_id = FileOwnerSessId,
    executioner_session_id = ExecutionerSessId,
    extra_data = ExtraData
}, ComplementaryPosixPermsPerFile, AllRequiredPosixPerms, Type) ->

    AllRequiredModes = lists:map(fun({Guid, PosixPerm}) ->
        {Guid, permissions_test_utils:posix_perm_to_mode(PosixPerm, Type)}
    end, AllRequiredPosixPerms),
    ComplementaryModesPerFile = maps:map(fun(_, Perms) ->
        lists:foldl(fun(Perm, Acc) ->
            Acc bor permissions_test_utils:posix_perm_to_mode(Perm, Type)
        end, 0, Perms)
    end, ComplementaryPosixPermsPerFile),

    [RequiredModesComb | EaccesModesCombs] = combinations(AllRequiredModes),

    % Granting all modes but required ones should result in eacces
    lists:foreach(fun(EaccesModeComb) ->
        EaccesModesPerFile = lists:foldl(fun({Guid, Mode}, Acc) ->
            Acc#{Guid => Mode bor maps:get(Guid, Acc)}
        end, ComplementaryModesPerFile, EaccesModeComb),

        permissions_test_utils:set_modes(Node, EaccesModesPerFile),

        ?assertMatchWithPerms(
            {error, ?EACCES},
            Operation(ExecutionerSessId, ScenarioRootDirPath, ExtraData),
            ScenarioName, Node, FileOwnerSessId,
            EaccesModesPerFile
        )
    end, EaccesModesCombs),

    % Granting only required modes should result in success
    RequiredModesPerFile = lists:foldl(fun({Guid, Mode}, Acc) ->
        Acc#{Guid => Mode bor maps:get(Guid, Acc, 0)}
    end, #{}, RequiredModesComb),

    permissions_test_utils:set_modes(Node, RequiredModesPerFile),

    ?assertMatchWithPerms(
        ok,
        Operation(ExecutionerSessId, ScenarioRootDirPath, ExtraData),
        ScenarioName, Node, FileOwnerSessId,
        RequiredModesPerFile
    ),
    run_final_ownership_check(ScenarioCtx).


%% @private
-spec perms_to_posix_perms([binary()]) -> [atom()].
perms_to_posix_perms(Perms) ->
    lists:usort(lists:flatmap(
        fun permissions_test_utils:perm_to_posix_perms/1, Perms
    )).


%% @private
-spec get_complementary_posix_perms(RequiredPosixPermsPerFile :: posix_perms_per_file()) ->
    {ComplementaryPosixPermsPerFile :: posix_perms_per_file(), [{file_id:file_guid(), PosixPerm :: atom()}]}.
get_complementary_posix_perms(PosixPermsPerFile)->
    maps:fold(fun(FileGuid, FileRequiredPerms, {BasePermsPerFileAcc, RequiredPermsAcc}) ->
        {
            BasePermsPerFileAcc#{FileGuid => ?ALL_POSIX_PERMS -- FileRequiredPerms},
            [{FileGuid, Perm} || Perm <- FileRequiredPerms] ++ RequiredPermsAcc
        }
    end, {#{}, []}, PosixPermsPerFile).


%%%===================================================================
%%% ACL TESTS SCENARIOS MECHANISM
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tests acl permissions needed to perform #perms_test_spec.operation.
%% For each type (`allow`, `deny`) and identifier (`OWNER@`, user_id,
%% group_id, `EVERYONE@`) it will setup environment and test combination
%% of acl perms. It will assert that combinations not having all required
%% perms fails and that combination consisting of only required perms succeeds.
%% @end
%%--------------------------------------------------------------------
-spec run_acl_perms_scenarios(file_meta:path(), perms_test_spec(), ct_config()) ->
    ok | no_return().
run_acl_perms_scenarios(ScenariosRootDirPath, #perms_test_spec{
    test_node = Node,
    owner_user = FileOwnerUser,
    space_user = SpaceUser,
    space_user_group = SpaceUserGroup,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    files = Files
} = TestSpec, Config) ->
    FileOwnerUserSessId = ?config({session_id, {FileOwnerUser, ?GET_DOMAIN(Node)}}, Config),
    SpaceUserSessId = ?config({session_id, {SpaceUser, ?GET_DOMAIN(Node)}}, Config),

    lists:foreach(fun({ExecutionerSessId, ScenarioType, ScenarioName, AceWho, AceFlags}) ->
        ScenarioRootDirPath = ?SCENARIO_DIR(ScenariosRootDirPath, ScenarioName),

        % Create necessary file hierarchy
        {PermsPerFile, ExtraData} = create_files(
            Node, FileOwnerUserSessId, ScenariosRootDirPath, #dir{
                name = ScenarioName,
                perms = case RequiresTraverseAncestors of
                    true -> [?traverse_container];
                    false -> []
                end,
                children = Files
            }
        ),

        ScenarioCtx = #scenario_ctx{
            meta_spec = TestSpec,
            scenario_name = ScenarioName,
            scenario_root_dir_path = ScenarioRootDirPath,
            files_owner_session_id = FileOwnerUserSessId,
            executioner_session_id = ExecutionerSessId,
            required_perms_per_file = PermsPerFile,
            extra_data = ExtraData
        },
        run_acl_perms_scenario(ScenarioCtx, ScenarioType, AceWho, AceFlags)
    end, [
        {FileOwnerUserSessId, allow, <<"acl_owner_allow">>, ?owner, ?no_flags_mask},
        {SpaceUserSessId, allow, <<"acl_user_allow">>, SpaceUser, ?no_flags_mask},
        {SpaceUserSessId, allow, <<"acl_user_group_allow">>, SpaceUserGroup, ?identifier_group_mask},
        {SpaceUserSessId, allow, <<"acl_everyone_allow">>, ?everyone, ?no_flags_mask},

        {FileOwnerUserSessId, deny, <<"acl_owner_deny">>, ?owner, ?no_flags_mask},
        {SpaceUserSessId, deny, <<"acl_user_deny">>, SpaceUser, ?no_flags_mask},
        {SpaceUserSessId, deny, <<"acl_user_group_deny">>, SpaceUserGroup, ?identifier_group_mask},
        {SpaceUserSessId, deny, <<"acl_everyone_deny">>, ?everyone, ?no_flags_mask}
    ]).


%% @private
-spec run_acl_perms_scenario(
    scenario_ctx(),
    Type :: allow | deny,
    AceWho :: binary(),
    AceFlags :: ace:bitmask()
) ->
    ok | no_return().
run_acl_perms_scenario(ScenarioCtx = #scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        operation = Operation
    },
    scenario_name = ScenarioName,
    scenario_root_dir_path = ScenarioRootDirPath,
    files_owner_session_id = FileOwnerSessId,
    executioner_session_id = ExecutionerSessId,
    required_perms_per_file = PermsPerFile,
    extra_data = ExtraData
}, allow, AceWho, AceFlags) ->
    {ComplementaryPermsPerFile, AllRequiredPerms} = get_complementary_perms(Node, PermsPerFile),
    [AllRequiredPermsComb | EaccesPermsCombs] = combinations(AllRequiredPerms),

    % Granting all perms without required ones should result in {error, ?EACCES}
    lists:foreach(fun(EaccesPermComb) ->
        EaccesPermsPerFile = lists:foldl(fun({Guid, Perm}, Acc) ->
            Acc#{Guid => [Perm | maps:get(Guid, Acc)]}
        end, ComplementaryPermsPerFile, EaccesPermComb),

        permissions_test_utils:set_acls(
            Node, EaccesPermsPerFile, #{}, AceWho, AceFlags
        ),
        ?assertMatchWithPerms(
            {error, ?EACCES},
            Operation(ExecutionerSessId, ScenarioRootDirPath, ExtraData),
            ScenarioName, Node, FileOwnerSessId,
            EaccesPermsPerFile
        )
    end, EaccesPermsCombs),

    % Granting only required perms should result in success
    RequiredPermsPerFile = lists:foldl(fun({Guid, Perm}, Acc) ->
        Acc#{Guid => [Perm | maps:get(Guid, Acc, [])]}
    end, #{}, AllRequiredPermsComb),

    permissions_test_utils:set_acls(
        Node, RequiredPermsPerFile, #{}, AceWho, AceFlags
    ),
    ?assertMatchWithPerms(
        ok,
        Operation(ExecutionerSessId, ScenarioRootDirPath, ExtraData),
        ScenarioName, Node, FileOwnerSessId,
        RequiredPermsPerFile
    ),
    run_final_ownership_check(ScenarioCtx);

run_acl_perms_scenario(ScenarioCtx = #scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        operation = Operation
    },
    scenario_name = ScenarioName,
    scenario_root_dir_path = ScenarioRootDirPath,
    files_owner_session_id = FileOwnerSessId,
    executioner_session_id = ExecutionerSessId,
    required_perms_per_file = PermsPerFile,
    extra_data = ExtraData
}, deny, AceWho, AceFlags) ->

    AllPermsPerFile = maps:map(fun(Guid, _) ->
        permissions_test_utils:all_perms(Node, Guid)
    end, PermsPerFile),
    {ComplementaryPermsPerFile, AllRequiredPerms} = get_complementary_perms(Node, PermsPerFile),

    % Denying only required perms and granting all others should result in eacces
    lists:foreach(fun({Guid, Perm}) ->
        EaccesPermsPerFile = #{Guid => [Perm]},

        permissions_test_utils:set_acls(
            Node, AllPermsPerFile, EaccesPermsPerFile, AceWho, AceFlags
        ),
        ?assertMatchWithPerms(
            {error, ?EACCES},
            Operation(ExecutionerSessId, ScenarioRootDirPath, ExtraData),
            ScenarioName, Node, FileOwnerSessId,
            EaccesPermsPerFile
        )
    end, AllRequiredPerms),

    % Denying all perms but required ones should result in success
    permissions_test_utils:set_acls(
        Node, #{}, ComplementaryPermsPerFile, AceWho, AceFlags
    ),
    ?assertMatchWithPerms(
        ok,
        Operation(ExecutionerSessId, ScenarioRootDirPath, ExtraData),
        ScenarioName, Node, FileOwnerSessId,
        ComplementaryPermsPerFile
    ),
    run_final_ownership_check(ScenarioCtx).


%% @private
-spec get_complementary_perms(node(), RequiredPermsPerFile :: perms_per_file()) ->
    {ComplementaryPermsPerFile :: perms_per_file(), [{file_id:file_guid(), Perm :: binary()}]}.
get_complementary_perms(Node, PermsPerFile)->
    maps:fold(fun(FileGuid, FileRequiredPerms, {BasePermsPerFileAcc, RequiredPermsAcc}) ->
        {
            BasePermsPerFileAcc#{FileGuid => permissions_test_utils:complementary_perms(
                Node, FileGuid, FileRequiredPerms
            )},
            [{FileGuid, Perm} || Perm <- FileRequiredPerms] ++ RequiredPermsAcc
        }
    end, {#{}, []}, PermsPerFile).


%%%===================================================================
%%% SPACE OWNER TESTS SCENARIOS MECHANISM
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tests permissions needed to perform operation as space owner.
%% Generally access to space owner should be granted regardless of posix/acl
%% permissions set or space privileges. The exceptions to this are caveats
%% constraints and access in share mode. In those cases space owner should not
%% be given any special treatment compared to any other space user.
%% @end
%%--------------------------------------------------------------------
-spec run_space_owner_test_scenarios(file_meta:path(), perms_test_spec(), ct_config()) ->
    ok | no_return().
run_space_owner_test_scenarios(ScenariosRootDirPath, #perms_test_spec{
    test_node = Node,
    space_id = SpaceId,
    owner_user = FileOwner,
    space_owner = SpaceOwner,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    files = Files,
    operation = Operation
} = TestSpec, Config) ->
    FileOwnerSessId = ?config({session_id, {FileOwner, ?GET_DOMAIN(Node)}}, Config),
    SpaceOwnerSessId = ?config({session_id, {SpaceOwner, ?GET_DOMAIN(Node)}}, Config),

    % Remove all space privileges for space owner
    initializer:testmaster_mock_space_user_privileges([Node], SpaceId, SpaceOwner, []),

    lists:foreach(fun
        ({share, ScenarioName}) ->
            TestCaseRootDirPath = ?SCENARIO_DIR(ScenariosRootDirPath, ScenarioName),
            ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, FileOwnerSessId, TestCaseRootDirPath, 8#777)),

            run_share_test_scenarios(TestCaseRootDirPath, TestSpec#perms_test_spec{
                space_user = SpaceOwner
            }, Config);
        ({caveats, ScenarioName}) ->
            TestCaseRootDirPath = ?SCENARIO_DIR(ScenariosRootDirPath, ScenarioName),
            ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, FileOwnerSessId, TestCaseRootDirPath, 8#777)),

            run_data_access_caveats_scenarios(TestCaseRootDirPath, TestSpec#perms_test_spec{
                space_user = SpaceOwner
            }, Config);
        ({ScenarioType, ScenarioName}) ->
            TestCaseRootDirPath = ?SCENARIO_DIR(ScenariosRootDirPath, ScenarioName),

            % Create necessary file hierarchy
            {PermsPerFile, ExtraData} = create_files(
                Node, FileOwnerSessId, ScenariosRootDirPath, #dir{
                    name = ScenarioName,
                    perms = case RequiresTraverseAncestors of
                        true -> [?traverse_container];
                        false -> []
                    end,
                    children = Files
                }
            ),

            % Deny all posix or acl (depending on scenario) perms to files
            deny_full_perms(ScenarioType, Node, maps:keys(PermsPerFile)),

            ?assertMatchWithPerms(
                ok,
                Operation(SpaceOwnerSessId, TestCaseRootDirPath, ExtraData),
                ScenarioName, Node, FileOwnerSessId,
                maps:map(fun(_, _) -> <<"none">> end, PermsPerFile)
            ),
            run_final_ownership_check(#scenario_ctx{
                meta_spec = TestSpec,
                scenario_name = ScenarioName,
                scenario_root_dir_path = TestCaseRootDirPath,
                files_owner_session_id = FileOwnerSessId,
                executioner_session_id = SpaceOwnerSessId
            })
        end, [
            {caveats, <<"space_owner_caveats">>},
            {share, <<"space_owner_shares">>},
            {posix, <<"space_owner_posix">>},
            {acl, <<"space_owner_acl">>}
        ]
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_all_scenarios_root_dir(perms_test_spec(), ct_config()) -> file_meta:path().
create_all_scenarios_root_dir(#perms_test_spec{
    test_node = Node,
    space_id = SpaceId,
    owner_user = FileOwner,
    root_dir_name = RootDir
}, Config) ->
    FileOwnerSessId = ?config({session_id, {FileOwner, ?GET_DOMAIN(Node)}}, Config),
    {ok, SpaceName} = rpc:call(Node, space_logic, get_name, [?ROOT_SESS_ID, SpaceId]),

    ScenariosRootDirPath = filename:join(["/", SpaceName, RootDir]),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, FileOwnerSessId, ScenariosRootDirPath, 8#777)),

    ScenariosRootDirPath.


%% @private
-spec create_files(node(), session:id(), file_meta:path(), #dir{} | #file{}) ->
    {#{file_id:file_guid() => [FilePerm :: binary()]}, ExtraData :: map()}.
create_files(Node, FileOwnerSessId, ParentDirPath, #file{
    name = FileName,
    perms = FilePerms,
    on_create = HookFun
}) ->
    FilePath = <<ParentDirPath/binary, "/", FileName/binary>>,
    {ok, FileGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:create(Node, FileOwnerSessId, FilePath, 8#777)
    ),
    permissions_test_utils:ensure_file_created_on_storage(Node, FileGuid),

    ExtraData = case HookFun of
        undefined ->
            #{FilePath => {guid, FileGuid}};
        _ when is_function(HookFun, 2) ->
            #{FilePath => HookFun(FileOwnerSessId, FileGuid)}
    end,
    {#{FileGuid => FilePerms}, ExtraData};
create_files(Node, FileOwnerSessId, ParentDirPath, #dir{
    name = DirName,
    perms = DirPerms,
    on_create = HookFun,
    children = Children
}) ->
    DirPath = <<ParentDirPath/binary, "/", DirName/binary>>,
    {ok, DirGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:mkdir(Node, FileOwnerSessId, DirPath)
    ),
    permissions_test_utils:ensure_dir_created_on_storage(Node, DirGuid),

    {PermsPerFile0, ExtraData0} = lists:foldl(fun(Child, {PermsPerFileAcc, ExtraDataAcc}) ->
        {ChildPerms, ChildExtraData} = create_files(Node, FileOwnerSessId, DirPath, Child),
        {maps:merge(PermsPerFileAcc, ChildPerms), maps:merge(ExtraDataAcc, ChildExtraData)}
    end, {#{}, #{}}, Children),

    ExtraData1 = case HookFun of
        undefined ->
            ExtraData0#{DirPath => {guid, DirGuid}};
        _ when is_function(HookFun, 2) ->
            ExtraData0#{DirPath => HookFun(FileOwnerSessId, DirGuid)}
    end,
    {PermsPerFile0#{DirGuid => DirPerms}, ExtraData1}.


%% @private
-spec get_file_path(node(), session:id(), file_id:file_guid()) ->
    file_meta:path().
get_file_path(Node, SessionId, Guid) ->
    UserCtx = rpc:call(Node, user_ctx, new, [SessionId]),
    {Path, _} = ?assertMatch({_, _}, rpc:call(
        Node,
        file_ctx,
        get_logical_path,
        [file_ctx:new_by_guid(Guid), UserCtx]
    )),
    Path.


%% @private
-spec set_full_perms(posix | acl, node(), [file_id:file_guid()]) -> ok.
set_full_perms(posix, Node, Files) ->
    AllPosixPermsPerFile = lists:foldl(fun(Guid, Acc) ->
        Acc#{Guid => 8#777}
    end, #{}, Files),
    permissions_test_utils:set_modes(Node, AllPosixPermsPerFile);
set_full_perms(acl, Node, Files) ->
    AllAclPermsPerFile = lists:foldl(fun(Guid, Acc) ->
        Acc#{Guid => permissions_test_utils:all_perms(Node, Guid)}
    end, #{}, Files),
    permissions_test_utils:set_acls(
        Node, AllAclPermsPerFile, #{}, ?everyone, ?no_flags_mask
    ).


%% @private
-spec deny_full_perms(posix | acl, node(), [file_id:file_guid()]) -> ok.
deny_full_perms(posix, Node, Files) ->
    AllPosixPermsPerFile = lists:foldl(fun(Guid, Acc) ->
        Acc#{Guid => 8#000}
    end, #{}, Files),
    permissions_test_utils:set_modes(Node, AllPosixPermsPerFile);
deny_full_perms(acl, Node, Files) ->
    AllAclPermsPerFile = lists:foldl(fun(Guid, Acc) ->
        Acc#{Guid => permissions_test_utils:all_perms(Node, Guid)}
    end, #{}, Files),
    permissions_test_utils:set_acls(
        Node, #{}, AllAclPermsPerFile, ?everyone, ?no_flags_mask
    ).


%% @private
-spec invalidate_perms_cache(node()) -> ok.
invalidate_perms_cache(Node) ->
    rpc:call(Node, permissions_cache, invalidate, []).


%% @private
-spec combinations([term()]) -> [[term()]].
combinations([]) ->
    [[]];
combinations([Item | Items]) ->
    Combinations = combinations(Items),
    [[Item | Comb] || Comb <- Combinations] ++ Combinations.


%% @private
-spec run_final_ownership_check(scenario_ctx()) -> ok | no_return().
run_final_ownership_check(#scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        space_id = SpaceId,
        final_ownership_check = FinalOwnershipCheckFun
    },
    scenario_root_dir_path = ScenarioRootDirPath,
    files_owner_session_id = OriginalFileOwnerSessId,
    executioner_session_id = OperationExecutionerSessId
}) ->
    case FinalOwnershipCheckFun(ScenarioRootDirPath) of
        skip ->
            ok;
        {should_preserve_ownership, LogicalFilePath} ->
            permissions_test_utils:assert_user_is_file_owner_on_storage(
                Node, SpaceId, LogicalFilePath, OriginalFileOwnerSessId
            );
        {should_change_ownership, LogicalFilePath} ->
            permissions_test_utils:assert_user_is_file_owner_on_storage(
                Node, SpaceId, LogicalFilePath, OperationExecutionerSessId
            )
    end.
