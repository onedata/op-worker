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
-module(permissions_test_scenarios).
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

-record(scenario_ctx, {
    meta_spec :: perms_test_spec(),
    scenario_name :: binary(),
    scenario_root_dir_path :: file_meta:path(),
    required_space_privs :: owner | [privileges:space_privilege()],
    extra_data :: map()
}).
-type scenario_ctx() :: #scenario_ctx{}.


-define(assertMatchWithPerms(__Expect, __Expression, __ScenarioName, __PermsPerGuid),
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
                        Acc#{get_file_path(Node, FileOwnerSessId, G) => SetPerms}
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
    owner_user = FileOwner,
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
        ScenarioCtx = #scenario_ctx{
            meta_spec = TestSpec,
            scenario_name = ScenarioName,
            scenario_root_dir_path = ScenarioRootDirPath,
            required_space_privs = RequiredPrivs,
            extra_data = ExtraData
        },

        % Set all posix or acl (depending on scenario) perms to files
        set_full_perms(ScenarioType, Node, maps:keys(PermsPerFile)),

        % Assert that even with all perms set operation cannot be performed
        % without space privileges
        run_space_privs_scenario(ScenarioCtx, Config)
    end, [
        {posix, PosixSpacePrivs},
        {acl, AclSpacePrivs}
    ]).


%% @private
-spec run_space_privs_scenario(scenario_ctx(), ct_config()) -> ok | no_return().
run_space_privs_scenario(#scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        space_id = SpaceId,
        owner_user = FileOwner,
        space_user = SpaceUser
    },
    scenario_name = ScenarioName,
    scenario_root_dir_path = ScenarioRootDirPath,
    required_space_privs = RequiredPrivs
} = ScenarioCtx, Config) ->
    try
        space_privs_test(ScenarioCtx, Config)
    catch _:Reason ->
        ct:pal(
            "SPACE PRIVS TEST FAILURE~n"
            "   Scenario: ~p~n"
            "   File owner: ~p~n"
            "   Test User: ~p~n"
            "   Root path: ~p~n"
            "   Required space priv: ~p~n"
            "   Reason: ~p~n",
            [
                ScenarioName,
                FileOwner, SpaceUser,
                ScenarioRootDirPath,
                RequiredPrivs,
                Reason
            ]
        ),
        erlang:error(space_privs_test_failed)
    after
        initializer:testmaster_mock_space_user_privileges(
            [Node], SpaceId, FileOwner, privileges:space_admin()
        ),
        initializer:testmaster_mock_space_user_privileges(
            [Node], SpaceId, SpaceUser, privileges:space_admin()
        )
    end.


%% @private
-spec space_privs_test(scenario_ctx(), ct_config()) -> ok | no_return().
space_privs_test(#scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        space_id = SpaceId,
        owner_user = FileOwnerId,
        space_user = UserId,
        operation = Operation
    } = MetaSpec,
    scenario_root_dir_path = RootDirPath,
    required_space_privs = owner,
    extra_data = ExtraData
}, Config) ->
    UserSessId = ?config({session_id, {UserId, ?GET_DOMAIN(Node)}}, Config),
    FileOwnerSessId = ?config({session_id, {FileOwnerId, ?GET_DOMAIN(Node)}}, Config),

    % invalidate permission cache as it is not done due to initializer mocks
    invalidate_perms_cache(Node),

    initializer:testmaster_mock_space_user_privileges([Node], SpaceId, UserId, privileges:space_admin()),
    ?assertMatch({error, ?EACCES}, Operation(FileOwnerSessId, UserSessId, RootDirPath, ExtraData)),

    initializer:testmaster_mock_space_user_privileges([Node], SpaceId, FileOwnerId, []),
    ?assertMatch(ok, Operation(FileOwnerSessId, FileOwnerSessId, RootDirPath, ExtraData)),
    run_final_ownership_check(FileOwnerSessId, FileOwnerSessId, RootDirPath, MetaSpec);

space_privs_test(#scenario_ctx{
    meta_spec = #perms_test_spec{
        test_node = Node,
        space_id = SpaceId,
        owner_user = FileOwnerId,
        space_user = UserId,
        operation = Operation
    } = MetaSpec,
    scenario_root_dir_path = RootDirPath,
    required_space_privs = RequiredPrivs,
    extra_data = ExtraData
}, Config) ->
    AllSpacePrivs = privileges:space_admin(),
    UserSessId = ?config({session_id, {UserId, ?GET_DOMAIN(Node)}}, Config),
    FileOwnerSessId = ?config({session_id, {FileOwnerId, ?GET_DOMAIN(Node)}}, Config),

    % If operation requires space privilege it should fail without
    lists:foreach(fun(SomeOfRequiredPrivs) ->
        initializer:testmaster_mock_space_user_privileges(
            [Node], SpaceId, UserId, AllSpacePrivs -- SomeOfRequiredPrivs
        ),
        ?assertMatch(
            {error, ?EACCES},
            Operation(FileOwnerSessId, UserSessId, RootDirPath, ExtraData)
        )
    end, combinations(RequiredPrivs) -- [[]]),

    % invalidate permission cache as it is not done due to initializer mocks
    invalidate_perms_cache(Node),

    initializer:testmaster_mock_space_user_privileges([Node], SpaceId, UserId, RequiredPrivs),
    ?assertMatch(ok, Operation(FileOwnerSessId, UserSessId, RootDirPath, ExtraData)),
    run_final_ownership_check(FileOwnerSessId, UserSessId, RootDirPath, MetaSpec).


%%%===================================================================
%%% CAVEATS TESTS SCENARIOS MECHANISM
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Tests data caveats. For that it will setup environment,
%% add full acl permissions and assert that even with full perms set
%% operations can be performed only when caveats (data constraints)
%% allow it.
%% @end
%%--------------------------------------------------------------------
run_data_access_caveats_scenarios(ScenariosRootDirPath, #perms_test_spec{
    test_node = Node,
    space_id = SpaceId,
    owner_user = FileOwner,
    space_user = User,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    files = Files,
    available_in_readonly_mode = IsReadonly,
    operation = Operation
} = TestSpec, Config) ->
    FileOwnerUserSessId = ?config({session_id, {FileOwner, ?GET_DOMAIN(Node)}}, Config),
    MainToken = initializer:create_access_token(User),
    initializer:testmaster_mock_space_user_privileges(
        [Node], SpaceId, User, privileges:space_admin()
    ),

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
        run_caveats_scenario(
            Node, MainToken, FileOwnerUserSessId, User, Operation, ScenarioType,
            ScenarioRootDirPath, ExtraData, IsReadonly, TestSpec
        )
    end, [data_path, data_objectid, data_readonly]).


run_caveats_scenario(
    Node, MainToken, FileOwnerUserSessId, User, Operation, data_path,
    ScenarioRootDirPath, ExtraData, _IsReadonly, TestSpec
) ->
    Token1 = tokens:confine(MainToken, #cv_data_path{whitelist = [<<"i_am_nowhere">>]}),
    SessId1 = permissions_test_utils:create_session(Node, User, Token1),
    ?assertMatch(
        {error, ?EACCES},
        Operation(FileOwnerUserSessId, SessId1, ScenarioRootDirPath, ExtraData)
    ),

    Token2 = tokens:confine(MainToken, #cv_data_path{
        whitelist = [ScenarioRootDirPath]
    }),
    SessId2 = permissions_test_utils:create_session(Node, User, Token2),
    ?assertMatch(
        ok,
        Operation(FileOwnerUserSessId, SessId2, ScenarioRootDirPath, ExtraData)
    ),
    run_final_ownership_check(
        FileOwnerUserSessId, SessId2, ScenarioRootDirPath, TestSpec
    );
run_caveats_scenario(
    Node, MainToken, FileOwnerUserSessId, User, Operation, data_objectid,
    ScenarioRootDirPath, ExtraData, _IsReadonly, TestSpec
) ->
    {guid, ScenarioRootDirGuid} = maps:get(ScenarioRootDirPath, ExtraData),
    {ok, ScenarioRootDirObjectId} = file_id:guid_to_objectid(ScenarioRootDirGuid),

    DummyGuid = <<"Z3VpZCNfaGFzaF9mNmQyOGY4OTNjOTkxMmVh">>,
    {ok, DummyObjectId} = file_id:guid_to_objectid(DummyGuid),

    Token1 = tokens:confine(MainToken, #cv_data_objectid{whitelist = [DummyObjectId]}),
    SessId1 = permissions_test_utils:create_session(Node, User, Token1),
    ?assertMatch(
        {error, ?EACCES},
        Operation(FileOwnerUserSessId, SessId1, ScenarioRootDirPath, ExtraData)
    ),

    Token2 = tokens:confine(MainToken, #cv_data_objectid{
        whitelist = [ScenarioRootDirObjectId]
    }),
    SessId2 = permissions_test_utils:create_session(Node, User, Token2),
    ?assertMatch(
        ok,
        Operation(FileOwnerUserSessId, SessId2, ScenarioRootDirPath, ExtraData), 100
    ),
    run_final_ownership_check(
        FileOwnerUserSessId, SessId2, ScenarioRootDirPath, TestSpec
    );
run_caveats_scenario(
    Node, MainToken, FileOwnerUserSessId, User, Operation, data_readonly,
    ScenarioRootDirPath, ExtraData, IsReadonly, TestSpec
) ->
    Token = tokens:confine(MainToken, #cv_data_readonly{}),
    SessId = permissions_test_utils:create_session(Node, User, Token),
    case IsReadonly of
        true ->
            % Operation should succeed
            ?assertMatch(
                ok,
                Operation(FileOwnerUserSessId, SessId, ScenarioRootDirPath, ExtraData)
            ),
            run_final_ownership_check(
                FileOwnerUserSessId, SessId, ScenarioRootDirPath, TestSpec
            );
        false ->
            % Operation should fail
            ?assertMatch(
                {error, ?EACCES},
                Operation(FileOwnerUserSessId, SessId, ScenarioRootDirPath, ExtraData)
            )
    end.


%%%===================================================================
%%% SHARE TESTS SCENARIOS MECHANISM
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Tests permissions needed to perform operation in share mode.
%% If operation is not nat available in share mode then checks
%% that even with all perms set ?EACCES should be returned.
%% @end
%%--------------------------------------------------------------------
run_share_test_scenarios(_ScenariosRootDirPath, #perms_test_spec{
    available_in_share_mode = inapplicable
}, _Config) ->
    ok;
run_share_test_scenarios(ScenariosRootDirPath, #perms_test_spec{
    test_node = Node,
    owner_user = FileOwner,
    space_user = SpaceUser,
    other_user = OtherUser,
    available_in_share_mode = IsAvailableInShareMode,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    files = Files,
    operation = Operation
} = TestSpec, Config) ->
    FileOwnerUserSessId = ?config({session_id, {FileOwner, ?GET_DOMAIN(Node)}}, Config),
    SpaceUserSessId = ?config({session_id, {SpaceUser, ?GET_DOMAIN(Node)}}, Config),
    OtherUserSessId = ?config({session_id, {OtherUser, ?GET_DOMAIN(Node)}}, Config),

    lists:foreach(fun({SessId, PermsType, ScenarioName}) ->
        TestCaseRootDirPath = ?SCENARIO_DIR(ScenariosRootDirPath, ScenarioName),

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

        TestCaseRootDirKey = maps:get(TestCaseRootDirPath, ExtraData0),
        {ok, ShareId} = lfm_proxy:create_share(
            Node, FileOwnerUserSessId, TestCaseRootDirKey, ScenarioName
        ),
        ExtraData1 = maps:map(fun
            (_, {guid, FileGuid}) ->
                {guid, file_id:guid_to_share_guid(FileGuid, ShareId)};
            (_, Val) ->
                Val
        end, ExtraData0),

        run_share_test_scenario(
            Node, FileOwnerUserSessId, SessId, TestCaseRootDirPath, ScenarioName, Operation,
            PermsPerFile, ExtraData1, PermsType, IsAvailableInShareMode, TestSpec
        )
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


run_share_test_scenario(
    Node, FileOwnerSessId, SessId, TestCaseRootDirPath, ScenarioName, Operation,
    PermsPerFile, ExtraData, PermsType0, _IsAvailableInShareMode = false, _TestSpec
) ->
    % Set all posix or acl (depending on scenario) perms to files
    PermsType1 = case PermsType0 of
        posix -> posix;
        {acl, _} -> acl
    end,
    set_full_perms(PermsType1, Node, maps:keys(PermsPerFile)),

    % Even with all perms set operation should fail
    ?assertMatchWithPerms(
        {error, ?EACCES},
        Operation(FileOwnerSessId, SessId, TestCaseRootDirPath, ExtraData),
        ScenarioName,
        maps:map(fun(_, _) -> <<"all">> end, PermsPerFile)
    );
run_share_test_scenario(
    Node, FileOwnerSessId, SessId, TestCaseRootDirPath, ScenarioName, Operation,
    PermsPerFile, ExtraData, posix, _IsAvailableInShareMode = true, TestSpec
) ->
    {ComplementaryPosixPermsPerFile, RequiredPosixPerms} = get_complementary_posix_perms(
        maps:map(fun(_, Perms) -> perms_to_posix_perms(Perms) end, PermsPerFile)
    ),
    run_standard_posix_tests(
        Node, FileOwnerSessId, SessId, TestCaseRootDirPath, ScenarioName,
        Operation, ComplementaryPosixPermsPerFile, RequiredPosixPerms,
        ExtraData, other, TestSpec
    );
run_share_test_scenario(
    Node, FileOwnerSessId, SessId, TestCaseRootDirPath, ScenarioName, _Operation,
    PermsPerFile, ExtraData, {acl, AllowOrDeny}, _IsAvailableInShareMode = true, TestSpec
) ->
    {ComplementaryPermsPerFile, AllRequiredPerms} = get_complementary_perms(
        Node, PermsPerFile
    ),
    run_acl_perms_scenario(
        Node, FileOwnerSessId, SessId, TestCaseRootDirPath, ScenarioName,
        ComplementaryPermsPerFile, AllRequiredPerms, ExtraData,
        ?everyone, ?no_flags_mask, AllowOrDeny, TestSpec
    ).


%%%===================================================================
%%% POSIX TESTS SCENARIOS MECHANISM
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Tests posix permissions needed to perform #perms_test_spec.operation.
%% For each bits group (`owner`, `group`, `other`) it will setup
%% environment and test combination of posix perms. It will assert
%% that combinations not having all required perms fails and that
%% combination consisting of only required perms succeeds.
%% @end
%%--------------------------------------------------------------------
run_posix_perms_scenarios(ScenariosRootDirPath, #perms_test_spec{
    test_node = Node,
    owner_user = FileOwner,
    space_user = User,
    other_user = OtherUser,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    files = Files,
    operation = Operation
} = TestSpec, Config) ->
    FileOwnerUserSessId = ?config({session_id, {FileOwner, ?GET_DOMAIN(Node)}}, Config),
    GroupUserSessId = ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config),
    OtherUserSessId = ?config({session_id, {OtherUser, ?GET_DOMAIN(Node)}}, Config),

    lists:foreach(fun({ScenarioType, SessId}) ->
        ScenarioName = ?SCENARIO_NAME("posix_", ScenarioType),
        TestCaseRootDirPath = ?SCENARIO_DIR(ScenariosRootDirPath, ScenarioName),

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

        run_posix_perms_scenario(
            Node, FileOwnerUserSessId, SessId, TestCaseRootDirPath, ScenarioName,
            Operation, PosixPermsPerFile, ExtraData, ScenarioType, TestSpec
        )
    end, [
        {owner, FileOwnerUserSessId},
        {group, GroupUserSessId},
        {other, OtherUserSessId}
    ]).


run_posix_perms_scenario(
    Node, FileOwnerSessId, SessId, TestCaseRootDirPath, ScenarioName,
    Operation, PosixPermsPerFile, ExtraData, Type, TestSpec
) ->
    {ComplementaryPosixPermsPerFile, RequiredPosixPerms} = get_complementary_posix_perms(
        PosixPermsPerFile
    ),

    try
        run_posix_tests(
            Node, FileOwnerSessId, SessId, TestCaseRootDirPath, ScenarioName,
            Operation, ComplementaryPosixPermsPerFile,
            RequiredPosixPerms, ExtraData, Type, TestSpec
        )
    catch _:Reason ->
        FilePathsToRequiredPerms = maps:fold(fun(Guid, RequiredPerms, Acc) ->
            Acc#{get_file_path(Node, FileOwnerSessId, Guid) => RequiredPerms}
        end, #{}, PosixPermsPerFile),

        ct:pal(
            "POSIX TESTS FAILURE~n"
            "   Type: ~p~n"
            "   Root path: ~p~n"
            "   Required Perms: ~p~n"
            "   Reason: ~p~n",
            [
                Type, TestCaseRootDirPath,
                FilePathsToRequiredPerms,
                Reason
            ]
        ),
        erlang:error(posix_perms_test_failed)
    end.


run_posix_tests(
    Node, FileOwnerSessId, SessId, TestCaseRootDirPath, ScenarioName, Operation,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, owner, TestSpec
) ->
    RequiredPermsWithoutFileOwnership = lists:filter(fun({_, Perm}) ->
        Perm == read orelse Perm == write orelse Perm == exec
    end, AllRequiredPerms),

    case RequiredPermsWithoutFileOwnership of
        [] ->
            % If operation requires only ownership then it should succeed
            % even if all files modes are set to 0
            permissions_test_utils:set_modes(Node, maps:map(fun(_, _) -> 0 end, ComplementaryPermsPerFile)),
            ?assertMatch(
                ok,
                Operation(FileOwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
            ),
            run_final_ownership_check(
                FileOwnerSessId, SessId, TestCaseRootDirPath, TestSpec
            );
        _ ->
            run_standard_posix_tests(
                Node, FileOwnerSessId, SessId, TestCaseRootDirPath, ScenarioName,
                Operation, ComplementaryPermsPerFile, RequiredPermsWithoutFileOwnership,
                ExtraData, owner, TestSpec
            )
    end;

run_posix_tests(
    Node, FileOwnerSessId, SessId, TestCaseRootDirPath, ScenarioName, Operation,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, group, TestSpec
) ->
    OperationRequiresFileOwnership = lists:any(fun({_, Perm}) ->
        Perm == owner
    end, AllRequiredPerms),

    case OperationRequiresFileOwnership of
        true ->
            % If operation requires ownership then for group member it should fail
            % even if all files modes are set to 777
            permissions_test_utils:set_modes(Node, maps:map(fun(_, _) -> 8#777 end, ComplementaryPermsPerFile)),
            ?assertMatch(
                {error, ?EACCES},
                Operation(FileOwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
            );
        false ->
            RequiredNormalPosixPerms = lists:filter(fun({_, Perm}) ->
                Perm == read orelse Perm == write orelse Perm == exec
            end, AllRequiredPerms),
            run_standard_posix_tests(
                Node, FileOwnerSessId, SessId, TestCaseRootDirPath, ScenarioName,
                Operation, ComplementaryPermsPerFile, RequiredNormalPosixPerms,
                ExtraData, group, TestSpec
            )
    end;

% Users not belonging to space or unauthorized should not be able to conduct any operation
run_posix_tests(
    Node, FileOwnerSessId, SessId, TestCaseRootDirPath, _ScenarioName, Operation,
    ComplementaryPermsPerFile, _AllRequiredPerms, ExtraData, other, _TestSpec
) ->
    permissions_test_utils:set_modes(Node, maps:map(fun(_, _) -> 8#777 end, ComplementaryPermsPerFile)),
    ?assertMatch(
        {error, ?ENOENT},
        Operation(FileOwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
    ),
    % Some operations cannot be performed with special session (either root or guest)
    % and result in eagain error instead of enoent
    ?assertMatch(
        {error, _},
        Operation(FileOwnerSessId, ?GUEST_SESS_ID, TestCaseRootDirPath, ExtraData)
    ).


run_standard_posix_tests(
    Node, FileOwnerSessId, SessId, TestCaseRootDirPath, ScenarioName, Operation,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, Type, TestSpec
) ->
    AllRequiredModes = lists:map(fun({Guid, PosixPerm}) ->
        {Guid, permissions_test_utils:posix_perm_to_mode(PosixPerm, Type)}
    end, AllRequiredPerms),
    ComplementaryModesPerFile = maps:map(fun(_, Perms) ->
        lists:foldl(fun(Perm, Acc) ->
            Acc bor permissions_test_utils:posix_perm_to_mode(Perm, Type)
        end, 0, Perms)
    end, ComplementaryPermsPerFile),

    [RequiredModesComb | EaccesModesCombs] = combinations(AllRequiredModes),

    % Granting all modes but required ones should result in eacces
    lists:foreach(fun(EaccessModeComb) ->
        EaccesModesPerFile = lists:foldl(fun({Guid, Mode}, Acc) ->
            Acc#{Guid => Mode bor maps:get(Guid, Acc)}
        end, ComplementaryModesPerFile, EaccessModeComb),

        permissions_test_utils:set_modes(Node, EaccesModesPerFile),

        ?assertMatchWithPerms(
            {error, ?EACCES},
            Operation(FileOwnerSessId, SessId, TestCaseRootDirPath, ExtraData),
            ScenarioName,
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
        Operation(FileOwnerSessId, SessId, TestCaseRootDirPath, ExtraData),
        ScenarioName,
        RequiredModesPerFile
    ),
    run_final_ownership_check(
        FileOwnerSessId, SessId, TestCaseRootDirPath, TestSpec
    ).


perms_to_posix_perms(Perms) ->
    lists:usort(lists:flatmap(
        fun permissions_test_utils:perm_to_posix_perms/1, Perms
    )).


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
%% @doc
%% Tests acl permissions needed to perform #perms_test_spec.operation.
%% For each type (`allow`, `deny`) and identifier (`OWNER@`, user_id,
%% group_id, `EVERYONE@`) it will setup environment and test combination
%% of acl perms. It will assert that combinations not having all required
%% perms fails and that combination consisting of only required perms succeeds.
%% @end
%%--------------------------------------------------------------------
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

    lists:foreach(fun({SessId, ScenarioType, ScenarioName, AceWho, AceFlags}) ->
        TestCaseRootDirPath = ?SCENARIO_DIR(ScenariosRootDirPath, ScenarioName),

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

        {ComplementaryPermsPerFile, AllRequiredPerms} = get_complementary_perms(
            Node, PermsPerFile
        ),
        run_acl_perms_scenario(
            Node, FileOwnerUserSessId, SessId, TestCaseRootDirPath, ScenarioName,
            ComplementaryPermsPerFile, AllRequiredPerms,
            ExtraData, AceWho, AceFlags, ScenarioType, TestSpec
        )
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


run_acl_perms_scenario(
    Node, FileOwnerSessId, SessId, TestCaseRootDirPath, ScenarioName,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, AceWho, AceFlags, allow,
    #perms_test_spec{operation = Operation} = TestSpec
) ->
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
            Operation(FileOwnerSessId, SessId, TestCaseRootDirPath, ExtraData),
            ScenarioName,
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
        Operation(FileOwnerSessId, SessId, TestCaseRootDirPath, ExtraData),
        ScenarioName,
        RequiredPermsPerFile
    ),
    run_final_ownership_check(
        FileOwnerSessId, SessId, TestCaseRootDirPath, TestSpec
    );

run_acl_perms_scenario(
    Node, FileOwnerSessId, SessId, TestCaseRootDirPath, ScenarioName,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, AceWho, AceFlags, deny,
    #perms_test_spec{operation = Operation} = TestSpec
) ->
    AllPermsPerFile = maps:map(fun(Guid, _) ->
        permissions_test_utils:all_perms(Node, Guid)
    end, ComplementaryPermsPerFile),

    % Denying only required perms and granting all others should result in eacces
    lists:foreach(fun({Guid, Perm}) ->
        EaccesPermsPerFile = #{Guid => [Perm]},

        permissions_test_utils:set_acls(
            Node, AllPermsPerFile, EaccesPermsPerFile, AceWho, AceFlags
        ),
        ?assertMatchWithPerms(
            {error, ?EACCES},
            Operation(FileOwnerSessId, SessId, TestCaseRootDirPath, ExtraData),
            ScenarioName,
            EaccesPermsPerFile
        )
    end, AllRequiredPerms),

    % Denying all perms but required ones should result in success
    permissions_test_utils:set_acls(
        Node, #{}, ComplementaryPermsPerFile, AceWho, AceFlags
    ),
    ?assertMatchWithPerms(
        ok,
        Operation(FileOwnerSessId, SessId, TestCaseRootDirPath, ExtraData),
        ScenarioName,
        ComplementaryPermsPerFile
    ),
    run_final_ownership_check(
        FileOwnerSessId, SessId, TestCaseRootDirPath, TestSpec
    ).


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
%% @doc
%% Tests permissions needed to perform operation as space owner.
%% Generally access to space owner should be granted regardless of posix/acl
%% permissions set or space privileges. The exceptions to this are caveats
%% constraints and access in share mode. In those cases space owner should not
%% be given any special treatment compared to any other space user.
%% @end
%%--------------------------------------------------------------------
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
                Operation(FileOwnerSessId, SpaceOwnerSessId, TestCaseRootDirPath, ExtraData),
                ScenarioName,
                maps:map(fun(_, _) -> <<"none">> end, PermsPerFile)
            ),
            run_final_ownership_check(
                FileOwnerSessId, SpaceOwnerSessId, TestCaseRootDirPath, TestSpec
            )
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
-spec run_final_ownership_check(
    session:id(),
    session:id(),
    file_meta:path(),
    #perms_test_spec{}
) ->
    ok | no_return().
run_final_ownership_check(
    OriginalFileOwnerSessId,
    OperationExecutionerSessId,
    TestCaseRootDirPath,
    #perms_test_spec{
        test_node = Node,
        space_id = SpaceId,
        final_ownership_check = FinalOwnershipCheckFun
    }
) ->
    case FinalOwnershipCheckFun(TestCaseRootDirPath) of
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
