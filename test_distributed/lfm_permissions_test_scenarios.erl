%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Runs various permissions test scenarios (e.g. space, posix, acl)
%%% using specified #perms_test_spec.
%%% Scenarios asserts that combinations not having all required perms
%%% (specified in #perms_test_spec) fails and that combination
%%% consisting of only required perms succeeds.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_permissions_test_scenarios).
-author("Bartosz Walkowicz").

-include("lfm_permissions_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

-export([run_scenarios/2]).

-define(SCENARIO_NAME(__PREFIX, __TYPE),
    <<__PREFIX, (atom_to_binary(__TYPE, utf8))/binary>>
).


%%%===================================================================
%%% TEST MECHANISM
%%%===================================================================


run_scenarios(#perms_test_spec{
    test_node = Node,
    space_id = SpaceId,
    owner_user = Owner,
    root_dir = RootDir
} = Spec, Config) ->
    OwnerSessId = ?config({session_id, {Owner, ?GET_DOMAIN(Node)}}, Config),

    {ok, SpaceName} = rpc:call(Node, space_logic, get_name, [?ROOT_SESS_ID, SpaceId]),
    ScenariosRootDirPath = <<"/", SpaceName/binary, "/", RootDir/binary>>,
    ?assertMatch(
        {ok, _},
        lfm_proxy:mkdir(Node, OwnerSessId, ScenariosRootDirPath, 8#777)
    ),

    run_space_privs_scenarios(ScenariosRootDirPath, Spec, Config),
    run_posix_tests(ScenariosRootDirPath, Spec, Config),
    run_acl_tests(ScenariosRootDirPath, Spec, Config).


%%%===================================================================
%%% SPACE PRIVILEGES TESTS SCENARIOS MECHANISM
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Tests space permissions needed to perform #perms_test_spec.operation.
%% It will setup environment, add full posix or acl permissions and
%% assert that without space privs operation cannot be performed and
%% with it it succeeds.
%% @end
%%--------------------------------------------------------------------
run_space_privs_scenarios(ScenariosRootDirPath, #perms_test_spec{
    test_node = Node,
    space_id = SpaceId,
    owner_user = Owner,
    space_user = User,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    posix_requires_space_privs = PosixSpacePrivs,
    acl_requires_space_privs = AclSpacePrivs,
    files = Files,
    operation = Operation
}, Config) ->
    OwnerUserSessId = ?config({session_id, {Owner, ?GET_DOMAIN(Node)}}, Config),

    lists:foreach(fun({ScenarioType, RequiredPrivs}) ->
        ScenarioName = ?SCENARIO_NAME("space_privs_", ScenarioType),
        ScenarioRootDirPath = <<
            ScenariosRootDirPath/binary,
            "/",
            ScenarioName/binary
        >>,

        % Create necessary file hierarchy
        {PermsPerFile, ExtraData} = create_files(
            Node, OwnerUserSessId, ScenariosRootDirPath, #dir{
                name = ScenarioName,
                perms = case RequiresTraverseAncestors of
                    true -> [?traverse_container];
                    false -> []
                end,
                children = Files
            }
        ),

        % Set all posix or acl (depending on scenario) perms to files
        set_all_perms(ScenarioType, Node, maps:keys(PermsPerFile)),

        % Assert that even with all perms set operation cannot be performed
        % without space privileges
        run_space_privs_scenario(
            Node, SpaceId, Owner, User, Operation, ScenarioName,
            ScenarioRootDirPath, ExtraData, RequiredPrivs, Config
        )
    end, [
        {posix, PosixSpacePrivs},
        {acl, AclSpacePrivs}
    ]).


run_space_privs_scenario(
    Node, SpaceId, Owner, SpaceUser, Operation, ScenarioName,
    ScenarioRootDirPath, ExtraData, RequiredPrivs, Config
) ->
    try
        space_privs_test(
            Node, SpaceId, Owner, SpaceUser,
            Operation, ScenarioRootDirPath, ExtraData,
            RequiredPrivs, Config
        )
    catch T:R ->
        ct:pal(
            "SPACE PRIVS TEST FAILURE~n"
            "   Scenario: ~p~n"
            "   Owner: ~p~n"
            "   User: ~p~n"
            "   Root path: ~p~n"
            "   Required space priv: ~p~n"
            "   Stacktrace: ~p~n",
            [
                ScenarioName,
                Owner, SpaceUser,
                ScenarioRootDirPath,
                RequiredPrivs,
                erlang:get_stacktrace()
            ]
        ),
        erlang:T(R)
    after
        initializer:testmaster_mock_space_user_privileges(
            [Node], SpaceId, Owner, privileges:space_privileges()
        ),
        initializer:testmaster_mock_space_user_privileges(
            [Node], SpaceId, SpaceUser, privileges:space_privileges()
        )
    end.


% Some operations can be performed only by owner which doesn't need any space privs.
space_privs_test(
    Node, SpaceId, OwnerId, UserId, Operation,
    RootDirPath, ExtraData, owner, Config
) ->
    % invalidate permission cache as it is not done due to initializer mocks
    invalidate_perms_cache(Node),

    UserSessId = ?config({session_id, {UserId, ?GET_DOMAIN(Node)}}, Config),
    OwnerSessId = ?config({session_id, {OwnerId, ?GET_DOMAIN(Node)}}, Config),

    initializer:testmaster_mock_space_user_privileges(
        [Node], SpaceId, UserId, privileges:space_privileges()
    ),
    ?assertMatch(
        {error, _},
        Operation(OwnerSessId, UserSessId, RootDirPath, ExtraData)
    ),

    initializer:testmaster_mock_space_user_privileges([Node], SpaceId, OwnerId, []),
    ?assertNotMatch(
        {error, _},
        Operation(OwnerSessId, OwnerSessId, RootDirPath, ExtraData)
    );

% When no space privs are required operation should succeed even with
% no space privs set for user.
space_privs_test(
    Node, SpaceId, OwnerId, UserId, Operation,
    RootDirPath, ExtraData, [], Config
) ->
    % invalidate permission cache as it is not done due to initializer mocks
    invalidate_perms_cache(Node),
    initializer:testmaster_mock_space_user_privileges([Node], SpaceId, UserId, []),

    UserSessId = ?config({session_id, {UserId, ?GET_DOMAIN(Node)}}, Config),
    OwnerSessId = ?config({session_id, {OwnerId, ?GET_DOMAIN(Node)}}, Config),
    ?assertNotMatch(
        {error, _},
        Operation(OwnerSessId, UserSessId, RootDirPath, ExtraData)
    );

% When specific space privs are required, the operation should fail if
% all privs but them are set and will succeed only with them.
space_privs_test(
    Node, SpaceId, OwnerId, UserId, Operation,
    RootDirPath, ExtraData, RequiredPrivs, Config
) ->
    AllSpacePrivs = privileges:space_privileges(),
    UserSessId = ?config({session_id, {UserId, ?GET_DOMAIN(Node)}}, Config),
    OwnerSessId = ?config({session_id, {OwnerId, ?GET_DOMAIN(Node)}}, Config),

    % If operation requires space priv it should fail without it and succeed with it
    initializer:testmaster_mock_space_user_privileges(
        [Node], SpaceId, UserId, AllSpacePrivs -- RequiredPrivs
    ),
    ?assertMatch(
        {error, ?EACCES},
        Operation(OwnerSessId, UserSessId, RootDirPath, ExtraData)
    ),

    % invalidate permission cache as it is not done due to initializer mocks
    invalidate_perms_cache(Node),

    initializer:testmaster_mock_space_user_privileges(
        [Node], SpaceId, UserId, RequiredPrivs
    ),
    ?assertNotMatch(
        {error, _},
        Operation(OwnerSessId, UserSessId, RootDirPath, ExtraData)
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
run_posix_tests(RootDirPath, #perms_test_spec{
    test_node = Node,
    owner_user = Owner,
    space_user = User,
    other_user = OtherUser,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    files = Files,
    operation = Operation
}, Config) ->
    OwnerUserSessId = ?config({session_id, {Owner, ?GET_DOMAIN(Node)}}, Config),
    GroupUserSessId = ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config),
    OtherUserSessId = ?config({session_id, {OtherUser, ?GET_DOMAIN(Node)}}, Config),

    TestCaseRootDirPerms = case RequiresTraverseAncestors of
        true -> [?traverse_container];
        false -> []
    end,
    lists:foreach(fun({SessId, Type, TestCaseRootDirName}) ->
        {PermsPerFile, ExtraData} = create_files(
            Node, OwnerUserSessId, RootDirPath, #dir{
                name = TestCaseRootDirName,
                perms = TestCaseRootDirPerms,
                children = Files
            }
        ),
        PosixPermsPerFile = maps:map(fun(_, Perms) ->
            lists:usort(lists:flatmap(
                fun lfm_permissions_test_utils:perm_to_posix_perms/1, Perms
            ))
        end, PermsPerFile),

        run_posix_tests(
            Node, OwnerUserSessId, SessId,
            <<RootDirPath/binary, "/", TestCaseRootDirName/binary>>,
            Operation, PosixPermsPerFile, ExtraData, Type
        )
    end, [
        {OwnerUserSessId, owner, <<"owner_posix">>},
        {GroupUserSessId, group, <<"group_posix">>},
        {OtherUserSessId, other, <<"other_posix">>}
    ]).


run_posix_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath,
    Operation, PosixPermsPerFile, ExtraData, Type
) ->
    {ComplementaryPosixPermsPerFile, RequiredPosixPerms} = maps:fold(
        fun(FileGuid, FileRequiredPerms, {BasePermsPerFileAcc, RequiredPermsAcc}) ->
            {
                BasePermsPerFileAcc#{FileGuid => ?ALL_POSIX_PERMS -- FileRequiredPerms},
                [{FileGuid, Perm} || Perm <- FileRequiredPerms] ++ RequiredPermsAcc
            }
        end,
        {#{}, []},
        PosixPermsPerFile
    ),

    try
        run_posix_tests(
            Node, OwnerSessId, SessId, TestCaseRootDirPath,
            Operation, ComplementaryPosixPermsPerFile,
            RequiredPosixPerms, ExtraData, Type
        )
    catch T:R ->
        FilePathsToRequiredPerms = maps:fold(fun(Guid, RequiredPerms, Acc) ->
            lfm_proxy:set_perms(Node, ?ROOT_SESS_ID, {guid, Guid}, 8#777),
            case lfm_proxy:get_file_path(Node, OwnerSessId, Guid) of
                {ok, Path} -> Acc#{Path => RequiredPerms};
                _ -> Acc#{Guid => RequiredPerms}
            end
        end, #{}, PosixPermsPerFile),

        ct:pal(
            "POSIX TESTS FAILURE~n"
            "   Type: ~p~n"
            "   Root path: ~p~n"
            "   Required Perms: ~p~n"
            "   Stacktrace: ~p~n",
            [
                Type, TestCaseRootDirPath,
                FilePathsToRequiredPerms,
                erlang:get_stacktrace()
            ]
        ),
        erlang:T(R)
    end.


run_posix_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Operation,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, owner
) ->
    RequiredPermsWithoutOwnership = lists:filter(fun({_, Perm}) ->
        Perm == read orelse Perm == write orelse Perm == exec
    end, AllRequiredPerms),

    case RequiredPermsWithoutOwnership of
        [] ->
            % If operation requires only ownership then it should succeed
            % even if all files modes are set to 0
            lfm_permissions_test_utils:set_modes(Node, maps:map(fun(_, _) -> 0 end, ComplementaryPermsPerFile)),
            ?assertNotMatch(
                {error, _},
                Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
            );
        _ ->
            run_standard_posix_tests(
                Node, OwnerSessId, SessId, TestCaseRootDirPath,
                Operation, ComplementaryPermsPerFile, RequiredPermsWithoutOwnership,
                ExtraData, owner
            )
    end;

run_posix_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Operation,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, group
) ->
    OperationRequiresOwnership = lists:any(fun({_, Perm}) ->
        Perm == owner
    end, AllRequiredPerms),

    case OperationRequiresOwnership of
        true ->
            % If operation requires ownership then for group member it should failed
            % even if all files modes are set to 777
            lfm_permissions_test_utils:set_modes(Node, maps:map(fun(_, _) -> 8#777 end, ComplementaryPermsPerFile)),
            ?assertMatch(
                {error, ?EACCES},
                Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
            );
        false ->
            RequiredNormalPosixPerms = lists:filter(fun({_, Perm}) ->
                Perm == read orelse Perm == write orelse Perm == exec
            end, AllRequiredPerms),
            run_standard_posix_tests(
                Node, OwnerSessId, SessId, TestCaseRootDirPath,
                Operation, ComplementaryPermsPerFile, RequiredNormalPosixPerms, ExtraData, group
            )
    end;

% Users not belonging to space or unauthorized should not be able to conduct any operation
run_posix_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Operation,
    ComplementaryPermsPerFile, _AllRequiredPerms, ExtraData, other
) ->
    lfm_permissions_test_utils:set_modes(Node, maps:map(fun(_, _) -> 8#777 end, ComplementaryPermsPerFile)),
    ?assertMatch(
        {error, _},
        Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
    ),
    ?assertMatch(
        {error, _},
        Operation(OwnerSessId, ?GUEST_SESS_ID, TestCaseRootDirPath, ExtraData)
    ).


run_standard_posix_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Operation,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, Type
) ->
    AllRequiredModes = lists:map(fun({Guid, PosixPerm}) ->
        {Guid, posix_perm_to_mode(PosixPerm, Type)}
    end, AllRequiredPerms),
    ComplementaryModesPerFile = maps:map(fun(_, Perms) ->
        lists:foldl(fun(Perm, Acc) ->
            Acc bor posix_perm_to_mode(Perm, Type)
        end, 0, Perms)
    end, ComplementaryPermsPerFile),

    [RequiredModesComb | EaccesModesCombs] = combinations(AllRequiredModes),

    % Granting all modes but required ones should result in eacces
    lists:foreach(fun(EaccessModeComb) ->
        EaccesModesPerFile = lists:foldl(fun({Guid, Mode}, Acc) ->
            Acc#{Guid => Mode bor maps:get(Guid, Acc)}
        end, ComplementaryModesPerFile, EaccessModeComb),
        lfm_permissions_test_utils:set_modes(Node, EaccesModesPerFile),
        ?assertMatch(
            {error, ?EACCES},
            Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
        )
    end, EaccesModesCombs),

    % Granting only required modes should result in success
    RequiredModesPerFile = lists:foldl(fun({Guid, Mode}, Acc) ->
        Acc#{Guid => Mode bor maps:get(Guid, Acc, 0)}
    end, #{}, RequiredModesComb),
    lfm_permissions_test_utils:set_modes(Node, RequiredModesPerFile),
    ?assertNotMatch(
        {error, _},
        Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
    ).


%%%===================================================================
%%% ACL TESTS SCENARIOS SCENARIOS
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
run_acl_tests(RootDirPath, #perms_test_spec{
    test_node = Node,
    owner_user = OwnerUser,
    space_user = SpaceUser,
    space_user_group = SpaceUserGroup,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    files = Files,
    operation = Operation
}, Config) ->
    OwnerSessId = ?config({session_id, {OwnerUser, ?GET_DOMAIN(Node)}}, Config),
    UserSessId = ?config({session_id, {SpaceUser, ?GET_DOMAIN(Node)}}, Config),

    TestCaseRootDirPerms = case RequiresTraverseAncestors of
        true -> [?traverse_container];
        false -> []
    end,
    lists:foreach(fun({SessId, Type, TestCaseRootDirName, AceWho, AceFlags}) ->
        {PermsPerFile, ExtraData} = create_files(
            Node, OwnerSessId, RootDirPath, #dir{
                name = TestCaseRootDirName,
                perms = TestCaseRootDirPerms,
                children = Files
            }
        ),
        run_acl_tests(
            Node, OwnerSessId, SessId,
            <<RootDirPath/binary, "/", TestCaseRootDirName/binary>>,
            Operation, PermsPerFile, ExtraData, AceWho, AceFlags, Type
        )
    end, [
        {OwnerSessId, allow, <<"owner_acl_allow">>, ?owner, ?no_flags_mask},
        {UserSessId, allow, <<"user_acl_allow">>, SpaceUser, ?no_flags_mask},
        {UserSessId, allow, <<"user_group_acl_allow">>, SpaceUserGroup, ?identifier_group_mask},
        {UserSessId, allow, <<"everyone_acl_allow">>, ?everyone, ?no_flags_mask},

        {OwnerSessId, deny, <<"owner_acl_deny">>, ?owner, ?no_flags_mask},
        {UserSessId, deny, <<"user_acl_deny">>, SpaceUser, ?no_flags_mask},
        {UserSessId, deny, <<"user_group_acl_deny">>, SpaceUserGroup, ?identifier_group_mask},
        {UserSessId, deny, <<"everyone_acl_deny">>, ?everyone, ?no_flags_mask}
    ]).


run_acl_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath,
    Operation, RequiredPermsPerFile, ExtraData, AceWho, AceFlags, Type
) ->
    {ComplementaryPermsPerFile, AllRequiredPerms} = maps:fold(
        fun(FileGuid, FileRequiredPerms, {BasePermsPerFileAcc, RequiredPermsAcc}) ->
            {
                BasePermsPerFileAcc#{FileGuid => lfm_permissions_test_utils:complementary_perms(
                    Node, FileGuid, FileRequiredPerms
                )},
                [{FileGuid, Perm} || Perm <- FileRequiredPerms] ++ RequiredPermsAcc
            }
        end,
        {#{}, []},
        RequiredPermsPerFile
    ),

    try
        run_acl_tests(
            Node, OwnerSessId, SessId, TestCaseRootDirPath,
            Operation, ComplementaryPermsPerFile, AllRequiredPerms,
            ExtraData, AceWho, AceFlags, Type
        )
    catch T:R ->
        RequiredPermsPerFileMap = maps:fold(fun(Guid, RequiredPerms, Acc) ->
            lfm_proxy:set_perms(Node, ?ROOT_SESS_ID, {guid, Guid}, 8#777),
            case lfm_proxy:get_file_path(Node, OwnerSessId, Guid) of
                {ok, Path} -> Acc#{Path => RequiredPerms};
                _ -> Acc#{Guid => RequiredPerms}
            end
        end, #{}, RequiredPermsPerFile),

        ct:pal(
            "ACL TESTS FAILURE~n"
            "   Type: ~p~n"
            "   Root path: ~p~n"
            "   Required Perms: ~p~n"
            "   Identifier: ~p~n"
            "   Is group identifier: ~p~n"
            "   Stacktrace: ~p~n",
            [
                Type, TestCaseRootDirPath,
                RequiredPermsPerFileMap,
                AceWho, AceFlags == ?identifier_group_mask,
                erlang:get_stacktrace()
            ]
        ),
        erlang:T(R)
    end.


run_acl_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Operation,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, AceWho, AceFlags, allow
) ->
    [AllRequiredPermsComb | EaccesPermsCombs] = combinations(AllRequiredPerms),

    % Granting all perms without required ones should result in eacces
    lists:foreach(fun(EaccessPermComb) ->
        EaccesPermsPerFile = lists:foldl(fun({Guid, Perm}, Acc) ->
            Acc#{Guid => [Perm | maps:get(Guid, Acc)]}
        end, ComplementaryPermsPerFile, EaccessPermComb),
        lfm_permissions_test_utils:set_acls(Node, EaccesPermsPerFile, #{}, AceWho, AceFlags),
        ?assertMatch(
            {error, ?EACCES},
            Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
        )
    end, EaccesPermsCombs),

    % Granting only required perms should result in success
    RequiredPermsPerFile = lists:foldl(fun({Guid, Perm}, Acc) ->
        Acc#{Guid => [Perm | maps:get(Guid, Acc, [])]}
    end, #{}, AllRequiredPermsComb),
    lfm_permissions_test_utils:set_acls(Node, RequiredPermsPerFile, #{}, AceWho, AceFlags),
    ?assertNotMatch(
        {error, _},
        Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
    );

run_acl_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Operation,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, AceWho, AceFlags, deny
) ->
    AllPermsPerFile = maps:map(fun(Guid, _) ->
        lfm_permissions_test_utils:all_perms(Node, Guid)
    end, ComplementaryPermsPerFile),

    % Denying only required perms and granting all others should result in eacces
    lists:foreach(fun({Guid, Perm}) ->
        lfm_permissions_test_utils:set_acls(Node, AllPermsPerFile, #{Guid => [Perm]}, AceWho, AceFlags),
        ?assertMatch(
            {error, ?EACCES},
            Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
        )
    end, AllRequiredPerms),

    % Denying all perms but required ones should result in success
    lfm_permissions_test_utils:set_acls(Node, #{}, ComplementaryPermsPerFile, AceWho, AceFlags),
    ?assertNotMatch(
        {error, _},
        Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec create_files(node(), session:id(), file_meta:path(), #dir{} | #file{}) ->
    {PermsPerFile :: map(), ExtraData :: map()}.
create_files(Node, OwnerSessId, ParentDirPath, #file{
    name = FileName,
    perms = FilePerms,
    on_create = HookFun
}) ->
    FilePath = <<ParentDirPath/binary, "/", FileName/binary>>,
    {ok, FileGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:create(Node, OwnerSessId, FilePath, 8#777)
    ),
    ExtraData = case HookFun of
        undefined ->
            #{FilePath => FileGuid};
        _ when is_function(HookFun, 2) ->
            #{FilePath => HookFun(OwnerSessId, FileGuid)}
    end,
    {#{FileGuid => FilePerms}, ExtraData};
create_files(Node, OwnerSessId, ParentDirPath, #dir{
    name = DirName,
    perms = DirPerms,
    on_create = HookFun,
    children = Children
}) ->
    DirPath = <<ParentDirPath/binary, "/", DirName/binary>>,
    {ok, DirGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:mkdir(Node, OwnerSessId, DirPath)
    ),
    {PermsPerFile0, ExtraData0} = lists:foldl(fun(Child, {PermsPerFileAcc, ExtraDataAcc}) ->
        {ChildPerms, ChildExtraData} = create_files(Node, OwnerSessId, DirPath, Child),
        {maps:merge(PermsPerFileAcc, ChildPerms), maps:merge(ExtraDataAcc, ChildExtraData)}
    end, {#{}, #{}}, Children),

    ExtraData1 = case HookFun of
        undefined ->
            ExtraData0#{DirPath => DirGuid};
        _ when is_function(HookFun, 2) ->
            ExtraData0#{DirPath => HookFun(OwnerSessId, DirGuid)}
    end,
    {PermsPerFile0#{DirGuid => DirPerms}, ExtraData1}.


-spec set_all_perms(posix | acl, node(), [file_id:file_guid()]) -> ok.
set_all_perms(posix, Node, Files) ->
    AllPosixPermsPerFile = lists:foldl(fun(Guid, Acc) ->
        Acc#{Guid => 8#777}
    end, #{}, Files),
    lfm_permissions_test_utils:set_modes(Node, AllPosixPermsPerFile);
set_all_perms(acl, Node, Files) ->
    AllAclPermsPerFile = lists:foldl(fun(Guid, Acc) ->
        Acc#{Guid => lfm_permissions_test_utils:all_perms(Node, Guid)}
    end, #{}, Files),
    lfm_permissions_test_utils:set_acls(
        Node, AllAclPermsPerFile, #{}, ?everyone, ?no_flags_mask
    ).


-spec posix_perm_to_mode(PosixPerm :: atom(), Type :: owner | group) ->
    non_neg_integer().
posix_perm_to_mode(read, owner) -> 8#4 bsl 6;
posix_perm_to_mode(write, owner) -> 8#2 bsl 6;
posix_perm_to_mode(exec, owner) -> 8#1 bsl 6;
posix_perm_to_mode(read, group) -> 8#4 bsl 3;
posix_perm_to_mode(write, group) -> 8#2 bsl 3;
posix_perm_to_mode(exec, group) -> 8#1 bsl 3;
posix_perm_to_mode(_, _) -> 8#0.


-spec combinations([term()]) -> [[term()]].
combinations([]) ->
    [[]];
combinations([Item | Items]) ->
    Combinations = combinations(Items),
    [[Item | Comb] || Comb <- Combinations] ++ Combinations.


-spec invalidate_perms_cache(node()) -> ok.
invalidate_perms_cache(Node) ->
    rpc:call(Node, permissions_cache, invalidate, []).
