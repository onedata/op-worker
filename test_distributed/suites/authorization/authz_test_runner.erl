%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2024 ACK CYFRONET AGH
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
-module(authz_test_runner).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("permissions_test.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include("onenv_test_utils.hrl").

-export([run_suite/1]).


-type authz_test_suite_spec() :: #authz_test_suite_spec{}.

-type permissions() :: [binary()].

-type perms_per_file() :: #{file_id:file_guid() => permissions()}.
-type posix_perms_per_file() :: #{file_id:file_guid() => [PosixPerm :: atom()]}.

-type posix_user_type() :: owner | group | other.

-type extra_data() :: #{file_meta:path() => term()}.

-record(authz_test_suite_ctx, {
    suite_spec :: authz_test_suite_spec(),
    suite_root_dir_path :: file_meta:path(),
    test_node :: node(),
    files_owner_session_id :: session:id()
}).
-type authz_test_suite_ctx() :: #authz_test_suite_ctx{}.

-record(authz_test_group_ctx, {
    name :: binary(),
    suite_ctx :: authz_test_suite_ctx(),
    group_root_dir_path :: file_meta:path(),
    files_owner_session_id :: session:id(),
    executioner_session_id :: session:id(),
    required_perms_per_file :: perms_per_file(),
    extra_data = #{} :: map()
}).
-type authz_test_group_ctx() :: #authz_test_group_ctx{}.

-record(authz_posix_test_group_ctx, {
    group_ctx :: authz_test_group_ctx(),
    required_posix_perms_per_file :: posix_perms_per_file(),
    complementary_posix_perms_per_file :: posix_perms_per_file()
}).
-type authz_posix_test_group_ctx() :: #authz_posix_test_group_ctx{}.

-define(assertMatchWithPerms(__Expect, __Expression, __ScenarioName, __Node, __PermsPerGuid),
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
                    format_perms_per_file(__Node, __PermsPerGuid),
                    __Reason
                ]
            ),
            erlang:error(perms_test_failed)
        end
    end)()
).

-define(TEST_GROUP_NAME(__PREFIX, __TYPE),
    <<__PREFIX, (atom_to_binary(__TYPE, utf8))/binary>>
).


%%%===================================================================
%%% TEST MECHANISM
%%%===================================================================


-spec run_suite(authz_test_suite_spec()) -> ok | no_return().
run_suite(TestSuiteSpec = #authz_test_suite_spec{
    name = TestSuiteName,
    provider_selector = ProviderSelector,
    space_selector = SpaceSelector,
    files_owner_selector = FilesOwnerSelector
}) ->
    TestNode = oct_background:get_random_provider_node(ProviderSelector),
    FileOwnerSessionId = oct_background:get_user_session_id(FilesOwnerSelector, ProviderSelector),

    SpaceName = oct_background:get_space_name(SpaceSelector),
    TestSuiteRootDirPath = filepath_utils:join([<<"/">>, SpaceName, TestSuiteName]),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(TestNode, FileOwnerSessionId, TestSuiteRootDirPath, 8#777)),

    TestSuiteCtx = #authz_test_suite_ctx{
        suite_spec = TestSuiteSpec,
        suite_root_dir_path = TestSuiteRootDirPath,
        test_node = TestNode,
        files_owner_session_id = FileOwnerSessionId
    },

    run_posix_permission_test_groups(TestSuiteCtx).


%%%===================================================================
%%% POSIX TESTS
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tests posix permissions needed to perform operation.
%% For each bits group (`owner`, `group`, `other`) it will setup
%% environment and test combination of posix perms. It will assert
%% that combinations not having all required perms fails and that
%% combination consisting of only required perms succeeds.
%% @end
%%--------------------------------------------------------------------
-spec run_posix_permission_test_groups(authz_test_suite_ctx()) ->
    ok | no_return().
run_posix_permission_test_groups(TestSuiteCtx = #authz_test_suite_ctx{
    suite_spec = TestSuiteSpec
}) ->
    lists:foreach(fun({PosixUserType, ExecutionerSelector}) ->
        TestGroupName = ?TEST_GROUP_NAME("posix_", PosixUserType),
        TestGroupCtx = init_test_group(TestGroupName, ExecutionerSelector, TestSuiteCtx),
        PosixTestGroupCtx = build_posix_test_group_ctx(TestGroupCtx),

        try
            run_posix_permission_test_group(PosixUserType, PosixTestGroupCtx)
        catch _:Reason:Stacktrace ->
            %% TODO stacktrace?
            ct:pal(
                "POSIX TESTS FAILURE~n"
                "   Type: ~p~n"
                "   Root path: ~p~n"
                "   Required Perms: ~p~n"
                "   Reason: ~p~n",
                [
                    PosixUserType,
                    TestGroupCtx#authz_test_group_ctx.group_root_dir_path,
                    format_posix_perms_per_file(PosixTestGroupCtx),
                    Reason
                ]
            ),
            erlang:error(posix_perms_test_failed)
        end
    end, [
        {owner, TestSuiteSpec#authz_test_suite_spec.files_owner_selector},
        {group, TestSuiteSpec#authz_test_suite_spec.space_user_selector},
        {other, TestSuiteSpec#authz_test_suite_spec.non_space_user}
    ]).


%% @private
-spec build_posix_test_group_ctx(authz_test_group_ctx()) ->
    authz_posix_test_group_ctx().
build_posix_test_group_ctx(TestGroupCtx = #authz_test_group_ctx{
    required_perms_per_file = RequiredPermsPerFile
}) ->
    RequiredPosixPermsPerFile = maps:map(fun(_, Perms) ->
        perms_to_posix_perms(Perms)
    end, RequiredPermsPerFile),
    ComplementaryPosixPermsPerFile = maps:fold(fun(FileGuid, FileRequiredPosixPerms, Acc) ->
        Acc#{FileGuid => ?ALL_POSIX_PERMS -- FileRequiredPosixPerms}
    end, #{}, RequiredPosixPermsPerFile),

    #authz_posix_test_group_ctx{
        group_ctx = TestGroupCtx,
        required_posix_perms_per_file = RequiredPosixPermsPerFile,
        complementary_posix_perms_per_file = ComplementaryPosixPermsPerFile
    }.


%% @private
-spec perms_to_posix_perms([binary()]) -> [atom()].
perms_to_posix_perms(Perms) ->
    lists:usort(lists:flatmap(
        fun permissions_test_utils:perm_to_posix_perms/1, Perms
    )).


%% @private
-spec format_posix_perms_per_file(authz_posix_test_group_ctx()) -> json_utils:json_map().
format_posix_perms_per_file(#authz_posix_test_group_ctx{
    group_ctx = #authz_test_group_ctx{suite_ctx = #authz_test_suite_ctx{test_node = TestNode}},
    required_posix_perms_per_file = RequiredPosixPermsPerFile
}) ->
    format_perms_per_file(TestNode, RequiredPosixPermsPerFile).


%% @private
-spec run_posix_permission_test_group(posix_user_type(), authz_posix_test_group_ctx()) ->
    ok | no_return().
run_posix_permission_test_group(owner, PosixTestGroupCtx = #authz_posix_test_group_ctx{
    group_ctx = TestGroupCtx = #authz_test_group_ctx{
        suite_ctx = #authz_test_suite_ctx{
            suite_spec = #authz_test_suite_spec{operation = Operation},
            test_node = TestNode
        },
        group_root_dir_path = TestGroupRootDirPath,
        executioner_session_id = ExecutionerSessionId,
        extra_data = ExtraData
    },
    required_posix_perms_per_file = RequiredPosixPermsPerFile,
    complementary_posix_perms_per_file = ComplementaryPosixPermsPerFile
}) ->
    RequiredRWXPosixPermsPerFile = filter_rwx_posix_perms_per_file(RequiredPosixPermsPerFile),

    case maps_utils:is_empty(RequiredRWXPosixPermsPerFile) of
        true ->
            % If operation requires only ownership then it should succeed
            % even if all files modes are set to 0
            ModesPerFile = maps:map(fun(_, _) -> 0 end, ComplementaryPosixPermsPerFile),
            permissions_test_utils:set_modes(TestNode, ModesPerFile),

            ?assertMatch(ok, Operation(TestNode, ExecutionerSessionId, TestGroupRootDirPath, ExtraData)),

            run_final_storage_ownership_check(TestGroupCtx);

        false ->
            run_posix_permission_test_cases(owner, PosixTestGroupCtx#authz_posix_test_group_ctx{
                required_posix_perms_per_file = RequiredRWXPosixPermsPerFile
            })
    end;

run_posix_permission_test_group(group, PosixTestGroupCtx = #authz_posix_test_group_ctx{
    group_ctx = #authz_test_group_ctx{
        suite_ctx = #authz_test_suite_ctx{
            suite_spec = TestSuiteSpec = #authz_test_suite_spec{operation = Operation},
            test_node = TestNode
        },
        group_root_dir_path = TestGroupRootDirPath,
        executioner_session_id = ExecutionerSessionId,
        extra_data = ExtraData
    },
    required_posix_perms_per_file = RequiredPosixPermsPerFile,
    complementary_posix_perms_per_file = ComplementaryPosixPermsPerFile
}) ->
    OperationRequiresFileOwnership = maps:fold(fun(_, Perms, RequiresOwnershipAcc) ->
        RequiresOwnershipAcc orelse lists:any(fun(Perm) -> Perm == owner end, Perms)
    end, false, RequiredPosixPermsPerFile),

    case OperationRequiresFileOwnership of
        true ->
            % If operation requires ownership then for group member it should fail
            % even if all files modes are set to 777
            ModesPerFile = maps:map(fun(_, _) -> 8#777 end, ComplementaryPosixPermsPerFile),
            permissions_test_utils:set_modes(TestNode, ModesPerFile),
            ExpError = get_exp_error(?EACCES, TestSuiteSpec),
            ?assertMatch(
                ExpError,
                Operation(TestNode, ExecutionerSessionId, TestGroupRootDirPath, ExtraData)
            );

        false ->
            RequiredRWXPosixPermsPerFile = filter_rwx_posix_perms_per_file(RequiredPosixPermsPerFile),

            run_posix_permission_test_cases(group, PosixTestGroupCtx#authz_posix_test_group_ctx{
                required_posix_perms_per_file = RequiredRWXPosixPermsPerFile
            })
    end;

run_posix_permission_test_group(other, #authz_posix_test_group_ctx{
    group_ctx = #authz_test_group_ctx{
        suite_ctx = #authz_test_suite_ctx{
            suite_spec = TestSuiteSpec = #authz_test_suite_spec{operation = Operation},
            test_node = TestNode
        },
        group_root_dir_path = TestGroupRootDirPath,
        executioner_session_id = ExecutionerSessionId,
        extra_data = ExtraData
    },
    complementary_posix_perms_per_file = ComplementaryPosixPermsPerFile
}) ->
    % Users not belonging to space or unauthorized should not be able to conduct any operation
    ModesPerFile = maps:map(fun(_, _) -> 8#777 end, ComplementaryPosixPermsPerFile),
    permissions_test_utils:set_modes(TestNode, ModesPerFile),

    %% TODO
    ExpError = get_exp_error(?ENOENT, TestSuiteSpec),
%%    ?assertMatch(ExpError, Operation(TestNode, ExecutionerSessionId, TestGroupRootDirPath, ExtraData)),

    % Some operations cannot be performed with special session (either root or guest)
    % and result in eagain error instead of enoent
    ?assertMatch({error, _}, Operation(TestNode, ?GUEST_SESS_ID, TestGroupRootDirPath, ExtraData)).


%% @private
-spec filter_rwx_posix_perms_per_file(posix_perms_per_file()) -> posix_perms_per_file().
filter_rwx_posix_perms_per_file(PosixPermsPerFile) ->
    maps:filtermap(fun(_, Perms) ->
        RWXOnlyPerms = lists:filter(fun is_rwx_posix_perm/1, Perms),
        RWXOnlyPerms /= [] andalso {true, RWXOnlyPerms}
    end, PosixPermsPerFile).


%% @private
-spec is_rwx_posix_perm(atom()) -> boolean().
is_rwx_posix_perm(read) -> true;
is_rwx_posix_perm(write) -> true;
is_rwx_posix_perm(exec) -> true;
is_rwx_posix_perm(_) -> false.


%% @private
-spec run_posix_permission_test_cases(posix_user_type(), authz_posix_test_group_ctx()) ->
    ok | no_return().
run_posix_permission_test_cases(PosixUserType, #authz_posix_test_group_ctx{
    group_ctx = TestGroupCtx = #authz_test_group_ctx{
        suite_ctx = #authz_test_suite_ctx{
            suite_spec = TestSuiteSpec = #authz_test_suite_spec{
                operation = Operation
            },
            test_node = TestNode
        },
        name = TestGroupName,
        group_root_dir_path = TestGroupRootDirPath,
        executioner_session_id = ExecutionerSessionId,
        extra_data = ExtraData
    },
    required_posix_perms_per_file = RequiredPosixPermsPerFile,
    complementary_posix_perms_per_file = ComplementaryPosixPermsPerFile
}) ->
    FlattenedRequiredModes = maps:fold(fun(Guid, Perms, OuterAcc) ->
        lists:foldl(fun(Perm, InnerAcc) ->
            [{Guid, permissions_test_utils:posix_perm_to_mode(Perm, PosixUserType)} | InnerAcc]
        end, OuterAcc, Perms)
    end, [], RequiredPosixPermsPerFile),

    [RequiredModesComb | EaccesModesCombs] = combinations(FlattenedRequiredModes),

    ComplementaryModesPerFile = maps:map(fun(_, Perms) ->
        lists:foldl(fun(Perm, Acc) ->
            Acc bor permissions_test_utils:posix_perm_to_mode(Perm, PosixUserType)
        end, 0, Perms)
    end, ComplementaryPosixPermsPerFile),

    % Granting all modes but required ones should result in eacces
    lists:foreach(fun(EaccesModeComb) ->
        EaccesModesPerFile = lists:foldl(fun({Guid, Mode}, Acc) ->
            Acc#{Guid => Mode bor maps:get(Guid, Acc)}
        end, ComplementaryModesPerFile, EaccesModeComb),

        permissions_test_utils:set_modes(TestNode, EaccesModesPerFile),

        ExpError = get_exp_error(?EACCES, TestSuiteSpec),

        ?assertMatchWithPerms(
            ExpError,
            Operation(TestNode, ExecutionerSessionId, TestGroupRootDirPath, ExtraData),
            TestGroupName, TestNode,
            EaccesModesPerFile
        )
    end, EaccesModesCombs),

    % Granting only required modes should result in success
    RequiredModesPerFile = lists:foldl(fun({Guid, Mode}, Acc) ->
        Acc#{Guid => Mode bor maps:get(Guid, Acc, 0)}
    end, #{}, RequiredModesComb),

    permissions_test_utils:set_modes(TestNode, RequiredModesPerFile),

    ?assertMatchWithPerms(
        ok,
        Operation(TestNode, ExecutionerSessionId, TestGroupRootDirPath, ExtraData),
        TestGroupName, TestNode,
        RequiredModesPerFile
    ),
    run_final_storage_ownership_check(TestGroupCtx).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec init_test_group(binary(), oct_background:entity_selector(), authz_test_suite_ctx()) ->
    authz_test_group_ctx().
init_test_group(TestGroupName, ExecutionerSelector, TestSuiteCtx = #authz_test_suite_ctx{
    suite_spec = TestSuiteSpec = #authz_test_suite_spec{
        provider_selector = ProviderSelector
    },
    suite_root_dir_path = TestSuiteRootDirPath,
    test_node = TestNode,
    files_owner_session_id = FilesOwnerSessionId
}) ->
    FileTreeSpec = #dir{
        name = TestGroupName,
        perms = infer_test_group_root_dir_permissions(TestSuiteSpec),
        children = TestSuiteSpec#authz_test_suite_spec.files
    },
    {RequiredPermsPerFile, ExtraData} = create_file_tree(
        TestNode, FilesOwnerSessionId, TestSuiteRootDirPath, FileTreeSpec
    ),

    #authz_test_group_ctx{
        suite_ctx = TestSuiteCtx,
        name = TestGroupName,
        group_root_dir_path = filepath_utils:join([TestSuiteRootDirPath, TestGroupName]),
        executioner_session_id = oct_background:get_user_session_id(
            ExecutionerSelector, ProviderSelector
        ),
        required_perms_per_file = RequiredPermsPerFile,
        extra_data = ExtraData
    }.


%% @private
-spec infer_test_group_root_dir_permissions(authz_test_suite_spec()) ->
    permissions().
infer_test_group_root_dir_permissions(#authz_test_suite_spec{requires_traverse_ancestors = true}) ->
    [?traverse_container];
infer_test_group_root_dir_permissions(_) ->
    [].


%% @private
-spec create_file_tree(node(), session:id(), file_meta:path(), #dir{} | #file{}) ->
    {perms_per_file(), extra_data()}.
create_file_tree(Node, FileOwnerSessId, ParentDirPath, #file{
    name = FileName,
    perms = RequiredFilePerms,
    on_create = HookFun
}) ->
    FilePath = filepath_utils:join([ParentDirPath, FileName]),
    FileGuid = create_file(Node, FileOwnerSessId, FilePath),

    ExtraData = #{FilePath => case HookFun of
        undefined -> ?FILE_REF(FileGuid);
        _ when is_function(HookFun, 3) -> HookFun(Node, FileOwnerSessId, FileGuid)
    end},

    {#{FileGuid => RequiredFilePerms}, ExtraData};

create_file_tree(Node, FileOwnerSessId, ParentDirPath, #dir{
    name = DirName,
    perms = RequiredDirPerms,
    on_create = HookFun,
    children = Children
}) ->
    DirPath = filepath_utils:join([ParentDirPath, DirName]),
    DirGuid = create_dir(Node, FileOwnerSessId, DirPath),

    {ChildrenPermsPerFile, ChildrenExtraData} = lists:foldl(fun(Child, {PermsPerFileAcc, ExtraDataAcc}) ->
        {ChildPerms, ChildExtraData} = create_file_tree(Node, FileOwnerSessId, DirPath, Child),
        {maps:merge(PermsPerFileAcc, ChildPerms), maps:merge(ExtraDataAcc, ChildExtraData)}
    end, {#{}, #{}}, Children),

    % 'on_create' should be called only after all dir children are created
    ExtraData = ChildrenExtraData#{DirPath => case HookFun of
        undefined -> ?FILE_REF(DirGuid);
        _ when is_function(HookFun, 3) -> HookFun(Node, FileOwnerSessId, DirGuid)
    end},

    {ChildrenPermsPerFile#{DirGuid => RequiredDirPerms}, ExtraData}.


%% @private
-spec create_file(node(), session:id(), file_meta:path()) -> file_id:file_guid().
create_file(Node, FileOwnerSessId, FilePath) ->
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, FileOwnerSessId, FilePath, 8#777)),
    permissions_test_utils:ensure_file_created_on_storage(Node, FileGuid),
    FileGuid.


%% @private
-spec create_dir(node(), session:id(), file_meta:path()) -> file_id:file_guid().
create_dir(Node, FileOwnerSessId, DirPath) ->
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, FileOwnerSessId, DirPath)),
    permissions_test_utils:ensure_dir_created_on_storage(Node, DirGuid),
    DirGuid.


%% @private
-spec get_file_path(oct_background:node_selector(), file_id:file_guid()) ->
    file_meta:path().
get_file_path(NodeSelector, Guid) ->
    {Path, _} = ?assertMatch({_, _}, ?rpc(NodeSelector, file_ctx:get_logical_path(
        file_ctx:new_by_guid(Guid), user_ctx:new(?ROOT_SESS_ID))
    )),
    Path.


%% @private
-spec combinations([term()]) -> [[term()]].
combinations([]) ->
    [[]];
combinations([Item | Items]) ->
    Combinations = combinations(Items),
    [[Item | Comb] || Comb <- Combinations] ++ Combinations.


%% @private
-spec format_perms_per_file(
    oct_background:node_selector(),
    perms_per_file() | posix_perms_per_file()
) ->
    json_utils:json_map().
format_perms_per_file(NodeSelector, PermsPerFile) ->
    maps:fold(fun(Guid, RequiredPerms, Acc) ->
        Acc#{get_file_path(NodeSelector, Guid) => case is_integer(RequiredPerms) of
            true ->
                ModeBin = list_to_binary(string:right(integer_to_list(RequiredPerms, 8), 3, $0)),
                <<"posix mode: ", ModeBin/binary>>;
            false ->
                [str_utils:to_binary(RequiredPerm) || RequiredPerm <- RequiredPerms]
        end}
    end, #{}, PermsPerFile).


%% @private
-spec get_exp_error(atom(), authz_test_suite_spec()) -> {error, term()}.
get_exp_error(Errno, #authz_test_suite_spec{returned_errors = api_errors}) ->
    ?ERROR_POSIX(Errno);
get_exp_error(Errno, #authz_test_suite_spec{returned_errors = errno_errors}) ->
    {error, Errno}.


%% @private
-spec run_final_storage_ownership_check(authz_test_group_ctx()) -> ok | no_return().
run_final_storage_ownership_check(#authz_test_group_ctx{
    suite_ctx = #authz_test_suite_ctx{
        suite_spec = #authz_test_suite_spec{
            space_selector = SpaceSelector,
            final_ownership_check = FinalOwnershipCheckFun
        },
        test_node = Node
    },
    group_root_dir_path = ScenarioRootDirPath,
    files_owner_session_id = OriginalFileOwnerSessionId,
    executioner_session_id = OperationExecutionerSessionId
}) ->
    SpaceId = oct_background:get_space_id(SpaceSelector),

    ok.
    %% TODO
%%    case FinalOwnershipCheckFun(ScenarioRootDirPath) of
%%        skip ->
%%            ok;
%%        {should_preserve_ownership, LogicalFilePath} ->
%%            permissions_test_utils:assert_user_is_file_owner_on_storage(
%%                Node, SpaceId, LogicalFilePath, OriginalFileOwnerSessionId
%%            );
%%        {should_change_ownership, LogicalFilePath} ->
%%            permissions_test_utils:assert_user_is_file_owner_on_storage(
%%                Node, SpaceId, LogicalFilePath, OperationExecutionerSessionId
%%            )
%%    end.