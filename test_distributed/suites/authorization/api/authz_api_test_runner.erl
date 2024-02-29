%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Runs various permissions test scenarios (e.g. space privs, posix mode, acl)
%%% using specified test_suite_spec().
%%% Scenarios asserts that combinations not having all required perms
%%% (specified in perms_test_spec()) fails and that combination
%%% consisting of only required perms succeeds.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_api_test_runner).
-author("Bartosz Walkowicz").

-include("authz_api_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([run_suite/1]).

-type file_tree_spec() :: #ct_authz_dir_spec{} | #ct_authz_file_spec{}.

-type perms() :: [binary()].

-type perms_per_file() :: #{file_id:file_guid() => perms()}.
-type posix_perms_per_file() :: #{file_id:file_guid() => [PosixPerm :: atom()]}.

-type posix_user_type() :: owner | group | other.

-type extra_data() :: #{file_meta:path() => term()}.

-type authz_test_suite_spec() :: #authz_test_suite_spec{}.

-record(authz_test_suite_ctx, {
    suite_spec :: authz_test_suite_spec(),
    suite_root_dir_path :: file_meta:path(),
    test_node :: node(),
    space_owner_session_id :: session:id(),
    files_owner_session_id :: session:id()
}).
-type authz_test_suite_ctx() :: #authz_test_suite_ctx{}.

-record(authz_test_case_ctx, {
    suite_ctx :: authz_test_suite_ctx(),
    test_case_name :: binary(),
    test_case_root_dir_path :: file_meta:path(),
    executioner_session_id :: session:id(),
    required_perms_per_file :: perms_per_file(),
    extra_data = #{} :: extra_data()
}).
-type authz_test_case_ctx() :: #authz_test_case_ctx{}.

-record(authz_privs_test_case_ctx, {
    test_case_ctx :: authz_test_case_ctx(),
    required_space_privs = [] :: {file_owner, [privileges:space_privilege()]} | [privileges:space_privilege()],
    full_perms_per_file :: perms_per_file() | #{file_id:file_guid() => file_meta:mode()},
    executioner_user_id :: od_user:id()
}).
-type authz_privs_test_case_ctx() :: #authz_privs_test_case_ctx{}.

-record(authz_cv_test_case_ctx, {
    test_case_ctx :: authz_test_case_ctx(),
    executioner_user_id :: od_user:id(),
    executioner_main_token :: tokens:serialized(),
    full_perms_per_file :: perms_per_file() | #{file_id:file_guid() => file_meta:mode()}
}).
-type authz_cv_test_case_ctx() :: #authz_cv_test_case_ctx{}.

-record(authz_posix_test_case_ctx, {
    test_case_ctx :: authz_test_case_ctx(),
    required_posix_perms_per_file :: posix_perms_per_file(),
    complementary_posix_perms_per_file :: posix_perms_per_file()
}).
-type authz_posix_test_case_ctx() :: #authz_posix_test_case_ctx{}.

-record(authz_acl_test_case_ctx, {
    test_case_ctx :: authz_test_case_ctx(),
    complementary_perms_per_file :: perms_per_file()
}).
-type authz_acl_test_case_ctx() :: #authz_acl_test_case_ctx{}.

-define(ATTEMPTS, 10).


%%%===================================================================
%%% TEST MECHANISM
%%%===================================================================


-spec run_suite(authz_test_suite_spec()) -> ok | no_return().
run_suite(TestSuiteSpec) ->
    TestSuiteCtx = init_test_suite(TestSuiteSpec),

    run_space_owner_test_group(TestSuiteCtx),
    run_space_privileges_test_group(TestSuiteCtx),
    run_file_protection_test_group(TestSuiteCtx),
    run_data_access_caveats_test_group(TestSuiteCtx),
    run_share_test_group(TestSuiteCtx),
    run_open_handle_mode_test_group(TestSuiteCtx),
    run_posix_permission_test_group(TestSuiteCtx),
    run_acl_permission_test_group(TestSuiteCtx).


%%%===================================================================
%%% SPACE PRIVILEGES TESTS
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tests permissions needed to perform operation as space owner.
%%
%% NOTE: below tests checks only posix/acl and space privileges cases as
%% space owner is specially handled for them. For all other mechanisms
%% (caveats, etc.) that works for space owner as for any other user -
%% space owner should be included in tests for that mechanism.
%% @end
%%--------------------------------------------------------------------
-spec run_space_owner_test_group(authz_test_suite_ctx()) ->
    ok | no_return().
run_space_owner_test_group(TestSuiteCtx = #authz_test_suite_ctx{
    suite_spec = #authz_test_suite_spec{
        space_id = SpaceId,
        space_owner_selector = SpaceOwnerSelector
    },
    test_node = TestNode
}) ->
    SpaceOwnerId = oct_background:get_user_id(SpaceOwnerSelector),
    ozt_spaces:set_privileges(SpaceId, SpaceOwnerId, []),

    lists:foreach(fun(PermsType) ->
        TestCaseName = build_test_case_name(["space_owner", PermsType]),
        TestCaseCtx = init_test_case(TestCaseName, SpaceOwnerSelector, TestSuiteCtx),

        % Assert that even with all perms and space privileges denied
        % operation will be performed
        FileGuids = maps:keys(TestCaseCtx#authz_test_case_ctx.required_perms_per_file),
        FullPermsPerFile = deny_full_perms(PermsType, TestNode, FileGuids),

        assert_operation(FullPermsPerFile, ok, TestCaseCtx),

        run_final_storage_ownership_check(TestCaseCtx)
    end, [posix, acl]).


%%%===================================================================
%%% SPACE PRIVILEGES TESTS
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tests space privileges needed to perform operation.
%% It will setup environment, add full posix or acl permissions and
%% assert that without space privileges operation cannot be performed and
%% with it it succeeds.
%% @end
%%--------------------------------------------------------------------
-spec run_space_privileges_test_group(authz_test_suite_ctx()) ->
    ok | no_return().
run_space_privileges_test_group(TestSuiteCtx) ->
    lists:foreach(fun(PermsType) ->
        PrivsTestCaseCtx = init_space_privileges_test_case(PermsType, TestSuiteCtx),

        try
            space_privileges_test_case(PrivsTestCaseCtx)
        after
            teardown_space_privileges_test_case(TestSuiteCtx)
        end

    end, [posix, acl]).


%% @private
-spec init_space_privileges_test_case(posix | acl, authz_test_suite_ctx()) ->
    authz_privs_test_case_ctx().
init_space_privileges_test_case(PermsType, TestSuiteCtx = #authz_test_suite_ctx{
    suite_spec = #authz_test_suite_spec{
        other_member_selector = OtherMemberSelector,
        posix_requires_space_privs = PosixSpacePrivs,
        acl_requires_space_privs = AclSpacePrivs
    },
    test_node = TestNode
}) ->
    TestCaseName = build_test_case_name(["space_privileges", PermsType]),
    TestCaseCtx = init_test_case(TestCaseName, OtherMemberSelector, TestSuiteCtx),

    % Assert that even with all perms set operation cannot be performed
    % without space privileges
    FileGuids = maps:keys(TestCaseCtx#authz_test_case_ctx.required_perms_per_file),
    FullPermsPerFile = set_full_perms(PermsType, TestNode, FileGuids),

    #authz_privs_test_case_ctx{
        test_case_ctx = TestCaseCtx,
        required_space_privs = case PermsType of
            posix -> PosixSpacePrivs;
            acl -> AclSpacePrivs
        end,
        full_perms_per_file = FullPermsPerFile,
        executioner_user_id = oct_background:get_user_id(OtherMemberSelector)
    }.


%% @private
-spec teardown_space_privileges_test_case(authz_test_suite_ctx()) ->
    authz_privs_test_case_ctx().
teardown_space_privileges_test_case(#authz_test_suite_ctx{
    suite_spec = #authz_test_suite_spec{
        space_id = SpaceId,
        files_owner_selector = FilesOwnerSelector,
        other_member_selector = OtherMemberSelector
    },
    test_node = TestNode
}) ->
    AdminPrivs = privileges:space_admin(),

    % Set full space privileges to not obstruct other test groups
    FilesOwnerId = oct_background:get_user_id(FilesOwnerSelector),
    set_user_space_privileges(TestNode, FilesOwnerId, SpaceId, AdminPrivs),

    OtherMemberId = oct_background:get_user_id(OtherMemberSelector),
    set_user_space_privileges(TestNode, OtherMemberId, SpaceId, AdminPrivs).


%% @private
-spec space_privileges_test_case(authz_privs_test_case_ctx()) ->
    ok | no_return().
space_privileges_test_case(PrivsTestCaseCtx = #authz_privs_test_case_ctx{
    % discriminator
    required_space_privs = {file_owner, RequiredSpacePrivs},

    % unpacked values
    test_case_ctx = TestCaseCtx = #authz_test_case_ctx{
        suite_ctx = #authz_test_suite_ctx{
            suite_spec = TestSuiteSpec = #authz_test_suite_spec{
                space_id = SpaceId,
                files_owner_selector = FilesOwnerSelector
            },
            files_owner_session_id = FilesOwnerSessionId,
            test_node = TestNode
        }
    },
    full_perms_per_file = FullPermsPerFile,
    executioner_user_id = ExecutionerUserId  % any user other than files owner!
}) ->
    % Operation should fail for any executioner with full privileges not being files owner
    set_user_space_privileges(TestNode, ExecutionerUserId, SpaceId, privileges:space_admin()),
    ExpError1 = get_exp_error(?EACCES, TestSuiteSpec),
    assert_operation(FullPermsPerFile, ExpError1, TestCaseCtx),

    % And succeed only for files owner with required privileges
    space_privileges_test_case(PrivsTestCaseCtx#authz_privs_test_case_ctx{
        required_space_privs = RequiredSpacePrivs,
        test_case_ctx = TestCaseCtx#authz_test_case_ctx{
            executioner_session_id = FilesOwnerSessionId
        },
        executioner_user_id = oct_background:get_user_id(FilesOwnerSelector)
    });

space_privileges_test_case(#authz_privs_test_case_ctx{
    required_space_privs = RequiredSpacePrivs,
    test_case_ctx = TestCaseCtx = #authz_test_case_ctx{
        suite_ctx = #authz_test_suite_ctx{
            suite_spec = TestSuiteSpec = #authz_test_suite_spec{space_id = SpaceId},
            test_node = TestNode
        }
    },
    full_perms_per_file = FullPermsPerFile,
    executioner_user_id = ExecutionerUserId
}) ->
    AllSpacePrivs = privileges:space_admin(),

    % Operation should fail if user doesn't have all of the required space privileges
    ExpError = get_exp_error(?EPERM, TestSuiteSpec),

    lists:foreach(fun(SomeOfRequiredPrivs) ->
        PrivsToSet = AllSpacePrivs -- SomeOfRequiredPrivs,
        set_user_space_privileges(TestNode, ExecutionerUserId, SpaceId, PrivsToSet),

        assert_operation(FullPermsPerFile, ExpError, TestCaseCtx)
    end, combinations(RequiredSpacePrivs) -- [[]]),

    % And should succeed if he has only required ones
    set_user_space_privileges(TestNode, ExecutionerUserId, SpaceId, RequiredSpacePrivs),
    assert_operation(FullPermsPerFile, ok, TestCaseCtx),

    run_final_storage_ownership_check(TestCaseCtx).


%%%===================================================================
%%% FILE PROTECTION TESTS
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tests file protection flags blocking operation.
%% It will setup environment, add full posix or acl permissions and
%% assert that with file protection flags operation cannot be performed
%% (even though full posix/acl perms are set).
%% @end
%%--------------------------------------------------------------------
-spec run_file_protection_test_group(authz_test_case_ctx()) ->
    ok | no_return().
run_file_protection_test_group(TestSuiteCtx = #authz_test_suite_ctx{suite_spec = TestSuiteSpec}) ->
    ProtectionFlagsToSet = infer_blocking_protection_flags(TestSuiteSpec),

    lists:foreach(fun({ExecutionerSelector, UserDesc}) ->

        TestCaseName = build_test_case_name(["file_protection", UserDesc]),
        TestCaseCtx = init_test_case(TestCaseName, ExecutionerSelector, TestSuiteCtx),
        run_file_protection_test_case(ProtectionFlagsToSet, TestCaseCtx)

    end, [
        {TestSuiteSpec#authz_test_suite_spec.space_owner_selector, "space_owner"},
        {TestSuiteSpec#authz_test_suite_spec.files_owner_selector, "files_owner"},
        {TestSuiteSpec#authz_test_suite_spec.other_member_selector, "other_space_member"}
    ]).


%% @private
-spec infer_blocking_protection_flags(authz_test_case_ctx()) ->
    data_access_control:bitmask().
infer_blocking_protection_flags(#authz_test_suite_spec{files = FileTreeSpec}) ->
    AllNeededPerms = gather_required_perms(FileTreeSpec),
    AllNeededPermsBitmask = authz_test_utils:perms_to_bitmask(AllNeededPerms),

    lists:foldl(fun({ProtectionFlag, BlockedPerms}, Acc) ->
        case ?has_any_flags(AllNeededPermsBitmask, BlockedPerms) of
            true -> ?set_flags(Acc, ProtectionFlag);
            false -> Acc
        end
    end, ?no_flags_mask, [
        {?DATA_PROTECTION, ?FILE_DATA_PROTECTION_BLOCKED_OPERATIONS},
        {?METADATA_PROTECTION bor ?DATA_PROTECTION, ?FILE_METADATA_PROTECTION_BLOCKED_OPERATIONS}
    ]).


%% @private
-spec gather_required_perms(file_tree_spec() | [file_tree_spec()]) -> perms().
gather_required_perms(FileTreeSpec) ->
    lists:usort(gather_required_perms(FileTreeSpec, [])).


%% @private
-spec gather_required_perms(file_tree_spec() | [file_tree_spec()], perms()) -> perms().
gather_required_perms(#ct_authz_file_spec{required_perms = RequiredPerms}, Acc) ->
    RequiredPerms ++ Acc;

gather_required_perms(#ct_authz_dir_spec{required_perms = RequiredPerms, children = ChildrenSpec}, Acc) ->
    lists:foldl(fun gather_required_perms/2, RequiredPerms ++ Acc, ChildrenSpec);

gather_required_perms(FileTreeSpec, Acc) when is_list(FileTreeSpec) ->
    lists:foldl(fun gather_required_perms/2, Acc, FileTreeSpec).


%% @private
-spec run_file_protection_test_case(data_access_control:bitmask(), authz_test_case_ctx()) ->
    ok | no_return().
run_file_protection_test_case(0, TestCaseCtx = #authz_test_case_ctx{
    suite_ctx = TestSuiteCtx = #authz_test_suite_ctx{
        suite_spec = #authz_test_suite_spec{space_id = SpaceId},
        test_node = TestNode
    },
    executioner_session_id = ExecutionerSessionId,
    required_perms_per_file = RequiredPermsPerFile,
    extra_data = ExtraData
}) ->
    {ok, ExecutionerUserId} = rpc:call(TestNode, session, get_user_id, [ExecutionerSessionId]),

    % Assert that even with full protection flags set operation can be performed
    FullPermsPerFile = set_full_perms(TestSuiteCtx, maps:keys(RequiredPermsPerFile)),

    FullProtectionFlags = ?METADATA_PROTECTION bor ?DATA_PROTECTION,
    establish_dataset_on_test_case_root_dir(FullProtectionFlags, TestCaseCtx),
    await_dataset_eff_cache_clearing(TestNode, SpaceId, ExecutionerUserId, ExtraData),
    assert_operation(FullPermsPerFile, ok, TestCaseCtx),

    run_final_storage_ownership_check(TestCaseCtx);

run_file_protection_test_case(BlockingProtectionFlags, TestCaseCtx = #authz_test_case_ctx{
    suite_ctx = TestSuiteCtx = #authz_test_suite_ctx{
        suite_spec = TestSuiteSpec = #authz_test_suite_spec{
            space_id = SpaceId
        },
        space_owner_session_id = SpaceOwnerSessionId,
        test_node = TestNode
    },
    executioner_session_id = ExecutionerSessionId,
    required_perms_per_file = RequiredPermsPerFile,
    extra_data = ExtraData
}) ->
    {ok, ExecutionerUserId} = rpc:call(TestNode, session, get_user_id, [ExecutionerSessionId]),

    % Assert that even with all perms set operation cannot be performed
    % with file protection flags set
    FullPermsPerFile = set_full_perms(TestSuiteCtx, maps:keys(RequiredPermsPerFile)),

    % With file protection set operation should fail
    ExpError = get_exp_error(?EPERM, TestSuiteSpec),

    DatasetId = establish_dataset_on_test_case_root_dir(BlockingProtectionFlags, TestCaseCtx),
    await_dataset_eff_cache_clearing(TestNode, SpaceId, ExecutionerUserId, ExtraData),
    assert_operation(FullPermsPerFile, ExpError, TestCaseCtx),

    % And should succeed without it
    ok = opt_datasets:update(
        TestNode, SpaceOwnerSessionId, DatasetId, undefined, ?no_flags_mask, BlockingProtectionFlags
    ),
    await_dataset_eff_cache_clearing(TestNode, SpaceId, ExecutionerUserId, ExtraData),
    assert_operation(FullPermsPerFile, ok, TestCaseCtx),

    run_final_storage_ownership_check(TestCaseCtx).


%% @private
-spec establish_dataset_on_test_case_root_dir(data_access_control:bitmask(), authz_test_case_ctx()) ->
    dataset:id().
establish_dataset_on_test_case_root_dir(InitialProtectionFlags, #authz_test_case_ctx{
    suite_ctx = #authz_test_suite_ctx{
        space_owner_session_id = SpaceOwnerSessionId,
        test_node = TestNode
    },
    test_case_root_dir_path = TestCaseRootDirPath,
    extra_data = ExtraData
}) ->
    TestCaseRootDirKey = maps:get(TestCaseRootDirPath, ExtraData),
    {ok, DatasetId} = opt_datasets:establish(
        TestNode, SpaceOwnerSessionId, TestCaseRootDirKey, InitialProtectionFlags
    ),
    DatasetId.


%% @private
-spec await_dataset_eff_cache_clearing(node(), od_space:id(), od_user:id(), extra_data()) -> ok.
await_dataset_eff_cache_clearing(Node, SpaceId, UserId, ExtraData) ->
    Attempts = 30,
    Interval = 100,
    ProtectionFlagsCache = binary_to_atom(<<"dataset_effective_cache_", SpaceId/binary>>, utf8),

    AreProtectionFlagsCached = fun(FileUuid) ->
        case rpc:call(Node, bounded_cache, get, [ProtectionFlagsCache, FileUuid]) of
            {ok, _} -> true;
            ?ERROR_NOT_FOUND -> false
        end
    end,

    IsPermEntryCached = fun(Entry) ->
        case rpc:call(Node, permissions_cache, check_permission, [Entry]) of
            {ok, _} -> true;
            _ -> false
        end
    end,

    lists:foreach(fun
        (?FILE_REF(FileGuid)) ->
            FileUuid = file_id:guid_to_uuid(FileGuid),
            ?assertMatch(false, AreProtectionFlagsCached(FileUuid), Attempts, Interval),
            ?assertMatch(
                false, IsPermEntryCached({{user_perms_matrix, UserId, FileGuid}}), Attempts, Interval
            );
        (_) ->
            ok
    end, maps:values(ExtraData)).


%%%===================================================================
%%% CAVEATS TESTS
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
-spec run_data_access_caveats_test_group(authz_test_suite_ctx()) ->
    ok | no_return().
run_data_access_caveats_test_group(TestSuiteCtx = #authz_test_suite_ctx{
    suite_spec = #authz_test_suite_spec{
        space_owner_selector = SpaceOwnerSelector,
        files_owner_selector = FilesOwnerSelector
    }
}) ->
    lists:foreach(fun({CaveatType, ExecutionerSelector}) ->

        TestCaseName = build_test_case_name(["cv", ExecutionerSelector, CaveatType]),
        CvTestCaseCtx = init_data_access_caveats_test_case(TestCaseName, ExecutionerSelector, TestSuiteCtx),
        run_data_access_caveats_test_case(CaveatType, CvTestCaseCtx)

    end, [
        {data_path, SpaceOwnerSelector},
        {data_path, FilesOwnerSelector},
        {data_objectid, SpaceOwnerSelector},
        {data_objectid, FilesOwnerSelector},
        {data_readonly, SpaceOwnerSelector},
        {data_readonly, FilesOwnerSelector}
    ]).


%% @private
-spec init_data_access_caveats_test_case(
    binary(),
    session:id() | oct_background:entity_selector(),
    authz_test_suite_ctx()
) ->
    authz_cv_test_case_ctx().
init_data_access_caveats_test_case(TestCaseName, ExecutionerSelector, TestSuiteCtx) ->
    TestCaseCtx = init_test_case(TestCaseName, ExecutionerSelector, TestSuiteCtx),

    % Assert that even with all perms set operation can be performed
    % only when caveats allows it
    FileGuids = maps:keys(TestCaseCtx#authz_test_case_ctx.required_perms_per_file),
    FullPermsPerFile = set_full_perms(TestSuiteCtx, FileGuids),

    ExecutionerUserId = oct_background:get_user_id(ExecutionerSelector),
    ExecutionerMainToken = provider_onenv_test_utils:create_oz_temp_access_token(ExecutionerUserId),

    #authz_cv_test_case_ctx{
        test_case_ctx = TestCaseCtx,
        executioner_user_id = ExecutionerUserId,
        executioner_main_token = ExecutionerMainToken,
        full_perms_per_file = FullPermsPerFile
    }.


%% @private
-spec run_data_access_caveats_test_case(
    data_path | data_objectid | data_readonly,
    authz_cv_test_case_ctx()
) ->
    ok | no_return().
run_data_access_caveats_test_case(data_path, CvTestCaseCtx = #authz_cv_test_case_ctx{
    test_case_ctx = TestCaseCtx = #authz_test_case_ctx{
        suite_ctx = #authz_test_suite_ctx{suite_spec = TestSuiteSpec}
    },
    full_perms_per_file = FullPermsPerFile
}) ->
    ExpError = get_exp_error(?EACCES, TestSuiteSpec),
    assert_operation(FullPermsPerFile, ExpError, constrain_executioner_session(
        CvTestCaseCtx, #cv_data_path{whitelist = [<<"i_am_nowhere">>]}
    )),

    ExpResult = cv_test_case_get_exp_result(CvTestCaseCtx),
    assert_operation(FullPermsPerFile, ExpResult, constrain_executioner_session(
        CvTestCaseCtx, #cv_data_path{whitelist = [get_test_case_root_dir_canonical_path(TestCaseCtx)]}
    )),
    run_final_storage_ownership_check(TestCaseCtx);

run_data_access_caveats_test_case(data_objectid, CvTestCaseCtx = #authz_cv_test_case_ctx{
    test_case_ctx = TestCaseCtx = #authz_test_case_ctx{
        suite_ctx = #authz_test_suite_ctx{suite_spec = TestSuiteSpec},
        test_case_root_dir_path = TestCaseRootDirPath,
        extra_data = ExtraData
    },
    full_perms_per_file = FullPermsPerFile
}) ->
    ExpError = get_exp_error(?EACCES, TestSuiteSpec),
    DummyGuid = <<"Z3VpZCNfaGFzaF9mNmQyOGY4OTNjOTkxMmVh">>,
    {ok, DummyObjectId} = file_id:guid_to_objectid(DummyGuid),
    assert_operation(FullPermsPerFile, ExpError, constrain_executioner_session(
        CvTestCaseCtx, #cv_data_objectid{whitelist = [DummyObjectId]}
    )),

    ExpResult = cv_test_case_get_exp_result(CvTestCaseCtx),
    ?FILE_REF(TestCaseRootDirGuid) = maps:get(TestCaseRootDirPath, ExtraData),
    {ok, TestCaseRootDirObjectId} = file_id:guid_to_objectid(TestCaseRootDirGuid),
    assert_operation(FullPermsPerFile, ExpResult, constrain_executioner_session(
        CvTestCaseCtx, #cv_data_objectid{whitelist = [TestCaseRootDirObjectId]}
    )),
    run_final_storage_ownership_check(TestCaseCtx);

run_data_access_caveats_test_case(data_readonly, CvTestCaseCtx = #authz_cv_test_case_ctx{
    test_case_ctx = #authz_test_case_ctx{
        suite_ctx = #authz_test_suite_ctx{
            suite_spec = TestSuiteSpec = #authz_test_suite_spec{
                available_in_readonly_mode = AvailableInReadonlyMode
            }
        }
    },
    full_perms_per_file = FullPermsPerFile
}) ->
    CvTestCaseCtx1 = constrain_executioner_session(CvTestCaseCtx, #cv_data_readonly{}),

    case AvailableInReadonlyMode of
        true ->
            ExpResult = cv_test_case_get_exp_result(CvTestCaseCtx),
            assert_operation(FullPermsPerFile, ExpResult, CvTestCaseCtx1),
            run_final_storage_ownership_check(CvTestCaseCtx1#authz_cv_test_case_ctx.test_case_ctx);
        false ->
            ExpError = get_exp_error(?EACCES, TestSuiteSpec),
            assert_operation(FullPermsPerFile, ExpError, CvTestCaseCtx1)
    end.


%% @private
-spec constrain_executioner_session(authz_cv_test_case_ctx(), caveats:caveat()) ->
    authz_cv_test_case_ctx().
constrain_executioner_session(CvTestCaseCtx = #authz_cv_test_case_ctx{
    test_case_ctx = TestCaseCtx = #authz_test_case_ctx{
        suite_ctx = #authz_test_suite_ctx{test_node = TestNode}
    },
    executioner_user_id = UserId,
    executioner_main_token = MainToken
}, Caveat) ->
    TokenWithCaveat = tokens:confine(MainToken, Caveat),
    ConstrainedSessionId = authz_test_utils:create_session(TestNode, UserId, TokenWithCaveat),
    CvTestCaseCtx#authz_cv_test_case_ctx{test_case_ctx = TestCaseCtx#authz_test_case_ctx{
        executioner_session_id = ConstrainedSessionId
    }}.


%% @private
-spec cv_test_case_get_exp_result(authz_cv_test_case_ctx()) -> ok | errors:error().
cv_test_case_get_exp_result(#authz_cv_test_case_ctx{test_case_ctx = #authz_test_case_ctx{
    suite_ctx = #authz_test_suite_ctx{suite_spec = #authz_test_suite_spec{
        blocked_by_data_access_caveats = {true, ExpError}
    }}
}}) ->
    ExpError;

cv_test_case_get_exp_result(_) ->
    ok.


%% @private
-spec get_test_case_root_dir_canonical_path(authz_test_case_ctx()) ->
    file_meta:path().
get_test_case_root_dir_canonical_path(#authz_test_case_ctx{
    suite_ctx = #authz_test_suite_ctx{suite_spec = #authz_test_suite_spec{
        space_id = SpaceId
    }},
    test_case_root_dir_path = TestCaseRootDirPath
}) ->
    [Sep, _SpaceName | PathTokens] = filepath_utils:split(TestCaseRootDirPath),
    filepath_utils:join([Sep, SpaceId | PathTokens]).


%%%===================================================================
%%% SHARE TESTS
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tests permissions needed to perform operation in share mode.
%% If operation is not nat available in share mode then checks
%% that even with all perms set ?EACCES should be returned.
%% @end
%%--------------------------------------------------------------------
-spec run_share_test_group(authz_test_suite_ctx()) ->
    ok | no_return().
run_share_test_group(#authz_test_suite_ctx{suite_spec = #authz_test_suite_spec{
    available_for_share_guid = not_a_file_guid_based_operation
}}) ->
    ok;

run_share_test_group(TestSuiteCtx = #authz_test_suite_ctx{
    suite_spec = #authz_test_suite_spec{
        space_owner_selector = SpaceOwnerSelector,
        files_owner_selector = FilesOwnerSelector,
        other_member_selector = OtherMemberSelector,
        non_member_selector = NonMemberSelector
    }
}) ->
    lists:foreach(fun({ExecutionerSelector, PermsType, TestCaseName}) ->

        TestCaseCtx = init_share_test_case(TestCaseName, ExecutionerSelector, TestSuiteCtx),
        run_share_test_case(PermsType, TestCaseCtx)

    end, [
        {SpaceOwnerSelector, posix, <<"share_space_owner_posix">>},
        {FilesOwnerSelector, posix, <<"share_files_owner_posix">>},
        {OtherMemberSelector, posix, <<"share_other_space_member_posix">>},
        {NonMemberSelector, posix, <<"share_non_space_member_posix">>},
        {?GUEST_SESS_ID, posix, <<"share_guest_posix">>},
        {SpaceOwnerSelector, {acl, allow}, <<"share_space_owner_acl_allow">>},
        {SpaceOwnerSelector, {acl, deny}, <<"share_space_owner_acl_deny">>},
        {FilesOwnerSelector, {acl, allow}, <<"share_files_owner_acl_allow">>},
        {FilesOwnerSelector, {acl, deny}, <<"share_files_owner_acl_deny">>},
        {OtherMemberSelector, {acl, allow}, <<"share_other_space_member_acl_allow">>},
        {OtherMemberSelector, {acl, deny}, <<"share_other_space_member_acl_deny">>},
        {NonMemberSelector, {acl, allow}, <<"share_non_space_member_acl_allow">>},
        {NonMemberSelector, {acl, deny}, <<"share_non_space_member_acl_deny">>},
        {?GUEST_SESS_ID, {acl, allow}, <<"share_guest_acl_allow">>},
        {?GUEST_SESS_ID, {acl, deny}, <<"share_guest_acl_deny">>}
    ]).


%% @private
-spec init_share_test_case(
    binary(),
    session:id() | oct_background:entity_selector(),
    authz_test_suite_ctx()
) ->
    authz_test_case_ctx().
init_share_test_case(TestCaseName, ExecutionerSelector, TestSuiteCtx = #authz_test_suite_ctx{
    space_owner_session_id = SpaceOwnerSessionId,
    test_node = TestNode
}) ->
    TestCaseCtx = init_test_case(TestCaseName, ExecutionerSelector, TestSuiteCtx),
    ExtraData = TestCaseCtx#authz_test_case_ctx.extra_data,

    TestCaseRootDirPath = TestCaseCtx#authz_test_case_ctx.test_case_root_dir_path,
    TestCaseRootDirKey = maps:get(TestCaseRootDirPath, ExtraData),
    {ok, ShareId} = opt_shares:create(
        TestNode, SpaceOwnerSessionId, TestCaseRootDirKey, TestCaseName
    ),
    TestCaseCtx#authz_test_case_ctx{
        extra_data = maps:map(fun
            (_, #file_ref{guid = FileGuid} = FileRef) ->
                FileRef#file_ref{guid = file_id:guid_to_share_guid(FileGuid, ShareId)};
            (_, Val) ->
                Val
        end, ExtraData)
    }.


%% @private
-spec run_share_test_case(posix | {acl, allow | deny}, authz_test_case_ctx()) ->
    ok | no_return().
run_share_test_case(PermsType0, TestCaseCtx = #authz_test_case_ctx{
    suite_ctx = #authz_test_suite_ctx{
        suite_spec = TestSuiteSpec = #authz_test_suite_spec{available_for_share_guid = false},
        test_node = TestNode
    },
    required_perms_per_file = RequiredPermsPerFile
}) ->
    % Set all posix or acl (depending on scenario) perms to files
    PermsType1 = case PermsType0 of
        posix -> posix;
        {acl, _} -> acl
    end,
    FullPermsPerFile = set_full_perms(PermsType1, TestNode, maps:keys(RequiredPermsPerFile)),

    % Even with all perms set operation should fail
    ExpError = get_exp_error(?EPERM, TestSuiteSpec),
    assert_operation(FullPermsPerFile, ExpError, TestCaseCtx);

run_share_test_case(posix, TestCaseCtx) ->
    PosixTestCaseCtx = build_posix_test_case_ctx(TestCaseCtx),
    run_posix_mode_test_case(other, PosixTestCaseCtx);

run_share_test_case({acl, AceType}, TestCaseCtx) ->
    AclTestCaseCtx = build_acl_test_case_ctx(TestCaseCtx),
    AceWho = lists_utils:random_element([?everyone, ?anonymous]),
    run_acl_permission_test_case(AceType, AceWho, ?no_flags_mask, AclTestCaseCtx).


%%%===================================================================
%%% OPEN_HANDLE MODE TESTS
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tests open data session mode. For that it will setup environment,
%% add full acl permissions and assert that even with full other/anonymous
%% perms set only operations also available in share mode can be performed.
%% @end
%%--------------------------------------------------------------------
-spec run_open_handle_mode_test_group(authz_test_suite_ctx()) ->
    ok | no_return().
run_open_handle_mode_test_group(TestSuiteCtx = #authz_test_suite_ctx{
    suite_spec = #authz_test_suite_spec{
        files_owner_selector = FilesOwnerSelector,
        space_owner_selector = SpaceOwnerSelector,
        other_member_selector = OtherMemberSelector
    }
}) ->
    lists:foreach(fun({ExecutionerSelector, TestCaseSuffix, PermsType}) ->

        TestCaseName = build_test_case_name(["open_handle_mode", TestCaseSuffix]),
        TestCaseCtx = init_open_handle_mode_test_case(TestCaseName, ExecutionerSelector, TestSuiteCtx),
        run_open_handle_mode_test_case(PermsType, TestCaseCtx)

    end, [
        {FilesOwnerSelector, "files_owner_posix", posix},
        {SpaceOwnerSelector, "space_owner_posix", posix},
        {OtherMemberSelector, "other_space_member_posix", posix},
        {FilesOwnerSelector, "files_owner_acl", acl},
        {SpaceOwnerSelector, "space_owner_acl", acl},
        {OtherMemberSelector, "other_space_member_acl", acl}
    ]).


%% @private
-spec init_open_handle_mode_test_case(
    binary(),
    session:id() | oct_background:entity_selector(),
    authz_test_suite_ctx()
) ->
    authz_test_case_ctx().
init_open_handle_mode_test_case(TestCaseName, ExecutionerSelector, TestSuiteCtx) ->
    TestNode = TestSuiteCtx#authz_test_suite_ctx.test_node,
    TestCaseCtx = init_test_case(TestCaseName, ExecutionerSelector, TestSuiteCtx),

    ExecutionerUserId = oct_background:get_user_id(ExecutionerSelector),
    ExecutionerToken = provider_onenv_test_utils:create_oz_temp_access_token(ExecutionerUserId),
    TestCaseCtx#authz_test_case_ctx{
        executioner_session_id = authz_test_utils:create_session(
            TestNode, ExecutionerUserId, ExecutionerToken, open_handle
        )
    }.


%% @private
-spec run_open_handle_mode_test_case(posix | acl, authz_test_case_ctx()) ->
    ok | no_return().
run_open_handle_mode_test_case(PermsType, TestCaseCtx = #authz_test_case_ctx{
    suite_ctx = #authz_test_suite_ctx{
        suite_spec = TestSuiteSpec = #authz_test_suite_spec{available_in_open_handle_mode = false},
        test_node = TestNode
    },
    required_perms_per_file = RequiredPermsPerFile
}) ->
    % If operation is not available in share/public mode then operation
    % should be rejected even if all permissions are granted
    FullPermsPerFile = set_full_perms(PermsType, TestNode, maps:keys(RequiredPermsPerFile)),
    ExpError = get_exp_error(?EPERM, TestSuiteSpec),
    assert_operation(FullPermsPerFile, ExpError, TestCaseCtx);

% Operation is available in share/public mode but access is still controlled
% (even for space owner) by posix mode (other bits) or acl
run_open_handle_mode_test_case(posix, TestCaseCtx) ->
    PosixTestCaseCtx = build_posix_test_case_ctx(TestCaseCtx),
    run_posix_mode_test_case(other, PosixTestCaseCtx);

run_open_handle_mode_test_case(acl, TestCaseCtx) ->
    AclTestCaseCtx = build_acl_test_case_ctx(TestCaseCtx),
    AceType = ?RAND_ELEMENT([allow, deny]),
    run_acl_permission_test_case(AceType, ?anonymous, ?no_flags_mask, AclTestCaseCtx).


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
-spec run_posix_permission_test_group(authz_test_suite_ctx()) ->
    ok | no_return().
run_posix_permission_test_group(TestSuiteCtx = #authz_test_suite_ctx{
    suite_spec = TestSuiteSpec
}) ->
    lists:foreach(fun({UserType, ExecutionerSelector}) ->
        TestCaseName = build_test_case_name(["posix", UserType]),
        TestCaseCtx = init_test_case(TestCaseName, ExecutionerSelector, TestSuiteCtx),
        PosixTestCaseCtx = build_posix_test_case_ctx(TestCaseCtx),

        try
            run_posix_permission_test_case(UserType, PosixTestCaseCtx)
        catch Class:Reason:Stacktrace ->
            TestCaseRootDirPath = TestCaseCtx#authz_test_case_ctx.test_case_root_dir_path,
            RequiredPerms = format_posix_perms_per_file(PosixTestCaseCtx),

            ?ct_pal_exception(
                "POSIX test case failure ~s",
                [?autoformat(TestCaseRootDirPath, RequiredPerms)],
                Class, Reason, Stacktrace
            ),
            error(posix_perms_test_failed)
        end
    end, [
        {files_owner, TestSuiteSpec#authz_test_suite_spec.files_owner_selector},
        {space_member, TestSuiteSpec#authz_test_suite_spec.other_member_selector},
        {non_space_member, TestSuiteSpec#authz_test_suite_spec.non_member_selector}
    ]).


%% @private
-spec build_posix_test_case_ctx(authz_test_case_ctx()) ->
    authz_posix_test_case_ctx().
build_posix_test_case_ctx(TestCaseCtx = #authz_test_case_ctx{
    required_perms_per_file = RequiredPermsPerFile
}) ->
    RequiredPosixPermsPerFile = maps:map(fun(_, Perms) ->
        lists:usort(lists:flatmap(fun authz_test_utils:perm_to_posix_perms/1, Perms))
    end, RequiredPermsPerFile),

    ComplementaryPosixPermsPerFile = maps:fold(fun(FileGuid, FileRequiredPosixPerms, Acc) ->
        Acc#{FileGuid => ?ALL_POSIX_PERMS -- FileRequiredPosixPerms}
    end, #{}, RequiredPosixPermsPerFile),

    #authz_posix_test_case_ctx{
        test_case_ctx = TestCaseCtx,
        required_posix_perms_per_file = RequiredPosixPermsPerFile,
        complementary_posix_perms_per_file = ComplementaryPosixPermsPerFile
    }.


%% @private
-spec format_posix_perms_per_file(authz_posix_test_case_ctx()) -> json_utils:json_map().
format_posix_perms_per_file(#authz_posix_test_case_ctx{
    test_case_ctx = #authz_test_case_ctx{suite_ctx = #authz_test_suite_ctx{test_node = TestNode}},
    required_posix_perms_per_file = RequiredPosixPermsPerFile
}) ->
    format_perms_per_file(TestNode, RequiredPosixPermsPerFile).


%% @private
-spec run_posix_permission_test_case(
    files_owner | space_member | non_space_member,
    authz_posix_test_case_ctx()
) ->
    ok | no_return().
run_posix_permission_test_case(files_owner, PosixTestCaseCtx = #authz_posix_test_case_ctx{
    test_case_ctx = TestCaseCtx = #authz_test_case_ctx{
        suite_ctx = #authz_test_suite_ctx{test_node = TestNode}
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
            authz_test_utils:set_modes(TestNode, ModesPerFile),
            assert_operation(ModesPerFile, ok, TestCaseCtx),

            run_final_storage_ownership_check(TestCaseCtx);

        false ->
            run_posix_mode_test_case(owner, PosixTestCaseCtx#authz_posix_test_case_ctx{
                required_posix_perms_per_file = RequiredRWXPosixPermsPerFile
            })
    end;

run_posix_permission_test_case(space_member, PosixTestCaseCtx = #authz_posix_test_case_ctx{
    test_case_ctx = TestCaseCtx = #authz_test_case_ctx{
        suite_ctx = #authz_test_suite_ctx{
            suite_spec = TestSuiteSpec,
            test_node = TestNode
        }
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
            ExpError = get_exp_error(?EACCES, TestSuiteSpec),
            ModesPerFile = maps:map(fun(_, _) -> 8#777 end, ComplementaryPosixPermsPerFile),
            authz_test_utils:set_modes(TestNode, ModesPerFile),
            assert_operation(ModesPerFile, ExpError, TestCaseCtx);

        false ->
            RequiredRWXPosixPermsPerFile = filter_rwx_posix_perms_per_file(RequiredPosixPermsPerFile),

            run_posix_mode_test_case(group, PosixTestCaseCtx#authz_posix_test_case_ctx{
                required_posix_perms_per_file = RequiredRWXPosixPermsPerFile
            })
    end;

run_posix_permission_test_case(non_space_member, #authz_posix_test_case_ctx{
    test_case_ctx = TestCaseCtx = #authz_test_case_ctx{
        suite_ctx = #authz_test_suite_ctx{
            suite_spec = TestSuiteSpec = #authz_test_suite_spec{operation = Operation},
            test_node = TestNode
        },
        test_case_root_dir_path = TestCaseRootDirPath,
        extra_data = ExtraData
    },
    required_posix_perms_per_file = RequiredPosixPermsPerFile
}) ->
    % Users not belonging to space or unauthorized should not be able to conduct any operation
    ExpEaccesError = get_exp_error(?EACCES, TestSuiteSpec),
    FullModesPerFile = maps:map(fun(_, _) -> 8#777 end, RequiredPosixPermsPerFile),
    authz_test_utils:set_modes(TestNode, FullModesPerFile),
    assert_operation(FullModesPerFile, ExpEaccesError, TestCaseCtx),

    % Some operations cannot be performed with special session (either root or guest)
    % and result in eagain error instead of enoent
    ?assertMatch({error, _}, Operation(TestNode, ?GUEST_SESS_ID, TestCaseRootDirPath, ExtraData)).


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
-spec run_posix_mode_test_case(posix_user_type(), authz_posix_test_case_ctx()) ->
    ok | no_return().
run_posix_mode_test_case(PosixUserType, #authz_posix_test_case_ctx{
    test_case_ctx = TestCaseCtx = #authz_test_case_ctx{
        suite_ctx = #authz_test_suite_ctx{
            suite_spec = TestSuiteSpec,
            test_node = TestNode
        }
    },
    required_posix_perms_per_file = RequiredPosixPermsPerFile,
    complementary_posix_perms_per_file = ComplementaryPosixPermsPerFile
}) ->
    FlattenedRequiredModes = lists:map(fun({Guid, PosixPerm}) ->
        {Guid, authz_test_utils:posix_perm_to_mode(PosixPerm, PosixUserType)}
    end, flatten_perms_per_file(RequiredPosixPermsPerFile)),

    [RequiredModesComb | EaccesModesCombs] = combinations(FlattenedRequiredModes),

    ComplementaryModesPerFile = maps:map(fun(_, Perms) ->
        lists:foldl(fun(Perm, Acc) ->
            Acc bor authz_test_utils:posix_perm_to_mode(Perm, PosixUserType)
        end, 0, Perms)
    end, ComplementaryPosixPermsPerFile),

    % Granting all modes but required ones should result in eacces
    lists:foreach(fun(EaccesModeComb) ->
        EaccesModesPerFile = lists:foldl(fun({Guid, Mode}, Acc) ->
            Acc#{Guid => Mode bor maps:get(Guid, Acc)}
        end, ComplementaryModesPerFile, EaccesModeComb),

        authz_test_utils:set_modes(TestNode, EaccesModesPerFile),

        ExpError = get_exp_error(?EACCES, TestSuiteSpec),
        assert_operation(EaccesModesPerFile, ExpError, TestCaseCtx)
    end, EaccesModesCombs),

    % Granting only required modes should result in success
    RequiredModesPerFile = lists:foldl(fun({Guid, Mode}, Acc) ->
        Acc#{Guid => Mode bor maps:get(Guid, Acc, 0)}
    end, #{}, RequiredModesComb),

    authz_test_utils:set_modes(TestNode, RequiredModesPerFile),
    assert_operation(RequiredModesPerFile, ok, TestCaseCtx),

    run_final_storage_ownership_check(TestCaseCtx).


%%%===================================================================
%%% ACL TESTS
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tests acl permissions needed to perform operation.
%% For each type (`allow`, `deny`) and identifier (`OWNER@`, user_id,
%% group_id, `EVERYONE@`) it will setup environment and test combination
%% of acl perms. It will assert that combinations not having all required
%% perms fails and that combination consisting of only required perms succeeds.
%% @end
%%--------------------------------------------------------------------
-spec run_acl_permission_test_group(authz_test_suite_ctx()) ->
    ok | no_return().
run_acl_permission_test_group(TestSuiteCtx = #authz_test_suite_ctx{
    suite_spec = #authz_test_suite_spec{
        files_owner_selector = FilesOwnerSelector,
        other_member_selector = OtherMemberSelector
    }
}) ->
    OtherMemberId = oct_background:get_user_id(OtherMemberSelector),
%%    OtherMemberGroupId = <<"todo">>,  %% TODO VFS-11774

    lists:foreach(fun({ExecutionerSelector, TestCaseName, AceType, AceWho, AceFlags}) ->

        TestCaseCtx = init_test_case(TestCaseName, ExecutionerSelector, TestSuiteCtx),
        AclTestCaseCtx = build_acl_test_case_ctx(TestCaseCtx),
        run_acl_permission_test_case(AceType, AceWho, AceFlags, AclTestCaseCtx)

    end, [
        {FilesOwnerSelector, <<"acl_allow-files_owner">>, allow, ?owner, ?no_flags_mask},
        {OtherMemberSelector, <<"acl_allow-other_space_member">>, allow, OtherMemberId, ?no_flags_mask},
%%        {OtherMemberGroupId, <<"acl_user_group_allow">>, allow, OtherMemberGroupId, ?identifier_group_mask},
        {OtherMemberSelector, <<"acl_allow-everyone">>, allow, ?everyone, ?no_flags_mask},

        {FilesOwnerSelector, <<"acl_deny-files_owner">>, deny, ?owner, ?no_flags_mask},
        {OtherMemberSelector, <<"acl_deny-other_space_member">>, deny, OtherMemberId, ?no_flags_mask},
%%        {OtherMemberGroupId, <<"acl_user_group_deny">>, deny, OtherMemberGroupId, ?identifier_group_mask},
        {OtherMemberSelector, <<"acl_deny-everyone">>, deny, ?everyone, ?no_flags_mask}
    ]).


%% @private
-spec build_acl_test_case_ctx(authz_test_case_ctx()) ->
    authz_acl_test_case_ctx().
build_acl_test_case_ctx(TestCaseCtx = #authz_test_case_ctx{
    suite_ctx = #authz_test_suite_ctx{test_node = TestNode},
    required_perms_per_file = RequiredPermsPerFile
}) ->
    ComplementaryPermsPerFile = maps:fold(fun(FileGuid, FileRequiredPerms, Acc) ->
        Acc#{FileGuid => authz_test_utils:complementary_perms(
            TestNode, FileGuid, FileRequiredPerms
        )}
    end, #{}, RequiredPermsPerFile),

    lists:flatmap(fun({Guid, Perms}) ->
        [{Guid, Perm} || Perm <- Perms]
    end, maps:to_list(RequiredPermsPerFile)),

    #authz_acl_test_case_ctx{
        test_case_ctx = TestCaseCtx,
        complementary_perms_per_file = ComplementaryPermsPerFile
    }.


%% @private
-spec run_acl_permission_test_case(
    AceType :: allow | deny,
    AceWho :: binary(),
    AceFlags :: data_access_control:bitmask(),
    authz_test_case_ctx()
) ->
    ok | no_return().
run_acl_permission_test_case(allow, AceWho, AceFlags, #authz_acl_test_case_ctx{
    test_case_ctx = TestCaseCtx = #authz_test_case_ctx{
        suite_ctx = #authz_test_suite_ctx{
            suite_spec = TestSuiteSpec,
            test_node = TestNode
        },
        required_perms_per_file = RequiredPermsPerFile
    },
    complementary_perms_per_file = ComplementaryPermsPerFile
}) ->
    [_ | EaccesPermsCombs] = combinations(flatten_perms_per_file(RequiredPermsPerFile)),

    % Granting all perms without required ones should result in ?EACCES
    ExpError = get_exp_error(?EACCES, TestSuiteSpec),

    lists:foreach(fun(EaccesPermComb) ->
        EaccesPermsPerFile = lists:foldl(fun({Guid, Perm}, Acc) ->
            Acc#{Guid => [Perm | maps:get(Guid, Acc)]}
        end, ComplementaryPermsPerFile, EaccesPermComb),

        authz_test_utils:set_acls(TestNode, EaccesPermsPerFile, #{}, AceWho, AceFlags),
        assert_operation(EaccesPermsPerFile, ExpError, TestCaseCtx)
    end, EaccesPermsCombs),

    % Granting only required perms should result in success
    authz_test_utils:set_acls(TestNode, RequiredPermsPerFile, #{}, AceWho, AceFlags),
    assert_operation(RequiredPermsPerFile, ok, TestCaseCtx),

    run_final_storage_ownership_check(TestCaseCtx);

run_acl_permission_test_case(deny, AceWho, AceFlags, #authz_acl_test_case_ctx{
    test_case_ctx = TestCaseCtx = #authz_test_case_ctx{
        suite_ctx = #authz_test_suite_ctx{
            suite_spec = TestSuiteSpec,
            test_node = TestNode
        },
        required_perms_per_file = RequiredPermsPerFile
    },
    complementary_perms_per_file = ComplementaryPermsPerFile
}) ->
    AllPermsPerFile = maps:map(fun(Guid, _) ->
        authz_test_utils:all_perms(TestNode, Guid)
    end, RequiredPermsPerFile),

    % Denying only required perms and granting all others should result in eacces
    ExpError = get_exp_error(?EACCES, TestSuiteSpec),

    lists:foreach(fun({Guid, Perm}) ->
        EaccesPermsPerFile = #{Guid => [Perm]},

        authz_test_utils:set_acls(TestNode, AllPermsPerFile, EaccesPermsPerFile, AceWho, AceFlags),
        assert_operation(EaccesPermsPerFile, ExpError, TestCaseCtx)
    end, flatten_perms_per_file(RequiredPermsPerFile)),

    % Denying all perms but required ones should result in success
    authz_test_utils:set_acls(TestNode, #{}, ComplementaryPermsPerFile, AceWho, AceFlags),
    assert_operation(ComplementaryPermsPerFile, ok, TestCaseCtx),

    run_final_storage_ownership_check(TestCaseCtx).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec init_test_suite(authz_test_suite_spec()) -> authz_test_suite_ctx().
init_test_suite(TestSuiteSpec = #authz_test_suite_spec{
    name = TestSuiteName,
    provider_selector = ProviderSelector,
    space_id = SpaceId,
    space_owner_selector = SpaceOwnerSelector,
    files_owner_selector = FilesOwnerSelector
}) ->
    TestNode = oct_background:get_random_provider_node(ProviderSelector),
    FileOwnerSessionId = oct_background:get_user_session_id(FilesOwnerSelector, ProviderSelector),

    {ok, SpaceName} = ?rpc(TestNode, space_logic:get_name(?ROOT_SESS_ID, SpaceId)),
    TestSuiteRootDirPath = filepath_utils:join([<<"/">>, SpaceName, TestSuiteName]),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(TestNode, FileOwnerSessionId, TestSuiteRootDirPath, 8#777)),

    #authz_test_suite_ctx{
        suite_spec = TestSuiteSpec,
        suite_root_dir_path = TestSuiteRootDirPath,
        test_node = TestNode,
        space_owner_session_id = oct_background:get_user_session_id(
            SpaceOwnerSelector, ProviderSelector
        ),
        files_owner_session_id = FileOwnerSessionId
    }.


%% @private
-spec build_test_case_name([term()]) -> binary().
build_test_case_name(Tokens) ->
    str_utils:join_binary([str_utils:to_binary(Token) || Token <- Tokens], <<"-">>).


%% @private
-spec init_test_case(
    binary(),
    session:id() | oct_background:entity_selector(),
    authz_test_suite_ctx()
) ->
    authz_test_case_ctx().
init_test_case(TestCaseName, ExecutionerSelector, TestSuiteCtx = #authz_test_suite_ctx{
    suite_spec = TestSuiteSpec = #authz_test_suite_spec{
        provider_selector = ProviderSelector
    },
    suite_root_dir_path = TestSuiteRootDirPath,
    test_node = TestNode,
    files_owner_session_id = FilesOwnerSessionId
}) ->
    FileTreeSpec = #ct_authz_dir_spec{
        name = TestCaseName,
        required_perms = infer_test_case_root_dir_permissions(TestSuiteSpec),
        children = TestSuiteSpec#authz_test_suite_spec.files
    },
    {RequiredPermsPerFile, ExtraData} = create_file_tree(
        TestNode, FilesOwnerSessionId, TestSuiteRootDirPath, FileTreeSpec
    ),

    #authz_test_case_ctx{
        suite_ctx = TestSuiteCtx,
        test_case_name = TestCaseName,
        test_case_root_dir_path = filepath_utils:join([TestSuiteRootDirPath, TestCaseName]),
        executioner_session_id = case ExecutionerSelector of
            ?GUEST_SESS_ID -> ?GUEST_SESS_ID;
            _ -> oct_background:get_user_session_id(ExecutionerSelector, ProviderSelector)
        end,
        required_perms_per_file = RequiredPermsPerFile,
        extra_data = ExtraData
    }.


%% @private
-spec infer_test_case_root_dir_permissions(authz_test_suite_spec()) ->
    perms().
infer_test_case_root_dir_permissions(#authz_test_suite_spec{requires_traverse_ancestors = true}) ->
    [?traverse_container];
infer_test_case_root_dir_permissions(_) ->
    [].


%% @private
-spec flatten_perms_per_file(perms_per_file() | posix_perms_per_file()) ->
    [{file_id:file_guid(), atom() | binary()}].
flatten_perms_per_file(PermsPerFile) ->
    maps:fold(fun(Guid, Perms, OuterAcc) ->
        lists:foldl(fun(Perm, InnerAcc) -> [{Guid, Perm} | InnerAcc] end, OuterAcc, Perms)
    end, [], PermsPerFile).


%% @private
-spec set_full_perms(authz_test_suite_ctx(), [file_id:file_guid()]) ->
    perms_per_file() | #{file_id:file_guid() => file_meta:mode()}.
set_full_perms(#authz_test_suite_ctx{
    test_node = TestNode,
    suite_spec = #authz_test_suite_spec{posix_requires_space_privs = PosixRequiresSpacePrivs}
}, FileGuids) ->
    % omit posix if operation requires posix ownership
    PossiblePermsTypes = case PosixRequiresSpacePrivs of
        {file_owner, _} -> [acl];
        _ -> [posix, acl]
    end,
    set_full_perms(?RAND_ELEMENT(PossiblePermsTypes), TestNode, FileGuids).


%% @private
-spec set_full_perms(posix | acl, node(), [file_id:file_guid()]) ->
    perms_per_file() | #{file_id:file_guid() => file_meta:mode()}.
set_full_perms(posix, Node, FileGuids) ->
    FullPosixPermsPerFile = lists:foldl(fun(Guid, Acc) -> Acc#{Guid => 8#777} end, #{}, FileGuids),
    authz_test_utils:set_modes(Node, FullPosixPermsPerFile),
    FullPosixPermsPerFile;

set_full_perms(acl, Node, FileGuids) ->
    FullAclPermsPerFile = get_full_perms_per_file(Node, FileGuids),
    authz_test_utils:set_acls(Node, FullAclPermsPerFile, #{}, ?everyone, ?no_flags_mask),
    FullAclPermsPerFile.


%% @private
-spec deny_full_perms(posix | acl, node(), [file_id:file_guid()]) -> ok.
deny_full_perms(posix, Node, FileGuids) ->
    ZeroPosixPermsPerFile = lists:foldl(fun(Guid, Acc) -> Acc#{Guid => 8#777} end, #{}, FileGuids),
    authz_test_utils:set_modes(Node, ZeroPosixPermsPerFile),
    ZeroPosixPermsPerFile;
deny_full_perms(acl, Node, FileGuids) ->
    FullAclPermsPerFile = get_full_perms_per_file(Node, FileGuids),
    authz_test_utils:set_acls(Node, #{}, FullAclPermsPerFile, ?everyone, ?no_flags_mask),
    FullAclPermsPerFile.


%% @private
-spec get_full_perms_per_file(node(), [file_id:file_guid()]) -> perms_per_file().
get_full_perms_per_file(Node, FileGuids) ->
    lists:foldl(fun(Guid, Acc) ->
        Acc#{Guid => authz_test_utils:all_perms(Node, Guid)}
    end, #{}, FileGuids).


%% @private
-spec assert_operation(
    perms_per_file() | posix_perms_per_file(),
    term(),
    authz_test_case_ctx() | authz_cv_test_case_ctx()
) ->
    ok | no_return().
assert_operation(ActualPermsPerFile, ExpResult, #authz_cv_test_case_ctx{
    test_case_ctx = TestCaseCtx
}) ->
    assert_operation(ActualPermsPerFile, ExpResult, TestCaseCtx);

assert_operation(ActualPermsPerFile, ExpResult, TestCaseCtx = #authz_test_case_ctx{}) ->
    try
        ?assertEqual(ExpResult, exec_operation(TestCaseCtx)),
        ok
    catch Class:Reason:Stacktrace ->
        ?ct_pal_exception(
            "OPERATION EXECUTION ASSERT FAILED!!!~n~s",
            [format_additional_log_data(ActualPermsPerFile, TestCaseCtx)],
            Class, Reason, Stacktrace
        ),
        error(assert_operation_failed)
    end.


%% @private
-spec format_additional_log_data(perms_per_file() | posix_perms_per_file(), authz_test_case_ctx()) ->
    binary().
format_additional_log_data(ActualPermsPerFile, #authz_test_case_ctx{
    suite_ctx = #authz_test_suite_ctx{
        suite_spec = #authz_test_suite_spec{
            space_id = SpaceId
        },
        test_node = TestNode
    },
    test_case_name = TestCaseName,
    test_case_root_dir_path = TestCaseRootDirPath,
    executioner_session_id = ExecutionerSessionId,
    extra_data = ExtraData
}) ->
    ?FILE_REF(TestCaseRootDirGuid) = maps:get(TestCaseRootDirPath, ExtraData),
    RootDirShareId = file_id:guid_to_share_id(TestCaseRootDirGuid),
    RootDirUuid = file_id:guid_to_uuid(TestCaseRootDirGuid),
    {ok, RootDirFileMetaDoc} = ?rpc(TestNode, file_meta:get(RootDirUuid)),
    RootDirProtectionFlagsBitMask = file_meta:get_protection_flags(RootDirFileMetaDoc),
    RootDirProtectionFlags = utils:ensure_defined(
        file_meta:protection_flags_to_json(RootDirProtectionFlagsBitMask), [], undefined
    ),

    ActualPermsPerFileJson = format_perms_per_file(TestNode, ActualPermsPerFile),

    {ok, ExecutionerSession} = ?rpc(TestNode, session:get(ExecutionerSessionId)),

    {ok, ExecutionerUserId} = session:get_user_id(ExecutionerSession),
    ExecutionerPlaceholder = case ExecutionerUserId of
        ?GUEST_USER_ID -> <<"@GUEST">>;
        _ -> oct_background:to_entity_placeholder(ExecutionerUserId)
    end,

    {ok, ExecutionerSpacePrivs} = get_user_space_privileges(TestNode, SpaceId, ExecutionerUserId),

    {ok, ExecutionerSessionMode} = session:get_mode(ExecutionerSession),
    ExecutionerSessionConstraints = session:get_data_constraints(ExecutionerSession),

    ?autoformat(
        TestCaseName,
        TestCaseRootDirPath,
        RootDirProtectionFlags,
        RootDirShareId,
        ActualPermsPerFileJson,
        ExecutionerPlaceholder,
        ExecutionerSpacePrivs,
        ExecutionerSessionMode,
        ExecutionerSessionConstraints
    ).


%% @private
-spec exec_operation(authz_test_case_ctx()) -> term().
exec_operation(#authz_test_case_ctx{
    suite_ctx = #authz_test_suite_ctx{
        suite_spec = #authz_test_suite_spec{operation = Operation},
        test_node = TestNode
    },
    test_case_root_dir_path = TestCaseRootDirPath,
    executioner_session_id = ExecutionerSessionId,
    extra_data = ExtraData
}) ->
    case Operation(TestNode, ExecutionerSessionId, TestCaseRootDirPath, ExtraData) of
        ok -> ok;
        {ok, _} -> ok;
        {ok, _, _} -> ok;
        {ok, _, _, _} -> ok;
        {error, _} = Error -> Error
    end.


%% @private
-spec set_user_space_privileges(
    node(), od_user:id(), od_space:id(), privileges:privileges(privileges:space_privilege())
) ->
    ok.
set_user_space_privileges(TestNode, UserId, SpaceId, Privileges) ->
    ozt_spaces:set_privileges(SpaceId, UserId, Privileges),
    ?assertMatch({ok, Privileges}, get_user_space_privileges(TestNode, SpaceId, UserId)),
    ok.


%% @private
-spec get_user_space_privileges(node(), od_space:id(), od_user:id()) ->
    {ok, privileges:privileges(privileges:space_privilege())}.
get_user_space_privileges(Node, SpaceId, UserId) ->
    ?rpc(Node, space_logic:get_eff_privileges(SpaceId, UserId)).


%% @private
-spec create_file_tree(node(), session:id(), file_meta:path(), file_tree_spec()) ->
    {perms_per_file(), extra_data()}.
create_file_tree(Node, FileOwnerSessId, ParentDirPath, #ct_authz_file_spec{
    name = FileName,
    required_perms = RequiredFilePerms,
    on_create = HookFun
}) ->
    FilePath = filepath_utils:join([ParentDirPath, FileName]),
    FileGuid = create_file(Node, FileOwnerSessId, FilePath),

    ExtraData = #{FilePath => case HookFun of
        undefined -> ?FILE_REF(FileGuid);
        _ when is_function(HookFun, 3) -> HookFun(Node, FileOwnerSessId, FileGuid)
    end},

    {#{FileGuid => RequiredFilePerms}, ExtraData};

create_file_tree(Node, FileOwnerSessId, ParentDirPath, #ct_authz_dir_spec{
    name = DirName,
    required_perms = RequiredDirPerms,
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
    storage_test_utils:ensure_file_created_on_storage(Node, FileGuid),
    FileGuid.


%% @private
-spec create_dir(node(), session:id(), file_meta:path()) -> file_id:file_guid().
create_dir(Node, FileOwnerSessId, DirPath) ->
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, FileOwnerSessId, DirPath)),
    storage_test_utils:ensure_dir_created_on_storage(Node, DirGuid),
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
-spec format_perms_per_file(node(), perms_per_file() | posix_perms_per_file()) ->
    json_utils:json_map().
format_perms_per_file(TestNode, PermsPerFile) ->
    maps:fold(fun(Guid, Perms, Acc) ->
        Acc#{get_file_path(TestNode, Guid) => case is_integer(Perms) of
            true ->
                ModeBin = list_to_binary(string:right(integer_to_list(Perms, 8), 3, $0)),
                <<"POSIX MODE: ", ModeBin/binary>>;
            false ->
                AllPossiblePerms = authz_test_utils:all_perms(TestNode, Guid),
                case lists:usort(Perms) == lists:usort(AllPossiblePerms) of
                    true -> <<"ACL: FULL">>;
                    false -> lists:map(fun str_utils:to_binary/1, Perms)
                end
        end}
    end, #{}, PermsPerFile).


%% @private
-spec get_exp_error(atom(), authz_test_suite_spec()) -> {error, term()}.
get_exp_error(Errno, #authz_test_suite_spec{returned_errors = api_errors}) ->
    ?ERROR_POSIX(Errno);
get_exp_error(Errno, #authz_test_suite_spec{returned_errors = errno_errors}) ->
    {error, Errno}.


%% @private
-spec run_final_storage_ownership_check(authz_test_case_ctx()) -> ok | no_return().
run_final_storage_ownership_check(#authz_test_case_ctx{}) ->
    %% TODO VFS-11732 rewrite from permissions_test_runner envup
    ok.


%%%% @private
%%-spec run_final_ownership_check(scenario_ctx()) -> ok | no_return().
%%run_final_ownership_check(#scenario_ctx{
%%    meta_spec = #perms_test_spec{
%%        test_node = Node,
%%        space_id = SpaceId,
%%        final_ownership_check = FinalOwnershipCheckFun
%%    },
%%    scenario_root_dir_path = ScenarioRootDirPath,
%%    files_owner_session_id = OriginalFileOwnerSessId,
%%    executioner_session_id = OperationExecutionerSessId
%%}) ->
%%    case FinalOwnershipCheckFun(ScenarioRootDirPath) of
%%        {inapplicable_due_to, _} ->
%%            ok;
%%        {should_preserve_ownership, LogicalFilePath} ->
%%            permissions_test_utils:assert_user_is_file_owner_on_storage(
%%                Node, SpaceId, LogicalFilePath, OriginalFileOwnerSessId
%%            );
%%        {should_assign_ownership, LogicalFilePath} ->
%%            permissions_test_utils:assert_user_is_file_owner_on_storage(
%%                Node, SpaceId, LogicalFilePath, OperationExecutionerSessId
%%            )
%%    end.
