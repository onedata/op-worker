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
-module(authz_test_runner).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include("permissions_test.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([run_suite/1]).

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

-record(authz_test_group_ctx, {
    name :: binary(),
    suite_ctx :: authz_test_suite_ctx(),
    group_root_dir_path :: file_meta:path(),
    files_owner_session_id :: session:id(),
    executioner_session_id :: session:id(),
    required_perms_per_file :: perms_per_file(),
    extra_data = #{} :: extra_data()
}).
-type authz_test_group_ctx() :: #authz_test_group_ctx{}.

-record(authz_cv_test_group_ctx, {
    group_ctx :: authz_test_group_ctx(),
    executioner_user_id :: od_user:id(),
    executioner_main_token :: tokens:serialized()
}).
-type authz_cv_test_group_ctx() :: #authz_cv_test_group_ctx{}.

-record(authz_posix_test_group_ctx, {
    group_ctx :: authz_test_group_ctx(),
    required_posix_perms_per_file :: posix_perms_per_file(),
    complementary_posix_perms_per_file :: posix_perms_per_file()
}).
-type authz_posix_test_group_ctx() :: #authz_posix_test_group_ctx{}.

-record(authz_acl_test_group_ctx, {
    group_ctx :: authz_test_group_ctx(),
    complementary_perms_per_file :: perms_per_file()
}).
-type authz_acl_test_group_ctx() :: #authz_acl_test_group_ctx{}.

-define(TEST_GROUP_NAME(__PREFIX, __TYPE),
    <<__PREFIX, (str_utils:to_binary(__TYPE))/binary>>
).


%%%===================================================================
%%% TEST MECHANISM
%%%===================================================================


-spec run_suite(authz_test_suite_spec()) -> ok | no_return().
run_suite(TestSuiteSpec) ->
    TestSuiteCtx = init_test_suite(TestSuiteSpec),

    run_file_protection_test_groups(TestSuiteCtx),
    run_data_access_caveats_test_groups(TestSuiteCtx),
    run_share_test_groups(TestSuiteCtx),
    run_open_handle_mode_test_groups(TestSuiteCtx),
    run_posix_permission_test_groups(TestSuiteCtx),
    run_acl_permission_test_groups(TestSuiteCtx).


%%%===================================================================
%%% FILE PROTECTION TESTS
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tests file protection blocking operation.
%% It will setup environment, add full posix or acl permissions and
%% assert that with file protection flags operation cannot be performed
%% (even though full posix/acl perms are set).
%% @end
%%--------------------------------------------------------------------
-spec run_file_protection_test_groups(authz_test_group_ctx()) ->
    ok | no_return().
run_file_protection_test_groups(TestSuiteCtx = #authz_test_suite_ctx{suite_spec = TestSuiteSpec}) ->
    lists:dropwhile(fun({ExecutionerSelector, TestGroupSuffix}) ->
        TestGroupName = ?TEST_GROUP_NAME("file_protection_", TestGroupSuffix),
        TestGroupCtx = init_test_group(TestGroupName, ExecutionerSelector, TestSuiteCtx),

        ProtectionFlagsToSet = infer_blocking_protection_flags(TestGroupCtx),
        run_file_protection_test_group(ProtectionFlagsToSet, TestGroupCtx)

    end, ?RAND_SUBLIST([
        {TestSuiteSpec#authz_test_suite_spec.space_owner_selector, "space_owner"},
        {TestSuiteSpec#authz_test_suite_spec.files_owner_selector, "files_owner"},
        {TestSuiteSpec#authz_test_suite_spec.space_user_selector, "space_user"}
    ], 3)).  %% TODO test also other users? / sublist?


%% @private
-spec run_file_protection_test_group(data_access_control:bitmask(), authz_test_group_ctx()) ->
    boolean().
run_file_protection_test_group(0, _TestGroupCtx) ->
    false;

run_file_protection_test_group(ProtectionFlagsToSet, TestGroupCtx = #authz_test_group_ctx{
    suite_ctx = #authz_test_suite_ctx{
        suite_spec = TestSuiteSpec = #authz_test_suite_spec{
            space_selector = SpaceSelector
        },
        space_owner_session_id = SpaceOwnerSessionId,
        test_node = TestNode
    },
    executioner_session_id = ExecutionerSessionId,
    required_perms_per_file = RequiredPermsPerFile,
    extra_data = ExtraData
}) ->
    % Assert that even with all perms set operation cannot be performed
    % with file protection flags set
    set_full_perms(acl, TestNode, maps:keys(RequiredPermsPerFile)),

    SpaceId = oct_background:get_space_id(SpaceSelector),
    DatasetId = establish_dataset_on_test_group_root_dir(TestGroupCtx),
    {ok, ExecutionerUserId} = rpc:call(TestNode, session, get_user_id, [ExecutionerSessionId]),

    % With file protection set operation should fail
    ExpError = get_exp_error(?EPERM, TestSuiteSpec),
    ok = opt_datasets:update(
        TestNode, SpaceOwnerSessionId, DatasetId, undefined, ProtectionFlagsToSet, ?no_flags_mask
    ),
    await_dataset_eff_cache_clearing(TestNode, SpaceId, ExecutionerUserId, ExtraData),
    assert_operation(#{}, ExpError, TestGroupCtx),      %% TODO

    % And should succeed without it
    ok = opt_datasets:update(
        TestNode, SpaceOwnerSessionId, DatasetId, undefined, ?no_flags_mask, ProtectionFlagsToSet
    ),
    await_dataset_eff_cache_clearing(TestNode, SpaceId, ExecutionerUserId, ExtraData),
    assert_operation(#{}, ok, TestGroupCtx),  %% TODO

    run_final_storage_ownership_check(TestGroupCtx),
    true.


%% @private
-spec infer_blocking_protection_flags(authz_test_group_ctx()) ->
    data_access_control:bitmask().
infer_blocking_protection_flags(#authz_test_group_ctx{
    required_perms_per_file = RequiredPermsPerFile
}) ->
    AllNeededPerms = lists:usort(lists:flatten(maps:values(RequiredPermsPerFile))),
    AllNeededPermsBitmask = permissions_test_utils:perms_to_bitmask(AllNeededPerms),

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
-spec establish_dataset_on_test_group_root_dir(authz_test_group_ctx()) -> dataset:id().
establish_dataset_on_test_group_root_dir(#authz_test_group_ctx{
    suite_ctx = #authz_test_suite_ctx{
        space_owner_session_id = SpaceOwnerSessionId,
        test_node = TestNode
    },
    group_root_dir_path = TestGroupRootDirPath,
    extra_data = ExtraData
}) ->
    TestGroupRootDirKey = maps:get(TestGroupRootDirPath, ExtraData),
    {ok, DatasetId} = opt_datasets:establish(TestNode, SpaceOwnerSessionId, TestGroupRootDirKey),
    DatasetId.


%% @private
-spec await_dataset_eff_cache_clearing(node(), od_space:id(), od_user:id(), map()) -> ok.
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
-spec run_data_access_caveats_test_groups(authz_test_suite_ctx()) ->
    ok | no_return().
run_data_access_caveats_test_groups(TestSuiteCtx = #authz_test_suite_ctx{
    suite_spec = #authz_test_suite_spec{
        files_owner_selector = FilesOwnerSelector
    },
    test_node = TestNode
}) ->
    FilesOwnerUserId = oct_background:get_user_id(FilesOwnerSelector),
    FilesOwnerMainToken = provider_onenv_test_utils:create_oz_temp_access_token(FilesOwnerUserId),

    lists:foreach(fun(CaveatType) ->
        TestGroupName = ?TEST_GROUP_NAME("cv_", CaveatType),
        TestGroupCtx = init_test_group(TestGroupName, FilesOwnerSelector, TestSuiteCtx),
        CvTestGroupTestCtx = #authz_cv_test_group_ctx{
            group_ctx = TestGroupCtx,
            executioner_user_id = FilesOwnerUserId,
            executioner_main_token = FilesOwnerMainToken
        },

        % Assert that even with all perms set operation can be performed
        % only when caveats allows it
        FileGuids = maps:keys(TestGroupCtx#authz_test_group_ctx.required_perms_per_file),
        set_full_perms(?RAND_ELEMENT([posix, acl]), TestNode, FileGuids),

        run_caveats_scenario(CaveatType, CvTestGroupTestCtx)

    end, ?RAND_SUBLIST([data_path, data_objectid, data_readonly], 3)).  %% TODO test also other users? / sublist?


%% @private
-spec run_caveats_scenario(
    data_path | data_objectid | data_readonly,
    authz_cv_test_group_ctx()
) ->
    ok | no_return().
run_caveats_scenario(data_path, CvTestGroupTestCtx0 = #authz_cv_test_group_ctx{
    group_ctx = TestGroupCtx = #authz_test_group_ctx{
        suite_ctx = #authz_test_suite_ctx{suite_spec = TestSuiteSpec}
    }
}) ->
    ExpError = get_exp_error(?EACCES, TestSuiteSpec),
    assert_operation(#{}, ExpError, constrain_executioner_session(  %% TODO
        CvTestGroupTestCtx0, #cv_data_path{whitelist = [<<"i_am_nowhere">>]}
    )),

    assert_operation(#{}, ok, constrain_executioner_session(CvTestGroupTestCtx0, #cv_data_path{ %% TODO
        whitelist = [get_test_group_root_dir_canonical_path(TestGroupCtx)]
    })),
    run_final_storage_ownership_check(TestGroupCtx);

run_caveats_scenario(data_objectid, CvTestGroupTestCtx0 = #authz_cv_test_group_ctx{
    group_ctx = TestGroupCtx = #authz_test_group_ctx{
        suite_ctx = #authz_test_suite_ctx{suite_spec = TestSuiteSpec},
        group_root_dir_path = TestGroupRootDirPath,
        extra_data = ExtraData
    }
}) ->
    ExpError = get_exp_error(?EACCES, TestSuiteSpec),
    DummyGuid = <<"Z3VpZCNfaGFzaF9mNmQyOGY4OTNjOTkxMmVh">>,
    {ok, DummyObjectId} = file_id:guid_to_objectid(DummyGuid),
    assert_operation(#{}, ExpError, constrain_executioner_session( %% TODO
        CvTestGroupTestCtx0, #cv_data_objectid{whitelist = [DummyObjectId]}
    )),

    ?FILE_REF(TestGroupRootDirGuid) = maps:get(TestGroupRootDirPath, ExtraData),
    {ok, TestGroupRootDirObjectId} = file_id:guid_to_objectid(TestGroupRootDirGuid),
    assert_operation(#{}, ok, constrain_executioner_session( %% TODO
        CvTestGroupTestCtx0, #cv_data_objectid{whitelist = [TestGroupRootDirObjectId]}
    )),
    run_final_storage_ownership_check(TestGroupCtx);

run_caveats_scenario(data_readonly, CvTestGroupTestCtx0 = #authz_cv_test_group_ctx{
    group_ctx = #authz_test_group_ctx{
        suite_ctx = #authz_test_suite_ctx{
            suite_spec = TestSuiteSpec = #authz_test_suite_spec{
                available_in_readonly_mode = AvailableInReadonlyMode
            }
        }
    }
}) ->
    CvTestGroupTestCtx1 = constrain_executioner_session(CvTestGroupTestCtx0, #cv_data_readonly{}),

    case AvailableInReadonlyMode of
        true ->
            assert_operation(#{}, ok, CvTestGroupTestCtx1),  %% TODO
            run_final_storage_ownership_check(CvTestGroupTestCtx1#authz_cv_test_group_ctx.group_ctx);
        false ->
            ExpError = get_exp_error(?EACCES, TestSuiteSpec),
            assert_operation(#{}, ExpError, CvTestGroupTestCtx1)  %% TODO
    end.


%% @private
-spec constrain_executioner_session(authz_cv_test_group_ctx(), caveats:caveat()) ->
    authz_cv_test_group_ctx().
constrain_executioner_session(CvTestGroupTestCtx = #authz_cv_test_group_ctx{
    group_ctx = TestGroupCtx = #authz_test_group_ctx{
        suite_ctx = #authz_test_suite_ctx{test_node = TestNode}
    },
    executioner_user_id = UserId,
    executioner_main_token = MainToken
}, Caveat) ->
    TokenWithCaveat = tokens:confine(MainToken, Caveat),
    ConstrainedSessionId = permissions_test_utils:create_session(TestNode, UserId, TokenWithCaveat),
    CvTestGroupTestCtx#authz_cv_test_group_ctx{group_ctx = TestGroupCtx#authz_test_group_ctx{
        executioner_session_id = ConstrainedSessionId
    }}.


%% @private
-spec get_test_group_root_dir_canonical_path(authz_test_group_ctx()) ->
    file_meta:path().
get_test_group_root_dir_canonical_path(#authz_test_group_ctx{
    suite_ctx = #authz_test_suite_ctx{suite_spec = #authz_test_suite_spec{
        space_selector = SpaceSelector
    }},
    group_root_dir_path = TestGroupRootDirPath
}) ->
    SpaceId = oct_background:get_space_id(SpaceSelector),
    [Sep, _SpaceName | PathTokens] = filepath_utils:split(TestGroupRootDirPath),
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
-spec run_share_test_groups(authz_test_suite_ctx()) ->
    ok | no_return().
run_share_test_groups(#authz_test_suite_ctx{suite_spec = #authz_test_suite_spec{
    available_in_share_mode = inapplicable
}}) ->
    ok;

run_share_test_groups(TestSuiteCtx = #authz_test_suite_ctx{
    suite_spec = #authz_test_suite_spec{
        files_owner_selector = FilesOwnerSelector,
        space_user_selector = SpaceUserSelector,
        non_space_user = NonSpaceUserSelector
    },
    space_owner_session_id = SpaceOwnerSessionId,
    test_node = TestNode
}) ->
    lists:foreach(fun({ExecutionerSelector, PermsType, TestGroupName}) ->
        TestGroupCtx = init_test_group(TestGroupName, ExecutionerSelector, TestSuiteCtx),
        TestGroupRootDirPath = TestGroupCtx#authz_test_group_ctx.group_root_dir_path,
        ExtraData = TestGroupCtx#authz_test_group_ctx.extra_data,

        TestCaseRootDirKey = maps:get(TestGroupRootDirPath, ExtraData),
        {ok, ShareId} = opt_shares:create(
            TestNode, SpaceOwnerSessionId, TestCaseRootDirKey, TestGroupName
        ),
        TestGroupCtxWithShareGuids = TestGroupCtx#authz_test_group_ctx{
            extra_data = maps:map(fun
                (_, #file_ref{guid = FileGuid} = FileRef) ->
                    FileRef#file_ref{guid = file_id:guid_to_share_guid(FileGuid, ShareId)};
                (_, Val) ->
                    Val
            end, ExtraData)
        },

        run_share_test_cases(PermsType, TestGroupCtxWithShareGuids)
    end, ?RAND_SUBLIST([
        {FilesOwnerSelector, posix, <<"owner_posix_share">>},
        {SpaceUserSelector, posix, <<"space_user_posix_share">>},
%%        {NonSpaceUserSelector, posix, <<"non_space_user_posix_share">>},  %% TODO
        {?GUEST_SESS_ID, posix, <<"guest_posix_share">>},
        {FilesOwnerSelector, {acl, allow}, <<"owner_acl_allow_share">>},
        {FilesOwnerSelector, {acl, deny}, <<"owner_acl_deny_share">>},
        {SpaceUserSelector, {acl, allow}, <<"space_user_acl_allow_share">>},
        {SpaceUserSelector, {acl, deny}, <<"space_user_acl_deny_share">>},
%%        {NonSpaceUserSelector, {acl, allow}, <<"non_space_user_acl_allow_share">>},   %% TODO
%%        {NonSpaceUserSelector, {acl, deny}, <<"non_space_user_acl_deny_share">>},   %% TODO
        {?GUEST_SESS_ID, {acl, allow}, <<"guest_acl_allow_share">>},
        {?GUEST_SESS_ID, {acl, deny}, <<"guest_acl_deny_share">>}
    ], 12)).  %% TODO sublis length?


%% @private
-spec run_share_test_cases(posix | {acl, allow | deny}, authz_test_group_ctx()) ->
    ok | no_return().
run_share_test_cases(PermsType0, TestGroupCtx = #authz_test_group_ctx{
    suite_ctx = #authz_test_suite_ctx{
        suite_spec = TestSuiteSpec = #authz_test_suite_spec{available_in_share_mode = false},
        test_node = TestNode
    },
    required_perms_per_file = RequiredPermsPerFile
}) ->
    FileGuids = maps:keys(RequiredPermsPerFile),

    % Set all posix or acl (depending on scenario) perms to files
    PermsType1 = case PermsType0 of
        posix -> posix;
        {acl, _} -> acl
    end,
    set_full_perms(PermsType1, TestNode, FileGuids),

    % Even with all perms set operation should fail
    ActualPermsPerFile = maps:map(fun(_, _) -> <<"all">> end, RequiredPermsPerFile),
    ExpError = get_exp_error(?EPERM, TestSuiteSpec),
    assert_operation(ActualPermsPerFile, ExpError, TestGroupCtx);

run_share_test_cases(posix, TestGroupCtx) ->
    PosixTestGroupCtx = build_posix_test_group_ctx(TestGroupCtx),
    run_posix_permission_test_cases(other, PosixTestGroupCtx);

run_share_test_cases({acl, AceType}, TestGroupCtx) ->
    AclTestGroupCtx = build_acl_test_group_ctx(TestGroupCtx),
    AceWho = lists_utils:random_element([?everyone, ?anonymous]),
    run_acl_permission_test_cases(AceType, AceWho, ?no_flags_mask, AclTestGroupCtx).


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
-spec run_open_handle_mode_test_groups(authz_test_suite_ctx()) ->
    ok | no_return().
run_open_handle_mode_test_groups(TestSuiteCtx = #authz_test_suite_ctx{
    suite_spec = #authz_test_suite_spec{
        files_owner_selector = FilesOwnerSelector,
        space_owner_selector = SpaceOwnerSelector,
        space_user_selector = SpaceUserSelector
    },
    test_node = TestNode
}) ->
    lists:foreach(fun({ExecutionerSelector, TestGroupSuffix, PermsType}) ->
        TestGroupName = ?TEST_GROUP_NAME("open_handle_mode_", TestGroupSuffix),
        TestGroupCtx0 = init_test_group(TestGroupName, ExecutionerSelector, TestSuiteCtx),

        ExecutionerUserId = oct_background:get_user_id(ExecutionerSelector),
        ExecutionerToken = provider_onenv_test_utils:create_oz_temp_access_token(ExecutionerUserId),
        TestGroupCtx1 = TestGroupCtx0#authz_test_group_ctx{
            executioner_session_id = permissions_test_utils:create_session(
                TestNode, ExecutionerUserId, ExecutionerToken, open_handle
            )
        },

        run_open_handle_mode_test_cases(PermsType, TestGroupCtx1)
    end, ?RAND_SUBLIST([
        {FilesOwnerSelector, "files_owner_posix", posix},
        {SpaceOwnerSelector, "space_owner_posix", posix},
        {SpaceUserSelector, "space_user_posix", posix},
        {FilesOwnerSelector, "files_owner_acl", acl},
        {SpaceOwnerSelector, "space_owner_acl", acl},
        {SpaceUserSelector, "space_user_acl", acl}
    ], 6)).  %% TODO sublis length?


%% @private
-spec run_open_handle_mode_test_cases(posix | acl_allow | acl_deny, authz_test_group_ctx()) ->
    ok | no_return().
run_open_handle_mode_test_cases(PermsType, TestGroupCtx = #authz_test_group_ctx{
    suite_ctx = #authz_test_suite_ctx{
        suite_spec = TestSuiteSpec = #authz_test_suite_spec{available_in_open_handle_mode = false},
        test_node = TestNode
    },
    required_perms_per_file = RequiredPermsPerFile
}) ->
    % If operation is not available in share/public mode then operation
    % should be rejected even if all permissions are granted
    set_full_perms(PermsType, TestNode, maps:keys(RequiredPermsPerFile)),
    ExpError2 = get_exp_error(?EPERM, TestSuiteSpec),
    ?assertMatch(ExpError2, exec_operation(TestGroupCtx));

% Operation is available in share/public mode but access is still controlled
% (even for space owner) by posix mode (other bits) or acl
run_open_handle_mode_test_cases(posix, TestGroupCtx) ->
    PosixTestGroupCtx = build_posix_test_group_ctx(TestGroupCtx),
    run_posix_permission_test_cases(other, PosixTestGroupCtx);

run_open_handle_mode_test_cases(acl, TestGroupCtx) ->
    AclTestGroupCtx = build_acl_test_group_ctx(TestGroupCtx),
    AceType = ?RAND_ELEMENT([allow, deny]),
    run_acl_permission_test_cases(AceType, ?anonymous, ?no_flags_mask, AclTestGroupCtx).


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
        catch Class:Reason:Stacktrace ->
            TestGroupRootDirPath = TestGroupCtx#authz_test_group_ctx.group_root_dir_path,
            RequiredPerms = format_posix_perms_per_file(PosixTestGroupCtx),

            ?ct_pal_exception(
                "POSIX test group failure ~s",
                [?autoformat(PosixUserType, TestGroupRootDirPath, RequiredPerms)],
                Class, Reason, Stacktrace
            ),
            error(posix_perms_test_failed)
        end
    end, ?RAND_SUBLIST([
        {owner, TestSuiteSpec#authz_test_suite_spec.files_owner_selector},
        {group, TestSuiteSpec#authz_test_suite_spec.space_user_selector},
        {other, TestSuiteSpec#authz_test_suite_spec.non_space_user}
    ], 3)). %% TODO sublis length?


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
            permissions_test_utils:set_modes(TestNode, ModesPerFile),
            assert_operation(ModesPerFile, ok, TestGroupCtx),

            run_final_storage_ownership_check(TestGroupCtx);

        false ->
            run_posix_permission_test_cases(owner, PosixTestGroupCtx#authz_posix_test_group_ctx{
                required_posix_perms_per_file = RequiredRWXPosixPermsPerFile
            })
    end;

run_posix_permission_test_group(group, PosixTestGroupCtx = #authz_posix_test_group_ctx{
    group_ctx = #authz_test_group_ctx{
        suite_ctx = TestGroupCtx = #authz_test_suite_ctx{
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
            permissions_test_utils:set_modes(TestNode, ModesPerFile),
            assert_operation(ModesPerFile, ExpError, TestGroupCtx);

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
            suite_spec = TestSuiteSpec,
            test_node = TestNode
        }
    },
    required_posix_perms_per_file = RequiredPosixPermsPerFile,
    complementary_posix_perms_per_file = ComplementaryPosixPermsPerFile
}) ->
    FlattenedRequiredModes = lists:map(fun({Guid, PosixPerm}) ->
        {Guid, permissions_test_utils:posix_perm_to_mode(PosixPerm, PosixUserType)}
    end, flatten_perms_per_file(RequiredPosixPermsPerFile)),

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
        assert_operation(EaccesModesPerFile, ExpError, TestGroupCtx)
    end, EaccesModesCombs),

    % Granting only required modes should result in success
    RequiredModesPerFile = lists:foldl(fun({Guid, Mode}, Acc) ->
        Acc#{Guid => Mode bor maps:get(Guid, Acc, 0)}
    end, #{}, RequiredModesComb),

    permissions_test_utils:set_modes(TestNode, RequiredModesPerFile),
    assert_operation(RequiredModesPerFile, ok, TestGroupCtx),

    run_final_storage_ownership_check(TestGroupCtx).


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
-spec run_acl_permission_test_groups(authz_test_suite_ctx()) ->
    ok | no_return().
run_acl_permission_test_groups(TestSuiteCtx = #authz_test_suite_ctx{
    suite_spec = #authz_test_suite_spec{
        provider_selector = ProviderSelector,
        files_owner_selector = FilesOwnerSelector,
        space_user_selector = SpaceUserSelector
    }
}) ->
    SpaceUserId = oct_background:get_user_id(SpaceUserSelector),
    SpaceUserGroupId = <<"todo">>,  %% TODO

    lists:foreach(fun({ExecutionerSelector, TestGroupName, AceType, AceWho, AceFlags}) ->
        TestGroupCtx = init_test_group(TestGroupName, ExecutionerSelector, TestSuiteCtx),
        AclTestGroupCtx = build_acl_test_group_ctx(TestGroupCtx),

        try
            run_acl_permission_test_cases(AceType, AceWho, AceFlags, AclTestGroupCtx)
        catch Class:Reason:Stacktrace ->
            TestGroupRootDirPath = TestGroupCtx#authz_test_group_ctx.group_root_dir_path,
            RequiredPerms = format_perms_per_file(
                ProviderSelector, TestGroupCtx#authz_test_group_ctx.required_perms_per_file
            ),

            ?ct_pal_exception(
                "ACL test group failure ~s",
                [?autoformat(TestGroupRootDirPath, RequiredPerms)],
                Class, Reason, Stacktrace
            ),
            error(acl_perms_test_failed)
        end
    end, ?RAND_SUBLIST([
        {FilesOwnerSelector, <<"acl_owner_allow">>, allow, ?owner, ?no_flags_mask},
        {SpaceUserSelector, <<"acl_user_allow">>, allow, SpaceUserId, ?no_flags_mask},
%%        {SpaceUserSelector, <<"acl_user_group_allow">>, allow, SpaceUserGroupId, ?identifier_group_mask},
        {SpaceUserSelector, <<"acl_everyone_allow">>, allow, ?everyone, ?no_flags_mask},

        {FilesOwnerSelector, <<"acl_owner_deny">>, deny, ?owner, ?no_flags_mask},
        {SpaceUserSelector, <<"acl_user_deny">>, deny, SpaceUserId, ?no_flags_mask},
%%        {SpaceUserSelector, <<"acl_user_group_deny">>, deny, SpaceUserGroupId, ?identifier_group_mask},
        {SpaceUserSelector, <<"acl_everyone_deny">>, deny, ?everyone, ?no_flags_mask}
    ], 6)).  %% TODO sublis length?


%% @private
-spec build_acl_test_group_ctx(authz_test_group_ctx()) ->
    authz_acl_test_group_ctx().
build_acl_test_group_ctx(TestGroupCtx = #authz_test_group_ctx{
    suite_ctx = #authz_test_suite_ctx{test_node = TestNode},
    required_perms_per_file = RequiredPermsPerFile
}) ->
    ComplementaryPermsPerFile = maps:fold(fun(FileGuid, FileRequiredPerms, Acc) ->
        Acc#{FileGuid => permissions_test_utils:complementary_perms(
            TestNode, FileGuid, FileRequiredPerms
        )}
    end, #{}, RequiredPermsPerFile),

    lists:flatmap(fun({Guid, Perms}) -> [{Guid, Perm} || Perm <- Perms] end, maps:to_list(RequiredPermsPerFile)),

    #authz_acl_test_group_ctx{
        group_ctx = TestGroupCtx,
        complementary_perms_per_file = ComplementaryPermsPerFile
    }.


%% @private
-spec run_acl_permission_test_cases(
    AceType :: allow | deny,
    AceWho :: binary(),
    AceFlags :: data_access_control:bitmask(),
    authz_test_group_ctx()
) ->
    ok | no_return().
run_acl_permission_test_cases(allow, AceWho, AceFlags, #authz_acl_test_group_ctx{
    group_ctx = TestGroupCtx = #authz_test_group_ctx{
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

        permissions_test_utils:set_acls(TestNode, EaccesPermsPerFile, #{}, AceWho, AceFlags),
        assert_operation(EaccesPermsPerFile, ExpError, TestGroupCtx)
    end, EaccesPermsCombs),

    % Granting only required perms should result in success
    permissions_test_utils:set_acls(TestNode, RequiredPermsPerFile, #{}, AceWho, AceFlags),
    assert_operation(RequiredPermsPerFile, ok, TestGroupCtx),

    run_final_storage_ownership_check(TestGroupCtx);

run_acl_permission_test_cases(deny, AceWho, AceFlags, #authz_acl_test_group_ctx{
    group_ctx = TestGroupCtx = #authz_test_group_ctx{
        suite_ctx = #authz_test_suite_ctx{
            suite_spec = TestSuiteSpec,
            test_node = TestNode
        },
        required_perms_per_file = RequiredPermsPerFile
    },
    complementary_perms_per_file = ComplementaryPermsPerFile
}) ->
    AllPermsPerFile = maps:map(fun(Guid, _) ->
        permissions_test_utils:all_perms(TestNode, Guid)
    end, RequiredPermsPerFile),

    % Denying only required perms and granting all others should result in eacces
    ExpError = get_exp_error(?EACCES, TestSuiteSpec),

    lists:foreach(fun({Guid, Perm}) ->
        EaccesPermsPerFile = #{Guid => [Perm]},

        permissions_test_utils:set_acls(TestNode, AllPermsPerFile, EaccesPermsPerFile, AceWho, AceFlags),
        assert_operation(EaccesPermsPerFile, ExpError, TestGroupCtx)
    end, flatten_perms_per_file(RequiredPermsPerFile)),

    % Denying all perms but required ones should result in success
    permissions_test_utils:set_acls(TestNode, #{}, ComplementaryPermsPerFile, AceWho, AceFlags),
    assert_operation(ComplementaryPermsPerFile, ok, TestGroupCtx),

    run_final_storage_ownership_check(TestGroupCtx).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec init_test_suite(authz_test_suite_spec()) -> authz_test_suite_ctx().
init_test_suite(TestSuiteSpec = #authz_test_suite_spec{
    name = TestSuiteName,
    provider_selector = ProviderSelector,
    space_selector = SpaceSelector,
    space_owner_selector = SpaceOwnerSelector,
    files_owner_selector = FilesOwnerSelector
}) ->
    TestNode = oct_background:get_random_provider_node(ProviderSelector),
    FileOwnerSessionId = oct_background:get_user_session_id(FilesOwnerSelector, ProviderSelector),

    SpaceName = oct_background:get_space_name(SpaceSelector),
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
-spec init_test_group(
    binary(),
    session:id() | oct_background:entity_selector(),
    authz_test_suite_ctx()
) ->
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
        executioner_session_id = case ExecutionerSelector of
            ?GUEST_SESS_ID -> ?GUEST_SESS_ID;
            _ -> oct_background:get_user_session_id(ExecutionerSelector, ProviderSelector)
        end,
        required_perms_per_file = RequiredPermsPerFile,
        extra_data = ExtraData
    }.


%% @private
-spec infer_test_group_root_dir_permissions(authz_test_suite_spec()) ->
    perms().
infer_test_group_root_dir_permissions(#authz_test_suite_spec{requires_traverse_ancestors = true}) ->
    [?traverse_container];
infer_test_group_root_dir_permissions(_) ->
    [].


%% @private
-spec flatten_perms_per_file(perms_per_file() | posix_perms_per_file()) ->
    [{file_id:file_guid(), atom() | binary()}].
flatten_perms_per_file(PermsPerFile) ->
    maps:fold(fun(Guid, Perms, OuterAcc) ->
        lists:foldl(fun(Perm, InnerAcc) -> [{Guid, Perm} | InnerAcc] end, OuterAcc, Perms)
    end, [], PermsPerFile).


%% @private
-spec set_full_perms(posix | acl, node(), [file_id:file_guid()]) -> ok.
set_full_perms(posix, Node, FileGuids) ->
    FullPosixPermsPerFile = lists:foldl(fun(Guid, Acc) -> Acc#{Guid => 8#777} end, #{}, FileGuids),
    permissions_test_utils:set_modes(Node, FullPosixPermsPerFile);

set_full_perms(acl, Node, FileGuids) ->
    FullAclPermsPerFile = get_full_perms_per_file(Node, FileGuids),
    permissions_test_utils:set_acls(Node, FullAclPermsPerFile, #{}, ?everyone, ?no_flags_mask).


%% @private
-spec get_full_perms_per_file(node(), [file_id:file_guid()]) -> perms_per_file().
get_full_perms_per_file(Node, FileGuids) ->
    lists:foldl(fun(Guid, Acc) ->
        Acc#{Guid => permissions_test_utils:all_perms(Node, Guid)}
    end, #{}, FileGuids).


%% @private
-spec assert_operation(
    perms_per_file() | posix_perms_per_file(),
    term(),
    authz_test_group_ctx() | authz_cv_test_group_ctx()
) ->
    ok | no_return().
assert_operation(ActualPermsPerFile, ExpResult, #authz_cv_test_group_ctx{
    group_ctx = TestGroupCtx
}) ->
    assert_operation(ActualPermsPerFile, ExpResult, TestGroupCtx);

assert_operation(ActualPermsPerFile, ExpResult, TestGroupCtx = #authz_test_group_ctx{
    suite_ctx = #authz_test_suite_ctx{test_node = TestNode},
    name = TestGroupName
}) ->
    try
        ?assertEqual(ExpResult, exec_operation(TestGroupCtx)),
        ok
    catch Class:Reason:Stacktrace ->
        ActualPermsPerFileJson = format_perms_per_file(TestNode, ActualPermsPerFile),

        ?ct_pal_exception(
            "Operation exec with permissions failed ~s",
            [?autoformat(TestGroupName, ActualPermsPerFileJson)],
            Class, Reason, Stacktrace
        ),
        error(assert_operation_failed)
    end.


%% @private
-spec exec_operation(authz_test_group_ctx()) -> term().
exec_operation(#authz_test_group_ctx{
    suite_ctx = #authz_test_suite_ctx{
        suite_spec = #authz_test_suite_spec{operation = Operation},
        test_node = TestNode
    },
    group_root_dir_path = TestGroupRootDirPath,
    executioner_session_id = ExecutionerSessionId,
    extra_data = ExtraData
}) ->
    Operation(TestNode, ExecutionerSessionId, TestGroupRootDirPath, ExtraData).


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