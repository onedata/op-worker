%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests reverse LUMA
%%% @end
%%%--------------------------------------------------------------------
-module(reverse_luma_test_SUITE).
-author("Jakub Kudzia").

-include("luma_test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2, init_per_suite/1, end_per_suite/1]).

-export([
    map_uid_to_onedata_user_on_storage_without_luma/1,
    map_acl_user_to_onedata_user_should_fail_on_storage_without_luma/1,
    map_acl_group_to_onedata_group_should_fail_on_storage_without_luma/1,
    map_uid_to_onedata_user_on_storage_with_ext_luma/1,
    map_uid_to_onedata_user_using_idp_on_storage_with_ext_luma/1,
    map_uid_to_onedata_user_failure_ext_luma/1,
    map_acl_user_to_onedata_user_on_storage_with_ext_luma/1,
    map_acl_user_to_onedata_user_using_idp_on_storage_with_ext_luma/1,
    map_acl_user_to_onedata_user_failure_ext_luma/1,
    map_acl_group_to_onedata_group_on_storage_with_ext_luma/1,
    map_acl_group_to_onedata_group_using_idp_on_storage_with_ext_luma/1,
    map_acl_group_to_onedata_group_failure_ext_luma/1
]).

all() ->
    ?ALL([
        map_uid_to_onedata_user_on_storage_without_luma,
        map_acl_user_to_onedata_user_should_fail_on_storage_without_luma,
        map_acl_group_to_onedata_group_should_fail_on_storage_without_luma,
        map_uid_to_onedata_user_on_storage_with_ext_luma,
        map_uid_to_onedata_user_using_idp_on_storage_with_ext_luma,
        map_uid_to_onedata_user_failure_ext_luma,
        map_acl_user_to_onedata_user_on_storage_with_ext_luma,
        map_acl_user_to_onedata_user_using_idp_on_storage_with_ext_luma,
        map_acl_user_to_onedata_user_failure_ext_luma,
        map_acl_group_to_onedata_group_on_storage_with_ext_luma,
        map_acl_group_to_onedata_group_using_idp_on_storage_with_ext_luma,
        map_acl_group_to_onedata_group_failure_ext_luma
    ]).

-define(IDP, <<"idp">>).
-define(SUBJECT_ID, <<"subjectId">>).
-define(ENTITLEMENT_ID, <<"entitlementId">>).

% there are 10 users for which mappings in luma.json are incorrect
-define(ERROR_USERS_NUM, 10).

% uids for which mappings defined in luma.json are incorrect
-define(ERR_UIDS, lists:seq(?UID(2), ?UID(2) + ?ERROR_USERS_NUM - 1)).

% acl users for which mappings defined in luma.json are incorrect
-define(ERR_ACL_USERS, [?ACL_USER(I) || I <- lists:seq(2, 2 + ?ERROR_USERS_NUM - 1)]).

% acl groups for which mappings defined in luma.json are incorrect
-define(ERR_ACL_GROUPS, [?ACL_GROUP(I) || I <- lists:seq(2, 2 + ?ERROR_USERS_NUM - 1)]).

%%%===================================================================
%%% Test functions
%%%===================================================================

map_uid_to_onedata_user_on_storage_without_luma(Config) ->
    ?RUN(Config, ?NO_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_uid_to_onedata_user_on_storage_without_luma_base/2
    ).

map_acl_user_to_onedata_user_should_fail_on_storage_without_luma(Config) ->
    ?RUN(Config, ?NO_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_acl_user_to_onedata_user_should_fail_on_storage_without_luma_base/2
    ).

map_acl_group_to_onedata_group_should_fail_on_storage_without_luma(Config) ->
    ?RUN(Config, ?NO_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_acl_group_to_onedata_group_should_fail_on_storage_without_luma_base/2
    ).

map_uid_to_onedata_user_on_storage_with_ext_luma(Config) ->
    ?RUN(Config, ?EXT_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_uid_to_onedata_user_on_storage_with_ext_luma_base/2
    ).

map_uid_to_onedata_user_using_idp_on_storage_with_ext_luma(Config) ->
    ?RUN(Config, ?EXT_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_uid_to_onedata_user_using_idp_on_storage_with_ext_luma_base/2
    ).

map_uid_to_onedata_user_failure_ext_luma(Config) ->
    ?RUN(Config, ?EXT_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_uid_to_onedata_user_failure_ext_luma_base/2
    ).

map_acl_user_to_onedata_user_on_storage_with_ext_luma(Config) ->
    ?RUN(Config, ?EXT_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_acl_user_to_onedata_user_on_storage_with_ext_luma_base/2
    ).

map_acl_user_to_onedata_user_using_idp_on_storage_with_ext_luma(Config) ->
    ?RUN(Config, ?EXT_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_acl_user_to_onedata_user_using_idp_on_storage_with_ext_luma_base/2
    ).

map_acl_user_to_onedata_user_failure_ext_luma(Config) ->
    ?RUN(Config, ?EXT_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_acl_user_to_onedata_user_failure_ext_luma_base/2
    ).

map_acl_group_to_onedata_group_on_storage_with_ext_luma(Config) ->
    ?RUN(Config, ?EXT_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_acl_group_to_onedata_group_on_storage_with_ext_luma_base/2
    ).

map_acl_group_to_onedata_group_using_idp_on_storage_with_ext_luma(Config) ->
    ?RUN(Config, ?EXT_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_acl_group_to_onedata_group_using_idp_on_storage_with_ext_luma_base/2
    ).

map_acl_group_to_onedata_group_failure_ext_luma(Config) ->
    ?RUN(Config, ?EXT_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_acl_group_to_onedata_group_failure_ext_luma_base/2
    ).

%%%===================================================================
%%% Test base functions
%%%===================================================================

map_uid_to_onedata_user_on_storage_without_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ?assertEqual({ok, ?SPACE_OWNER_ID(?SPACE_ID)},
        luma_test_utils:map_uid_to_onedata_user(Worker, ?UID0, ?SPACE_ID, Storage)).

map_acl_user_to_onedata_user_should_fail_on_storage_without_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ?assertEqual({error, luma_disabled},
        luma_test_utils:map_acl_user_to_onedata_user(Worker, ?ACL_USER0, Storage)).

map_acl_group_to_onedata_group_should_fail_on_storage_without_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ?assertEqual({error, luma_disabled},
        luma_test_utils:map_acl_group_to_onedata_group(Worker, ?ACL_GROUP0, Storage)).

map_uid_to_onedata_user_on_storage_with_ext_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ?assertEqual({ok, ?USER_ID},
        luma_test_utils:map_uid_to_onedata_user(Worker, ?UID0, ?SPACE_ID, Storage)),
    % reverse mapping should be cached automatically,
    UidBin = integer_to_binary(?UID0),
    ?assertMatch({ok, #{<<"uid">> := UidBin}},
        luma_test_utils:map_to_storage_creds(Worker, ?USER_ID, ?SPACE_ID, Storage)),
    ?assertMatch({ok, {?UID0, ?SPACE_MOUNT_GID}},
        luma_test_utils:map_to_display_creds(Worker, ?USER_ID, ?SPACE_ID, Storage)).

map_uid_to_onedata_user_using_idp_on_storage_with_ext_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ?assertEqual({ok, ?USER_ID2},
        luma_test_utils:map_uid_to_onedata_user(Worker, ?UID1, ?SPACE_ID, Storage)),
    % reverse mapping should be cached automatically,
    UidBin = integer_to_binary(?UID1),
    ?assertMatch({ok, #{<<"uid">> := UidBin}},
        luma_test_utils:map_to_storage_creds(Worker, ?USER_ID2, ?SPACE_ID, Storage)),
    ?assertMatch({ok, {?UID1, ?SPACE_MOUNT_GID}},
        luma_test_utils:map_to_display_creds(Worker, ?USER_ID2, ?SPACE_ID, Storage)).

map_uid_to_onedata_user_failure_ext_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    lists:foreach(fun(Uid) ->
        ?assertEqual({error, external_luma_error},
            luma_test_utils:map_uid_to_onedata_user(Worker, Uid, ?SPACE_ID, Storage))
    end, ?ERR_UIDS).

map_acl_user_to_onedata_user_on_storage_with_ext_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ?assertEqual({ok, ?USER_ID},
        luma_test_utils:map_acl_user_to_onedata_user(Worker, ?ACL_USER0, Storage)).

map_acl_user_to_onedata_user_using_idp_on_storage_with_ext_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ?assertEqual({ok, ?USER_ID2},
        luma_test_utils:map_acl_user_to_onedata_user(Worker, ?ACL_USER1, Storage)).

map_acl_user_to_onedata_user_failure_ext_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    lists:foreach(fun(AclUser) ->
        ?assertEqual({error, external_luma_error},
            luma_test_utils:map_acl_user_to_onedata_user(Worker, AclUser, Storage))
    end, ?ERR_ACL_USERS).

map_acl_group_to_onedata_group_on_storage_with_ext_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ?assertEqual({ok, ?GROUP_ID},
        luma_test_utils:map_acl_group_to_onedata_group(Worker, ?ACL_GROUP0, Storage)).

map_acl_group_to_onedata_group_using_idp_on_storage_with_ext_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ?assertEqual({ok, ?GROUP_ID2},
        luma_test_utils:map_acl_group_to_onedata_group(Worker, ?ACL_GROUP1, Storage)).

map_acl_group_to_onedata_group_failure_ext_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    lists:foreach(fun(AclGroup) ->
        ?assertEqual({error, external_luma_error},
            luma_test_utils:map_acl_group_to_onedata_group(Worker, AclGroup, Storage))
    end, ?ERR_ACL_GROUPS).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig, "env_desc.json"), NewConfig)
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, ?MODULE, luma_test_utils]}
        | Config
    ].

end_per_suite(Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config).

init_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    mock_map_idp_user_to_onedata(Worker),
    mock_map_idp_group_to_onedata(Worker),
    luma_test_utils:mock_stat_on_space_mount_dir(Worker),
    Config.

end_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    luma_test_utils:clear_luma_db_for_all_storages(Worker),
    ok = test_utils:mock_unload(Worker, [storage_file_ctx]).


%%%===================================================================
%%% Internal functions
%%%===================================================================

mock_map_idp_user_to_onedata(Worker) ->
    test_utils:mock_expect(Worker, provider_logic, map_idp_user_to_onedata, fun
        (?IDP, ?SUBJECT_ID) ->
            {ok, ?USER_ID2}
    end).

mock_map_idp_group_to_onedata(Worker) ->
    test_utils:mock_expect(Worker, provider_logic, map_idp_group_to_onedata, fun
        (?IDP, ?ENTITLEMENT_ID) ->
            {ok, ?GROUP_ID2}
    end).