%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests LUMA
%%% @end
%%%--------------------------------------------------------------------
-module(luma_test_SUITE).
-author("Jakub Kudzia").

-include("luma/luma_test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/1, init_per_testcase/2, end_per_testcase/1, end_per_testcase/2]).

-export([
    luma_db_namespace_isolation/1,
    stale_luma_db_namespaces_are_garbage_collected/1,
    luma_db_is_cleared_with_its_stale_namespaces/1,

    % tests of mapping user to storage credentials
    map_root_to_storage_creds_returns_admin_creds/1,
    map_space_owner_to_storage_creds_on_storage_with_auto_feed_luma_posix_compatible/1,
    map_space_owner_to_storage_creds_on_storage_with_user_defined_luma_posix_compatible/1,
    map_space_owner_to_storage_creds_posix_incompatible/1,
    map_user_to_storage_creds_on_storage_with_auto_feed_luma_posix_compatible/1,
    map_user_to_storage_creds_on_storage_with_auto_feed_luma_posix_incompatible/1,
    map_user_to_storage_creds_on_storage_with_user_defined_luma_posix_compatible/1,
    map_user_to_storage_creds_on_storage_with_user_defined_luma_imported/1,
    map_user_to_storage_creds_on_storage_with_user_defined_luma_posix_incompatible/1,
    map_user_to_storage_creds_fails_on_invalid_response_from_external_feed/1,
    map_user_to_storage_creds_fails_when_mapping_is_not_found_in_luma/1,

    % tests of mapping user to display (oneclient) credentials
    map_root_to_display_creds_returns_root_creds/1,
    map_space_owner_to_display_creds_on_storage_with_auto_feed_luma_posix_compatible/1,
    map_space_owner_to_display_creds_on_storage_with_user_defined_luma_posix_compatible/1,
    map_space_owner_to_display_creds_posix_incompatible/1,
    map_user_to_display_creds_on_storage_with_auto_feed_luma/1,
    map_user_to_display_creds_on_storage_with_user_defined_luma/1,
    map_user_to_display_creds_fails_on_invalid_response_from_external_feed/1,
    map_user_to_display_creds_fails_when_mapping_is_not_found_in_user_defined_luma/1
]).

all() ->
    ?ALL([
        luma_db_namespace_isolation,
        stale_luma_db_namespaces_are_garbage_collected,
        luma_db_is_cleared_with_its_stale_namespaces,

        % tests of mapping user to storage credentials
        map_root_to_storage_creds_returns_admin_creds,
        map_space_owner_to_storage_creds_on_storage_with_auto_feed_luma_posix_compatible,
        map_space_owner_to_storage_creds_on_storage_with_user_defined_luma_posix_compatible,
        map_space_owner_to_storage_creds_posix_incompatible,
        map_user_to_storage_creds_on_storage_with_auto_feed_luma_posix_compatible,
        map_user_to_storage_creds_on_storage_with_auto_feed_luma_posix_incompatible,
        map_user_to_storage_creds_on_storage_with_user_defined_luma_posix_compatible,
        map_user_to_storage_creds_on_storage_with_user_defined_luma_imported,
        map_user_to_storage_creds_on_storage_with_user_defined_luma_posix_incompatible,
        map_user_to_storage_creds_fails_on_invalid_response_from_external_feed,
        map_user_to_storage_creds_fails_when_mapping_is_not_found_in_luma,

        % tests of mapping user to display (oneclient) credentials
        map_root_to_display_creds_returns_root_creds,
        map_space_owner_to_display_creds_on_storage_with_auto_feed_luma_posix_compatible,
        map_space_owner_to_display_creds_on_storage_with_user_defined_luma_posix_compatible,
        map_space_owner_to_display_creds_posix_incompatible,
        map_user_to_display_creds_on_storage_with_auto_feed_luma,
        map_user_to_display_creds_on_storage_with_user_defined_luma,
        map_user_to_display_creds_fails_on_invalid_response_from_external_feed,
        map_user_to_display_creds_fails_when_mapping_is_not_found_in_user_defined_luma
    ]).

% users for which mappings defined in luma.json are incorrect
-define(ERR_USERS, [<<"user", (integer_to_binary(I))/binary>> || I <- lists:seq(2, 10)]).

% Credentials used to tell the mappings of the two namespaces apart in the
% garbage collection tests
-define(STALE_NAMESPACE_CREDENTIALS,
    luma_test_utils:new_s3_credentials(<<"STALE_ACCESS_KEY">>, <<"STALE_SECRET_KEY">>)).
-define(LIVE_NAMESPACE_CREDENTIALS,
    luma_test_utils:new_s3_credentials(<<"LIVE_ACCESS_KEY">>, <<"LIVE_SECRET_KEY">>)).

%% @TODO VFS-12827 - implement tests for all helper types

%%%===================================================================
%%% Test functions - luma db namespaces
%%%===================================================================

luma_db_namespace_isolation(Config) ->
    ?RUN(Config, [?LOCAL_FEED_LUMA_S3_STORAGE_CONFIG], fun luma_db_namespace_isolation_base/2).

luma_db_namespace_isolation_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = #document{value = StorageConfig}  = maps:get(storage_record, StorageLumaConfig),
    ExpectedUserCreds = maps:get(user_credentials, StorageLumaConfig),

    MapFun = fun(Storage_) ->
        luma_test_utils:map_to_storage_creds(Worker, ?LUMA_SESS_ID, ?LUMA_USER_ID, ?LUMA_SPACE_ID, Storage_)
    end,

    % Already filled luma db for current luma db namespace should properly map
    ?assertMatch({ok, ExpectedUserCreds}, MapFun(Storage)),

    % But it is not available once the storage draws a new namespace
    Storage2 = Storage#document{value = StorageConfig#storage_config{
        luma_db_namespace = rpc:call(Worker, luma_db, draw_namespace, [])
    }},
    ?assertMatch({error, not_found}, MapFun(Storage2)),

    % Unless it is explicitly added
    {ok, _} = rpc:call(Worker, luma_crud_api, storage_users_store, [Storage2, ?LUMA_USER_ID, #{
        <<"storageCredentials">> => ExpectedUserCreds
    }]),
    ?assertMatch({ok, ExpectedUserCreds}, MapFun(Storage2)),

    % Which does not invalidate entries in other namespaces (they must be deleted separately)
    ?assertMatch({ok, ExpectedUserCreds}, MapFun(Storage)),

    % Clearing luma db clears entries only for specific namespace
    ok = rpc:call(Worker, luma_crud_api, clear_db, [Storage2]),
    ?assertMatch({error, not_found}, MapFun(Storage2)),
    ?assertMatch({ok, ExpectedUserCreds}, MapFun(Storage)),

    ok.


stale_luma_db_namespaces_are_garbage_collected(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {StorageId, StaleStorageData, LiveStorageData} = set_up_storage_with_stale_namespace(
        Worker, ?FUNCTION_NAME
    ),

    StaleCredentials = ?STALE_NAMESPACE_CREDENTIALS,
    LiveCredentials = ?LIVE_NAMESPACE_CREDENTIALS,

    store_user_mapping(Worker, StaleStorageData, StaleCredentials),
    store_user_mapping(Worker, LiveStorageData, LiveCredentials),
    ?assertMatch({ok, #{<<"storageCredentials">> := StaleCredentials}},
        get_user_mapping(Worker, StaleStorageData)),
    ?assertMatch({ok, #{<<"storageCredentials">> := LiveCredentials}},
        get_user_mapping(Worker, LiveStorageData)),

    ok = rpc:call(Worker, luma_db_garbage_collector, run, []),

    % the stale namespace is emptied...
    ?assertEqual({error, not_found}, get_user_mapping(Worker, StaleStorageData)),

    % ...while the namespace the storage reads through is left alone - a collector
    % that swept it would be deleting live mappings
    ?assertMatch({ok, #{<<"storageCredentials">> := LiveCredentials}},
        get_user_mapping(Worker, LiveStorageData)),

    % and the namespace is struck off, so the next run has nothing to do
    ?assertEqual([], rpc:call(Worker, storage_config, get_stale_luma_db_namespaces, [StorageId])).


luma_db_is_cleared_with_its_stale_namespaces(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {StorageId, StaleStorageData, LiveStorageData} = set_up_storage_with_stale_namespace(
        Worker, ?FUNCTION_NAME
    ),

    store_user_mapping(Worker, StaleStorageData, ?STALE_NAMESPACE_CREDENTIALS),
    store_user_mapping(Worker, LiveStorageData, ?LIVE_NAMESPACE_CREDENTIALS),

    % this is what a storage being deleted goes through - once its config is gone
    % the stale namespaces can no longer be found, so they must be cleared here
    {ok, StorageConfigDoc} = rpc:call(Worker, storage_config, get, [StorageId]),
    ok = rpc:call(Worker, luma_crud_api, clear_db_with_stale_namespaces, [StorageConfigDoc]),

    ?assertEqual({error, not_found}, get_user_mapping(Worker, StaleStorageData)),
    ?assertEqual({error, not_found}, get_user_mapping(Worker, LiveStorageData)).


%%%===================================================================
%%% Test functions - mapping user to storage credentials
%%%===================================================================

map_root_to_storage_creds_returns_admin_creds(Config) ->
    ?RUN(Config, ?ALL_STORAGE_CONFIGS,
        fun map_root_to_storage_creds_returns_admin_creds_base/2
    ).

map_space_owner_to_storage_creds_on_storage_with_auto_feed_luma_posix_compatible(Config) ->
    ?RUN(Config, ?AUTO_FEED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_space_owner_to_storage_creds_on_storage_with_auto_feed_luma_posix_compatible_base/2
    ).

map_space_owner_to_storage_creds_on_storage_with_user_defined_luma_posix_compatible(Config) ->
    ?RUN(Config, ?USER_DEFINED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_space_owner_to_storage_creds_on_storage_with_user_defined_luma_posix_compatible_base/2
    ).

map_space_owner_to_storage_creds_posix_incompatible(Config) ->
    ?RUN(Config, ?POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
        fun map_space_owner_to_storage_creds_posix_incompatible_base/2
    ).

map_user_to_storage_creds_on_storage_with_auto_feed_luma_posix_compatible(Config) ->
    ?RUN(Config, ?AUTO_FEED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_user_to_storage_creds_on_storage_with_auto_feed_luma_posix_compatible_base/2
    ).

map_user_to_storage_creds_on_storage_with_auto_feed_luma_posix_incompatible(Config) ->
    ?RUN(Config, ?AUTO_FEED_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
        fun map_user_to_storage_creds_on_storage_with_auto_feed_luma_not_posix_incompatible_base/2
    ).

map_user_to_storage_creds_on_storage_with_user_defined_luma_posix_compatible(Config) ->
    ?RUN(Config, ?USER_DEFINED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_user_to_storage_creds_on_storage_with_user_defined_luma_posix_compatible_base/2
    ).

map_user_to_storage_creds_on_storage_with_user_defined_luma_imported(Config) ->
    ?RUN(Config, ?IMPORTED_STORAGE_CONFIGS,
        fun map_user_to_storage_creds_on_storage_with_user_defined_luma_imported_base/2
    ).

map_user_to_storage_creds_on_storage_with_user_defined_luma_posix_incompatible(Config) ->
    ?RUN(Config, ?USER_DEFINED_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
        fun map_user_to_storage_creds_on_storage_with_user_defined_luma_posix_incompatible_base/2
    ).

map_user_to_storage_creds_fails_on_invalid_response_from_external_feed(Config) ->
    ?RUN(Config, ?EXTERNAL_FEED_LUMA_STORAGE_CONFIGS,
        fun map_user_to_storage_creds_fails_on_invalid_response_from_external_feed_base/2
    ).

map_user_to_storage_creds_fails_when_mapping_is_not_found_in_luma(Config) ->
    ?RUN(Config, ?USER_DEFINED_LUMA_STORAGE_CONFIGS,
        fun map_user_to_storage_creds_fails_when_mapping_is_not_found_in_luma_base/2
    ).

%%%===================================================================
%%% Test functions - mapping user to display credentials
%%%===================================================================

map_root_to_display_creds_returns_root_creds(Config) ->
    ?RUN(Config, ?ALL_STORAGE_CONFIGS,
        fun map_root_to_display_creds_returns_root_creds_base/2
    ).

map_space_owner_to_display_creds_on_storage_with_auto_feed_luma_posix_compatible(Config) ->
    ?RUN(Config, ?AUTO_FEED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_space_owner_to_display_creds_on_storage_with_auto_feed_luma_posix_compatible_base/2
    ).

map_space_owner_to_display_creds_on_storage_with_user_defined_luma_posix_compatible(Config) ->
    ?RUN(Config, ?USER_DEFINED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_space_owner_to_display_creds_on_storage_with_user_defined_luma_posix_compatible_base/2
    ).

map_space_owner_to_display_creds_posix_incompatible(Config) ->
    ?RUN(Config, ?POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
        fun map_space_owner_to_display_creds_posix_incompatible_base/2
    ).

map_user_to_display_creds_on_storage_with_auto_feed_luma(Config) ->
    ?RUN(Config, ?AUTO_FEED_LUMA_STORAGE_CONFIGS,
        fun map_user_to_display_creds_on_storage_with_auto_feed_luma_base/2
    ).

map_user_to_display_creds_on_storage_with_user_defined_luma(Config) ->
    ?RUN(Config, ?USER_DEFINED_LUMA_STORAGE_CONFIGS,
        fun map_user_to_display_creds_on_storage_with_user_defined_luma_base/2
    ).

map_user_to_display_creds_fails_on_invalid_response_from_external_feed(Config) ->
    ?RUN(Config, ?EXTERNAL_FEED_LUMA_STORAGE_CONFIGS,
        fun map_user_to_display_creds_fails_on_invalid_response_from_external_feed_base/2
    ).

map_user_to_display_creds_fails_when_mapping_is_not_found_in_user_defined_luma(Config) ->
    ?RUN(Config, ?USER_DEFINED_LUMA_STORAGE_CONFIGS,
        fun map_user_to_display_creds_fails_when_mapping_is_not_found_in_user_defined_luma_base/2
    ).

%%%===================================================================
%%% Test bases - mapping user to storage credentials
%%%===================================================================

map_root_to_storage_creds_returns_admin_creds_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    AdminCreds = maps:get(admin_credentials, StorageLumaConfig),
    ?assertEqual({ok, AdminCreds},
        luma_test_utils:map_to_storage_creds(Worker, ?ROOT_SESS_ID, ?ROOT_USER_ID, ?LUMA_SPACE_ID, Storage)).

map_space_owner_to_storage_creds_on_storage_with_auto_feed_luma_posix_compatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    DefaultCreds = maps:get(default_credentials, StorageLumaConfig),
    ?assertEqual({ok, DefaultCreds},
        luma_test_utils:map_to_storage_creds(Worker, ?SPACE_OWNER_ID(?LUMA_SPACE_ID), ?LUMA_SPACE_ID, Storage)).

map_space_owner_to_storage_creds_on_storage_with_user_defined_luma_posix_compatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    DefaultCreds = maps:get(default_credentials, StorageLumaConfig),
    ?assertEqual({ok, DefaultCreds},
        luma_test_utils:map_to_storage_creds(Worker, ?SPACE_OWNER_ID(?LUMA_SPACE_ID), ?LUMA_SPACE_ID, Storage)).

map_space_owner_to_storage_creds_posix_incompatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    AdminCreds = maps:get(admin_credentials, StorageLumaConfig),
    ?assertEqual({ok, AdminCreds},
        luma_test_utils:map_to_storage_creds(Worker, ?SPACE_OWNER_ID(?LUMA_SPACE_ID), ?LUMA_SPACE_ID, Storage)),
    % changing admin creds should change mapping
    {ChangedAdminCreds, ChangedStorage} = luma_test_utils:change_admin_creds(Storage),
    ?assertEqual({ok, ChangedAdminCreds},
        luma_test_utils:map_to_storage_creds(Worker, ?SPACE_OWNER_ID(?LUMA_SPACE_ID), ?LUMA_SPACE_ID, ChangedStorage)).

map_user_to_storage_creds_on_storage_with_auto_feed_luma_posix_compatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    UserCreds = maps:get(user_credentials, StorageLumaConfig),
    ?assertMatch({ok, UserCreds},
        luma_test_utils:map_to_storage_creds(Worker, ?LUMA_SESS_ID, ?LUMA_USER_ID, ?LUMA_SPACE_ID, Storage)),
    % changing admin creds should NOT change mapping
    {_ChangedAdminCreds, ChangedStorage} = luma_test_utils:change_admin_creds(Storage),
    ?assertMatch({ok, UserCreds},
        luma_test_utils:map_to_storage_creds(Worker, ?LUMA_SESS_ID, ?LUMA_USER_ID, ?LUMA_SPACE_ID, ChangedStorage)).


map_user_to_storage_creds_on_storage_with_auto_feed_luma_not_posix_incompatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    AdminCreds = maps:get(admin_credentials, StorageLumaConfig),
    ?assertMatch({ok, AdminCreds},
        luma_test_utils:map_to_storage_creds(Worker, ?LUMA_SESS_ID, ?LUMA_USER_ID, ?LUMA_SPACE_ID, Storage)).

map_user_to_storage_creds_on_storage_with_user_defined_luma_posix_compatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ExpectedUserCreds = maps:get(user_credentials, StorageLumaConfig),
    ?assertMatch({ok, ExpectedUserCreds},
        luma_test_utils:map_to_storage_creds(Worker, ?LUMA_SESS_ID, ?LUMA_USER_ID, ?LUMA_SPACE_ID, Storage)).

map_user_to_storage_creds_on_storage_with_user_defined_luma_imported_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ExpectedUserCreds = maps:get(user_credentials, StorageLumaConfig),
    ?assertMatch({ok, ExpectedUserCreds},
        luma_test_utils:map_to_storage_creds(Worker, ?LUMA_SESS_ID, ?LUMA_USER_ID, ?LUMA_SPACE_ID, Storage)),
    Uid = binary_to_integer(maps:get(<<"uid">>, ExpectedUserCreds)),
    % reverse mapping should be stored automatically,
    ?assertMatch({ok, ?LUMA_USER_ID},
        luma_test_utils:map_uid_to_onedata_user(Worker, Uid, ?LUMA_SPACE_ID, Storage)).

map_user_to_storage_creds_on_storage_with_user_defined_luma_posix_incompatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ExpectedUserCreds = maps:get(user_credentials, StorageLumaConfig),
    ?assertMatch({ok, ExpectedUserCreds},
        luma_test_utils:map_to_storage_creds(Worker, ?LUMA_SESS_ID, ?LUMA_USER_ID, ?LUMA_SPACE_ID, Storage)).

map_user_to_storage_creds_fails_on_invalid_response_from_external_feed_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    lists:foreach(fun(User) ->
        ?assertEqual({error, not_found},
            luma_test_utils:map_to_storage_creds(Worker, ?LUMA_SESS_ID, User, ?LUMA_SPACE_ID, Storage))
    end, ?ERR_USERS).

map_user_to_storage_creds_fails_when_mapping_is_not_found_in_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ?assertEqual({error, not_found},
        luma_test_utils:map_to_storage_creds(Worker, ?LUMA_SESS_ID, <<"not existing user id">>, ?LUMA_SPACE_ID, Storage)).

%%%===================================================================
%%% Test bases - mapping user to display credentials
%%%===================================================================

map_root_to_display_creds_returns_root_creds_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ?assertEqual({ok, ?ROOT_DISPLAY_CREDS},
        luma_test_utils:map_to_display_creds(Worker, ?ROOT_USER_ID, ?LUMA_SPACE_ID, Storage)).

map_space_owner_to_display_creds_on_storage_with_auto_feed_luma_posix_compatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    DisplayCreds = maps:get(default_credentials, StorageLumaConfig),
    ?assertEqual({ok, ?POSIX_CREDS_TO_TUPLE(DisplayCreds)},
        luma_test_utils:map_to_display_creds(Worker, ?SPACE_OWNER_ID(?LUMA_SPACE_ID), ?LUMA_SPACE_ID, Storage)).

map_space_owner_to_display_creds_on_storage_with_user_defined_luma_posix_compatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    DisplayCreds = maps:get(display_credentials, StorageLumaConfig),
    ?assertEqual({ok, ?POSIX_CREDS_TO_TUPLE(DisplayCreds)},
        luma_test_utils:map_to_display_creds(Worker, ?SPACE_OWNER_ID(?LUMA_SPACE_ID), ?LUMA_SPACE_ID, Storage)).

map_space_owner_to_display_creds_posix_incompatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    DispCreds = maps:get(display_credentials, StorageLumaConfig),
    ?assertEqual({ok, ?POSIX_CREDS_TO_TUPLE(DispCreds)},
        luma_test_utils:map_to_display_creds(Worker, ?SPACE_OWNER_ID(?LUMA_SPACE_ID), ?LUMA_SPACE_ID, Storage)).

map_user_to_display_creds_on_storage_with_auto_feed_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    DispCreds = maps:get(user_display_credentials, StorageLumaConfig),
    ?assertMatch({ok, DispCreds},
        luma_test_utils:map_to_display_creds(Worker, ?LUMA_USER_ID, ?LUMA_SPACE_ID, Storage)).

map_user_to_display_creds_on_storage_with_user_defined_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ExpectedUserCreds = maps:get(user_display_credentials, StorageLumaConfig),
    ?assertMatch({ok, ExpectedUserCreds},
        luma_test_utils:map_to_display_creds(Worker, ?LUMA_USER_ID, ?LUMA_SPACE_ID, Storage)).

map_user_to_display_creds_fails_on_invalid_response_from_external_feed_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    lists:foreach(fun(User) ->
        ?assertEqual({error, not_found},
            luma_test_utils:map_to_display_creds(Worker, User, ?LUMA_SPACE_ID, Storage))
    end, ?ERR_USERS).

map_user_to_display_creds_fails_when_mapping_is_not_found_in_user_defined_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ?assertEqual({error, not_found},
        luma_test_utils:map_to_display_creds(Worker, <<"not existing user id">>, ?LUMA_SPACE_ID, Storage)).


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

init_per_testcase(Config) ->
    % Test cases from all/0 function are called many times for different configs
    % by luma_test_utils:run_test_for_all_storage_configs function.
    % This init_per_testcase is called before running test on each config.
    init_per_testcase(default, Config).

init_per_testcase(default, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    luma_test_utils:mock_storage_is_imported(Worker),
    luma_test_utils:mock_stat_on_space_mount_dir(Worker),
    luma_test_utils:setup_local_feed_luma(Worker, Config, <<"local_feed_luma.json">>),
    ok = test_utils:mock_new(Worker, [idp_access_token]),
    ok = test_utils:mock_expect(Worker, idp_access_token, acquire, fun
        (?LUMA_ADMIN_ID, TokenCredentials, ?OAUTH2_IDP) when element(1, TokenCredentials) == token_credentials ->
            {ok, {?IDP_ADMIN_TOKEN, ?TTL}};
        (?LUMA_USER_ID, ?LUMA_SESS_ID, ?OAUTH2_IDP) ->
            {ok, {?IDP_USER_TOKEN, ?TTL}}
    end),
    Config;
init_per_testcase(_Case, Config) ->
    % this case is called by CT framework before each testcase listed in all/0 function
    Config.

end_per_testcase(Config) ->
    end_per_testcase(default, Config).

end_per_testcase(default, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    luma_test_utils:clear_luma_db_for_all_storages(Worker),
    ok = test_utils:mock_unload(Worker, [storage_file_ctx, idp_access_token, storage_config]);
end_per_testcase(Case, Config) when
    Case =:= stale_luma_db_namespaces_are_garbage_collected;
    Case =:= luma_db_is_cleared_with_its_stale_namespaces
->
    [Worker | _] = ?config(op_worker_nodes, Config),
    % the storage exists only for the duration of the case; left behind, its
    % stale namespace would be swept by every later garbage collector run
    rpc:call(Worker, storage_config, delete, [gc_test_storage_id(Case)]),
    Config;
end_per_testcase(_Case, Config) ->
    Config.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
%% Registers a storage config (only the op-worker side of a storage - the LUMA
%% DB never consults Onezone) that already has one stale namespace, and
%% returns storage data addressing each of the two namespaces.
set_up_storage_with_stale_namespace(Worker, Case) ->
    StorageId = gc_test_storage_id(Case),
    StaleNamespace = rpc:call(Worker, luma_db, draw_namespace, []),
    LiveNamespace = rpc:call(Worker, luma_db, draw_namespace, []),

    % NOTE: deliberately a posix INCOMPATIBLE storage - storing a mapping on a
    % posix one adds a reverse mapping, which consults storage:is_imported/1 and
    % so would drag Onezone into a test that otherwise needs nothing but the
    % local storage config
    StorageConfig = #storage_config{
        helper_spec = ?S3_HELPER(?S3_ADMIN_CREDENTIALS),
        luma_config = ?LUMA_CONFIG(?LOCAL_FEED),
        luma_db_namespace = LiveNamespace,
        stale_luma_db_namespaces = [StaleNamespace]
    },
    {ok, _} = rpc:call(Worker, storage_config, create, [StorageId, StorageConfig]),

    LiveStorageData = #document{key = StorageId, value = StorageConfig},
    StaleStorageData = LiveStorageData#document{
        value = StorageConfig#storage_config{luma_db_namespace = StaleNamespace}
    },
    {StorageId, StaleStorageData, LiveStorageData}.


%% @private
gc_test_storage_id(Case) ->
    <<"lumaDbGcStorage_", (atom_to_binary(Case, utf8))/binary>>.


%% @private
store_user_mapping(Worker, StorageData, StorageCredentials) ->
    {ok, _} = rpc:call(Worker, luma_crud_api, storage_users_store, [
        StorageData, ?LUMA_USER_ID, #{<<"storageCredentials">> => StorageCredentials}
    ]).


%% @private
get_user_mapping(Worker, StorageData) ->
    rpc:call(Worker, luma_crud_api, storage_users_get_and_describe, [StorageData, ?LUMA_USER_ID]).
