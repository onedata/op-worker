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

-include("luma_test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2, init_per_suite/1, end_per_suite/1]).

-export([
    % tests of mapping user to storage credentials
    map_root_to_storage_creds_returns_admin_creds/1,
    map_space_owner_to_storage_creds_on_storage_without_luma_posix_compatible/1,
    map_space_owner_to_storage_creds_on_storage_with_ext_luma_posix_compatible/1,
    map_space_owner_to_storage_creds_posix_incompatible/1,
    map_user_to_storage_creds_on_storage_without_luma_posix_compatible/1,
    map_user_to_storage_creds_on_storage_without_luma_posix_incompatible/1,
    map_user_to_storage_creds_on_storage_with_external_luma/1,
    map_user_to_storage_creds_fails_on_invalid_response_from_external_luma/1,
    map_user_to_storage_creds_fails_when_mapping_is_not_found_in_external_luma/1,

    % tests of mapping user to display (oneclient) credentials
    map_root_to_display_creds_returns_root_creds/1,
    map_space_owner_to_display_creds_on_storage_without_luma_posix_compatible/1,
    map_space_owner_to_display_creds_on_storage_with_ext_luma_posix_compatible/1,
    map_space_owner_to_display_creds_posix_incompatible/1,
    map_user_to_display_creds_on_storage_without_luma/1,
    map_user_to_display_creds_on_storage_with_external_luma/1,
    map_user_to_display_creds_fails_on_invalid_response_from_external_luma/1,
    map_user_to_display_creds_fails_when_mapping_is_not_found_in_external_luma/1
]).

all() ->
    ?ALL([
        % tests of mapping user to storage credentials
        map_root_to_storage_creds_returns_admin_creds,
        map_space_owner_to_storage_creds_on_storage_without_luma_posix_compatible,
        map_space_owner_to_storage_creds_on_storage_with_ext_luma_posix_compatible,
        map_space_owner_to_storage_creds_posix_incompatible,
        map_user_to_storage_creds_on_storage_without_luma_posix_compatible,
        map_user_to_storage_creds_on_storage_without_luma_posix_incompatible,
        map_user_to_storage_creds_on_storage_with_external_luma,
        map_user_to_storage_creds_fails_on_invalid_response_from_external_luma,
        map_user_to_storage_creds_fails_when_mapping_is_not_found_in_external_luma,

        % tests of mapping user to display (oneclient) credentials
        map_root_to_display_creds_returns_root_creds,
        map_space_owner_to_display_creds_on_storage_without_luma_posix_compatible,
        map_space_owner_to_display_creds_on_storage_with_ext_luma_posix_compatible,
        map_space_owner_to_display_creds_posix_incompatible,
        map_user_to_display_creds_on_storage_without_luma,
        map_user_to_display_creds_on_storage_with_external_luma,
        map_user_to_display_creds_fails_on_invalid_response_from_external_luma,
        map_user_to_display_creds_fails_when_mapping_is_not_found_in_external_luma
    ]).

% users for which mappings defined in luma.json are incorrect
-define(ERR_USERS, [<<"user", (integer_to_binary(I))/binary>> || I <- lists:seq(2, 10)]).

%%%===================================================================
%%% Test functions - mapping user to storage credentials
%%%===================================================================

map_root_to_storage_creds_returns_admin_creds(Config) ->
    ?RUN(Config, ?ALL_STORAGE_CONFIGS,
        fun map_root_to_storage_creds_returns_admin_creds_base/2
    ).

map_space_owner_to_storage_creds_on_storage_without_luma_posix_compatible(Config) ->
    ?RUN(Config, ?NO_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_space_owner_to_storage_creds_on_storage_without_luma_posix_compatible_base/2
    ).

map_space_owner_to_storage_creds_on_storage_with_ext_luma_posix_compatible(Config) ->
    ?RUN(Config, ?EXT_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_space_owner_to_storage_creds_on_storage_with_ext_luma_posix_compatible_base/2
    ).

map_space_owner_to_storage_creds_posix_incompatible(Config) ->
    ?RUN(Config, ?POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
        fun map_space_owner_to_storage_creds_posix_incompatible_base/2
    ).

map_user_to_storage_creds_on_storage_without_luma_posix_compatible(Config) ->
    ?RUN(Config, ?NO_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_user_to_storage_creds_on_storage_without_luma_posix_compatible_base/2
    ).

map_user_to_storage_creds_on_storage_without_luma_posix_incompatible(Config) ->
    ?RUN(Config, ?NO_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
        fun map_user_to_storage_creds_on_storage_without_luma_not_posix_incompatible_base/2
    ).

map_user_to_storage_creds_on_storage_with_external_luma(Config) ->
    ?RUN(Config, ?EXT_LUMA_STORAGE_CONFIGS,
        fun map_user_to_storage_creds_on_storage_with_external_luma_base/2
    ).

map_user_to_storage_creds_fails_on_invalid_response_from_external_luma(Config) ->
    ?RUN(Config, ?EXT_LUMA_STORAGE_CONFIGS,
        fun map_user_to_storage_creds_fails_on_invalid_response_from_external_luma_base/2
    ).

map_user_to_storage_creds_fails_when_mapping_is_not_found_in_external_luma(Config) ->
    ?RUN(Config, ?EXT_LUMA_STORAGE_CONFIGS,
        fun map_user_to_storage_creds_fails_when_mapping_is_not_found_in_external_luma_base/2
    ).

%%%===================================================================
%%% Test functions - mapping user to display credentials
%%%===================================================================

map_root_to_display_creds_returns_root_creds(Config) ->
    ?RUN(Config, ?ALL_STORAGE_CONFIGS,
        fun map_root_to_display_creds_returns_root_creds_base/2
    ).

map_space_owner_to_display_creds_on_storage_without_luma_posix_compatible(Config) ->
    ?RUN(Config, ?NO_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_space_owner_to_display_creds_on_storage_without_luma_posix_compatible_base/2
    ).

map_space_owner_to_display_creds_on_storage_with_ext_luma_posix_compatible(Config) ->
    ?RUN(Config, ?EXT_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun map_space_owner_to_display_creds_on_storage_with_ext_luma_posix_compatible_base/2
    ).

map_space_owner_to_display_creds_posix_incompatible(Config) ->
    ?RUN(Config, ?POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
        fun map_space_owner_to_display_creds_posix_incompatible_base/2
    ).

map_user_to_display_creds_on_storage_without_luma(Config) ->
    ?RUN(Config, ?NO_LUMA_STORAGE_CONFIGS,
        fun map_user_to_display_creds_on_storage_without_luma_base/2
    ).

map_user_to_display_creds_on_storage_with_external_luma(Config) ->
    ?RUN(Config, ?EXT_LUMA_STORAGE_CONFIGS,
        fun map_user_to_display_creds_on_storage_with_external_luma_base/2
    ).

map_user_to_display_creds_fails_on_invalid_response_from_external_luma(Config) ->
    ?RUN(Config, ?EXT_LUMA_STORAGE_CONFIGS,
        fun map_user_to_display_creds_fails_on_invalid_response_from_external_luma_base/2
    ).

map_user_to_display_creds_fails_when_mapping_is_not_found_in_external_luma(Config) ->
    ?RUN(Config, ?EXT_LUMA_STORAGE_CONFIGS,
        fun map_user_to_display_creds_fails_when_mapping_is_not_found_in_external_luma_base/2
    ).

%%%===================================================================
%%% Test bases - mapping user to storage credentials
%%%===================================================================

map_root_to_storage_creds_returns_admin_creds_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    AdminCreds = maps:get(admin_credentials, StorageLumaConfig),
    ?assertEqual({ok, AdminCreds},
        map_to_storage_creds(Worker, ?ROOT_SESS_ID, ?ROOT_USER_ID, ?SPACE_ID, Storage)).

map_space_owner_to_storage_creds_on_storage_without_luma_posix_compatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    DefaultCreds = maps:get(default_credentials, StorageLumaConfig),
    ?assertEqual({ok, DefaultCreds},
        map_to_storage_creds(Worker, ?SPACE_OWNER_ID(?SPACE_ID), ?SPACE_ID, Storage)).

map_space_owner_to_storage_creds_on_storage_with_ext_luma_posix_compatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    DefaultCreds = maps:get(default_credentials, StorageLumaConfig),
    ?assertEqual({ok, DefaultCreds},
        map_to_storage_creds(Worker, ?SPACE_OWNER_ID(?SPACE_ID), ?SPACE_ID, Storage)).

map_space_owner_to_storage_creds_posix_incompatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    AdminCreds = maps:get(admin_credentials, StorageLumaConfig),
    ?assertEqual({ok, AdminCreds},
        map_to_storage_creds(Worker, ?SPACE_OWNER_ID(?SPACE_ID), ?SPACE_ID, Storage)).

map_user_to_storage_creds_on_storage_without_luma_posix_compatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    UserCreds = maps:get(user_credentials, StorageLumaConfig),
    ?assertMatch({ok, UserCreds},
        map_to_storage_creds(Worker, ?SESS_ID, ?USER_ID, ?SPACE_ID, Storage)).

map_user_to_storage_creds_on_storage_without_luma_not_posix_incompatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    AdminCreds = maps:get(admin_credentials, StorageLumaConfig),
    ?assertMatch({ok, AdminCreds},
        map_to_storage_creds(Worker, ?SESS_ID, ?USER_ID, ?SPACE_ID, Storage)).

map_user_to_storage_creds_on_storage_with_external_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ExpectedUserCreds = maps:get(user_credentials, StorageLumaConfig),
    ?assertMatch({ok, ExpectedUserCreds},
        map_to_storage_creds(Worker, ?SESS_ID, ?USER_ID, ?SPACE_ID, Storage)).

map_user_to_storage_creds_fails_on_invalid_response_from_external_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    lists:foreach(fun(User) ->
        ?assertEqual({error, not_found},
            map_to_storage_creds(Worker, ?SESS_ID, User, ?SPACE_ID, Storage))
    end, ?ERR_USERS).

map_user_to_storage_creds_fails_when_mapping_is_not_found_in_external_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ?assertEqual({error, not_found},
        map_to_storage_creds(Worker, ?SESS_ID, <<"not existing user id">>, ?SPACE_ID, Storage)).

%%%===================================================================
%%% Test bases - mapping user to display credentials
%%%===================================================================

map_root_to_display_creds_returns_root_creds_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ?assertEqual({ok, ?ROOT_DISPLAY_CREDS},
        map_to_display_creds(Worker, ?ROOT_USER_ID, ?SPACE_ID, Storage)).

map_space_owner_to_display_creds_on_storage_without_luma_posix_compatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    DisplayCreds = maps:get(default_credentials, StorageLumaConfig),
    ?assertEqual({ok, ?POSIX_CREDS_TO_TUPLE(DisplayCreds)},
        map_to_display_creds(Worker, ?SPACE_OWNER_ID(?SPACE_ID), ?SPACE_ID, Storage)).

map_space_owner_to_display_creds_on_storage_with_ext_luma_posix_compatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    DisplayCreds = maps:get(display_credentials, StorageLumaConfig),
    ?assertEqual({ok, ?POSIX_CREDS_TO_TUPLE(DisplayCreds)},
        map_to_display_creds(Worker, ?SPACE_OWNER_ID(?SPACE_ID), ?SPACE_ID, Storage)).

map_space_owner_to_display_creds_posix_incompatible_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    DispCreds = maps:get(display_credentials, StorageLumaConfig),
    ?assertEqual({ok, ?POSIX_CREDS_TO_TUPLE(DispCreds)},
        map_to_display_creds(Worker, ?SPACE_OWNER_ID(?SPACE_ID), ?SPACE_ID, Storage)).

map_user_to_display_creds_on_storage_without_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    DispCreds = maps:get(user_display_credentials, StorageLumaConfig),
    ?assertMatch({ok, DispCreds},
        map_to_display_creds(Worker, ?USER_ID, ?SPACE_ID, Storage)).

map_user_to_display_creds_on_storage_with_external_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ExpectedUserCreds = maps:get(user_display_credentials, StorageLumaConfig),
    ?assertMatch({ok, ExpectedUserCreds},
        map_to_display_creds(Worker, ?USER_ID, ?SPACE_ID, Storage)).

map_user_to_display_creds_fails_on_invalid_response_from_external_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    lists:foreach(fun(User) ->
        ?assertEqual({error, not_found},
            map_to_display_creds(Worker, User, ?SPACE_ID, Storage))
    end, ?ERR_USERS).

map_user_to_display_creds_fails_when_mapping_is_not_found_in_external_luma_base(Config, StorageLumaConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Storage = maps:get(storage_record, StorageLumaConfig),
    ?assertEqual({error, not_found},
        map_to_display_creds(Worker, <<"not existing user id">>, ?SPACE_ID, Storage)).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig, "env_desc.json"), NewConfig)
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer]}
        | Config
    ].

end_per_suite(Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config).

init_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Worker, storage_file_ctx),
    ok = test_utils:mock_expect(Worker, storage_file_ctx, stat, fun(StFileCtx) ->
        {#statbuf{st_uid = ?SPACE_MOUNT_UID, st_gid = ?SPACE_MOUNT_GID}, StFileCtx}
    end),
    ok = test_utils:mock_new(Worker, [idp_access_token]),
    ok = test_utils:mock_expect(Worker, idp_access_token, acquire, fun
        (?ADMIN_ID, TokenCredentials, ?OAUTH2_IDP) when element(1, TokenCredentials) == token_credentials ->
            {ok, {?IDP_ADMIN_TOKEN, ?TTL}};
        (?USER_ID, ?SESS_ID, ?OAUTH2_IDP) ->
            {ok, {?IDP_USER_TOKEN, ?TTL}}
    end),
    Config.


end_per_testcase(_Case, Config) ->
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    luma_test_utils:invalidate_cache_for_all_storages(Worker),
    ok = test_utils:mock_unload(Workers, [storage_file_ctx, idp_access_token]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

map_to_storage_creds(Worker, UserId, SpaceId, Storage) ->
    rpc:call(Worker, luma, map_to_storage_credentials, [UserId, SpaceId, Storage]).

map_to_storage_creds(Worker, SessId, UserId, SpaceId, Storage) ->
    rpc:call(Worker, luma, map_to_storage_credentials, [SessId, UserId, SpaceId, Storage]).

map_to_display_creds(Worker, UserId, SpaceId, Storage) ->
    rpc:call(Worker, luma, map_to_display_credentials, [UserId, SpaceId, Storage]).