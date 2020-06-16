%%%-------------------------------------------------------------------
%%% @author Wojciech Geisler
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for luma tests
%%% @end
%%%-------------------------------------------------------------------
-module(luma_test_utils).
-author("Wojciech Geisler").

-include("luma_test_utils.hrl").
-include("modules/storage/helpers/helpers.hrl").

-export([run_test_for_all_storage_configs/5, clear_luma_db_for_all_storages/1,
    mock_stat_on_space_mount_dir/1, setup_local_feed_luma/3
]).

% LUMA API
-export([map_to_storage_creds/4, map_to_storage_creds/5, map_to_display_creds/4,
    map_uid_to_onedata_user/4, map_acl_user_to_onedata_user/3, map_acl_group_to_onedata_group/3,
    clear_luma_db/2]).

-export([new_ceph_user_ctx/2, new_cephrados_user_ctx/2, new_posix_user_ctx/2,
    new_s3_user_ctx/2, new_swift_user_ctx/2, new_glusterfs_user_ctx/2,
    new_webdav_user_ctx/2, new_nulldevice_user_ctx/2]).

-type user_ctx() :: helper:user_ctx().

%%%===================================================================
%%% API functions
%%%===================================================================

run_test_for_all_storage_configs(TestCase, TestFun, Module, Config, StorageConfigs) when is_list(StorageConfigs) ->
    lists:foreach(fun(StorageLumaConfig) ->
        Name = maps:get(name, StorageLumaConfig),
        try
            run_test(TestFun, Module, Config, StorageLumaConfig)
        catch
            Error:Reason ->
                ct:pal("Testcase \"~p\" failed for config ~p due to ~p:~p~nStacktrace: ~p",
                    [TestCase, Name, Error, Reason, erlang:get_stacktrace()]
                ),
                ct:fail("Failed testcase")
        end
    end, StorageConfigs);
run_test_for_all_storage_configs(TestCase, TestFun, Module, Config, StorageConfig) ->
    run_test_for_all_storage_configs(TestCase, TestFun, Module, Config, [StorageConfig]).

run_test(TestFun, Module, Config, StorageConfig) ->
    Config2 = Module:init_per_testcase(Config),
    try
        TestFun(Config, StorageConfig)
    after
        Module:end_per_testcase(Config2)
    end.

clear_luma_db_for_all_storages(Worker) ->
    StorageIds = lists:usort(lists:map(fun(StorageLumaConfig) ->
        Storage = maps:get(storage_record, StorageLumaConfig),
        storage:get_id(Storage)
    end, ?ALL_STORAGE_CONFIGS)),

    lists:foreach(fun(StorageId) ->
        clear_luma_db(Worker, StorageId)
    end, StorageIds).

mock_stat_on_space_mount_dir(Worker) ->
    ok = test_utils:mock_new(Worker, storage_file_ctx),
    ok = test_utils:mock_expect(Worker, storage_file_ctx, stat, fun(StFileCtx) ->
        {#statbuf{st_uid = ?SPACE_MOUNT_UID, st_gid = ?SPACE_MOUNT_GID}, StFileCtx}
    end).

setup_local_feed_luma(Worker, Config, LocalFeedConfigFile) ->
    % in this test suite storages are mocked, therefore we have to call
    % function for setting mappings in LUMA with local feed manually
    StorageDocs = lists:foldl(fun(StorageConfig, Acc) ->
        Doc = maps:get(storage_record, StorageConfig),
        Acc#{storage:get_id(Doc) => Doc}
    end, #{}, ?LOCAL_FEED_LUMA_STORAGE_CONFIGS),
    ok = test_utils:mock_new(Worker, storage_config),
    ok = test_utils:mock_expect(Worker, storage_config, get, fun(StorageId) ->
        {ok, maps:get(StorageId, StorageDocs)}
    end),
    initializer:setup_luma_local_feed(Worker, Config, LocalFeedConfigFile).

%%%===================================================================
%%% LUMA API functions
%%%===================================================================

map_to_storage_creds(Worker, UserId, SpaceId, Storage) ->
    rpc:call(Worker, luma, map_to_storage_credentials, [UserId, SpaceId, Storage]).

map_to_storage_creds(Worker, SessId, UserId, SpaceId, Storage) ->
    rpc:call(Worker, luma, map_to_storage_credentials, [SessId, UserId, SpaceId, Storage]).

map_to_display_creds(Worker, UserId, SpaceId, Storage) ->
    rpc:call(Worker, luma, map_to_display_credentials, [UserId, SpaceId, Storage]).

map_uid_to_onedata_user(Worker, Uid, SpaceId, Storage) ->
    rpc:call(Worker, luma, map_uid_to_onedata_user, [Uid, SpaceId, Storage]).

map_acl_user_to_onedata_user(Worker, AclUser, Storage) ->
    rpc:call(Worker, luma, map_acl_user_to_onedata_user, [AclUser, Storage]).

map_acl_group_to_onedata_group(Worker, AclGroup, Storage) ->
    rpc:call(Worker, luma, map_acl_group_to_onedata_group, [AclGroup, Storage]).

clear_luma_db(Worker, StorageId) ->
    ok = rpc:call(Worker, luma, clear_db, [StorageId]).

%%%===================================================================
%%% Helpers API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Constructs Ceph storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_ceph_user_ctx(binary(), binary()) -> user_ctx().
new_ceph_user_ctx(Username, Key) ->
    #{
        <<"username">> => Username,
        <<"key">> => Key
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs CephRados storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_cephrados_user_ctx(binary(), binary()) -> user_ctx().
new_cephrados_user_ctx(Username, Key) ->
    #{
        <<"username">> => Username,
        <<"key">> => Key
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs POSIX storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_posix_user_ctx(integer(), integer()) -> user_ctx().
new_posix_user_ctx(Uid, Gid) ->
    #{
        <<"uid">> => integer_to_binary(Uid),
        <<"gid">> => integer_to_binary(Gid)
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs S3 storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_s3_user_ctx(binary(), binary()) -> user_ctx().
new_s3_user_ctx(AccessKey, SecretKey) ->
    #{
        <<"accessKey">> => AccessKey,
        <<"secretKey">> => SecretKey
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs Swift storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_swift_user_ctx(binary(), binary()) -> user_ctx().
new_swift_user_ctx(Username, Password) ->
    #{
        <<"username">> => Username,
        <<"password">> => Password
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs GlusterFS storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_glusterfs_user_ctx(integer(), integer()) -> user_ctx().
new_glusterfs_user_ctx(Uid, Gid) ->
    #{
        <<"uid">> => integer_to_binary(Uid),
        <<"gid">> => integer_to_binary(Gid)
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs WebDAV storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_webdav_user_ctx(binary(), binary()) -> user_ctx().
new_webdav_user_ctx(CredentialsType = <<"none">>, _Credentials) ->
    #{
        <<"credentialsType">> => CredentialsType
    };
new_webdav_user_ctx(CredentialsType, Credentials) ->
    #{
        <<"credentialsType">> => CredentialsType,
        <<"credentials">> => Credentials
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs Null Device storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_nulldevice_user_ctx(integer(), integer()) -> user_ctx().
new_nulldevice_user_ctx(Uid, Gid) ->
    #{
        <<"uid">> => integer_to_binary(Uid),
        <<"gid">> => integer_to_binary(Gid)
    }.
