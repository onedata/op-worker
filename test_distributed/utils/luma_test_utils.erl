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
    mock_stat_on_space_mount_dir/1, setup_local_feed_luma/3, mock_storage_is_imported/1, change_admin_creds/1]).

% LUMA API
-export([map_to_storage_creds/4, map_to_storage_creds/5, map_to_display_creds/4,
    map_uid_to_onedata_user/4, map_acl_user_to_onedata_user/3, map_acl_group_to_onedata_group/3,
    clear_luma_db/2]).

-export([new_ceph_credentials/2, new_cephrados_credentials/2, new_posix_credentials/2,
    new_s3_credentials/2, new_swift_credentials/3, new_glusterfs_credentials/2,
    new_webdav_credentials/2, new_nulldevice_credentials/2]).

-type credentials() :: helper_spec:credentials().

%%%===================================================================
%%% API functions
%%%===================================================================

run_test_for_all_storage_configs(TestCase, TestFun, Module, Config, StorageConfigs) when is_list(StorageConfigs) ->
    lists:foreach(fun(StorageLumaConfig) ->
        Name = maps:get(name, StorageLumaConfig),
        try
            run_test(TestFun, Module, Config, StorageLumaConfig)
        catch
            Error:Reason:Stacktrace ->
                ct:pal("Testcase \"~tp\" failed for config ~tp due to ~tp:~tp~nStacktrace: ~tp",
                    [TestCase, Name, Error, Reason, Stacktrace]
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
    Storages = lists:usort(lists:map(fun(StorageLumaConfig) ->
        maps:get(storage_record, StorageLumaConfig)
    end, ?ALL_STORAGE_CONFIGS )),

    lists:foreach(fun(Storage) ->
        clear_luma_db(Worker, Storage)
    end, Storages).

mock_stat_on_space_mount_dir(Worker) ->
    ok = test_utils:mock_new(Worker, storage_file_ctx),
    ok = test_utils:mock_expect(Worker, storage_file_ctx, stat, fun(StFileCtx) ->
        {#statbuf{st_uid = ?SPACE_MOUNT_UID, st_gid = ?SPACE_MOUNT_GID}, StFileCtx}
    end).

mock_storage_is_imported(Worker) ->
    IsImportedFun = fun(StorageId) ->
        ImportedStorageDocs = [maps:get(storage_record, SC) || SC <- ?IMPORTED_STORAGE_CONFIGS],
        ImportedStorageIds = [Id || #document{key = Id} <- ImportedStorageDocs],
        lists:member(StorageId, ImportedStorageIds)
    end,
    ok = test_utils:mock_new(Worker, storage),
    ok = test_utils:mock_expect(Worker, storage, is_imported, fun
        (#document{key = StorageId}) -> IsImportedFun(StorageId);
        (StorageId) when is_binary(StorageId) -> IsImportedFun(StorageId)
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

change_admin_creds(
    #document{value = #storage_config{helper_spec = #helper_spec{name = ?POSIX_HELPER_NAME}} = StorageConfig} = StorageDoc
) ->
    ChangedAdminCreds = luma_test_utils:new_posix_credentials(?UID1, ?ROOT_GID),
    {ChangedAdminCreds, StorageDoc#document{
        value = StorageConfig#storage_config{helper_spec = ?POSIX_HELPER(ChangedAdminCreds)}}
    };
change_admin_creds(
    #document{value = #storage_config{helper_spec = #helper_spec{name = ?CEPH_HELPER_NAME}} = StorageConfig} = StorageDoc
) ->
    ChangedAdminCreds = luma_test_utils:new_ceph_credentials(<<"ADMIN1">>, <<"ADMIN_KEY">>),
    {ChangedAdminCreds, StorageDoc#document{
        value = StorageConfig#storage_config{helper_spec = ?CEPH_HELPER(ChangedAdminCreds)}}
    };
change_admin_creds(
    #document{value = #storage_config{helper_spec = #helper_spec{name = ?S3_HELPER_NAME}} = StorageConfig} = StorageDoc
) ->
    ChangedAdminCreds = luma_test_utils:new_s3_credentials(<<"ADMIN_ACCESS_KEY1">>, <<"ADMIN_SECRET_KEY">>),
    {ChangedAdminCreds, StorageDoc#document{
        value = StorageConfig#storage_config{helper_spec = ?S3_HELPER(ChangedAdminCreds)}}
    };
change_admin_creds(
    #document{value = #storage_config{helper_spec = #helper_spec{name = ?SWIFT_HELPER_NAME}} = StorageConfig} = StorageDoc
) ->
    ChangedAdminCreds = luma_test_utils:new_swift_credentials(<<"ADMIN1">>, <<"ADMIN_PASSWD">>, <<"PROJECT_NAME">>),
    {ChangedAdminCreds, StorageDoc#document{
        value = StorageConfig#storage_config{helper_spec = ?SWIFT_HELPER(ChangedAdminCreds)}}};
change_admin_creds(
    #document{value = #storage_config{helper_spec = #helper_spec{name = ?CEPHRADOS_HELPER_NAME}} = StorageConfig} = StorageDoc
) ->
    ChangedAdminCreds = luma_test_utils:new_cephrados_credentials(<<"ADMIN1">>, <<"ADMIN_KEY">>),
    {ChangedAdminCreds, StorageDoc#document{
        value = StorageConfig#storage_config{helper_spec = ?CEPHRADOS_HELPER(ChangedAdminCreds)}}
    };
change_admin_creds(
    #document{value = #storage_config{helper_spec = #helper_spec{name = ?GLUSTERFS_HELPER_NAME}} = StorageConfig} = StorageDoc
) ->
    ChangedAdminCreds = luma_test_utils:new_glusterfs_credentials(1, 0),
    {ChangedAdminCreds, StorageDoc#document{
        value = StorageConfig#storage_config{helper_spec = ?GLUSTERFS_HELPER(ChangedAdminCreds)}}
    };
change_admin_creds(
    #document{value = #storage_config{helper_spec = #helper_spec{name = ?NULL_DEVICE_HELPER_NAME}} = StorageConfig} = StorageDoc
) ->
    ChangedAdminCreds = luma_test_utils:new_nulldevice_credentials(1, 0),
    {ChangedAdminCreds, StorageDoc#document{
        value = StorageConfig#storage_config{helper_spec = ?NULLDEVICE_HELPER(ChangedAdminCreds)}}
    };
change_admin_creds(
    #document{value = #storage_config{helper_spec = #helper_spec{name = ?WEBDAV_HELPER_NAME}} = StorageConfig} = StorageDoc
) ->
    ChangedAdminCreds = ?WEBDAV_BASIC_CREDENTIALS(<<"admin1:password">>),
    {ChangedAdminCreds, StorageDoc#document{
        value = StorageConfig#storage_config{helper_spec = ?WEBDAV_HELPER(ChangedAdminCreds)}}
    }.

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

clear_luma_db(Worker, Storage) ->
    ok = rpc:call(Worker, luma_crud_api, clear_db, [Storage]).

%%%===================================================================
%%% Helpers API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Constructs Ceph storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_ceph_credentials(binary(), binary()) -> credentials().
new_ceph_credentials(Username, Key) ->
    #{
        <<"username">> => Username,
        <<"key">> => Key
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs CephRados storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_cephrados_credentials(binary(), binary()) -> credentials().
new_cephrados_credentials(Username, Key) ->
    #{
        <<"username">> => Username,
        <<"key">> => Key
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs POSIX storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_posix_credentials(integer(), integer()) -> credentials().
new_posix_credentials(Uid, Gid) ->
    #{
        <<"uid">> => integer_to_binary(Uid),
        <<"gid">> => integer_to_binary(Gid)
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs S3 storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_s3_credentials(binary(), binary()) -> credentials().
new_s3_credentials(AccessKey, SecretKey) ->
    #{
        <<"accessKey">> => AccessKey,
        <<"secretKey">> => SecretKey
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs Swift storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_swift_credentials(binary(), binary(), binary()) -> credentials().
new_swift_credentials(Username, Password, ProjectName) ->
    #{
        <<"username">> => Username,
        <<"password">> => Password,
        <<"projectName">> => ProjectName
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs GlusterFS storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_glusterfs_credentials(integer(), integer()) -> credentials().
new_glusterfs_credentials(Uid, Gid) ->
    #{
        <<"uid">> => integer_to_binary(Uid),
        <<"gid">> => integer_to_binary(Gid)
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs WebDAV storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_webdav_credentials(binary(), binary()) -> credentials().
new_webdav_credentials(CredentialsType = <<"none">>, _Credentials) ->
    #{
        <<"credentialsType">> => CredentialsType
    };
new_webdav_credentials(CredentialsType = <<"oauth2">>, Credentials) ->
    #{
        <<"credentialsType">> => CredentialsType,
        <<"credentials">> => Credentials,
        <<"oauth2IdP">> => ?OAUTH2_IDP
    };
new_webdav_credentials(CredentialsType, Credentials) ->
    #{
        <<"credentialsType">> => CredentialsType,
        <<"credentials">> => Credentials
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs Null Device storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_nulldevice_credentials(integer(), integer()) -> credentials().
new_nulldevice_credentials(Uid, Gid) ->
    #{
        <<"uid">> => integer_to_binary(Uid),
        <<"gid">> => integer_to_binary(Gid)
    }.
