%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used in tests of LUMA.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(LUMA_TEST_UTILS_HRL).
-define(LUMA_TEST_UTILS_HRL, 1).

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/helpers/helpers.hrl").


-define(STRIP_OK(Result), element(2, {ok, _} = Result)).

-define(TEST_BASE,
    binary_to_atom(<<(atom_to_binary(?FUNCTION_NAME, latin1))/binary, "_base">>, latin1)).
-define(RUN(Config, StorageConfigs, TestFun),
    luma_test_utils:run_test_for_all_storage_configs(?TEST_BASE, TestFun, ?MODULE, Config, StorageConfigs)).

%%%===================================================================
%%% Users and spaces macros
%%%===================================================================

-define(SESS_ID, <<"sessionId">>).
-define(USER_ID, <<"user1">>).
-define(USER_ID2, <<"user2">>).
-define(GROUP_ID, <<"group1">>).
-define(GROUP_ID2, <<"group2">>).
-define(ADMIN_ID, <<"adminId">>).
-define(SPACE_ID, <<"space1">>).
-define(ACL_USER(N), <<"user", (integer_to_binary(N))/binary, "@nfs.domain.org">>).
-define(ACL_USER0, ?ACL_USER(0)).
-define(ACL_USER1, ?ACL_USER(1)).
-define(ACL_GROUP(N), <<"group", (integer_to_binary(N))/binary, "@nfs.domain.org">>).
-define(ACL_GROUP0, ?ACL_GROUP(0)).
-define(ACL_GROUP1, ?ACL_GROUP(1)).

%% default space owner
-define(SPACE_UID1, 1).
-define(SPACE_GID1, 1).

%% mocked UID and GID of space mountpoint
-define(SPACE_MOUNT_UID, 2).
-define(SPACE_MOUNT_GID, 2).

%% default space display owner
-define(SPACE_DISPLAY_UID1, 10).
-define(SPACE_DISPLAY_GID1, 10).

%% UID and display UID macros
-define(BASE_UID, 1000).
-define(UID(N), ?BASE_UID + N).
-define(UID0, ?UID(0)).
-define(UID1, ?UID(1)).
-define(DISPLAY_UID0, 1111).

-define(POSIX_ID_RANGE, {100000, 2000000}).
-define(POSIX_CREDS_TO_TUPLE(UserCtx), begin
    #{<<"uid">> := __UID, <<"gid">> := __GID} = UserCtx,
    {binary_to_integer(__UID),binary_to_integer(__GID)}
end).

-define(ROOT_DISPLAY_CREDS, {?ROOT_UID, ?ROOT_GID}).

-define(EXT_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    luma_test_utils:new_posix_user_ctx(?SPACE_DISPLAY_UID1, ?SPACE_DISPLAY_GID1)).
-define(DEFAULT_NO_LUMA_DISPLAY_CREDENTIALS, luma_test_utils:new_posix_user_ctx(
    luma_utils:generate_posix_identifier(?SPACE_OWNER_ID(?SPACE_ID), ?POSIX_ID_RANGE),
    luma_utils:generate_posix_identifier(?SPACE_ID, ?POSIX_ID_RANGE)
)).

-define(NO_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX, {
    luma_utils:generate_posix_identifier(?USER_ID, ?POSIX_ID_RANGE),
    luma_utils:generate_posix_identifier(?SPACE_ID, ?POSIX_ID_RANGE)
}).

-define(NO_LUMA_USER_DISPLAY_CREDENTIALS_POSIX, {
    luma_utils:generate_posix_identifier(?USER_ID, ?POSIX_ID_RANGE),
    ?SPACE_MOUNT_GID
}).

-define(EXT_LUMA_USER_DISPLAY_CREDS, {
    ?DISPLAY_UID0,
    ?SPACE_DISPLAY_GID1
}).

%%%===================================================================
%%% LUMA macros
%%%===================================================================

-define(LUMA_CONFIG, #luma_config{
    url = <<"http://127.0.0.1:5000">>,
    api_key = <<"test_api_key">>
}).
%%%===================================================================
%%% Posix helper and storage macros
%%%===================================================================

-define(POSIX_ADMIN_CREDENTIALS, luma_test_utils:new_posix_user_ctx(?ROOT_UID, ?ROOT_GID)).
-define(POSIX_USER_CREDENTIALS, luma_test_utils:new_posix_user_ctx(?UID0, ?SPACE_GID1)).
-define(POSIX_GENERATED_USER_CREDENTIALS, luma_test_utils:new_posix_user_ctx(?GEN_UID(?USER_ID), ?SPACE_MOUNT_GID)).
-define(POSIX_MOUNT_CREDENTIALS, luma_test_utils:new_posix_user_ctx(?SPACE_MOUNT_UID, ?SPACE_MOUNT_GID)).
-define(POSIX_EXT_LUMA_DEFAULT_CREDENTIALS, luma_test_utils:new_posix_user_ctx(?SPACE_UID1, ?SPACE_GID1)).

% Macros used to define posix compatible ownerships (UID, GID)
-define(GEN_UID(UserId), luma_utils:generate_posix_identifier(UserId, ?UID_RANGE)).

-define(UID_RANGE, {100000, 2000000}).

-define(POSIX_STORAGE_ID, <<"posixStorageId">>).
-define(POSIX_HELPER,
    ?STRIP_OK(helper:new_helper(?POSIX_HELPER_NAME,
    #{<<"mountPoint">> => <<"mountPoint">>}, ?POSIX_ADMIN_CREDENTIALS, false,
    ?CANONICAL_STORAGE_PATH))
).

-define(POSIX_STORAGE_DOC(LumaConfig),
    ?STORAGE_RECORD(?POSIX_STORAGE_ID, <<"POSIX">>, ?POSIX_HELPER, LumaConfig)).
-define(POSIX_STORAGE_DOC_EXT_LUMA, ?POSIX_STORAGE_DOC(?LUMA_CONFIG)).
-define(POSIX_STORAGE_DOC_NO_LUMA, ?POSIX_STORAGE_DOC(undefined)).

%%%===================================================================
%%% CEPH helper and storage macros
%%%===================================================================

-define(CEPH_ADMIN_CREDENTIALS, luma_test_utils:new_ceph_user_ctx(<<"ADMIN">>, <<"ADMIN_KEY">>)).
-define(CEPH_USER_CREDENTIALS, luma_test_utils:new_ceph_user_ctx(<<"USER">>, <<"USER_KEY">>)).

-define(CEPH_STORAGE_ID, <<"cephStorageId">>).

-define(CEPH_HELPER(Insecure), ?STRIP_OK(helper:new_helper(?CEPH_HELPER_NAME,
    #{<<"monitorHostname">> => <<"monitorHostname">>,
        <<"clusterName">> => <<"clusterName">>,
        <<"poolName">> => <<"poolName">>},
    ?CEPH_ADMIN_CREDENTIALS, Insecure, ?FLAT_STORAGE_PATH)
)).

-define(CEPH_STORAGE_DOC(Insecure, LumaConfig),
    ?STORAGE_RECORD(?CEPH_STORAGE_ID, <<"CEPH">>,
        ?CEPH_HELPER(Insecure), LumaConfig)
).
-define(CEPH_STORAGE_DOC_EXT_LUMA,
    ?CEPH_STORAGE_DOC(false, ?LUMA_CONFIG)).
-define(CEPH_STORAGE_DOC_NO_LUMA,
    ?CEPH_STORAGE_DOC(true, undefined)).

%%%===================================================================
%%% S3 helper and storage macros
%%%===================================================================

-define(S3_ADMIN_CREDENTIALS,
     luma_test_utils:new_s3_user_ctx(<<"ADMIN_ACCESS_KEY">>, <<"ADMIN_SECRET_KEY">>)).
-define(S3_USER_CREDENTIALS,
     luma_test_utils:new_s3_user_ctx(<<"USER_ACCESS_KEY">>, <<"USER_SECRET_KEY">>)).

-define(S3_STORAGE_ID, <<"s3StorageId">>).

-define(S3_HELPER(Insecure), ?STRIP_OK(helper:new_helper(?S3_HELPER_NAME,
    #{<<"scheme">> => <<"https">>, <<"hostname">> => <<"hostname">>,
        <<"bucketName">> => <<"bucketName">>},
    ?S3_ADMIN_CREDENTIALS, Insecure, ?FLAT_STORAGE_PATH)
)).

-define(S3_STORAGE_DOC(Insecure, LumaConfig),
    ?STORAGE_RECORD(?S3_STORAGE_ID, <<"S3">>, ?S3_HELPER(Insecure), LumaConfig)
).
-define(S3_STORAGE_DOC_EXT_LUMA,
    ?S3_STORAGE_DOC(false, ?LUMA_CONFIG)).
-define(S3_STORAGE_DOC_NO_LUMA,
    ?S3_STORAGE_DOC(true, undefined)).

%%%===================================================================
%%% SWIFT helper and storage macros
%%%===================================================================

-define(SWIFT_ADMIN_CREDENTIALS,
     luma_test_utils:new_swift_user_ctx(<<"ADMIN">>, <<"ADMIN_PASSWD">>)).
-define(SWIFT_USER_CREDENTIALS,
     luma_test_utils:new_swift_user_ctx(<<"USER">>, <<"USER_PASSWD">>)).

-define(SWIFT_STORAGE_ID, <<"swiftStorageId">>).

-define(SWIFT_HELPER(Insecure), ?STRIP_OK(helper:new_helper(?SWIFT_HELPER_NAME,
    #{<<"authUrl">> => <<"authUrl">>,
        <<"containerName">> => <<"containerName">>,
        <<"tenantName">> => <<"tenantName">>},
    ?SWIFT_ADMIN_CREDENTIALS, Insecure, ?FLAT_STORAGE_PATH)
)).

-define(SWIFT_STORAGE_DOC(Insecure, LumaConfig),
    ?STORAGE_RECORD(?SWIFT_STORAGE_ID, <<"SWIFT">>, ?SWIFT_HELPER(Insecure), LumaConfig)
).
-define(SWIFT_STORAGE_DOC_EXT_LUMA,
    ?SWIFT_STORAGE_DOC(false, ?LUMA_CONFIG)).
-define(SWIFT_STORAGE_DOC_NO_LUMA,
    ?SWIFT_STORAGE_DOC(true, undefined)).

%%%===================================================================
%%% CEPHRADOS helper and storage macros
%%%===================================================================

-define(CEPHRADOS_ADMIN_CREDENTIALS,
     luma_test_utils:new_cephrados_user_ctx(<<"ADMIN">>, <<"ADMIN_KEY">>)).
-define(CEPHRADOS_USER_CREDENTIALS,
     luma_test_utils:new_cephrados_user_ctx(<<"USER">>, <<"USER_KEY">>)).

-define(CEPHRADOS_STORAGE_ID, <<"cephradosStorageId">>).

-define(CEPHRADOS_HELPER(Insecure), ?STRIP_OK(helper:new_helper(?CEPHRADOS_HELPER_NAME,
    #{<<"monitorHostname">> => <<"monitorHostname">>,
        <<"clusterName">> => <<"clusterName">>,
        <<"poolName">> => <<"poolName">>},
    ?CEPHRADOS_ADMIN_CREDENTIALS, Insecure, ?FLAT_STORAGE_PATH)
)).

-define(CEPHRADOS_STORAGE_DOC(Insecure, LumaConfig),
    ?STORAGE_RECORD(?CEPHRADOS_STORAGE_ID, <<"CEPHRADOS">>,
        ?CEPHRADOS_HELPER(Insecure), LumaConfig)
).
-define(CEPHRADOS_STORAGE_DOC_EXT_LUMA,
    ?CEPHRADOS_STORAGE_DOC(false, ?LUMA_CONFIG)).
-define(CEPHRADOS_STORAGE_DOC_NO_LUMA,
    ?CEPHRADOS_STORAGE_DOC(true, undefined)).

%%%===================================================================
%%% GLUSTERFS helper and storage macros
%%%===================================================================

-define(GLUSTERFS_ADMIN_CREDENTIALS, luma_test_utils:new_glusterfs_user_ctx(0, 0)).
-define(GLUSTERFS_USER_CREDENTIALS, luma_test_utils:new_glusterfs_user_ctx(?UID0, ?SPACE_GID1)).
-define(GLUSTERFS_GENERATED_USER_CREDENTIALS, luma_test_utils:new_glusterfs_user_ctx(?GEN_UID(?USER_ID), ?SPACE_MOUNT_GID)).
-define(GLUSTERFS_MOUNT_CREDENTIALS, luma_test_utils:new_glusterfs_user_ctx(?SPACE_MOUNT_UID, ?SPACE_MOUNT_GID)).
-define(GLUSTERFS_EXT_LUMA_DEFAULT_CREDENTIALS, luma_test_utils:new_glusterfs_user_ctx(?SPACE_UID1, ?SPACE_GID1)).

-define(GLUSTERFS_STORAGE_ID, <<"glusterfsStorageId">>).

-define(GLUSTERFS_HELPER(Insecure), ?STRIP_OK(helper:new_helper(
    ?GLUSTERFS_HELPER_NAME,
    #{<<"volume">> => <<"volume">>, <<"hostname">> => <<"hostname">>},
    ?GLUSTERFS_ADMIN_CREDENTIALS, Insecure, ?FLAT_STORAGE_PATH)
)).

-define(GLUSTERFS_STORAGE_DOC(Insecure, LumaConfig),
    ?STORAGE_RECORD(?GLUSTERFS_STORAGE_ID, <<"GLUSTERFS">>,
        ?GLUSTERFS_HELPER(Insecure), LumaConfig)
).
-define(GLUSTERFS_STORAGE_DOC_EXT_LUMA,
    ?GLUSTERFS_STORAGE_DOC(false, ?LUMA_CONFIG)).
-define(GLUSTERFS_STORAGE_DOC_NO_LUMA,
    ?GLUSTERFS_STORAGE_DOC(true, undefined)).

%%%===================================================================
%%% NULLDEVICE helper and storage macros
%%%===================================================================

-define(NULLDEVICE_ADMIN_CREDENTIALS, luma_test_utils:new_nulldevice_user_ctx(0, 0)).
-define(NULLDEVICE_USER_CREDENTIALS, luma_test_utils:new_nulldevice_user_ctx(?UID0, ?SPACE_GID1)).
-define(NULLDEVICE_GENERATED_USER_CREDENTIALS, luma_test_utils:new_nulldevice_user_ctx(?GEN_UID(?USER_ID), ?SPACE_MOUNT_GID)).
-define(NULLDEVICE_MOUNT_CREDENTIALS, luma_test_utils:new_nulldevice_user_ctx(?SPACE_MOUNT_UID, ?SPACE_MOUNT_GID)).
-define(NULLDEVICE_EXT_LUMA_DEFAULT_CREDENTIALS, luma_test_utils:new_nulldevice_user_ctx(?SPACE_UID1, ?SPACE_GID1)).

-define(NULLDEVICE_STORAGE_ID, <<"nulldeviceStorageId">>).

-define(NULLDEVICE_HELPER(Insecure), ?STRIP_OK(helper:new_helper(
    ?NULL_DEVICE_HELPER_NAME, #{},
    ?NULLDEVICE_ADMIN_CREDENTIALS, Insecure, ?FLAT_STORAGE_PATH))).

-define(NULLDEVICE_STORAGE_DOC(Insecure, LumaConfig),
    ?STORAGE_RECORD(?NULLDEVICE_STORAGE_ID, <<"NULLDEVICE">>,
        ?NULLDEVICE_HELPER(Insecure), LumaConfig)
).
-define(NULLDEVICE_STORAGE_DOC_EXT_LUMA,
    ?NULLDEVICE_STORAGE_DOC(false, ?LUMA_CONFIG)).
-define(NULLDEVICE_STORAGE_DOC_NO_LUMA,
    ?NULLDEVICE_STORAGE_DOC(true, undefined)).


%%%===================================================================
%%% WEBDAV helper and storage macros
%%%===================================================================

-define(OD_ACCESS_TOKEN, <<"ONEDATA_ACCESS_TOKEN">>).
-define(OAUTH2_IDP, <<"OAUTH2_IDP">>).
-define(IDP_ADMIN_TOKEN, <<"IDP_ADMIN_TOKEN">>).
-define(IDP_USER_TOKEN, <<"IDP_USER_TOKEN">>).
-define(TTL, 3600).


-define(WEBDAV_BASIC_CREDENTIALS_TYPE, <<"basic">>).
-define(WEBDAV_BASIC_CREDENTIALS(Credentials),
     luma_test_utils:new_webdav_user_ctx(?WEBDAV_BASIC_CREDENTIALS_TYPE, Credentials)).
-define(WEBDAV_BASIC_ADMIN_CREDENTIALS, ?WEBDAV_BASIC_CREDENTIALS(<<"admin:password">>)).
-define(WEBDAV_BASIC_USER_CREDENTIALS, ?WEBDAV_BASIC_CREDENTIALS(<<"user:password">>)).

-define(WEBDAV_TOKEN_CREDENTIALS_TYPE, <<"token">>).
-define(WEBDAV_TOKEN_CTX(Credentials),
     luma_test_utils:new_webdav_user_ctx(?WEBDAV_TOKEN_CREDENTIALS_TYPE, Credentials)).
-define(WEBDAV_TOKEN_ADMIN_CREDENTIALS, ?WEBDAV_TOKEN_CTX(<<"ADMIN_TOKEN">>)).
-define(WEBDAV_TOKEN_USER_CREDENTIALS, ?WEBDAV_TOKEN_CTX(<<"USER_TOKEN">>)).

-define(WEBDAV_NONE_CREDENTIALS_TYPE, <<"none">>).
-define(WEBDAV_NONE_CTX,
     luma_test_utils:new_webdav_user_ctx(?WEBDAV_NONE_CREDENTIALS_TYPE, <<"">>)).

-define(WEBDAV_OAUTH2_CREDENTIALS_TYPE, <<"oauth2">>).

-define(WEBDAV_OAUTH2_ADMIN_CREDENTIALS,
    (luma_test_utils:new_webdav_user_ctx(
        ?WEBDAV_OAUTH2_CREDENTIALS_TYPE,
        <<"ADMIN_OAUTH2">>
    ))#{
        <<"onedataAccessToken">> => ?OD_ACCESS_TOKEN,
        <<"adminId">> => ?ADMIN_ID
    }
).
-define(EXPECTED_WEBDAV_OAUTH2_ADMIN_CREDENTIALS,
    (luma_test_utils:new_webdav_user_ctx(
        ?WEBDAV_OAUTH2_CREDENTIALS_TYPE,
        <<"ADMIN_OAUTH2">>
    ))#{
        <<"accessToken">> => ?IDP_ADMIN_TOKEN,
        <<"accessTokenTTL">> => integer_to_binary(?TTL),
        <<"adminId">> => ?ADMIN_ID
    }
).

-define(WEBDAV_OAUTH2_USER_CREDENTIALS,
    luma_test_utils:new_webdav_user_ctx(?WEBDAV_OAUTH2_CREDENTIALS_TYPE,
        <<"USER_OAUTH2">>)
    ).

-define(EXPECTED_WEBDAV_OAUTH2_USER_CREDENTIALS,
    (?WEBDAV_OAUTH2_USER_CREDENTIALS)#{
        <<"accessToken">> => ?IDP_USER_TOKEN,
        <<"accessTokenTTL">> => integer_to_binary(?TTL)
    }
).

-define(WEBDAV_NONE_STORAGE_ID, <<"webdavNoneStorageId">>).
-define(WEBDAV_BASIC_STORAGE_ID, <<"webdavBasicStorageId">>).
-define(WEBDAV_TOKEN_STORAGE_ID, <<"webdavTokenStorageId">>).
-define(WEBDAV_OAUTH2_STORAGE_ID, <<"webdavOauth2StorageId">>).


-define(WEBDAV_HELPER(Insecure, AdminCtx), ?STRIP_OK(helper:new_helper(
    ?WEBDAV_HELPER_NAME,
    #{<<"endpoint">> => <<"endpoint">>, <<"oauth2IdP">> => ?OAUTH2_IDP},
    AdminCtx, Insecure, ?FLAT_STORAGE_PATH)
)).
-define(WEBDAV_BASIC_HELPER(Insecure),
    ?WEBDAV_HELPER(Insecure, ?WEBDAV_BASIC_ADMIN_CREDENTIALS)).
-define(WEBDAV_TOKEN_HELPER(Insecure),
    ?WEBDAV_HELPER(Insecure, ?WEBDAV_TOKEN_ADMIN_CREDENTIALS)).
-define(WEBDAV_NONE_HELPER(Insecure),
    ?WEBDAV_HELPER(Insecure, ?WEBDAV_NONE_CTX)).
-define(WEBDAV_OAUTH2_HELPER(Insecure),
    ?WEBDAV_HELPER(Insecure, ?WEBDAV_OAUTH2_ADMIN_CREDENTIALS)).


-define(WEBDAV_STORAGE_DOC(StorageId, LumaConfig, Helper),
    ?STORAGE_RECORD(StorageId, <<"WEBDAV">>, Helper, LumaConfig)
).
-define(WEBDAV_BASIC_STORAGE_DOC_EXT_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_BASIC_STORAGE_ID, ?LUMA_CONFIG, ?WEBDAV_BASIC_HELPER(false))).
-define(WEBDAV_BASIC_STORAGE_DOC_NO_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_BASIC_STORAGE_ID, undefined, ?WEBDAV_BASIC_HELPER(true))).
-define(WEBDAV_TOKEN_STORAGE_DOC_EXT_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_TOKEN_STORAGE_ID, ?LUMA_CONFIG, ?WEBDAV_TOKEN_HELPER(false))).
-define(WEBDAV_TOKEN_STORAGE_DOC_NO_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_TOKEN_STORAGE_ID, undefined, ?WEBDAV_TOKEN_HELPER(true))).
-define(WEBDAV_NONE_STORAGE_DOC_EXT_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_NONE_STORAGE_ID, ?LUMA_CONFIG, ?WEBDAV_NONE_HELPER(false))).
-define(WEBDAV_NONE_STORAGE_DOC_NO_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_NONE_STORAGE_ID, undefined, ?WEBDAV_NONE_HELPER(true))).
-define(WEBDAV_OAUTH2_STORAGE_DOC_EXT_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_OAUTH2_STORAGE_ID, ?LUMA_CONFIG, ?WEBDAV_OAUTH2_HELPER(false))).
-define(WEBDAV_OAUTH2_STORAGE_DOC_NO_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_OAUTH2_STORAGE_ID, undefined, ?WEBDAV_OAUTH2_HELPER(true))).

%%%===================================================================
%%% storage macros
%%%===================================================================

-define(STORAGE_RECORD(Id, Name, Helper, LumaConfig), #document{
    key = Id,
    value = #storage_config{
        helper = Helper,
        readonly = false,
        imported_storage = false,
        luma_config = LumaConfig
    }
}).

-define(EXT_LUMA_POSIX_STORAGE_CONFIG, #{
    name => "POSIX storage with external LUMA",
    admin_credentials => ?POSIX_ADMIN_CREDENTIALS,
    user_credentials => ?POSIX_USER_CREDENTIALS,
    default_credentials => ?POSIX_EXT_LUMA_DEFAULT_CREDENTIALS,
    display_credentials => ?EXT_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_display_credentials => ?EXT_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?POSIX_STORAGE_DOC_EXT_LUMA
}).

-define(EXT_LUMA_CEPH_STORAGE_CONFIG, #{
    name => "CEPH storage with external LUMA",
    admin_credentials => ?CEPH_ADMIN_CREDENTIALS,
    display_credentials => ?EXT_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?CEPH_USER_CREDENTIALS,
    user_display_credentials => ?EXT_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?CEPH_STORAGE_DOC_EXT_LUMA
}).

-define(EXT_LUMA_S3_STORAGE_CONFIG, #{
    name => "S3 storage with external LUMA",
    admin_credentials => ?S3_ADMIN_CREDENTIALS,
    display_credentials => ?EXT_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?S3_USER_CREDENTIALS,
    user_display_credentials => ?EXT_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?S3_STORAGE_DOC_EXT_LUMA
}).

-define(EXT_LUMA_SWIFT_STORAGE_CONFIG, #{
    name => "SWIFT storage with external LUMA",
    admin_credentials => ?SWIFT_ADMIN_CREDENTIALS,
    display_credentials => ?EXT_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?SWIFT_USER_CREDENTIALS,
    user_display_credentials => ?EXT_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?SWIFT_STORAGE_DOC_EXT_LUMA
}).

-define(EXT_LUMA_CEPHRADOS_STORAGE_CONFIG, #{
    name => "CEPHRADOS storage with external LUMA",
    admin_credentials => ?CEPHRADOS_ADMIN_CREDENTIALS,
    display_credentials => ?EXT_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?CEPHRADOS_USER_CREDENTIALS,
    user_display_credentials => ?EXT_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?CEPHRADOS_STORAGE_DOC_EXT_LUMA
}).

-define(EXT_LUMA_GLUSTERFS_STORAGE_CONFIG, #{
    name => "GLUSTERFS storage with external LUMA",
    admin_credentials => ?GLUSTERFS_ADMIN_CREDENTIALS,
    default_credentials => ?GLUSTERFS_EXT_LUMA_DEFAULT_CREDENTIALS,
    display_credentials => ?EXT_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?GLUSTERFS_USER_CREDENTIALS,
    user_display_credentials => ?EXT_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?GLUSTERFS_STORAGE_DOC_EXT_LUMA
}).

-define(EXT_LUMA_NULLDEVICE_STORAGE_CONFIG, #{
    name => "NULLDEVICE storage with external LUMA",
    admin_credentials => ?NULLDEVICE_ADMIN_CREDENTIALS,
    default_credentials => ?NULLDEVICE_EXT_LUMA_DEFAULT_CREDENTIALS,
    display_credentials => ?EXT_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?NULLDEVICE_USER_CREDENTIALS,
    user_display_credentials => ?EXT_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?NULLDEVICE_STORAGE_DOC_EXT_LUMA
}).

-define(EXT_LUMA_WEBDAV_BASIC_STORAGE_CONFIG, #{
    name => "WEBDAV_BASIC storage with external LUMA",
    admin_credentials => ?WEBDAV_BASIC_ADMIN_CREDENTIALS,
    display_credentials => ?EXT_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?WEBDAV_BASIC_USER_CREDENTIALS,
    user_display_credentials => ?EXT_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?WEBDAV_BASIC_STORAGE_DOC_EXT_LUMA
}).

-define(EXT_LUMA_WEBDAV_TOKEN_STORAGE_CONFIG, #{
    name => "WEBDAV_TOKEN storage with external LUMA",
    admin_credentials => ?WEBDAV_TOKEN_ADMIN_CREDENTIALS,
    display_credentials => ?EXT_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?WEBDAV_TOKEN_USER_CREDENTIALS,
    user_display_credentials => ?EXT_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?WEBDAV_TOKEN_STORAGE_DOC_EXT_LUMA
}).

-define(EXT_LUMA_WEBDAV_NONE_STORAGE_CONFIG, #{
    name => "WEBDAV_NONE storage with external LUMA",
    admin_credentials => ?WEBDAV_NONE_CTX,
    display_credentials => ?EXT_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?WEBDAV_NONE_CTX,
    user_display_credentials => ?EXT_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?WEBDAV_NONE_STORAGE_DOC_EXT_LUMA
}).

-define(EXT_LUMA_WEBDAV_OAUTH2_STORAGE_CONFIG, #{
    name => "WEBDAV_OAUTH2 storage with external LUMA",
    admin_credentials => ?EXPECTED_WEBDAV_OAUTH2_ADMIN_CREDENTIALS,
    display_credentials => ?EXT_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?EXPECTED_WEBDAV_OAUTH2_USER_CREDENTIALS,
    user_display_credentials => ?EXT_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?WEBDAV_OAUTH2_STORAGE_DOC_EXT_LUMA
}).


-define(EXT_LUMA_STORAGE_CONFIGS, [
    ?EXT_LUMA_POSIX_STORAGE_CONFIG,
    ?EXT_LUMA_CEPH_STORAGE_CONFIG,
    ?EXT_LUMA_CEPHRADOS_STORAGE_CONFIG,
    ?EXT_LUMA_S3_STORAGE_CONFIG,
    ?EXT_LUMA_SWIFT_STORAGE_CONFIG,
    ?EXT_LUMA_GLUSTERFS_STORAGE_CONFIG,
    ?EXT_LUMA_NULLDEVICE_STORAGE_CONFIG,
    ?EXT_LUMA_WEBDAV_BASIC_STORAGE_CONFIG,
    ?EXT_LUMA_WEBDAV_TOKEN_STORAGE_CONFIG,
    ?EXT_LUMA_WEBDAV_NONE_STORAGE_CONFIG,
    ?EXT_LUMA_WEBDAV_OAUTH2_STORAGE_CONFIG
]).

-define(NO_LUMA_POSIX_STORAGE_CONFIG, #{
    name => "POSIX storage without LUMA",
    admin_credentials => ?POSIX_ADMIN_CREDENTIALS,
    default_credentials => ?POSIX_MOUNT_CREDENTIALS,
    user_credentials => ?POSIX_GENERATED_USER_CREDENTIALS,
    display_credentials => ?POSIX_MOUNT_CREDENTIALS,
    user_display_credentials => ?NO_LUMA_USER_DISPLAY_CREDENTIALS_POSIX,
    storage_record => ?POSIX_STORAGE_DOC_NO_LUMA
}).

-define(NO_LUMA_CEPH_STORAGE_CONFIG, #{
    name => "CEPH storage without LUMA",
    admin_credentials => ?CEPH_ADMIN_CREDENTIALS,
    user_credentials => ?CEPH_USER_CREDENTIALS,
    display_credentials => ?DEFAULT_NO_LUMA_DISPLAY_CREDENTIALS,
    user_display_credentials => ?NO_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?CEPH_STORAGE_DOC_NO_LUMA
}).

-define(NO_LUMA_S3_STORAGE_CONFIG, #{
    name => "S3 storage without LUMA",
    admin_credentials => ?S3_ADMIN_CREDENTIALS,
    user_credentials => ?S3_USER_CREDENTIALS,
    display_credentials => ?DEFAULT_NO_LUMA_DISPLAY_CREDENTIALS,
    user_display_credentials => ?NO_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?S3_STORAGE_DOC_NO_LUMA
}).

-define(NO_LUMA_SWIFT_STORAGE_CONFIG, #{
    name => "SWIFT storage without LUMA",
    admin_credentials => ?SWIFT_ADMIN_CREDENTIALS,
    user_credentials => ?SWIFT_USER_CREDENTIALS,
    display_credentials => ?DEFAULT_NO_LUMA_DISPLAY_CREDENTIALS,
    user_display_credentials => ?NO_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?SWIFT_STORAGE_DOC_NO_LUMA
}).

-define(NO_LUMA_CEPHRADOS_STORAGE_CONFIG, #{
    name => "CEPHRADOS storage without LUMA",
    admin_credentials => ?CEPHRADOS_ADMIN_CREDENTIALS,
    user_credentials => ?CEPHRADOS_USER_CREDENTIALS,
    display_credentials => ?DEFAULT_NO_LUMA_DISPLAY_CREDENTIALS,
    user_display_credentials => ?NO_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?CEPHRADOS_STORAGE_DOC_NO_LUMA
}).

-define(NO_LUMA_GLUSTERFS_STORAGE_CONFIG, #{
    name => "GLUSTERFS storage without LUMA",
    admin_credentials => ?GLUSTERFS_ADMIN_CREDENTIALS,
    user_credentials => ?GLUSTERFS_GENERATED_USER_CREDENTIALS,
    default_credentials => ?GLUSTERFS_MOUNT_CREDENTIALS,
    display_credentials => ?GLUSTERFS_MOUNT_CREDENTIALS,
    user_display_credentials => ?NO_LUMA_USER_DISPLAY_CREDENTIALS_POSIX,
    storage_record => ?GLUSTERFS_STORAGE_DOC_NO_LUMA
}).

-define(NO_LUMA_NULLDEVICE_STORAGE_CONFIG, #{
    name => "NULLDEVICE storage without LUMA",
    admin_credentials => ?NULLDEVICE_ADMIN_CREDENTIALS,
    user_credentials => ?NULLDEVICE_GENERATED_USER_CREDENTIALS,
    default_credentials => ?NULLDEVICE_MOUNT_CREDENTIALS,
    display_credentials => ?NULLDEVICE_MOUNT_CREDENTIALS,
    user_display_credentials => ?NO_LUMA_USER_DISPLAY_CREDENTIALS_POSIX,
    storage_record => ?NULLDEVICE_STORAGE_DOC_NO_LUMA
}).

-define(NO_LUMA_WEBDAV_BASIC_STORAGE_CONFIG, #{
    name => "WEBDAV_BASIC storage without LUMA",
    admin_credentials => ?WEBDAV_BASIC_ADMIN_CREDENTIALS,
    user_credentials => ?WEBDAV_BASIC_USER_CREDENTIALS,
    display_credentials => ?DEFAULT_NO_LUMA_DISPLAY_CREDENTIALS,
    user_display_credentials => ?NO_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?WEBDAV_BASIC_STORAGE_DOC_NO_LUMA
}).

-define(NO_LUMA_WEBDAV_TOKEN_STORAGE_CONFIG, #{
    name => "WEBDAV_TOKEN storage without LUMA",
    admin_credentials => ?WEBDAV_TOKEN_ADMIN_CREDENTIALS,
    user_credentials => ?WEBDAV_TOKEN_USER_CREDENTIALS,
    display_credentials => ?DEFAULT_NO_LUMA_DISPLAY_CREDENTIALS,
    user_display_credentials => ?NO_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?WEBDAV_TOKEN_STORAGE_DOC_NO_LUMA
}).

-define(NO_LUMA_WEBDAV_NONE_STORAGE_CONFIG, #{
    name => "WEBDAV_NONE storage without LUMA",
    admin_credentials => ?WEBDAV_NONE_CTX,
    user_credentials => ?WEBDAV_NONE_CTX,
    display_credentials => ?DEFAULT_NO_LUMA_DISPLAY_CREDENTIALS,
    user_display_credentials => ?NO_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?WEBDAV_NONE_STORAGE_DOC_NO_LUMA
}).

-define(NO_LUMA_WEBDAV_OAUTH2_STORAGE_CONFIG, #{
    name => "WEBDAV_OAUTH2 storage without LUMA",
    admin_credentials => ?EXPECTED_WEBDAV_OAUTH2_ADMIN_CREDENTIALS,
    user_credentials => ?EXPECTED_WEBDAV_OAUTH2_USER_CREDENTIALS,
    display_credentials => ?DEFAULT_NO_LUMA_DISPLAY_CREDENTIALS,
    user_display_credentials => ?NO_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?WEBDAV_OAUTH2_STORAGE_DOC_NO_LUMA
}).

-define(NO_LUMA_STORAGE_CONFIGS, [
    ?NO_LUMA_POSIX_STORAGE_CONFIG,
    ?NO_LUMA_CEPH_STORAGE_CONFIG,
    ?NO_LUMA_S3_STORAGE_CONFIG,
    ?NO_LUMA_SWIFT_STORAGE_CONFIG,
    ?NO_LUMA_CEPHRADOS_STORAGE_CONFIG,
    ?NO_LUMA_GLUSTERFS_STORAGE_CONFIG,
    ?NO_LUMA_NULLDEVICE_STORAGE_CONFIG,
    ?NO_LUMA_WEBDAV_BASIC_STORAGE_CONFIG,
    ?NO_LUMA_WEBDAV_TOKEN_STORAGE_CONFIG,
    ?NO_LUMA_WEBDAV_NONE_STORAGE_CONFIG,
    ?NO_LUMA_WEBDAV_OAUTH2_STORAGE_CONFIG
]).

-define(ALL_STORAGE_CONFIGS,
    (?EXT_LUMA_STORAGE_CONFIGS ++ ?NO_LUMA_STORAGE_CONFIGS)).

-define(NO_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS, [
    ?NO_LUMA_POSIX_STORAGE_CONFIG,
    ?NO_LUMA_NULLDEVICE_STORAGE_CONFIG,
    ?NO_LUMA_GLUSTERFS_STORAGE_CONFIG
]).

-define(EXT_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS, [
    ?EXT_LUMA_POSIX_STORAGE_CONFIG,
    ?EXT_LUMA_NULLDEVICE_STORAGE_CONFIG,
    ?EXT_LUMA_GLUSTERFS_STORAGE_CONFIG
]).

-define(EXT_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
    (?EXT_LUMA_STORAGE_CONFIGS -- ?EXT_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS)
).

-define(NO_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
    (?NO_LUMA_STORAGE_CONFIGS -- ?NO_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS)
).

-define(POSIX_COMPATIBLE_STORAGE_CONFIGS,
    (?NO_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS ++ ?EXT_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS)).

-define(POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
    (?ALL_STORAGE_CONFIGS -- ?POSIX_COMPATIBLE_STORAGE_CONFIGS)).


-endif.