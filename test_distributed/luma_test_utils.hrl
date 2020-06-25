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
-define(SPACE_MOUNT_UID, 100).
-define(SPACE_MOUNT_GID, 100).

%% default space display owner
-define(SPACE_DISPLAY_UID1, 10).
-define(SPACE_DISPLAY_GID1, 10).
-define(SPACE_DISPLAY_UID2, 20).
-define(SPACE_DISPLAY_GID2, 20).

%% UID and display UID macros
-define(BASE_UID, 1000).
-define(UID(N), ?BASE_UID + N).
-define(UID0, ?UID(0)).
-define(UID1, ?UID(1)).
-define(DISPLAY_UID0, 1111).
-define(DISPLAY_UID1, 2222).

-define(POSIX_ID_RANGE, {100000, 2000000}).
-define(POSIX_CREDS_TO_TUPLE(UserCtx), begin
    #{<<"uid">> := __UID, <<"gid">> := __GID} = UserCtx,
    {binary_to_integer(__UID),binary_to_integer(__GID)}
end).

-define(ROOT_DISPLAY_CREDS, {?ROOT_UID, ?ROOT_GID}).

-define(AUTO_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS, luma_test_utils:new_posix_user_ctx(
    luma_auto_feed:generate_posix_identifier(?SPACE_OWNER_ID(?SPACE_ID), ?POSIX_ID_RANGE),
    luma_auto_feed:generate_posix_identifier(?SPACE_ID, ?POSIX_ID_RANGE)
)).
-define(EXTERNAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    luma_test_utils:new_posix_user_ctx(?SPACE_DISPLAY_UID1, ?SPACE_DISPLAY_GID1)).
-define(LOCAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    luma_test_utils:new_posix_user_ctx(?SPACE_DISPLAY_UID2, ?SPACE_DISPLAY_GID2)).

-define(AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX, {
    luma_auto_feed:generate_posix_identifier(?USER_ID, ?POSIX_ID_RANGE),
    luma_auto_feed:generate_posix_identifier(?SPACE_ID, ?POSIX_ID_RANGE)
}).
-define(AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_POSIX, {
    luma_auto_feed:generate_posix_identifier(?USER_ID, ?POSIX_ID_RANGE),
    ?SPACE_MOUNT_GID
}).

-define(EXTERNAL_FEED_LUMA_USER_DISPLAY_CREDS, {
    ?DISPLAY_UID0,
    ?SPACE_DISPLAY_GID1
}).
-define(LOCAL_FEED_LUMA_USER_DISPLAY_CREDS, {
    ?DISPLAY_UID1,
    ?SPACE_DISPLAY_GID2
}).

%%%===================================================================
%%% LUMA macros
%%%===================================================================

-define(LUMA_CONFIG(LumaMode), luma_config:new(LumaMode)).

%%%===================================================================
%%% Posix helper and storage macros
%%%===================================================================

-define(POSIX_ADMIN_CREDENTIALS, luma_test_utils:new_posix_user_ctx(?ROOT_UID, ?ROOT_GID)).
-define(POSIX_USER_CREDENTIALS, luma_test_utils:new_posix_user_ctx(?UID0, ?SPACE_GID1)).
-define(IMPORTED_POSIX_USER_CREDENTIALS, luma_test_utils:new_posix_user_ctx(?UID0, ?SPACE_MOUNT_GID)).
-define(POSIX_GENERATED_USER_CREDENTIALS, luma_test_utils:new_posix_user_ctx(?GEN_UID(?USER_ID), ?SPACE_MOUNT_GID)).
-define(POSIX_MOUNT_CREDENTIALS, luma_test_utils:new_posix_user_ctx(?SPACE_MOUNT_UID, ?SPACE_MOUNT_GID)).
-define(POSIX_EXTERNAL_FEED_LUMA_DEFAULT_CREDENTIALS, luma_test_utils:new_posix_user_ctx(?SPACE_UID1, ?SPACE_GID1)).
-define(POSIX_LOCAL_FEED_LUMA_DEFAULT_CREDENTIALS, luma_test_utils:new_posix_user_ctx(?SPACE_UID1, ?SPACE_GID1)).

% Macros used to define posix compatible ownerships (UID, GID)
-define(GEN_UID(UserId), luma_auto_feed:generate_posix_identifier(UserId, ?UID_RANGE)).

-define(UID_RANGE, {100000, 2000000}).

-define(POSIX_STORAGE_ID_AUTO_FEED_LUMA, <<"posixStorageIdAutoFeedLuma">>).
-define(POSIX_STORAGE_ID_EXTERNAL_FEED_LUMA, <<"posixStorageIdExternalFeedLuma">>).
-define(POSIX_STORAGE_ID_LOCAL_FEED_LUMA, <<"posixStorageIdLocalFeedLuma">>).
-define(POSIX_IMPORTED_STORAGE_ID_AUTO_FEED_LUMA, <<"posixImportedStorageIdAutoFeedLuma">>).
-define(POSIX_IMPORTED_STORAGE_ID_EXTERNAL_FEED_LUMA, <<"posixImportedStorageIdExternalFeedLuma">>).
-define(POSIX_IMPORTED_STORAGE_ID_LOCAL_FEED_LUMA, <<"posixImportedStorageIdLocalFeedLuma">>).

-define(POSIX_HELPER, ?STRIP_OK(helper:new_helper(
        ?POSIX_HELPER_NAME,
        #{
            <<"mountPoint">> => <<"mountPoint">>,
            <<"storagePathType">> => ?CANONICAL_STORAGE_PATH,
            <<"skipStorageDetection">> => <<"false">>
        },
        ?POSIX_ADMIN_CREDENTIALS
))).

-define(POSIX_STORAGE_DOC(Id, LumaMode),
    ?STORAGE_RECORD(Id, <<"POSIX">>, ?POSIX_HELPER, LumaMode)).
-define(POSIX_STORAGE_DOC_AUTO_FEED_LUMA, ?POSIX_STORAGE_DOC(?POSIX_STORAGE_ID_AUTO_FEED_LUMA, ?AUTO_FEED)).
-define(POSIX_STORAGE_DOC_EXTERNAL_FEED_LUMA, ?POSIX_STORAGE_DOC(?POSIX_STORAGE_ID_EXTERNAL_FEED_LUMA, ?EXTERNAL_FEED)).
-define(POSIX_STORAGE_DOC_LOCAL_FEED_LUMA, ?POSIX_STORAGE_DOC(?POSIX_STORAGE_ID_LOCAL_FEED_LUMA, ?LOCAL_FEED)).

-define(POSIX_IMPORTED_STORAGE_DOC_AUTO_FEED_LUMA,
    ?POSIX_STORAGE_DOC(?POSIX_IMPORTED_STORAGE_ID_AUTO_FEED_LUMA, ?AUTO_FEED)).
-define(POSIX_IMPORTED_STORAGE_DOC_EXTERNAL_FEED_LUMA, 
    ?POSIX_STORAGE_DOC(?POSIX_IMPORTED_STORAGE_ID_EXTERNAL_FEED_LUMA, ?EXTERNAL_FEED)).
-define(POSIX_IMPORTED_STORAGE_DOC_LOCAL_FEED_LUMA, 
    ?POSIX_STORAGE_DOC(?POSIX_IMPORTED_STORAGE_ID_LOCAL_FEED_LUMA, ?LOCAL_FEED)).

%%%===================================================================
%%% CEPH helper and storage macros
%%%===================================================================

-define(CEPH_ADMIN_CREDENTIALS, luma_test_utils:new_ceph_user_ctx(<<"ADMIN">>, <<"ADMIN_KEY">>)).
-define(CEPH_USER_CREDENTIALS, luma_test_utils:new_ceph_user_ctx(<<"USER">>, <<"USER_KEY">>)).

-define(CEPH_STORAGE_ID_AUTO_FEED_LUMA, <<"cephStorageIdAutoFeedLuma">>).
-define(CEPH_STORAGE_ID_EXTERNAL_FEED_LUMA, <<"cephStorageIdExternalFeedLuma">>).
-define(CEPH_STORAGE_ID_LOCAL_FEED_LUMA, <<"cephStorageIdLocalFeedLuma">>).

-define(CEPH_HELPER, ?STRIP_OK(helper:new_helper(?CEPH_HELPER_NAME,
    #{
        <<"monitorHostname">> => <<"monitorHostname">>,
        <<"clusterName">> => <<"clusterName">>,
        <<"poolName">> => <<"poolName">>,
        <<"storagePathType">> => ?FLAT_STORAGE_PATH,
        <<"skipStorageDetection">> => <<"false">>
    },
    ?CEPH_ADMIN_CREDENTIALS
))).

-define(CEPH_STORAGE_DOC(Id, LumaMode),
    ?STORAGE_RECORD(Id, <<"CEPH">>, ?CEPH_HELPER, LumaMode)
).
-define(CEPH_STORAGE_DOC_AUTO_FEED_LUMA,
    ?CEPH_STORAGE_DOC(?CEPH_STORAGE_ID_AUTO_FEED_LUMA, ?AUTO_FEED)).
-define(CEPH_STORAGE_DOC_EXTERNAL_FEED_LUMA,
    ?CEPH_STORAGE_DOC(?CEPH_STORAGE_ID_EXTERNAL_FEED_LUMA, ?EXTERNAL_FEED)).
-define(CEPH_STORAGE_DOC_LOCAL_FEED_LUMA,
    ?CEPH_STORAGE_DOC(?CEPH_STORAGE_ID_LOCAL_FEED_LUMA, ?LOCAL_FEED)).

%%%===================================================================
%%% S3 helper and storage macros
%%%===================================================================

-define(S3_ADMIN_CREDENTIALS,
     luma_test_utils:new_s3_user_ctx(<<"ADMIN_ACCESS_KEY">>, <<"ADMIN_SECRET_KEY">>)).
-define(S3_USER_CREDENTIALS,
     luma_test_utils:new_s3_user_ctx(<<"USER_ACCESS_KEY">>, <<"USER_SECRET_KEY">>)).

-define(S3_STORAGE_ID_AUTO_FEED_LUMA, <<"s3StorageIdAutoFeedLuma">>).
-define(S3_STORAGE_ID_EXTERNAL_FEED_LUMA, <<"s3StorageIdExternalFeedLuma">>).
-define(S3_STORAGE_ID_LOCAL_FEED_LUMA, <<"s3StorageIdLocalFeedLuma">>).

-define(S3_HELPER, ?STRIP_OK(helper:new_helper(?S3_HELPER_NAME,
    #{
        <<"scheme">> => <<"https">>,
        <<"hostname">> => <<"hostname">>,
        <<"bucketName">> => <<"bucketName">>,
        <<"storagePathType">> => ?FLAT_STORAGE_PATH,
        <<"skipStorageDetection">> => <<"false">>
    },
    ?S3_ADMIN_CREDENTIALS
))).

-define(S3_STORAGE_DOC(Id, LumaMode),
    ?STORAGE_RECORD(Id, <<"S3">>, ?S3_HELPER, LumaMode)
).
-define(S3_STORAGE_DOC_AUTO_FEED_LUMA,
    ?S3_STORAGE_DOC(?S3_STORAGE_ID_AUTO_FEED_LUMA, ?AUTO_FEED)).
-define(S3_STORAGE_DOC_EXTERNAL_FEED_LUMA,
    ?S3_STORAGE_DOC(?S3_STORAGE_ID_EXTERNAL_FEED_LUMA, ?EXTERNAL_FEED)).
-define(S3_STORAGE_DOC_LOCAL_FEED_LUMA,
    ?S3_STORAGE_DOC(?S3_STORAGE_ID_LOCAL_FEED_LUMA, ?LOCAL_FEED)).

%%%===================================================================
%%% SWIFT helper and storage macros
%%%===================================================================

-define(SWIFT_ADMIN_CREDENTIALS,
     luma_test_utils:new_swift_user_ctx(<<"ADMIN">>, <<"ADMIN_PASSWD">>)).
-define(SWIFT_USER_CREDENTIALS,
     luma_test_utils:new_swift_user_ctx(<<"USER">>, <<"USER_PASSWD">>)).

-define(SWIFT_STORAGE_ID_AUTO_FEED_LUMA, <<"swiftStorageIdAutoFeedLuma">>).
-define(SWIFT_STORAGE_ID_EXTERNAL_FEED_LUMA, <<"swiftStorageIdExternalFeedLuma">>).
-define(SWIFT_STORAGE_ID_LOCAL_FEED_LUMA, <<"swiftStorageIdLocalFeedLuma">>).

-define(SWIFT_HELPER, ?STRIP_OK(helper:new_helper(?SWIFT_HELPER_NAME,
    #{<<"authUrl">> => <<"authUrl">>,
        <<"containerName">> => <<"containerName">>,
        <<"tenantName">> => <<"tenantName">>,
        <<"storagePathType">> => ?FLAT_STORAGE_PATH,
        <<"skipStorageDetection">> => <<"false">>
    },
    ?SWIFT_ADMIN_CREDENTIALS
))).

-define(SWIFT_STORAGE_DOC(Id, LumaMode),
    ?STORAGE_RECORD(Id, <<"SWIFT">>, ?SWIFT_HELPER, LumaMode)
).
-define(SWIFT_STORAGE_DOC_AUTO_FEED_LUMA,
    ?SWIFT_STORAGE_DOC(?SWIFT_STORAGE_ID_AUTO_FEED_LUMA, ?AUTO_FEED)).
-define(SWIFT_STORAGE_DOC_EXTERNAL_FEED_LUMA,
    ?SWIFT_STORAGE_DOC(?SWIFT_STORAGE_ID_EXTERNAL_FEED_LUMA, ?EXTERNAL_FEED)).
-define(SWIFT_STORAGE_DOC_LOCAL_FEED_LUMA,
    ?SWIFT_STORAGE_DOC(?SWIFT_STORAGE_ID_LOCAL_FEED_LUMA, ?LOCAL_FEED)).

%%%===================================================================
%%% CEPHRADOS helper and storage macros
%%%===================================================================

-define(CEPHRADOS_ADMIN_CREDENTIALS,
     luma_test_utils:new_cephrados_user_ctx(<<"ADMIN">>, <<"ADMIN_KEY">>)).
-define(CEPHRADOS_USER_CREDENTIALS,
     luma_test_utils:new_cephrados_user_ctx(<<"USER">>, <<"USER_KEY">>)).

-define(CEPHRADOS_STORAGE_ID_AUTO_FEED_LUMA, <<"cephradosStorageIdAutoFeedLuma">>).
-define(CEPHRADOS_STORAGE_ID_EXTERNAL_FEED_LUMA, <<"cephradosStorageIdExternalFeedLuma">>).
-define(CEPHRADOS_STORAGE_ID_LOCAL_FEED_LUMA, <<"cephradosStorageIdLocalFeedLuma">>).

-define(CEPHRADOS_HELPER, ?STRIP_OK(helper:new_helper(?CEPHRADOS_HELPER_NAME,
    #{
        <<"monitorHostname">> => <<"monitorHostname">>,
        <<"clusterName">> => <<"clusterName">>,
        <<"poolName">> => <<"poolName">>,
        <<"storagePathType">> => ?FLAT_STORAGE_PATH,
        <<"skipStorageDetection">> => <<"false">>
    },
    ?CEPHRADOS_ADMIN_CREDENTIALS
))).

-define(CEPHRADOS_STORAGE_DOC(Id, LumaMode),
    ?STORAGE_RECORD(Id, <<"CEPHRADOS">>, ?CEPHRADOS_HELPER, LumaMode)
).
-define(CEPHRADOS_STORAGE_DOC_AUTO_FEED_LUMA,
    ?CEPHRADOS_STORAGE_DOC(?CEPHRADOS_STORAGE_ID_AUTO_FEED_LUMA, ?AUTO_FEED)).
-define(CEPHRADOS_STORAGE_DOC_EXTERNAL_FEED_LUMA,
    ?CEPHRADOS_STORAGE_DOC(?CEPHRADOS_STORAGE_ID_EXTERNAL_FEED_LUMA, ?EXTERNAL_FEED)).
-define(CEPHRADOS_STORAGE_DOC_LOCAL_FEED_LUMA,
    ?CEPHRADOS_STORAGE_DOC(?CEPHRADOS_STORAGE_ID_LOCAL_FEED_LUMA, ?LOCAL_FEED)).

%%%===================================================================
%%% GLUSTERFS helper and storage macros
%%%===================================================================

-define(GLUSTERFS_ADMIN_CREDENTIALS, luma_test_utils:new_glusterfs_user_ctx(0, 0)).
-define(GLUSTERFS_USER_CREDENTIALS, luma_test_utils:new_glusterfs_user_ctx(?UID0, ?SPACE_GID1)).
-define(IMPORTED_GLUSTERFS_USER_CREDENTIALS, luma_test_utils:new_glusterfs_user_ctx(?UID0, ?SPACE_MOUNT_GID)).
-define(GLUSTERFS_GENERATED_USER_CREDENTIALS, luma_test_utils:new_glusterfs_user_ctx(?GEN_UID(?USER_ID), ?SPACE_MOUNT_GID)).
-define(GLUSTERFS_MOUNT_CREDENTIALS, luma_test_utils:new_glusterfs_user_ctx(?SPACE_MOUNT_UID, ?SPACE_MOUNT_GID)).
-define(GLUSTERFS_EXTERNAL_FEED_LUMA_DEFAULT_CREDENTIALS, luma_test_utils:new_glusterfs_user_ctx(?SPACE_UID1, ?SPACE_GID1)).
-define(GLUSTERFS_LOCAL_FEED_LUMA_DEFAULT_CREDENTIALS, luma_test_utils:new_glusterfs_user_ctx(?SPACE_UID1, ?SPACE_GID1)).


-define(GLUSTERFS_STORAGE_ID_AUTO_FEED_LUMA, <<"glusterfsStorageIdAutoFeedLuma">>).
-define(GLUSTERFS_STORAGE_ID_EXTERNAL_FEED_LUMA, <<"glusterfsStorageIdExternalFeedLuma">>).
-define(GLUSTERFS_STORAGE_ID_LOCAL_FEED_LUMA, <<"glusterfsStorageIdLocalFeedLuma">>).
-define(IMPORTED_GLUSTERFS_STORAGE_ID_AUTO_FEED_LUMA, <<"glusterfsImportedStorageIdAutoFeedLuma">>).
-define(IMPORTED_GLUSTERFS_STORAGE_ID_EXTERNAL_FEED_LUMA, <<"glusterfsImportedStorageIdExternalFeedLuma">>).
-define(IMPORTED_GLUSTERFS_STORAGE_ID_LOCAL_FEED_LUMA, <<"glusterfsImportedStorageIdLocalFeedLuma">>).

-define(GLUSTERFS_HELPER, ?STRIP_OK(helper:new_helper(
    ?GLUSTERFS_HELPER_NAME,
    #{
        <<"volume">> => <<"volume">>,
        <<"hostname">> => <<"hostname">>,
        <<"storagePathType">> => ?CANONICAL_STORAGE_PATH,
        <<"skipStorageDetection">> => <<"false">>
    },
    ?GLUSTERFS_ADMIN_CREDENTIALS
))).

-define(GLUSTERFS_STORAGE_DOC(Id, LumaMode),
    ?STORAGE_RECORD(Id, <<"GLUSTERFS">>, ?GLUSTERFS_HELPER, LumaMode)
).

-define(GLUSTERFS_STORAGE_DOC_AUTO_FEED_LUMA,
    ?GLUSTERFS_STORAGE_DOC(?GLUSTERFS_STORAGE_ID_AUTO_FEED_LUMA, ?AUTO_FEED)).
-define(GLUSTERFS_STORAGE_DOC_EXTERNAL_FEED_LUMA,
    ?GLUSTERFS_STORAGE_DOC(?GLUSTERFS_STORAGE_ID_EXTERNAL_FEED_LUMA, ?EXTERNAL_FEED)).
-define(GLUSTERFS_STORAGE_DOC_LOCAL_FEED_LUMA,
    ?GLUSTERFS_STORAGE_DOC(?GLUSTERFS_STORAGE_ID_LOCAL_FEED_LUMA, ?LOCAL_FEED)).
-define(IMPORTED_GLUSTERFS_STORAGE_DOC_AUTO_FEED_LUMA,
    ?GLUSTERFS_STORAGE_DOC(?IMPORTED_GLUSTERFS_STORAGE_ID_AUTO_FEED_LUMA, ?AUTO_FEED)).
-define(IMPORTED_GLUSTERFS_STORAGE_DOC_EXTERNAL_FEED_LUMA,
    ?GLUSTERFS_STORAGE_DOC(?IMPORTED_GLUSTERFS_STORAGE_ID_EXTERNAL_FEED_LUMA, ?EXTERNAL_FEED)).
-define(IMPORTED_GLUSTERFS_STORAGE_DOC_LOCAL_FEED_LUMA,
    ?GLUSTERFS_STORAGE_DOC(?IMPORTED_GLUSTERFS_STORAGE_ID_LOCAL_FEED_LUMA, ?LOCAL_FEED)).

%%%===================================================================
%%% NULLDEVICE helper and storage macros
%%%===================================================================

-define(NULLDEVICE_ADMIN_CREDENTIALS, luma_test_utils:new_nulldevice_user_ctx(0, 0)).
-define(NULLDEVICE_USER_CREDENTIALS, luma_test_utils:new_nulldevice_user_ctx(?UID0, ?SPACE_GID1)).
-define(IMPORTED_NULLDEVICE_USER_CREDENTIALS, luma_test_utils:new_nulldevice_user_ctx(?UID0, ?SPACE_MOUNT_GID)).
-define(NULLDEVICE_GENERATED_USER_CREDENTIALS, luma_test_utils:new_nulldevice_user_ctx(?GEN_UID(?USER_ID), ?SPACE_MOUNT_GID)).
-define(NULLDEVICE_MOUNT_CREDENTIALS, luma_test_utils:new_nulldevice_user_ctx(?SPACE_MOUNT_UID, ?SPACE_MOUNT_GID)).
-define(NULLDEVICE_EXTERNAL_FEED_LUMA_DEFAULT_CREDENTIALS, luma_test_utils:new_nulldevice_user_ctx(?SPACE_UID1, ?SPACE_GID1)).
-define(NULLDEVICE_LOCAL_FEED_LUMA_DEFAULT_CREDENTIALS, luma_test_utils:new_nulldevice_user_ctx(?SPACE_UID1, ?SPACE_GID1)).

-define(NULLDEVICE_STORAGE_ID_AUTO_FEED_LUMA, <<"nulldeviceStorageIdAutoFeedLuma">>).
-define(NULLDEVICE_STORAGE_ID_EXTERNAL_FEED_LUMA, <<"nulldeviceStorageIdExternalFeedLuma">>).
-define(NULLDEVICE_STORAGE_ID_LOCAL_FEED_LUMA, <<"nulldeviceStorageIdLocalFeedLuma">>).
-define(IMPORTED_NULLDEVICE_STORAGE_ID_AUTO_FEED_LUMA, <<"nulldeviceImportedStorageIdAutoFeedLuma">>).
-define(IMPORTED_NULLDEVICE_STORAGE_ID_EXTERNAL_FEED_LUMA, <<"nulldeviceImportedStorageIdExternalFeedLuma">>).
-define(IMPORTED_NULLDEVICE_STORAGE_ID_LOCAL_FEED_LUMA, <<"nulldeviceImportedStorageIdLocalFeedLuma">>).

-define(NULLDEVICE_HELPER, ?STRIP_OK(helper:new_helper(
    ?NULL_DEVICE_HELPER_NAME, #{
        <<"storagePathType">> => ?CANONICAL_STORAGE_PATH,
        <<"skipStorageDetection">> => <<"false">>
    },
    ?NULLDEVICE_ADMIN_CREDENTIALS
))).

-define(NULLDEVICE_STORAGE_DOC(Id, LumaMode),
    ?STORAGE_RECORD(Id, <<"NULLDEVICE">>, ?NULLDEVICE_HELPER, LumaMode)
).

-define(NULLDEVICE_STORAGE_DOC_AUTO_FEED_LUMA,
    ?NULLDEVICE_STORAGE_DOC(?NULLDEVICE_STORAGE_ID_AUTO_FEED_LUMA, ?AUTO_FEED)).
-define(NULLDEVICE_STORAGE_DOC_EXTERNAL_FEED_LUMA,
    ?NULLDEVICE_STORAGE_DOC(?NULLDEVICE_STORAGE_ID_EXTERNAL_FEED_LUMA, ?EXTERNAL_FEED)).
-define(NULLDEVICE_STORAGE_DOC_LOCAL_FEED_LUMA,
    ?NULLDEVICE_STORAGE_DOC(?NULLDEVICE_STORAGE_ID_LOCAL_FEED_LUMA, ?LOCAL_FEED)).
-define(IMPORTED_NULLDEVICE_STORAGE_DOC_AUTO_FEED_LUMA,
    ?NULLDEVICE_STORAGE_DOC(?IMPORTED_NULLDEVICE_STORAGE_ID_AUTO_FEED_LUMA, ?AUTO_FEED)).
-define(IMPORTED_NULLDEVICE_STORAGE_DOC_EXTERNAL_FEED_LUMA,
    ?NULLDEVICE_STORAGE_DOC(?IMPORTED_NULLDEVICE_STORAGE_ID_EXTERNAL_FEED_LUMA, ?EXTERNAL_FEED)).
-define(IMPORTED_NULLDEVICE_STORAGE_DOC_LOCAL_FEED_LUMA,
    ?NULLDEVICE_STORAGE_DOC(?IMPORTED_NULLDEVICE_STORAGE_ID_LOCAL_FEED_LUMA, ?LOCAL_FEED)).

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

-define(WEBDAV_NONE_STORAGE_ID_AUTO_FEED_LUMA, <<"webdavNoneStorageIdAutoFeedLuma">>).
-define(WEBDAV_NONE_STORAGE_ID_EXTERNAL_FEED_LUMA, <<"webdavNoneStorageIdExternalFeedLuma">>).
-define(WEBDAV_NONE_STORAGE_ID_LOCAL_FEED_LUMA, <<"webdavNoneStorageIdLocalFeedLuma">>).
-define(WEBDAV_BASIC_STORAGE_ID_AUTO_FEED_LUMA, <<"webdavBasicStorageIdAutoFeedLuma">>).
-define(WEBDAV_BASIC_STORAGE_ID_EXTERNAL_FEED_LUMA, <<"webdavBasicStorageIdExternalFeedLuma">>).
-define(WEBDAV_BASIC_STORAGE_ID_LOCAL_FEED_LUMA, <<"webdavBasicStorageIdLocalFeedLuma">>).
-define(WEBDAV_TOKEN_STORAGE_ID_AUTO_FEED_LUMA, <<"webdavTokenStorageIdAutoFeedLuma">>).
-define(WEBDAV_TOKEN_STORAGE_ID_EXTERNAL_FEED_LUMA, <<"webdavTokenStorageIdExternalFeedLuma">>).
-define(WEBDAV_TOKEN_STORAGE_ID_LOCAL_FEED_LUMA, <<"webdavTokenStorageIdLocalFeedLuma">>).
-define(WEBDAV_OAUTH2_STORAGE_ID_AUTO_FEED_LUMA, <<"webdavOauth2StorageIdAutoFeedLuma">>).
-define(WEBDAV_OAUTH2_STORAGE_ID_EXTERNAL_FEED_LUMA, <<"webdavOauth2StorageIdExternalFeedLuma">>).
-define(WEBDAV_OAUTH2_STORAGE_ID_LOCAL_FEED_LUMA, <<"webdavOauth2StorageIdLocalFeedLuma">>).


-define(WEBDAV_HELPER(AdminCtx), ?STRIP_OK(helper:new_helper(
    ?WEBDAV_HELPER_NAME,
    #{
        <<"endpoint">> => <<"endpoint">>,
        <<"oauth2IdP">> => ?OAUTH2_IDP,
        <<"storagePathType">> => ?FLAT_STORAGE_PATH,
        <<"skipStorageDetection">> => <<"false">>
    },
    AdminCtx
))).
-define(WEBDAV_BASIC_HELPER,
    ?WEBDAV_HELPER(?WEBDAV_BASIC_ADMIN_CREDENTIALS)).
-define(WEBDAV_TOKEN_HELPER,
    ?WEBDAV_HELPER(?WEBDAV_TOKEN_ADMIN_CREDENTIALS)).
-define(WEBDAV_NONE_HELPER,
    ?WEBDAV_HELPER(?WEBDAV_NONE_CTX)).
-define(WEBDAV_OAUTH2_HELPER,
    ?WEBDAV_HELPER(?WEBDAV_OAUTH2_ADMIN_CREDENTIALS)).

-define(WEBDAV_STORAGE_DOC(StorageId, LumaMode, Helper),
    ?STORAGE_RECORD(StorageId, <<"WEBDAV">>, Helper, LumaMode)
).

-define(WEBDAV_NONE_STORAGE_DOC_AUTO_FEED_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_NONE_STORAGE_ID_AUTO_FEED_LUMA, ?AUTO_FEED,?WEBDAV_NONE_HELPER)).
-define(WEBDAV_NONE_STORAGE_DOC_EXTERNAL_FEED_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_NONE_STORAGE_ID_EXTERNAL_FEED_LUMA, ?EXTERNAL_FEED,?WEBDAV_NONE_HELPER)).
-define(WEBDAV_NONE_STORAGE_DOC_LOCAL_FEED_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_NONE_STORAGE_ID_LOCAL_FEED_LUMA, ?LOCAL_FEED,?WEBDAV_NONE_HELPER)).

-define(WEBDAV_BASIC_STORAGE_DOC_AUTO_FEED_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_BASIC_STORAGE_ID_AUTO_FEED_LUMA, ?AUTO_FEED,?WEBDAV_BASIC_HELPER)).
-define(WEBDAV_BASIC_STORAGE_DOC_EXTERNAL_FEED_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_BASIC_STORAGE_ID_EXTERNAL_FEED_LUMA, ?EXTERNAL_FEED,?WEBDAV_BASIC_HELPER)).
-define(WEBDAV_BASIC_STORAGE_DOC_LOCAL_FEED_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_BASIC_STORAGE_ID_LOCAL_FEED_LUMA, ?LOCAL_FEED,?WEBDAV_BASIC_HELPER)).

-define(WEBDAV_TOKEN_STORAGE_DOC_AUTO_FEED_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_TOKEN_STORAGE_ID_AUTO_FEED_LUMA, ?AUTO_FEED,?WEBDAV_TOKEN_HELPER)).
-define(WEBDAV_TOKEN_STORAGE_DOC_EXTERNAL_FEED_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_TOKEN_STORAGE_ID_EXTERNAL_FEED_LUMA, ?EXTERNAL_FEED,?WEBDAV_TOKEN_HELPER)).
-define(WEBDAV_TOKEN_STORAGE_DOC_LOCAL_FEED_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_TOKEN_STORAGE_ID_LOCAL_FEED_LUMA, ?LOCAL_FEED,?WEBDAV_TOKEN_HELPER)).


-define(WEBDAV_OAUTH2_STORAGE_DOC_AUTO_FEED_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_OAUTH2_STORAGE_ID_AUTO_FEED_LUMA, ?AUTO_FEED,?WEBDAV_OAUTH2_HELPER)).
-define(WEBDAV_OAUTH2_STORAGE_DOC_EXTERNAL_FEED_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_OAUTH2_STORAGE_ID_EXTERNAL_FEED_LUMA, ?EXTERNAL_FEED,?WEBDAV_OAUTH2_HELPER)).
-define(WEBDAV_OAUTH2_STORAGE_DOC_LOCAL_FEED_LUMA,
    ?WEBDAV_STORAGE_DOC(?WEBDAV_OAUTH2_STORAGE_ID_LOCAL_FEED_LUMA, ?LOCAL_FEED,?WEBDAV_OAUTH2_HELPER)).

%%%===================================================================
%%% storage test configs
%%%===================================================================

-define(STORAGE_RECORD(Id, Name, Helper, LumaMode), #document{
    key = Id,
    value = #storage_config{
        helper = Helper,
        imported_storage = false,
        luma_config = ?LUMA_CONFIG(LumaMode)
    }
}).

-define(ALL_STORAGE_CONFIGS, (
    ?USER_DEFINED_LUMA_STORAGE_CONFIGS ++
    ?AUTO_FEED_LUMA_STORAGE_CONFIGS
)).

-define(USER_DEFINED_LUMA_STORAGE_CONFIGS,(
    ?EXTERNAL_FEED_LUMA_STORAGE_CONFIGS ++
    ?LOCAL_FEED_LUMA_STORAGE_CONFIGS
)).

-define(POSIX_COMPATIBLE_STORAGE_CONFIGS, (
    ?AUTO_FEED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS ++
    ?EXTERNAL_FEED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS ++
    ?LOCAL_FEED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS
)).

-define(USER_DEFINED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS, (
    ?EXTERNAL_FEED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS ++
    ?LOCAL_FEED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS
)).

-define(USER_DEFINED_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS, (
    ?EXTERNAL_FEED_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS ++
    ?LOCAL_FEED_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS
)).

-define(POSIX_INCOMPATIBLE_STORAGE_CONFIGS, (
    ?AUTO_FEED_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS ++
    ?EXTERNAL_FEED_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS ++
    ?LOCAL_FEED_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS
)).

-define(USER_DEFINED_LUMA_IMPORTED_STORAGE_CONFIGS, (
    ?LOCAL_FEED_LUMA_IMPORTED_STORAGE_CONFIGS ++
    ?EXTERNAL_FEED_LUMA_IMPORTED_STORAGE_CONFIGS
)).

-define(IMPORTED_STORAGE_CONFIGS, (
    ?USER_DEFINED_LUMA_IMPORTED_STORAGE_CONFIGS ++
    ?AUTO_FEED_LUMA_IMPORTED_STORAGE_CONFIGS
)).

-define(NON_IMPORTED_STORAGE_CONFIGS, (
    ?ALL_STORAGE_CONFIGS -- ?IMPORTED_STORAGE_CONFIGS
)).

-define(POSIX_COMPATIBLE_NON_IMPORTED_STORAGE_CONFIGS, (
    ?POSIX_COMPATIBLE_STORAGE_CONFIGS -- ?IMPORTED_STORAGE_CONFIGS
)).

%%%===================================================================
%%% storage test configs with external LUMA
%%%===================================================================

-define(EXTERNAL_FEED_LUMA_POSIX_STORAGE_CONFIG, #{
    name => "POSIX storage with external feed for LUMA DB",
    admin_credentials => ?POSIX_ADMIN_CREDENTIALS,
    user_credentials => ?POSIX_USER_CREDENTIALS,
    default_credentials => ?POSIX_EXTERNAL_FEED_LUMA_DEFAULT_CREDENTIALS,
    display_credentials => ?EXTERNAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_display_credentials => ?EXTERNAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?POSIX_STORAGE_DOC_EXTERNAL_FEED_LUMA
}).

-define(EXTERNAL_FEED_LUMA_IMPORTED_POSIX_STORAGE_CONFIG, #{
    name => "imported POSIX storage with external feed for LUMA DB",
    admin_credentials => ?POSIX_ADMIN_CREDENTIALS,
    user_credentials => ?IMPORTED_POSIX_USER_CREDENTIALS,
    default_credentials => ?POSIX_MOUNT_CREDENTIALS,
    display_credentials => ?EXTERNAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_display_credentials => ?EXTERNAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?POSIX_IMPORTED_STORAGE_DOC_EXTERNAL_FEED_LUMA
}).

-define(EXTERNAL_FEED_LUMA_CEPH_STORAGE_CONFIG, #{
    name => "CEPH storage with external feed for LUMA DB",
    admin_credentials => ?CEPH_ADMIN_CREDENTIALS,
    display_credentials => ?EXTERNAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?CEPH_USER_CREDENTIALS,
    user_display_credentials => ?EXTERNAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?CEPH_STORAGE_DOC_EXTERNAL_FEED_LUMA
}).

-define(EXTERNAL_FEED_LUMA_S3_STORAGE_CONFIG, #{
    name => "S3 storage with external feed for LUMA DB",
    admin_credentials => ?S3_ADMIN_CREDENTIALS,
    display_credentials => ?EXTERNAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?S3_USER_CREDENTIALS,
    user_display_credentials => ?EXTERNAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?S3_STORAGE_DOC_EXTERNAL_FEED_LUMA
}).

-define(EXTERNAL_FEED_LUMA_SWIFT_STORAGE_CONFIG, #{
    name => "SWIFT storage with external feed for LUMA DB",
    admin_credentials => ?SWIFT_ADMIN_CREDENTIALS,
    display_credentials => ?EXTERNAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?SWIFT_USER_CREDENTIALS,
    user_display_credentials => ?EXTERNAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?SWIFT_STORAGE_DOC_EXTERNAL_FEED_LUMA
}).

-define(EXTERNAL_FEED_LUMA_CEPHRADOS_STORAGE_CONFIG, #{
    name => "CEPHRADOS storage with external feed for LUMA DB",
    admin_credentials => ?CEPHRADOS_ADMIN_CREDENTIALS,
    display_credentials => ?EXTERNAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?CEPHRADOS_USER_CREDENTIALS,
    user_display_credentials => ?EXTERNAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?CEPHRADOS_STORAGE_DOC_EXTERNAL_FEED_LUMA
}).

-define(EXTERNAL_FEED_LUMA_GLUSTERFS_STORAGE_CONFIG, #{
    name => "GLUSTERFS storage with external feed for LUMA DB",
    admin_credentials => ?GLUSTERFS_ADMIN_CREDENTIALS,
    default_credentials => ?GLUSTERFS_EXTERNAL_FEED_LUMA_DEFAULT_CREDENTIALS,
    display_credentials => ?EXTERNAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?GLUSTERFS_USER_CREDENTIALS,
    user_display_credentials => ?EXTERNAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?GLUSTERFS_STORAGE_DOC_EXTERNAL_FEED_LUMA
}).

-define(EXTERNAL_FEED_LUMA_IMPORTED_GLUSTERFS_STORAGE_CONFIG, #{
    name => "imported GLUSTERFS storage with external feed for LUMA DB",
    admin_credentials => ?GLUSTERFS_ADMIN_CREDENTIALS,
    default_credentials => ?POSIX_MOUNT_CREDENTIALS,
    display_credentials => ?EXTERNAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?IMPORTED_GLUSTERFS_USER_CREDENTIALS,
    user_display_credentials => ?EXTERNAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?IMPORTED_GLUSTERFS_STORAGE_DOC_EXTERNAL_FEED_LUMA
}).

-define(EXTERNAL_FEED_LUMA_NULLDEVICE_STORAGE_CONFIG, #{
    name => "NULLDEVICE storage with external feed for LUMA DB",
    admin_credentials => ?NULLDEVICE_ADMIN_CREDENTIALS,
    default_credentials => ?NULLDEVICE_EXTERNAL_FEED_LUMA_DEFAULT_CREDENTIALS,
    display_credentials => ?EXTERNAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?NULLDEVICE_USER_CREDENTIALS,
    user_display_credentials => ?EXTERNAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?NULLDEVICE_STORAGE_DOC_EXTERNAL_FEED_LUMA
}).

-define(EXTERNAL_FEED_LUMA_IMPORTED_NULLDEVICE_STORAGE_CONFIG, #{
    name => "imported NULLDEVICE storage with external feed for LUMA DB",
    admin_credentials => ?NULLDEVICE_ADMIN_CREDENTIALS,
    default_credentials => ?POSIX_MOUNT_CREDENTIALS,
    display_credentials => ?EXTERNAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?IMPORTED_NULLDEVICE_USER_CREDENTIALS,
    user_display_credentials => ?EXTERNAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?IMPORTED_NULLDEVICE_STORAGE_DOC_EXTERNAL_FEED_LUMA
}).

-define(EXTERNAL_FEED_LUMA_WEBDAV_BASIC_STORAGE_CONFIG, #{
    name => "WEBDAV_BASIC storage with external feed for LUMA DB",
    admin_credentials => ?WEBDAV_BASIC_ADMIN_CREDENTIALS,
    display_credentials => ?EXTERNAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?WEBDAV_BASIC_USER_CREDENTIALS,
    user_display_credentials => ?EXTERNAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?WEBDAV_BASIC_STORAGE_DOC_EXTERNAL_FEED_LUMA
}).

-define(EXTERNAL_FEED_LUMA_WEBDAV_TOKEN_STORAGE_CONFIG, #{
    name => "WEBDAV_TOKEN storage with external feed for LUMA DB",
    admin_credentials => ?WEBDAV_TOKEN_ADMIN_CREDENTIALS,
    display_credentials => ?EXTERNAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?WEBDAV_TOKEN_USER_CREDENTIALS,
    user_display_credentials => ?EXTERNAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?WEBDAV_TOKEN_STORAGE_DOC_EXTERNAL_FEED_LUMA
}).

-define(EXTERNAL_FEED_LUMA_WEBDAV_NONE_STORAGE_CONFIG, #{
    name => "WEBDAV_NONE storage with external feed for LUMA DB",
    admin_credentials => ?WEBDAV_NONE_CTX,
    display_credentials => ?EXTERNAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?WEBDAV_NONE_CTX,
    user_display_credentials => ?EXTERNAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?WEBDAV_NONE_STORAGE_DOC_EXTERNAL_FEED_LUMA
}).

-define(EXTERNAL_FEED_LUMA_WEBDAV_OAUTH2_STORAGE_CONFIG, #{
    name => "WEBDAV_OAUTH2 storage with external feed for LUMA DB",
    admin_credentials => ?EXPECTED_WEBDAV_OAUTH2_ADMIN_CREDENTIALS,
    display_credentials => ?EXTERNAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?EXPECTED_WEBDAV_OAUTH2_USER_CREDENTIALS,
    user_display_credentials => ?EXTERNAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?WEBDAV_OAUTH2_STORAGE_DOC_EXTERNAL_FEED_LUMA
}).


-define(EXTERNAL_FEED_LUMA_STORAGE_CONFIGS,
    ?EXTERNAL_FEED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS ++
    ?EXTERNAL_FEED_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS
).

-define(EXTERNAL_FEED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
    [
        ?EXTERNAL_FEED_LUMA_POSIX_STORAGE_CONFIG,
        ?EXTERNAL_FEED_LUMA_NULLDEVICE_STORAGE_CONFIG,
        ?EXTERNAL_FEED_LUMA_GLUSTERFS_STORAGE_CONFIG
    ] ++ ?EXTERNAL_FEED_LUMA_IMPORTED_STORAGE_CONFIGS
).

-define(EXTERNAL_FEED_LUMA_IMPORTED_STORAGE_CONFIGS, [
    ?EXTERNAL_FEED_LUMA_IMPORTED_POSIX_STORAGE_CONFIG,
    ?EXTERNAL_FEED_LUMA_IMPORTED_GLUSTERFS_STORAGE_CONFIG,
    ?EXTERNAL_FEED_LUMA_IMPORTED_NULLDEVICE_STORAGE_CONFIG
]).

-define(EXTERNAL_FEED_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS, [
    ?EXTERNAL_FEED_LUMA_CEPH_STORAGE_CONFIG,
    ?EXTERNAL_FEED_LUMA_CEPHRADOS_STORAGE_CONFIG,
    ?EXTERNAL_FEED_LUMA_S3_STORAGE_CONFIG,
    ?EXTERNAL_FEED_LUMA_SWIFT_STORAGE_CONFIG,
    ?EXTERNAL_FEED_LUMA_WEBDAV_BASIC_STORAGE_CONFIG,
    ?EXTERNAL_FEED_LUMA_WEBDAV_TOKEN_STORAGE_CONFIG,
    ?EXTERNAL_FEED_LUMA_WEBDAV_NONE_STORAGE_CONFIG,
    ?EXTERNAL_FEED_LUMA_WEBDAV_OAUTH2_STORAGE_CONFIG
]).

%%%===================================================================
%%% storage test configs with NO LUMA
%%%===================================================================

-define(AUTO_FEED_LUMA_POSIX_STORAGE_CONFIG, #{
    name => "POSIX storage with auto feed for LUMA DB",
    admin_credentials => ?POSIX_ADMIN_CREDENTIALS,
    default_credentials => ?POSIX_MOUNT_CREDENTIALS,
    user_credentials => ?POSIX_GENERATED_USER_CREDENTIALS,
    display_credentials => ?POSIX_MOUNT_CREDENTIALS,
    user_display_credentials => ?AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_POSIX,
    storage_record => ?POSIX_STORAGE_DOC_AUTO_FEED_LUMA
}).

-define(AUTO_FEED_LUMA_IMPORTED_POSIX_STORAGE_CONFIG, #{
    name => "imported POSIX storage with auto feed for LUMA DB",
    admin_credentials => ?POSIX_ADMIN_CREDENTIALS,
    default_credentials => ?POSIX_MOUNT_CREDENTIALS,
    user_credentials => ?POSIX_GENERATED_USER_CREDENTIALS,
    display_credentials => ?POSIX_MOUNT_CREDENTIALS,
    user_display_credentials => ?AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_POSIX,
    storage_record => ?POSIX_IMPORTED_STORAGE_DOC_AUTO_FEED_LUMA
}).

-define(AUTO_FEED_LUMA_CEPH_STORAGE_CONFIG, #{
    name => "CEPH storage with auto feed for LUMA DB",
    admin_credentials => ?CEPH_ADMIN_CREDENTIALS,
    user_credentials => ?CEPH_USER_CREDENTIALS,
    display_credentials => ?AUTO_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_display_credentials => ?AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?CEPH_STORAGE_DOC_AUTO_FEED_LUMA
}).

-define(AUTO_FEED_LUMA_S3_STORAGE_CONFIG, #{
    name => "S3 storage with auto feed for LUMA DB",
    admin_credentials => ?S3_ADMIN_CREDENTIALS,
    user_credentials => ?S3_USER_CREDENTIALS,
    display_credentials => ?AUTO_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_display_credentials => ?AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?S3_STORAGE_DOC_AUTO_FEED_LUMA
}).

-define(AUTO_FEED_LUMA_SWIFT_STORAGE_CONFIG, #{
    name => "SWIFT storage with auto feed for LUMA DB",
    admin_credentials => ?SWIFT_ADMIN_CREDENTIALS,
    user_credentials => ?SWIFT_USER_CREDENTIALS,
    display_credentials => ?AUTO_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_display_credentials => ?AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?SWIFT_STORAGE_DOC_AUTO_FEED_LUMA
}).

-define(AUTO_FEED_LUMA_CEPHRADOS_STORAGE_CONFIG, #{
    name => "CEPHRADOS storage with auto feed for LUMA DB",
    admin_credentials => ?CEPHRADOS_ADMIN_CREDENTIALS,
    user_credentials => ?CEPHRADOS_USER_CREDENTIALS,
    display_credentials => ?AUTO_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_display_credentials => ?AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?CEPHRADOS_STORAGE_DOC_AUTO_FEED_LUMA
}).

-define(AUTO_FEED_LUMA_GLUSTERFS_STORAGE_CONFIG, #{
    name => "GLUSTERFS storage with auto feed for LUMA DB",
    admin_credentials => ?GLUSTERFS_ADMIN_CREDENTIALS,
    user_credentials => ?GLUSTERFS_GENERATED_USER_CREDENTIALS,
    default_credentials => ?GLUSTERFS_MOUNT_CREDENTIALS,
    display_credentials => ?GLUSTERFS_MOUNT_CREDENTIALS,
    user_display_credentials => ?AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_POSIX,
    storage_record => ?GLUSTERFS_STORAGE_DOC_AUTO_FEED_LUMA
}).

-define(AUTO_FEED_LUMA_IMPORTED_GLUSTERFS_STORAGE_CONFIG, #{
    name => "imported GLUSTERFS storage with auto feed for LUMA DB",
    admin_credentials => ?GLUSTERFS_ADMIN_CREDENTIALS,
    user_credentials => ?GLUSTERFS_GENERATED_USER_CREDENTIALS,
    default_credentials => ?GLUSTERFS_MOUNT_CREDENTIALS,
    display_credentials => ?GLUSTERFS_MOUNT_CREDENTIALS,
    user_display_credentials => ?AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_POSIX,
    storage_record => ?IMPORTED_GLUSTERFS_STORAGE_DOC_AUTO_FEED_LUMA
}).

-define(AUTO_FEED_LUMA_NULLDEVICE_STORAGE_CONFIG, #{
    name => "NULLDEVICE storage with auto feed for LUMA DB",
    admin_credentials => ?NULLDEVICE_ADMIN_CREDENTIALS,
    user_credentials => ?NULLDEVICE_GENERATED_USER_CREDENTIALS,
    default_credentials => ?NULLDEVICE_MOUNT_CREDENTIALS,
    display_credentials => ?NULLDEVICE_MOUNT_CREDENTIALS,
    user_display_credentials => ?AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_POSIX,
    storage_record => ?NULLDEVICE_STORAGE_DOC_AUTO_FEED_LUMA
}).

-define(AUTO_FEED_LUMA_IMPORTED_NULLDEVICE_STORAGE_CONFIG, #{
    name => "imported NULLDEVICE storage with auto feed for LUMA DB",
    admin_credentials => ?NULLDEVICE_ADMIN_CREDENTIALS,
    user_credentials => ?NULLDEVICE_GENERATED_USER_CREDENTIALS,
    default_credentials => ?NULLDEVICE_MOUNT_CREDENTIALS,
    display_credentials => ?NULLDEVICE_MOUNT_CREDENTIALS,
    user_display_credentials => ?AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_POSIX,
    storage_record => ?IMPORTED_NULLDEVICE_STORAGE_DOC_AUTO_FEED_LUMA
}).

-define(AUTO_FEED_LUMA_WEBDAV_BASIC_STORAGE_CONFIG, #{
    name => "WEBDAV_BASIC storage with auto feed for LUMA DB",
    admin_credentials => ?WEBDAV_BASIC_ADMIN_CREDENTIALS,
    user_credentials => ?WEBDAV_BASIC_USER_CREDENTIALS,
    display_credentials => ?AUTO_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_display_credentials => ?AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?WEBDAV_BASIC_STORAGE_DOC_AUTO_FEED_LUMA
}).

-define(AUTO_FEED_LUMA_WEBDAV_TOKEN_STORAGE_CONFIG, #{
    name => "WEBDAV_TOKEN storage with auto feed for LUMA DB",
    admin_credentials => ?WEBDAV_TOKEN_ADMIN_CREDENTIALS,
    user_credentials => ?WEBDAV_TOKEN_USER_CREDENTIALS,
    display_credentials => ?AUTO_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_display_credentials => ?AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?WEBDAV_TOKEN_STORAGE_DOC_AUTO_FEED_LUMA
}).

-define(AUTO_FEED_LUMA_WEBDAV_NONE_STORAGE_CONFIG, #{
    name => "WEBDAV_NONE storage with auto feed for LUMA DB",
    admin_credentials => ?WEBDAV_NONE_CTX,
    user_credentials => ?WEBDAV_NONE_CTX,
    display_credentials => ?AUTO_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_display_credentials => ?AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?WEBDAV_NONE_STORAGE_DOC_AUTO_FEED_LUMA
}).

-define(AUTO_FEED_LUMA_WEBDAV_OAUTH2_STORAGE_CONFIG, #{
    name => "WEBDAV_OAUTH2 storage with auto feed for LUMA DB",
    admin_credentials => ?EXPECTED_WEBDAV_OAUTH2_ADMIN_CREDENTIALS,
    user_credentials => ?EXPECTED_WEBDAV_OAUTH2_USER_CREDENTIALS,
    display_credentials => ?AUTO_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_display_credentials => ?AUTO_FEED_LUMA_USER_DISPLAY_CREDENTIALS_NON_POSIX,
    storage_record => ?WEBDAV_OAUTH2_STORAGE_DOC_AUTO_FEED_LUMA
}).

-define(AUTO_FEED_LUMA_STORAGE_CONFIGS,
    ?AUTO_FEED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS ++
    ?AUTO_FEED_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS
).

-define(AUTO_FEED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
    [
        ?AUTO_FEED_LUMA_POSIX_STORAGE_CONFIG,
        ?AUTO_FEED_LUMA_NULLDEVICE_STORAGE_CONFIG,
        ?AUTO_FEED_LUMA_GLUSTERFS_STORAGE_CONFIG
    ] ++ ?AUTO_FEED_LUMA_IMPORTED_STORAGE_CONFIGS
).

-define(AUTO_FEED_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS, [
    ?AUTO_FEED_LUMA_CEPH_STORAGE_CONFIG,
    ?AUTO_FEED_LUMA_S3_STORAGE_CONFIG,
    ?AUTO_FEED_LUMA_SWIFT_STORAGE_CONFIG,
    ?AUTO_FEED_LUMA_CEPHRADOS_STORAGE_CONFIG,
    ?AUTO_FEED_LUMA_WEBDAV_BASIC_STORAGE_CONFIG,
    ?AUTO_FEED_LUMA_WEBDAV_TOKEN_STORAGE_CONFIG,
    ?AUTO_FEED_LUMA_WEBDAV_NONE_STORAGE_CONFIG,
    ?AUTO_FEED_LUMA_WEBDAV_OAUTH2_STORAGE_CONFIG
]).

-define(AUTO_FEED_LUMA_IMPORTED_STORAGE_CONFIGS, [
    ?AUTO_FEED_LUMA_IMPORTED_POSIX_STORAGE_CONFIG,
    ?AUTO_FEED_LUMA_IMPORTED_GLUSTERFS_STORAGE_CONFIG,
    ?AUTO_FEED_LUMA_IMPORTED_NULLDEVICE_STORAGE_CONFIG
]).


%%%===================================================================
%%% storage test configs with LOCAL FEED LUMA
%%%===================================================================

-define(LOCAL_FEED_LUMA_POSIX_STORAGE_CONFIG, #{
    name => "POSIX storage with local feed for LUMA DB",
    admin_credentials => ?POSIX_ADMIN_CREDENTIALS,
    user_credentials => ?POSIX_USER_CREDENTIALS,
    default_credentials => ?POSIX_LOCAL_FEED_LUMA_DEFAULT_CREDENTIALS,
    display_credentials => ?LOCAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_display_credentials => ?LOCAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?POSIX_STORAGE_DOC_LOCAL_FEED_LUMA
}).

-define(LOCAL_FEED_LUMA_IMPORTED_POSIX_STORAGE_CONFIG, #{
    name => "imported POSIX storage with local feed for LUMA DB",
    admin_credentials => ?POSIX_ADMIN_CREDENTIALS,
    user_credentials => ?IMPORTED_POSIX_USER_CREDENTIALS,
    default_credentials => ?POSIX_MOUNT_CREDENTIALS,
    display_credentials => ?LOCAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_display_credentials => ?LOCAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?POSIX_IMPORTED_STORAGE_DOC_LOCAL_FEED_LUMA
}).

-define(LOCAL_FEED_LUMA_CEPH_STORAGE_CONFIG, #{
    name => "CEPH storage with local feed for LUMA DB",
    admin_credentials => ?CEPH_ADMIN_CREDENTIALS,
    display_credentials => ?LOCAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?CEPH_USER_CREDENTIALS,
    user_display_credentials => ?LOCAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?CEPH_STORAGE_DOC_LOCAL_FEED_LUMA
}).

-define(LOCAL_FEED_LUMA_S3_STORAGE_CONFIG, #{
    name => "S3 storage with local feed for LUMA DB",
    admin_credentials => ?S3_ADMIN_CREDENTIALS,
    display_credentials => ?LOCAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?S3_USER_CREDENTIALS,
    user_display_credentials => ?LOCAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?S3_STORAGE_DOC_LOCAL_FEED_LUMA
}).

-define(LOCAL_FEED_LUMA_SWIFT_STORAGE_CONFIG, #{
    name => "SWIFT storage with local feed for LUMA DB",
    admin_credentials => ?SWIFT_ADMIN_CREDENTIALS,
    display_credentials => ?LOCAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?SWIFT_USER_CREDENTIALS,
    user_display_credentials => ?LOCAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?SWIFT_STORAGE_DOC_LOCAL_FEED_LUMA
}).

-define(LOCAL_FEED_LUMA_CEPHRADOS_STORAGE_CONFIG, #{
    name => "CEPHRADOS storage with local feed for LUMA DB",
    admin_credentials => ?CEPHRADOS_ADMIN_CREDENTIALS,
    display_credentials => ?LOCAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?CEPHRADOS_USER_CREDENTIALS,
    user_display_credentials => ?LOCAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?CEPHRADOS_STORAGE_DOC_LOCAL_FEED_LUMA
}).

-define(LOCAL_FEED_LUMA_GLUSTERFS_STORAGE_CONFIG, #{
    name => "GLUSTERFS storage with local feed for LUMA DB",
    admin_credentials => ?GLUSTERFS_ADMIN_CREDENTIALS,
    default_credentials => ?GLUSTERFS_LOCAL_FEED_LUMA_DEFAULT_CREDENTIALS,
    display_credentials => ?LOCAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?GLUSTERFS_USER_CREDENTIALS,
    user_display_credentials => ?LOCAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?GLUSTERFS_STORAGE_DOC_LOCAL_FEED_LUMA
}).

-define(LOCAL_FEED_LUMA_IMPORTED_GLUSTERFS_STORAGE_CONFIG, #{
    name => "imported GLUSTERFS storage with local feed for LUMA DB",
    admin_credentials => ?GLUSTERFS_ADMIN_CREDENTIALS,
    default_credentials => ?POSIX_MOUNT_CREDENTIALS,
    display_credentials => ?LOCAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?IMPORTED_GLUSTERFS_USER_CREDENTIALS,
    user_display_credentials => ?LOCAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?IMPORTED_GLUSTERFS_STORAGE_DOC_LOCAL_FEED_LUMA
}).

-define(LOCAL_FEED_LUMA_NULLDEVICE_STORAGE_CONFIG, #{
    name => "NULLDEVICE storage with local feed for LUMA DB",
    admin_credentials => ?NULLDEVICE_ADMIN_CREDENTIALS,
    default_credentials => ?NULLDEVICE_LOCAL_FEED_LUMA_DEFAULT_CREDENTIALS,
    display_credentials => ?LOCAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?NULLDEVICE_USER_CREDENTIALS,
    user_display_credentials => ?LOCAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?NULLDEVICE_STORAGE_DOC_LOCAL_FEED_LUMA
}).

-define(LOCAL_FEED_LUMA_IMPORTED_NULLDEVICE_STORAGE_CONFIG, #{
    name => "imported NULLDEVICE storage with local feed for LUMA DB",
    admin_credentials => ?NULLDEVICE_ADMIN_CREDENTIALS,
    default_credentials => ?POSIX_MOUNT_CREDENTIALS,
    display_credentials => ?LOCAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?IMPORTED_NULLDEVICE_USER_CREDENTIALS,
    user_display_credentials => ?LOCAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?IMPORTED_NULLDEVICE_STORAGE_DOC_LOCAL_FEED_LUMA
}).

-define(LOCAL_FEED_LUMA_WEBDAV_BASIC_STORAGE_CONFIG, #{
    name => "WEBDAV_BASIC storage with local feed for LUMA DB",
    admin_credentials => ?WEBDAV_BASIC_ADMIN_CREDENTIALS,
    display_credentials => ?LOCAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?WEBDAV_BASIC_USER_CREDENTIALS,
    user_display_credentials => ?LOCAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?WEBDAV_BASIC_STORAGE_DOC_LOCAL_FEED_LUMA
}).

-define(LOCAL_FEED_LUMA_WEBDAV_TOKEN_STORAGE_CONFIG, #{
    name => "WEBDAV_TOKEN storage with local feed for LUMA DB",
    admin_credentials => ?WEBDAV_TOKEN_ADMIN_CREDENTIALS,
    display_credentials => ?LOCAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?WEBDAV_TOKEN_USER_CREDENTIALS,
    user_display_credentials => ?LOCAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?WEBDAV_TOKEN_STORAGE_DOC_LOCAL_FEED_LUMA
}).

-define(LOCAL_FEED_LUMA_WEBDAV_NONE_STORAGE_CONFIG, #{
    name => "WEBDAV_NONE storage with local feed for LUMA DB",
    admin_credentials => ?WEBDAV_NONE_CTX,
    display_credentials => ?LOCAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?WEBDAV_NONE_CTX,
    user_display_credentials => ?LOCAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?WEBDAV_NONE_STORAGE_DOC_LOCAL_FEED_LUMA
}).

-define(LOCAL_FEED_LUMA_WEBDAV_OAUTH2_STORAGE_CONFIG, #{
    name => "WEBDAV_OAUTH2 storage with local feed for LUMA DB",
    admin_credentials => ?EXPECTED_WEBDAV_OAUTH2_ADMIN_CREDENTIALS,
    display_credentials => ?LOCAL_FEED_LUMA_DEFAULT_DISPLAY_CREDENTIALS,
    user_credentials => ?EXPECTED_WEBDAV_OAUTH2_USER_CREDENTIALS,
    user_display_credentials => ?LOCAL_FEED_LUMA_USER_DISPLAY_CREDS,
    storage_record => ?WEBDAV_OAUTH2_STORAGE_DOC_LOCAL_FEED_LUMA
}).

-define(LOCAL_FEED_LUMA_STORAGE_CONFIGS,
    ?LOCAL_FEED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS ++
    ?LOCAL_FEED_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS
).


-define(LOCAL_FEED_LUMA_POSIX_COMPATIBLE_STORAGE_CONFIGS,
    [
        ?LOCAL_FEED_LUMA_POSIX_STORAGE_CONFIG,
        ?LOCAL_FEED_LUMA_NULLDEVICE_STORAGE_CONFIG,
        ?LOCAL_FEED_LUMA_GLUSTERFS_STORAGE_CONFIG
    ] ++ ?LOCAL_FEED_LUMA_IMPORTED_STORAGE_CONFIGS
).

-define(LOCAL_FEED_LUMA_IMPORTED_STORAGE_CONFIGS, [
    ?LOCAL_FEED_LUMA_IMPORTED_POSIX_STORAGE_CONFIG,
    ?LOCAL_FEED_LUMA_IMPORTED_NULLDEVICE_STORAGE_CONFIG,
    ?LOCAL_FEED_LUMA_IMPORTED_GLUSTERFS_STORAGE_CONFIG
]).

-define(LOCAL_FEED_LUMA_POSIX_INCOMPATIBLE_STORAGE_CONFIGS, [
    ?LOCAL_FEED_LUMA_CEPH_STORAGE_CONFIG,
    ?LOCAL_FEED_LUMA_S3_STORAGE_CONFIG,
    ?LOCAL_FEED_LUMA_SWIFT_STORAGE_CONFIG,
    ?LOCAL_FEED_LUMA_CEPHRADOS_STORAGE_CONFIG,
    ?LOCAL_FEED_LUMA_WEBDAV_BASIC_STORAGE_CONFIG,
    ?LOCAL_FEED_LUMA_WEBDAV_TOKEN_STORAGE_CONFIG,
    ?LOCAL_FEED_LUMA_WEBDAV_NONE_STORAGE_CONFIG,
    ?LOCAL_FEED_LUMA_WEBDAV_OAUTH2_STORAGE_CONFIG
]).


-endif.