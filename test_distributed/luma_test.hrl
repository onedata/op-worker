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

-ifndef(LUMA_TEST_HRL).
-define(LUMA_TEST_HRL, 1).

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/helpers/helpers.hrl").

-define(STRIP_OK(Result), begin {ok, _} = Result, element(2, Result) end).

%%%===================================================================
%%% Users and spaces macros
%%%===================================================================

-define(SESS_ID, <<"sessionId">>).
-define(USER_ID, <<"userId">>).
-define(ADMIN_ID, <<"adminId">>).

-define(SPACE_ID, <<"spaceId">>).
-define(GROUP_ID, <<"groupId">>).

-define(UID1, 1).
-define(GID1, 1).
-define(GID2, 2).
-define(GID_NULL, <<"null">>).

-define(POSIX_ID_RANGE, {100000, 2000000}).

-define(POSIX_CTX_TU_TUPLE(UserCtx), begin
    #{<<"uid">> := __UID, <<"gid">> := __GID} = UserCtx,
    {binary_to_integer(__UID),binary_to_integer(__GID)}
end).

%%%===================================================================
%%% LUMA config macros
%%%===================================================================

-define(LUMA_CONFIG, #luma_config{
    url = <<"http://127.0.0.1:5000">>,
    api_key = <<"test_api_key">>
}).

%%%===================================================================
%%% Posix helper and storage macros
%%%===================================================================

-define(POSIX_ADMIN_CTX, luma_test_utils:new_posix_user_ctx(0, 0)).
-define(POSIX_USER_CTX, luma_test_utils:new_posix_user_ctx(?UID1, ?GID1)).
-define(GENERATED_POSIX_USER_CTX, luma_test_utils:new_posix_user_ctx(
    luma_test_SUITE:generate_posix_identifier(?USER_ID, ?POSIX_ID_RANGE),
    luma_test_SUITE:generate_posix_identifier(?SPACE_ID, ?POSIX_ID_RANGE)
)).


-define(POSIX_STORAGE_ID, <<"posixStorageId">>).

-define(POSIX_HELPER,
    ?STRIP_OK(helper:new_helper(?POSIX_HELPER_NAME,
    #{<<"mountPoint">> => <<"mountPoint">>}, ?POSIX_ADMIN_CTX, false,
    ?CANONICAL_STORAGE_PATH))
).

-define(POSIX_STORAGE_DOC(LumaConfig),
    ?STORAGE_RECORD(?POSIX_STORAGE_ID, <<"POSIX">>, ?POSIX_HELPER, LumaConfig)).
-define(POSIX_STORAGE_DOC_SECURE, ?POSIX_STORAGE_DOC(?LUMA_CONFIG)).
-define(POSIX_STORAGE_DOC_INSECURE, ?POSIX_STORAGE_DOC(undefined)).


%%%===================================================================
%%% CEPH helper and storage macros
%%%===================================================================

-define(CEPH_ADMIN_CTX, luma_test_utils:new_ceph_user_ctx(<<"ADMIN">>, <<"ADMIN_KEY">>)).
-define(CEPH_USER_CTX, luma_test_utils:new_ceph_user_ctx(<<"USER">>, <<"USER_KEY">>)).

-define(CEPH_STORAGE_ID, <<"cephStorageId">>).

-define(CEPH_HELPER(Insecure), ?STRIP_OK(helper:new_helper(?CEPH_HELPER_NAME,
    #{<<"monitorHostname">> => <<"monitorHostname">>,
        <<"clusterName">> => <<"clusterName">>,
        <<"poolName">> => <<"poolName">>},
    ?CEPH_ADMIN_CTX, Insecure, ?FLAT_STORAGE_PATH)
)).

-define(CEPH_STORAGE_DOC(Insecure, LumaConfig),
    ?STORAGE_RECORD(?CEPH_STORAGE_ID, <<"CEPH">>,
        ?CEPH_HELPER(Insecure), LumaConfig)
).
-define(CEPH_STORAGE_DOC_SECURE,
    ?CEPH_STORAGE_DOC(false, ?LUMA_CONFIG)).
-define(CEPH_STORAGE_DOC_INSECURE,
    ?CEPH_STORAGE_DOC(true, undefined)).

%%%===================================================================
%%% S3 helper and storage macros
%%%===================================================================

-define(S3_ADMIN_CTX,
     luma_test_utils:new_s3_user_ctx(<<"ADMIN_ACCESS_KEY">>, <<"ADMIN_SECRET_KEY">>)).
-define(S3_USER_CTX,
     luma_test_utils:new_s3_user_ctx(<<"USER_ACCESS_KEY">>, <<"USER_SECRET_KEY">>)).

-define(S3_STORAGE_ID, <<"s3StorageId">>).

% are passed through translater_helper_args
-define(S3_HELPER(Insecure), ?STRIP_OK(helper:new_helper(?S3_HELPER_NAME,
    #{<<"scheme">> => <<"https">>, <<"hostname">> => <<"hostname">>,
        <<"bucketName">> => <<"bucketName">>},
    ?S3_ADMIN_CTX, Insecure, ?FLAT_STORAGE_PATH)
)).

-define(S3_STORAGE_DOC(Insecure, LumaConfig),
    ?STORAGE_RECORD(?S3_STORAGE_ID, <<"S3">>, ?S3_HELPER(Insecure), LumaConfig)
).
-define(S3_STORAGE_DOC_SECURE,
    ?S3_STORAGE_DOC(false, ?LUMA_CONFIG)).
-define(S3_STORAGE_DOC_INSECURE,
    ?S3_STORAGE_DOC(true, undefined)).

%%%===================================================================
%%% SWIFT helper and storage macros
%%%===================================================================

-define(SWIFT_ADMIN_CTX,
     luma_test_utils:new_swift_user_ctx(<<"ADMIN">>, <<"ADMIN_PASSWD">>)).
-define(SWIFT_USER_CTX,
     luma_test_utils:new_swift_user_ctx(<<"USER">>, <<"USER_PASSWD">>)).

-define(SWIFT_STORAGE_ID, <<"swiftStorageId">>).

-define(SWIFT_HELPER(Insecure), ?STRIP_OK(helper:new_helper(?SWIFT_HELPER_NAME,
    #{<<"authUrl">> => <<"authUrl">>,
        <<"containerName">> => <<"containerName">>,
        <<"tenantName">> => <<"tenantName">>},
    ?SWIFT_ADMIN_CTX, Insecure, ?FLAT_STORAGE_PATH)
)).

-define(SWIFT_STORAGE_DOC(Insecure, LumaConfig),
    ?STORAGE_RECORD(?SWIFT_STORAGE_ID, <<"SWIFT">>, ?SWIFT_HELPER(Insecure), LumaConfig)
).
-define(SWIFT_STORAGE_DOC_SECURE,
    ?SWIFT_STORAGE_DOC(false, ?LUMA_CONFIG)).
-define(SWIFT_STORAGE_DOC_INSECURE,
    ?SWIFT_STORAGE_DOC(true, undefined)).

%%%===================================================================
%%% CEPHRADOS helper and storage macros
%%%===================================================================

-define(CEPHRADOS_ADMIN_CTX,
     luma_test_utils:new_cephrados_user_ctx(<<"ADMIN">>, <<"ADMIN_KEY">>)).
-define(CEPHRADOS_USER_CTX,
     luma_test_utils:new_cephrados_user_ctx(<<"USER">>, <<"USER_KEY">>)).

-define(CEPHRADOS_STORAGE_ID, <<"cephradosStorageId">>).

-define(CEPHRADOS_HELPER(Insecure), ?STRIP_OK(helper:new_helper(?CEPHRADOS_HELPER_NAME,
    #{<<"monitorHostname">> => <<"monitorHostname">>,
        <<"clusterName">> => <<"clusterName">>,
        <<"poolName">> => <<"poolName">>},
    ?CEPHRADOS_ADMIN_CTX, Insecure, ?FLAT_STORAGE_PATH)
)).

-define(CEPHRADOS_STORAGE_DOC(Insecure, LumaConfig),
    ?STORAGE_RECORD(?CEPHRADOS_STORAGE_ID, <<"CEPHRADOS">>,
        ?CEPHRADOS_HELPER(Insecure), LumaConfig)
).
-define(CEPHRADOS_STORAGE_DOC_SECURE,
    ?CEPHRADOS_STORAGE_DOC(false, ?LUMA_CONFIG)).
-define(CEPHRADOS_STORAGE_DOC_INSECURE,
    ?CEPHRADOS_STORAGE_DOC(true, undefined)).

%%%===================================================================
%%% GLUSTERFS helper and storage macros
%%%===================================================================

-define(GLUSTERFS_ADMIN_CTX, luma_test_utils:new_glusterfs_user_ctx(0, 0)).
-define(GLUSTERFS_USER_CTX, luma_test_utils:new_glusterfs_user_ctx(?UID1, ?GID1)).

-define(GLUSTERFS_STORAGE_ID, <<"glusterfsStorageId">>).

-define(GLUSTERFS_HELPER(Insecure), ?STRIP_OK(helper:new_helper(
    ?GLUSTERFS_HELPER_NAME,
    #{<<"volume">> => <<"volume">>, <<"hostname">> => <<"hostname">>},
    ?GLUSTERFS_ADMIN_CTX, Insecure, ?FLAT_STORAGE_PATH)
)).

-define(GLUSTERFS_STORAGE_DOC(Insecure, LumaConfig),
    ?STORAGE_RECORD(?GLUSTERFS_STORAGE_ID, <<"GLUSTERFS">>,
        ?GLUSTERFS_HELPER(Insecure), LumaConfig)
).
-define(GLUSTERFS_STORAGE_DOC_SECURE,
    ?GLUSTERFS_STORAGE_DOC(false, ?LUMA_CONFIG)).
-define(GLUSTERFS_STORAGE_DOC_INSECURE,
    ?GLUSTERFS_STORAGE_DOC(true, undefined)).

%%%===================================================================
%%% NULLDEVICE helper and storage macros
%%%===================================================================

-define(NULLDEVICE_ADMIN_CTX, luma_test_utils:new_nulldevice_user_ctx(0, 0)).
-define(NULLDEVICE_USER_CTX, luma_test_utils:new_nulldevice_user_ctx(?UID1, ?GID1)).

-define(NULLDEVICE_STORAGE_ID, <<"nulldeviceStorageId">>).

-define(NULLDEVICE_HELPER(Insecure), ?STRIP_OK(helper:new_helper(
    ?NULL_DEVICE_HELPER_NAME, #{},
    ?NULLDEVICE_ADMIN_CTX, Insecure, ?FLAT_STORAGE_PATH))).

-define(NULLDEVICE_STORAGE_DOC(Insecure, LumaConfig),
    ?STORAGE_RECORD(?NULLDEVICE_STORAGE_ID, <<"NULLDEVICE">>,
        ?NULLDEVICE_HELPER(Insecure), LumaConfig)
).
-define(NULLDEVICE_STORAGE_DOC_SECURE,
    ?NULLDEVICE_STORAGE_DOC(false, ?LUMA_CONFIG)).
-define(NULLDEVICE_STORAGE_DOC_INSECURE,
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
-define(WEBDAV_BASIC_ADMIN_CREDENTIALS, <<"admin:password">>).
-define(WEBDAV_BASIC_USER_CREDENTIALS, <<"token:password">>).
-define(WEBDAV_BASIC_CTX(Credentials),
     luma_test_utils:new_webdav_user_ctx(?WEBDAV_BASIC_CREDENTIALS_TYPE, Credentials)).
-define(WEBDAV_BASIC_ADMIN_CTX, ?WEBDAV_BASIC_CTX(?WEBDAV_BASIC_ADMIN_CREDENTIALS)).
-define(WEBDAV_BASIC_USER_CTX, ?WEBDAV_BASIC_CTX(?WEBDAV_BASIC_USER_CREDENTIALS)).

-define(WEBDAV_TOKEN_CREDENTIALS_TYPE, <<"token">>).
-define(WEBDAV_TOKEN_ADMIN_CREDENTIALS, <<"ADMIN_TOKEN">>).
-define(WEBDAV_TOKEN_USER_CREDENTIALS, <<"USER_TOKEN">>).
-define(WEBDAV_TOKEN_CTX(Credentials),
     luma_test_utils:new_webdav_user_ctx(?WEBDAV_TOKEN_CREDENTIALS_TYPE, Credentials)).
-define(WEBDAV_TOKEN_ADMIN_CTX, ?WEBDAV_TOKEN_CTX(?WEBDAV_TOKEN_ADMIN_CREDENTIALS)).
-define(WEBDAV_TOKEN_USER_CTX, ?WEBDAV_TOKEN_CTX(?WEBDAV_TOKEN_USER_CREDENTIALS)).

-define(WEBDAV_NONE_CREDENTIALS_TYPE, <<"none">>).
-define(WEBDAV_NONE_CTX,
     luma_test_utils:new_webdav_user_ctx(?WEBDAV_NONE_CREDENTIALS_TYPE, <<"">>)).

-define(WEBDAV_OAUTH2_CREDENTIALS_TYPE, <<"oauth2">>).
-define(WEBDAV_OAUTH2_ADMIN_CREDENTIALS, <<"ADMIN_OAUTH2">>).
-define(WEBDAV_OAUTH2_USER_CREDENTIALS, <<"USER_OAUTH2">>).

-define(WEBDAV_OAUTH2_ADMIN_CTX,
    (luma_test_utils:new_webdav_user_ctx(
        ?WEBDAV_OAUTH2_CREDENTIALS_TYPE,
        ?WEBDAV_OAUTH2_ADMIN_CREDENTIALS
    ))#{
        <<"onedataAccessToken">> => ?OD_ACCESS_TOKEN,
        <<"adminId">> => ?ADMIN_ID
    }
).
-define(EXPECTED_WEBDAV_OAUTH2_ADMIN_CTX,
    (luma_test_utils:new_webdav_user_ctx(
        ?WEBDAV_OAUTH2_CREDENTIALS_TYPE,
        ?WEBDAV_OAUTH2_ADMIN_CREDENTIALS
    ))#{
        <<"accessToken">> => ?IDP_ADMIN_TOKEN,
        <<"accessTokenTTL">> => integer_to_binary(?TTL),
        <<"adminId">> => ?ADMIN_ID
    }
).

-define(WEBDAV_OAUTH2_USER_CTX,
    luma_test_utils:new_webdav_user_ctx(?WEBDAV_OAUTH2_CREDENTIALS_TYPE,
        ?WEBDAV_OAUTH2_USER_CREDENTIALS)
    ).

-define(EXPECTED_WEBDAV_OAUTH2_USER_CTX,
    (?WEBDAV_OAUTH2_USER_CTX)#{
        <<"accessToken">> => ?IDP_USER_TOKEN,
        <<"accessTokenTTL">> => integer_to_binary(?TTL)
    }
).

-define(WEBDAV_STORAGE_ID, <<"webdavStorageId">>).

-define(WEBDAV_HELPER(Insecure, AdminCtx), ?STRIP_OK(helper:new_helper(
    ?WEBDAV_HELPER_NAME,
    #{<<"endpoint">> => <<"endpoint">>, <<"oauth2IdP">> => ?OAUTH2_IDP},
    AdminCtx, Insecure, ?FLAT_STORAGE_PATH)
)).
-define(WEBDAV_BASIC_HELPER(Insecure),
    ?WEBDAV_HELPER(Insecure, ?WEBDAV_BASIC_ADMIN_CTX)).
-define(WEBDAV_TOKEN_HELPER(Insecure),
    ?WEBDAV_HELPER(Insecure, ?WEBDAV_TOKEN_ADMIN_CTX)).
-define(WEBDAV_NONE_HELPER(Insecure),
    ?WEBDAV_HELPER(Insecure, ?WEBDAV_NONE_CTX)).
-define(WEBDAV_OAUTH2_HELPER(Insecure),
    ?WEBDAV_HELPER(Insecure, ?WEBDAV_OAUTH2_ADMIN_CTX)).

-define(WEBDAV_STORAGE_DOC(LumaConfig, Helper),
    ?STORAGE_RECORD(?WEBDAV_STORAGE_ID, <<"WEBDAV">>, Helper, LumaConfig)
).

-define(WEBDAV_BASIC_STORAGE_DOC_SECURE,
    ?WEBDAV_STORAGE_DOC(?LUMA_CONFIG, ?WEBDAV_BASIC_HELPER(false))).
-define(WEBDAV_BASIC_STORAGE_DOC_INSECURE,
    ?WEBDAV_STORAGE_DOC(undefined, ?WEBDAV_BASIC_HELPER(true))).
-define(WEBDAV_TOKEN_STORAGE_DOC_SECURE,
    ?WEBDAV_STORAGE_DOC(?LUMA_CONFIG, ?WEBDAV_TOKEN_HELPER(false))).
-define(WEBDAV_TOKEN_STORAGE_DOC_INSECURE,
    ?WEBDAV_STORAGE_DOC(undefined, ?WEBDAV_TOKEN_HELPER(true))).
-define(WEBDAV_NONE_STORAGE_DOC_SECURE,
    ?WEBDAV_STORAGE_DOC(?LUMA_CONFIG, ?WEBDAV_NONE_HELPER(false))).
-define(WEBDAV_NONE_STORAGE_DOC_INSECURE,
    ?WEBDAV_STORAGE_DOC(undefined, ?WEBDAV_NONE_HELPER(true))).
-define(WEBDAV_OAUTH2_STORAGE_DOC_SECURE,
    ?WEBDAV_STORAGE_DOC(?LUMA_CONFIG, ?WEBDAV_OAUTH2_HELPER(false))).
-define(WEBDAV_OAUTH2_STORAGE_DOC_INSECURE,
    ?WEBDAV_STORAGE_DOC(undefined, ?WEBDAV_OAUTH2_HELPER(true))).

%%%===================================================================
%%% storage macros
%%%===================================================================

-define(STORAGE_RECORD(Id, Name, Helper, LumaConfig), {new_storage,
    Id,
    Name,
    Helper,
    false, % readonly
    LumaConfig,
    false, % imported storage
    #{} % QoS parameters
}).

-define(SECURE_POSIX_STORAGE_CONFIG, #{
    name => "secure POSIX storage",
    admin_ctx => ?POSIX_ADMIN_CTX,
    user_ctx => ?POSIX_USER_CTX,
    helper_name => ?POSIX_HELPER_NAME,
    storage_record => ?POSIX_STORAGE_DOC_SECURE
}).

-define(SECURE_CEPH_STORAGE_CONFIG, #{
    name => "secure CEPH storage",
    admin_ctx => ?CEPH_ADMIN_CTX,
    user_ctx => ?CEPH_USER_CTX,
    helper_name => ?CEPH_HELPER_NAME,
    storage_record => ?CEPH_STORAGE_DOC_SECURE
}).

-define(SECURE_S3_STORAGE_CONFIG, #{
    name => "secure S3 storage",
    admin_ctx => ?S3_ADMIN_CTX,
    user_ctx => ?S3_USER_CTX,
    helper_name => ?S3_HELPER_NAME,
    storage_record => ?S3_STORAGE_DOC_SECURE
}).

-define(SECURE_SWIFT_STORAGE_CONFIG, #{
    name => "secure SWIFT storage",
    admin_ctx => ?SWIFT_ADMIN_CTX,
    user_ctx => ?SWIFT_USER_CTX,
    helper_name => ?SWIFT_HELPER_NAME,
    storage_record => ?SWIFT_STORAGE_DOC_SECURE
}).

-define(SECURE_CEPHRADOS_STORAGE_CONFIG, #{
    name => "secure CEPHRADOS storage",
    admin_ctx => ?CEPHRADOS_ADMIN_CTX,
    user_ctx => ?CEPHRADOS_USER_CTX,
    helper_name => ?CEPHRADOS_HELPER_NAME,
    storage_record => ?CEPHRADOS_STORAGE_DOC_SECURE
}).

-define(SECURE_GLUSTERFS_STORAGE_CONFIG, #{
    name => "secure GLUSTERFS storage",
    admin_ctx => ?GLUSTERFS_ADMIN_CTX,
    user_ctx => ?GLUSTERFS_USER_CTX,
    helper_name => ?GLUSTERFS_HELPER_NAME,
    storage_record => ?GLUSTERFS_STORAGE_DOC_SECURE
}).

-define(SECURE_NULLDEVICE_STORAGE_CONFIG, #{
    name => "secure NULLDEVICE storage",
    admin_ctx => ?NULLDEVICE_ADMIN_CTX,
    user_ctx => ?NULLDEVICE_USER_CTX,
    helper_name => ?NULL_DEVICE_HELPER_NAME,
    storage_record => ?NULLDEVICE_STORAGE_DOC_SECURE
}).

-define(SECURE_WEBDAV_BASIC_STORAGE_CONFIG, #{
    name => "secure WEBDAV_BASIC storage",
    admin_ctx => ?WEBDAV_BASIC_ADMIN_CTX,
    user_ctx => ?WEBDAV_BASIC_USER_CTX,
    helper_name => ?WEBDAV_HELPER_NAME,
    storage_record => ?WEBDAV_BASIC_STORAGE_DOC_SECURE
}).

-define(SECURE_WEBDAV_TOKEN_STORAGE_CONFIG, #{
    name => "secure WEBDAV_TOKEN storage",
    admin_ctx => ?WEBDAV_TOKEN_ADMIN_CTX,
    user_ctx => ?WEBDAV_TOKEN_USER_CTX,
    helper_name => ?WEBDAV_HELPER_NAME,
    storage_record => ?WEBDAV_TOKEN_STORAGE_DOC_SECURE
}).

-define(SECURE_WEBDAV_NONE_STORAGE_CONFIG, #{
    name => "secure WEBDAV_NONE storage",
    admin_ctx => ?WEBDAV_NONE_CTX,
    user_ctx => ?WEBDAV_NONE_CTX,
    helper_name => ?WEBDAV_HELPER_NAME,
    storage_record => ?WEBDAV_NONE_STORAGE_DOC_SECURE
}).

-define(SECURE_WEBDAV_OAUTH2_STORAGE_CONFIG, #{
    name => "secure WEBDAV_OAUTH2 storage",
    admin_ctx => ?EXPECTED_WEBDAV_OAUTH2_ADMIN_CTX,
    user_ctx => ?EXPECTED_WEBDAV_OAUTH2_USER_CTX,
    user_ctx_luma_mock => ?WEBDAV_OAUTH2_USER_CTX,
    helper_name => ?WEBDAV_HELPER_NAME,
    storage_record => ?WEBDAV_OAUTH2_STORAGE_DOC_SECURE
}).


-define(SECURE_STORAGE_CONFIGS, [
    ?SECURE_POSIX_STORAGE_CONFIG,
    ?SECURE_CEPH_STORAGE_CONFIG,
    ?SECURE_S3_STORAGE_CONFIG,
    ?SECURE_SWIFT_STORAGE_CONFIG,
    ?SECURE_GLUSTERFS_STORAGE_CONFIG,
    ?SECURE_NULLDEVICE_STORAGE_CONFIG,
    ?SECURE_WEBDAV_BASIC_STORAGE_CONFIG,
    ?SECURE_WEBDAV_TOKEN_STORAGE_CONFIG,
    ?SECURE_WEBDAV_NONE_STORAGE_CONFIG,
    ?SECURE_WEBDAV_OAUTH2_STORAGE_CONFIG
]).

-define(INSECURE_POSIX_STORAGE_CONFIG, #{
    name => "insecure POSIX storage",
    admin_ctx => ?POSIX_ADMIN_CTX,
    user_ctx => ?POSIX_USER_CTX,
    helper_name => ?POSIX_HELPER_NAME,
    storage_record => ?POSIX_STORAGE_DOC_INSECURE
}).

-define(INSECURE_CEPH_STORAGE_CONFIG, #{
    name => "insecure CEPH storage",
    admin_ctx => ?CEPH_ADMIN_CTX,
    user_ctx => ?CEPH_USER_CTX,
    helper_name => ?CEPH_HELPER_NAME,
    storage_record => ?CEPH_STORAGE_DOC_INSECURE
}).

-define(INSECURE_S3_STORAGE_CONFIG, #{
    name => "insecure S3 storage",
    admin_ctx => ?S3_ADMIN_CTX,
    user_ctx => ?S3_USER_CTX,
    helper_name => ?S3_HELPER_NAME,
    storage_record => ?S3_STORAGE_DOC_INSECURE
}).

-define(INSECURE_SWIFT_STORAGE_CONFIG, #{
    name => "insecure SWIFT storage",
    admin_ctx => ?SWIFT_ADMIN_CTX,
    user_ctx => ?SWIFT_USER_CTX,
    helper_name => ?SWIFT_HELPER_NAME,
    storage_record => ?SWIFT_STORAGE_DOC_INSECURE
}).

-define(INSECURE_CEPHRADOS_STORAGE_CONFIG, #{
    name => "insecure CEPHRADOS storage",
    admin_ctx => ?CEPHRADOS_ADMIN_CTX,
    user_ctx => ?CEPHRADOS_USER_CTX,
    helper_name => ?CEPHRADOS_HELPER_NAME,
    storage_record => ?CEPHRADOS_STORAGE_DOC_INSECURE
}).

-define(INSECURE_GLUSTERFS_STORAGE_CONFIG, #{
    name => "insecure GLUSTERFS storage",
    admin_ctx => ?GLUSTERFS_ADMIN_CTX,
    user_ctx => ?GLUSTERFS_USER_CTX,
    helper_name => ?GLUSTERFS_HELPER_NAME,
    storage_record => ?GLUSTERFS_STORAGE_DOC_INSECURE
}).

-define(INSECURE_NULLDEVICE_STORAGE_CONFIG, #{
    name => "insecure NULLDEVICE storage",
    admin_ctx => ?NULLDEVICE_ADMIN_CTX,
    user_ctx => ?NULLDEVICE_USER_CTX,
    helper_name => ?NULL_DEVICE_HELPER_NAME,
    storage_record => ?NULLDEVICE_STORAGE_DOC_INSECURE
}).

-define(INSECURE_WEBDAV_BASIC_STORAGE_CONFIG, #{
    name => "insecure WEBDAV_BASIC storage",
    admin_ctx => ?WEBDAV_BASIC_ADMIN_CTX,
    user_ctx => ?WEBDAV_BASIC_USER_CTX,
    helper_name => ?WEBDAV_HELPER_NAME,
    storage_record => ?WEBDAV_BASIC_STORAGE_DOC_INSECURE
}).

-define(INSECURE_WEBDAV_TOKEN_STORAGE_CONFIG, #{
    name => "insecure WEBDAV_TOKEN storage",
    admin_ctx => ?WEBDAV_TOKEN_ADMIN_CTX,
    user_ctx => ?WEBDAV_TOKEN_USER_CTX,
    helper_name => ?WEBDAV_HELPER_NAME,
    storage_record => ?WEBDAV_TOKEN_STORAGE_DOC_INSECURE
}).

-define(INSECURE_WEBDAV_NONE_STORAGE_CONFIG, #{
    name => "insecure WEBDAV_NONE storage",
    admin_ctx => ?WEBDAV_NONE_CTX,
    user_ctx => ?WEBDAV_NONE_CTX,
    helper_name => ?WEBDAV_HELPER_NAME,
    storage_record => ?WEBDAV_NONE_STORAGE_DOC_INSECURE
}).

-define(INSECURE_WEBDAV_OAUTH2_STORAGE_CONFIG, #{
    name => "insecure WEBDAV_OAUTH2 storage",
    admin_ctx => ?EXPECTED_WEBDAV_OAUTH2_ADMIN_CTX,
    user_ctx => ?EXPECTED_WEBDAV_OAUTH2_USER_CTX,
    helper_name => ?WEBDAV_HELPER_NAME,
    storage_record => ?WEBDAV_OAUTH2_STORAGE_DOC_INSECURE
}).

-define(INSECURE_STORAGE_CONFIGS, [
    ?INSECURE_POSIX_STORAGE_CONFIG,
    ?INSECURE_CEPH_STORAGE_CONFIG,
    ?INSECURE_S3_STORAGE_CONFIG,
    ?INSECURE_SWIFT_STORAGE_CONFIG,
    ?INSECURE_CEPHRADOS_STORAGE_CONFIG,
    ?INSECURE_GLUSTERFS_STORAGE_CONFIG,
    ?INSECURE_NULLDEVICE_STORAGE_CONFIG,
    ?INSECURE_WEBDAV_BASIC_STORAGE_CONFIG,
    ?INSECURE_WEBDAV_TOKEN_STORAGE_CONFIG,
    ?INSECURE_WEBDAV_NONE_STORAGE_CONFIG,
    ?INSECURE_WEBDAV_OAUTH2_STORAGE_CONFIG
]).

-define(ALL_STORAGE_CONFIGS,
    ?SECURE_STORAGE_CONFIGS ++ ?INSECURE_STORAGE_CONFIGS).

-define(INSECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS, [
    ?INSECURE_POSIX_STORAGE_CONFIG, ?INSECURE_NULLDEVICE_STORAGE_CONFIG,
    ?INSECURE_GLUSTERFS_STORAGE_CONFIG
]).

-define(SECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS, [
    ?SECURE_POSIX_STORAGE_CONFIG, ?SECURE_NULLDEVICE_STORAGE_CONFIG,
    ?SECURE_GLUSTERFS_STORAGE_CONFIG
]).

-define(INSECURE_POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
    ?INSECURE_STORAGE_CONFIGS -- ?INSECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS
).

-define(POSIX_COMPATIBLE_STORAGE_CONFIGS,
    ?INSECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS ++ ?SECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS).

-define(POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
    ?ALL_STORAGE_CONFIGS -- ?POSIX_COMPATIBLE_STORAGE_CONFIGS).

%%%===================================================================
%%% Possible erroneous LUMA responses
%%%===================================================================

-define(POSIX_ERRONEOUS_RESPONSES, [
    {<<"{\"gid\": 2}">>, {missing_field, <<"uid">>}},
    {<<"{\"uid\": \[1,2,3\],\"gid\": 2}">>, {invalid_field_value, <<"uid">>, [1, 2, 3]}},
    {<<"{\"uid\": \"null\",\"gid\": 2}">>, {invalid_field_value, <<"uid">>, <<"null">>}},
    {<<"{\"uid\": null,\"gid\": 2}">>, {invalid_field_value, <<"uid">>, null}},
    {<<"{\"uid\": 1,\"gid\": 2,\"other\": \"value\"}">>,
        {invalid_additional_fields, #{<<"other">> => <<"value">>}}}
]).

-define(CEPH_ERRONEOUS_RESPONSES, [
    {<<"{\"username\": \"some_user\"}">>, {missing_field, <<"key">>}},
    {<<"{\"username\": \[1,2,3\],\"key\": \"some_key\"}">>, {invalid_field_value, <<"username">>, [1, 2, 3]}},
    {<<"{\"username\": \"null\",\"key\": \"some_key\"}">>, {invalid_field_value, <<"username">>, <<"null">>}},
    {<<"{\"username\": null,\"key\": \"some_key\"}">>, {invalid_field_value, <<"username">>, null}},
    {<<"{\"username\": \"some_user\",\"key\": \"some_key\",\"other\": \"value\"}">>,
        {invalid_additional_fields, #{<<"other">> => <<"value">>}}}
]).

-define(S3_ERRONEOUS_RESPONSES, [
    {<<"{\"accessKey\": \"some_key\"}">>, {missing_field, <<"secretKey">>}},
    {<<"{\"accessKey\": \[1,2,3\],\"secretKey\": \"some_key\"}">>, {invalid_field_value, <<"accessKey">>, [1, 2, 3]}},
    {<<"{\"accessKey\": \"null\",\"secretKey\": \"some_key\"}">>, {invalid_field_value, <<"accessKey">>, <<"null">>}},
    {<<"{\"accessKey\": null,\"secretKey\": \"some_key\"}">>, {invalid_field_value, <<"accessKey">>, null}},
    {<<"{\"accessKey\": \"some_key\",\"secretKey\": \"some_key\",\"other\": \"value\"}">>,
        {invalid_additional_fields, #{<<"other">> => <<"value">>}}}
]).

-define(SWIFT_ERRONEOUS_RESPONSES, [
    {<<"{\"username\": \"some_user\"}">>, {missing_field, <<"password">>}},
    {<<"{\"username\": \[1,2,3\],\"password\": \"some_key\"}">>, {invalid_field_value, <<"username">>, [1, 2, 3]}},
    {<<"{\"username\": \"null\",\"password\": \"some_key\"}">>, {invalid_field_value, <<"username">>, <<"null">>}},
    {<<"{\"username\": null,\"password\": \"some_key\"}">>, {invalid_field_value, <<"username">>, null}},
    {<<"{\"username\": \"some_user\",\"password\": \"some_key\",\"other\": \"value\"}">>,
        {invalid_additional_fields, #{<<"other">> => <<"value">>}}}
]).


-define(WEBDAV_ERRONEOUS_RESPONSES, [
    {<<"{\"credentials\": \"some_creds\"}">>, {missing_field, <<"credentialsType">>}},
    {<<"{\"credentialsType\": \[1,2,3\],\"credentials\": \"some_creds\"}">>, {invalid_field_value, <<"credentialsType">>, [1, 2, 3]}},
    {<<"{\"credentialsType\": \"null\",\"credentials\": \"some_creds\"}">>, {invalid_field_value, <<"credentialsType">>, <<"null">>}},
    {<<"{\"credentialsType\": null,\"credentials\": \"some_creds\"}">>, {invalid_field_value, <<"credentialsType">>, null}},
    {<<"{\"credentialsType\": \"some_type\",\"credentials\": \"some_creds\",\"onedataAccessToken\": \"someToken\",\"other\": \"value\"}">>,
        {invalid_additional_fields, #{<<"other">> => <<"value">>}}}
]).

-define(HELPERS_TO_ERRONEOUS_LUMA_RESPONSES_MAP, #{
    ?POSIX_HELPER_NAME => ?POSIX_ERRONEOUS_RESPONSES,
    ?GLUSTERFS_HELPER_NAME => ?POSIX_ERRONEOUS_RESPONSES,
    ?NULL_DEVICE_HELPER_NAME => ?POSIX_ERRONEOUS_RESPONSES,
    ?CEPH_HELPER_NAME => ?CEPH_ERRONEOUS_RESPONSES,
    ?CEPHRADOS_HELPER_NAME => ?CEPH_ERRONEOUS_RESPONSES,
    ?S3_HELPER_NAME => ?S3_ERRONEOUS_RESPONSES,
    ?SWIFT_HELPER_NAME => ?SWIFT_ERRONEOUS_RESPONSES,
    ?WEBDAV_HELPER_NAME => ?WEBDAV_ERRONEOUS_RESPONSES
}).


-endif.