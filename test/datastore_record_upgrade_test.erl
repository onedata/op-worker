%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of datastore record upgrades. Every upgrade step of a model's
%%% record (from one version to the next) is tested on its own, so introducing
%%% a new record version leaves the tests of the earlier steps intact.
%%%
%%% Records are written as raw tuples in the shape of their version, as record
%%% definitions follow only the current one. For each model tested here all of
%%% its upgrade steps must be covered - a new record version requires a test of
%%% the step leading to it.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_record_upgrade_test).
-author("Bartosz Walkowicz").

-include("modules/storage/helpers/helpers.hrl").
-include("modules/storage/luma/luma.hrl").
-include_lib("eunit/include/eunit.hrl").


%%%===================================================================
%%% Tests
%%%===================================================================


storage_config_upgrade_test_() ->
    Args = #{<<"hostname">> => <<"hostname">>, <<"bucketName">> => <<"bucketName">>},
    AdminCtx = #{<<"accessKey">> => <<"accessKey">>, <<"secretKey">> => <<"secretKey">>},
    Helper = {helper, ?S3_HELPER_NAME, Args, AdminCtx},
    LumaUrl = <<"https://luma.example.com">>,
    LumaApiKey = <<"lumaApiKey">>,
    AutoFeedLumaConfig = {luma_config, ?AUTO_FEED, undefined, undefined},
    ExternalFeedLumaConfig = {luma_config, ?EXTERNAL_FEED, LumaUrl, LumaApiKey},
    WebdavConfiguration = #{<<"endpoint">> => <<"https://webdav.example.com">>, <<"storagePathType">> => ?FLAT_STORAGE_PATH},
    HttpConfiguration = #{<<"endpoint">> => <<"https://http.example.com">>, <<"storagePathType">> => ?FLAT_STORAGE_PATH},
    OAuth2Credentials = #{
        <<"credentialsType">> => <<"oauth2">>,
        <<"credentials">> => <<"credentials">>,
        <<"onedataAccessToken">> => <<"onedataAccessToken">>,
        <<"adminId">> => <<"adminId">>
    },
    GlusterfsConfiguration = #{
        <<"volume">> => <<"volume">>,
        <<"hostname">> => <<"hostname">>,
        <<"storagePathType">> => ?CANONICAL_STORAGE_PATH
    },
    NfsConfiguration = #{
        <<"host">> => <<"host">>,
        <<"volume">> => <<"/volume">>,
        <<"storagePathType">> => ?CANONICAL_STORAGE_PATH
    },

    upgrade_steps_tests(storage_config, [
        {1, "writable storage without LUMA",
            {storage_config,
                {helper, ?S3_HELPER_NAME, Args, AdminCtx, true, true, ?FLAT_STORAGE_PATH},
                false,
                undefined,
                false
            },
            {storage_config,
                {helper, ?S3_HELPER_NAME, Args#{
                    <<"skipStorageDetection">> => <<"false">>,
                    <<"storagePathType">> => ?FLAT_STORAGE_PATH
                }, AdminCtx},
                AutoFeedLumaConfig,
                false
            }
        },
        {1, "readonly storage with external LUMA",
            {storage_config,
                {helper, ?S3_HELPER_NAME, Args, AdminCtx, false, false, ?CANONICAL_STORAGE_PATH},
                true,
                {luma_config, LumaUrl, LumaApiKey},
                true
            },
            {storage_config,
                {helper, ?S3_HELPER_NAME, Args#{
                    <<"skipStorageDetection">> => <<"true">>,
                    <<"storagePathType">> => ?CANONICAL_STORAGE_PATH
                }, AdminCtx},
                ExternalFeedLumaConfig,
                true
            }
        },
        {2, "imported storage",
            {storage_config, Helper, ExternalFeedLumaConfig, true},
            {storage_config, Helper, ExternalFeedLumaConfig}
        },
        {3, "without timeout",
            {storage_config, Helper, AutoFeedLumaConfig},
            {storage_config, {helper_spec, ?S3_HELPER_NAME, undefined, Args, AdminCtx}, AutoFeedLumaConfig, undefined, []}
        },
        {3, "with timeout and archive storage option",
            {storage_config,
                {helper, ?S3_HELPER_NAME, Args#{<<"timeout">> => <<"120000">>, <<"archiveStorage">> => <<"true">>}, AdminCtx},
                ExternalFeedLumaConfig
            },
            {storage_config, {helper_spec, ?S3_HELPER_NAME, 120000, Args, AdminCtx}, ExternalFeedLumaConfig, undefined, []}
        },
        {4, "storage with nothing to move or drop keeps its LUMA DB namespaces",
            {storage_config, {helper_spec, ?S3_HELPER_NAME, 120000, Args, AdminCtx},
                ExternalFeedLumaConfig, <<"namespace2">>, [undefined, <<"namespace1">>]},
            {storage_config, {helper_spec, ?S3_HELPER_NAME, 120000, Args, AdminCtx},
                ExternalFeedLumaConfig, <<"namespace2">>, [undefined, <<"namespace1">>]}
        },
        {4, "WebDAV storage with the OAuth2 IdP among the configuration params",
            {storage_config,
                {helper_spec, ?WEBDAV_HELPER_NAME, undefined, WebdavConfiguration#{<<"oauth2IdP">> => <<"idp">>},
                    OAuth2Credentials},
                AutoFeedLumaConfig, undefined, []
            },
            {storage_config,
                {helper_spec, ?WEBDAV_HELPER_NAME, undefined, WebdavConfiguration,
                    OAuth2Credentials#{<<"oauth2IdP">> => <<"idp">>}},
                AutoFeedLumaConfig, undefined, []
            }
        },
        {4, "HTTP storage with the OAuth2 IdP among both the configuration and the credentials params",
            {storage_config,
                {helper_spec, ?HTTP_HELPER_NAME, undefined, HttpConfiguration#{<<"oauth2IdP">> => <<"oldIdp">>},
                    OAuth2Credentials#{<<"oauth2IdP">> => <<"newIdp">>}},
                AutoFeedLumaConfig, undefined, []
            },
            {storage_config,
                {helper_spec, ?HTTP_HELPER_NAME, undefined, HttpConfiguration,
                    OAuth2Credentials#{<<"oauth2IdP">> => <<"newIdp">>}},
                AutoFeedLumaConfig, undefined, []
            }
        },
        {4, "S3 storage with the params left from the storage's args",
            {storage_config,
                {helper_spec, ?S3_HELPER_NAME, undefined, Args#{
                    <<"blockSize">> => <<"0">>,
                    <<"maximumCanonicalObjectSize">> => <<"67108864">>,
                    <<"skipStorageDetection">> => <<"false">>
                }, AdminCtx},
                AutoFeedLumaConfig, undefined, []
            },
            {storage_config,
                {helper_spec, ?S3_HELPER_NAME, undefined, Args#{<<"blockSize">> => <<"0">>}, AdminCtx},
                AutoFeedLumaConfig, undefined, []
            }
        },
        {4, "GlusterFS storage with a block size",
            {storage_config,
                {helper_spec, ?GLUSTERFS_HELPER_NAME, undefined, GlusterfsConfiguration#{
                    <<"blockSize">> => <<"4194304">>,
                    <<"skipStorageDetection">> => <<"true">>
                }, #{}},
                AutoFeedLumaConfig, undefined, []
            },
            {storage_config,
                {helper_spec, ?GLUSTERFS_HELPER_NAME, undefined, GlusterfsConfiguration, #{}},
                AutoFeedLumaConfig, undefined, []
            }
        },
        {4, "NFS storage keeps the param read only by the storage helper",
            {storage_config,
                {helper_spec, ?NFS_HELPER_NAME, undefined, NfsConfiguration#{
                    <<"tcpSyncnt">> => <<"2">>,
                    <<"skipStorageDetection">> => <<"false">>
                }, #{}},
                AutoFeedLumaConfig, undefined, []
            },
            {storage_config,
                {helper_spec, ?NFS_HELPER_NAME, undefined, NfsConfiguration#{<<"tcpSyncnt">> => <<"2">>}, #{}},
                AutoFeedLumaConfig, undefined, []
            }
        }
    ]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


upgrade_steps_tests(Model, Steps) ->
    TestedVersions = lists:usort([Version || {Version, _Description, _Record, _ExpUpgradedRecord} <- Steps]),
    AllVersions = lists:seq(1, Model:get_record_version() - 1),
    CoverageTest = {
        title("~tp: all upgrade steps are tested", [Model]),
        ?_assertEqual(AllVersions, TestedVersions)
    },

    StepTests = lists:map(fun({Version, Description, Record, ExpUpgradedRecord}) -> {
        title("~tp ~B -> ~B: ~ts", [Model, Version, Version + 1, Description]),
        ?_assertEqual({Version + 1, ExpUpgradedRecord}, Model:upgrade_record(Version, Record))
    } end, Steps),

    [CoverageTest | StepTests].


title(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).
