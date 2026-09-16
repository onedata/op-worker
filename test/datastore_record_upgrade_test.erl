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
            {storage_config, {helper_spec, ?S3_HELPER_NAME, undefined, Args, AdminCtx}, AutoFeedLumaConfig, 0}
        },
        {3, "with timeout and archive storage option",
            {storage_config,
                {helper, ?S3_HELPER_NAME, Args#{<<"timeout">> => <<"120000">>, <<"archiveStorage">> => <<"true">>}, AdminCtx},
                ExternalFeedLumaConfig
            },
            {storage_config, {helper_spec, ?S3_HELPER_NAME, 120000, Args, AdminCtx}, ExternalFeedLumaConfig, 0}
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
