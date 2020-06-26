%%%-------------------------------------------------------------------
%%% @author Wojciech Geisler
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Test for creation of helper records.
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_creation_test).
-author("Wojciech Geisler").

-ifdef(TEST).

-include("global_definitions.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(USER_CTXS, [
    {?CEPH_HELPER_NAME, [<<"username">>, <<"key">>]},
    {?CEPHRADOS_HELPER_NAME, [<<"username">>, <<"key">>]},
    {?POSIX_HELPER_NAME, []},
    {?S3_HELPER_NAME, [<<"accessKey">>, <<"secretKey">>]},
    {?SWIFT_HELPER_NAME, [<<"username">>, <<"password">>]},
    {?GLUSTERFS_HELPER_NAME, []},
    {?WEBDAV_HELPER_NAME, [<<"credentialsType">>, <<"credentials">>]},
    {?NULL_DEVICE_HELPER_NAME, []}
]).

-define(HELPER_ARGS, [
    {?CEPH_HELPER_NAME, [<<"monitorHostname">>, <<"clusterName">>, <<"poolName">>]},
    {?CEPHRADOS_HELPER_NAME, [<<"monitorHostname">>, <<"clusterName">>, <<"poolName">>]},
    {?POSIX_HELPER_NAME, [<<"mountPoint">>]},
    {?S3_HELPER_NAME, [<<"hostname">>, <<"bucketName">>, <<"scheme">>]},
    {?SWIFT_HELPER_NAME, [<<"authUrl">>, <<"containerName">>, <<"tenantName">>]},
    {?GLUSTERFS_HELPER_NAME, [<<"volume">>, <<"hostname">>]},
    {?WEBDAV_HELPER_NAME, [<<"endpoint">>]},
    {?NULL_DEVICE_HELPER_NAME, []}
]).


new_helper_test_() ->
    lists:map(fun({HelperName, ArgsKeys}) ->
        Args = keys_to_map(ArgsKeys),
        Args2 = Args#{
            <<"storagePathType">> => <<"flat">>,
            <<"skipStorageDetection">> => <<"false">>
        },
        AdminCtx = keys_to_map(proplists:get_value(HelperName, ?USER_CTXS)),

        {str_utils:format("~s helper should be created", [HelperName]),
            ?_assertMatch({ok, #helper{}},
                helper:new_helper(HelperName, Args2, AdminCtx))}
    end, ?HELPER_ARGS).


user_ctx_validation_test_() ->
    lists:flatmap(fun({HelperName, AdminCtxKeys}) ->
        Args = keys_to_map(proplists:get_value(HelperName, ?HELPER_ARGS)),
        Args2 = Args#{
            <<"storagePathType">> => <<"flat">>,
            <<"skipStorageDetection">> => <<"false">>
        },
        AdminCtx = keys_to_map(AdminCtxKeys),

        lists:map(fun(Remove) ->
            BadCtx = maps:without([Remove], AdminCtx),
            {ctx_test_name(HelperName, Remove), ?_assertError(_,
                helper:new_helper(HelperName, Args2, BadCtx)
            )}
        end, maps:keys(AdminCtx))
    end, ?USER_CTXS).

ctx_test_name(HelperName, Key) ->
    str_utils:format("~s helper creation should fail without ~p in admin ctx",
        [HelperName, Key]).


helper_args_validation_test_() ->
    lists:flatmap(fun({HelperName, ArgsKeys}) ->
        Args = keys_to_map(ArgsKeys),
        Args2 = Args#{
            <<"storagePathType">> => <<"flat">>,
            <<"skipStorageDetection">> => <<"false">>
        },
        AdminCtx = keys_to_map(proplists:get_value(HelperName, ?USER_CTXS)),

        lists:map(fun(Remove) ->
            BadArgs = maps:without([Remove], Args2),
            {args_test_name(HelperName, Remove), ?_assertError(_,
                helper:new_helper(HelperName, BadArgs, AdminCtx)
            )}
        end, maps:keys(Args))
    end, ?HELPER_ARGS).

args_test_name(HelperName, Key) ->
    str_utils:format("~s helper creation should fail without ~p in helper args",
        [HelperName, Key]).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec keys_to_map([Key]) -> #{Key => Key}.
keys_to_map(Keys) ->
    maps:from_list(lists:zip(Keys, Keys)).

-endif.

