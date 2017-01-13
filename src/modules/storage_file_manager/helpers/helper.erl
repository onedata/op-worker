%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides a synchronous interface to the helpers NIF library.
%%% It wraps {@link helpers_nif} module by calling its functions and awaiting
%%% results.
%%% @end
%%%-------------------------------------------------------------------
-module(helper).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/fuse_messages.hrl").

%% API
-export([new_ceph_helper/6, new_posix_helper/3, new_s3_helper/6,
    new_swift_helper/6]).
-export([new_ceph_user_ctx/2, new_posix_user_ctx/2, new_s3_user_ctx/2,
    new_swift_user_ctx/2, validate_user_ctx/2]).
-export([get_name/1, get_args/1, is_insecure/1, get_params/2,
    get_proxy_params/2, get_timeout/1]).
-export([set_user_ctx/2]).

-type name() :: binary().
-type args() :: #{binary() => binary()}.
-type params() :: #helper_params{}.
-type user_ctx() :: #{binary() => binary()}.

-export_type([name/0, args/0, params/0, user_ctx/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Constructs Ceph storage helper record.
%% @end
%%--------------------------------------------------------------------
-spec new_ceph_helper(binary(), binary(), binary(), args(), user_ctx(), boolean()) ->
    helpers:helper().
new_ceph_helper(MonitorHostname, ClusterName, PoolName, OptArgs, AdminCtx,
    Insecure) ->

    #helper{
        name = ?CEPH_HELPER_NAME,
        args = maps:merge(OptArgs, #{
            <<"monitorHostname">> => MonitorHostname,
            <<"clusterName">> => ClusterName,
            <<"poolName">> => PoolName
        }),
        admin_ctx = AdminCtx,
        insecure = Insecure
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs POSIX storage helper record.
%% @end
%%--------------------------------------------------------------------
-spec new_posix_helper(binary(), args(), user_ctx()) -> helpers:helper().
new_posix_helper(MountPoint, OptArgs, AdminCtx) ->
    #helper{
        name = ?POSIX_HELPER_NAME,
        args = maps:merge(OptArgs, #{<<"mountPoint">> => MountPoint}),
        admin_ctx = AdminCtx,
        insecure = false
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs S3 storage helper record.
%% @end
%%--------------------------------------------------------------------
-spec new_s3_helper(binary(), binary(), boolean(), args(), user_ctx(), boolean()) ->
    helpers:helper().
new_s3_helper(Hostname, BucketName, UseHttps, OptArgs, AdminCtx, Insecure) ->
    #helper{
        name = ?S3_HELPER_NAME,
        args = maps:merge(OptArgs, #{
            <<"hostname">> => Hostname,
            <<"bucketName">> => BucketName,
            <<"scheme">> => case UseHttps of
                true -> <<"https">>;
                false -> <<"http">>
            end
        }),
        admin_ctx = AdminCtx,
        insecure = Insecure
    }.

%%--------------------------------------------------------------------
%% @doc
%% Constructs Swift storage helper record.
%% @end
%%--------------------------------------------------------------------
-spec new_swift_helper(binary(), binary(), binary(), args(), user_ctx(), boolean()) ->
    helpers:helper().
new_swift_helper(AuthUrl, ContainerName, TenantName, OptArgs, AdminCtx, Insecure) ->
    #helper{
        name = ?SWIFT_HELPER_NAME,
        args = maps:merge(OptArgs, #{
            <<"authUrl">> => AuthUrl,
            <<"containerName">> => ContainerName,
            <<"tenantName">> => TenantName
        }),
        admin_ctx = AdminCtx,
        insecure = Insecure
    }.

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
%% Checks whether user context is valid for the storage helper.
%% @end
%%--------------------------------------------------------------------
-spec validate_user_ctx(storage:helper(), user_ctx()) ->
    ok | {error, Reason :: term()}.
validate_user_ctx(#helper{name = ?CEPH_HELPER_NAME}, UserCtx) ->
    check_user_ctx_fields([<<"username">>, <<"key">>], UserCtx);
validate_user_ctx(#helper{name = ?POSIX_HELPER_NAME}, UserCtx) ->
    check_user_ctx_fields([<<"uid">>, <<"gid">>], UserCtx);
validate_user_ctx(#helper{name = ?S3_HELPER_NAME}, UserCtx) ->
    check_user_ctx_fields([<<"accessKey">>, <<"secretKey">>], UserCtx);
validate_user_ctx(#helper{name = ?SWIFT_HELPER_NAME}, UserCtx) ->
    check_user_ctx_fields([<<"username">>, <<"password">>], UserCtx).

%%--------------------------------------------------------------------
%% @doc
%% Returns helper name.
%% @end
%%--------------------------------------------------------------------
-spec get_name(helpers:helper()) -> name().
get_name(#helper{name = Name}) ->
    Name.

%%--------------------------------------------------------------------
%% @doc
%% Returns helper arguments.
%% @end
%%--------------------------------------------------------------------
-spec get_args(helpers:helper()) -> args().
get_args(#helper{args = Args}) ->
    Args.

%%--------------------------------------------------------------------
%% @doc
%% Returns helper insecure status.
%% @end
%%--------------------------------------------------------------------
-spec is_insecure(helpers:helper()) -> boolean().
is_insecure(#helper{insecure = Insecure}) ->
    Insecure.

%%--------------------------------------------------------------------
%% @doc
%% Returns helper parameters.
%% @end
%%--------------------------------------------------------------------
-spec get_params(helpers:helper(), user_ctx()) -> params().
get_params(Helper, UserCtx) ->
    {ok, #helper{name = Name, args = Args}} = set_user_ctx(Helper, UserCtx),
    #helper_params{
        helper_name = Name,
        helper_args = maps:fold(fun(Key, Value, Acc) ->
            [#helper_arg{key = Key, value = Value} | Acc]
        end, [], Args)
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns proxy helper parameters.
%% @end
%%--------------------------------------------------------------------
-spec get_proxy_params(helpers:helper(), storage:id()) -> params().
get_proxy_params(Helper, StorageId) ->
    Timeout = get_timeout(Helper),
    {ok, Latency} = application:get_env(?APP_NAME, proxy_helper_latency_milliseconds),
    TimeoutValue = integer_to_binary(Timeout + Latency),
    #helper_params{
        helper_name = ?PROXY_HELPER_NAME,
        helper_args = [
            #helper_arg{key = <<"storageId">>, value = StorageId},
            #helper_arg{key = <<"timeout">>, value = TimeoutValue}
        ]
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns timeout for storage helper async operations.
%% @end
%%--------------------------------------------------------------------
-spec get_timeout(helpers:helper()) -> Timeout :: timeout().
get_timeout(#helper{args = Args}) ->
    case maps:find(<<"timeout">>, Args) of
        {ok, Value} ->
            erlang:binary_to_integer(Value);
        error ->
            {ok, Value} = application:get_env(?APP_NAME,
                helpers_async_operation_timeout_milliseconds),
            Value
    end.

%%--------------------------------------------------------------------
%% @doc
%% Injects user context into helper parameters.
%% @end
%%--------------------------------------------------------------------
-spec set_user_ctx(helpers:helper(), user_ctx()) ->
    {ok, helpers:helper()} | {error, Reason :: term()}.
set_user_ctx(#helper{args = Args} = Helper, UserCtx) ->
    case validate_user_ctx(Helper, UserCtx) of
        ok -> {ok, Helper#helper{args = maps:merge(Args, UserCtx)}};
        {error, Reason} -> {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether user context map contains only provided fields and they
%% have valid type.
%% @end
%%--------------------------------------------------------------------
-spec check_user_ctx_fields([binary()], user_ctx()) ->
    ok | {error, Reason :: term()}.
check_user_ctx_fields([], UserCtx) ->
    case maps:size(UserCtx) of
        0 -> ok;
        _ -> {error, {invalid_additional_fields, UserCtx}}
    end;
check_user_ctx_fields([Field | Fields], UserCtx) ->
    case maps:find(Field, UserCtx) of
        {ok, <<_/binary>>} ->
            check_user_ctx_fields(Fields, maps:remove(Field, UserCtx));
        {ok, _} ->
            {error, {invalid_field_value, Field}};
        error ->
            {error, {missing_field, Field}}
    end.