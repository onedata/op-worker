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
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([new_helper/5]).
-export([validate_user_ctx/2, validate_group_ctx/2]).
-export([get_name/1, get_args/1, get_admin_ctx/1, is_insecure/1, get_params/2,
    get_proxy_params/2, get_timeout/1, get_storage_path_type/1]).
-export([set_user_ctx/2]).
-export([translate_name/1, translate_arg_name/1]).
-export([webdav_fill_admin_id/1]).

%% Test utils
-export([new_ceph_user_ctx/2,new_cephrados_user_ctx/2,  new_posix_user_ctx/2,
    new_s3_user_ctx/2, new_swift_user_ctx/2, new_glusterfs_user_ctx/2,
    new_webdav_user_ctx/2, new_webdav_user_ctx/3, new_nulldevice_user_ctx/2]).


%% For Onepanel RPC
-export([filter_args/2, filter_user_ctx/2]).

-type name() :: binary().
-type args() :: #{binary() => binary()}.
-type params() :: #helper_params{}.
-type user_ctx() :: #{binary() => binary() | integer()}.
-type group_ctx() :: #{binary() => binary() | integer()}.
-type optional_field() :: {optional, binary()}.

-export_type([name/0, args/0, params/0, user_ctx/0, group_ctx/0]).



%%%===================================================================
%%% API
%%%===================================================================

-spec new_helper(name(), args(), user_ctx(), Insecure :: boolean(),
    storage_path_type()) -> {ok, helpers:helper()}.
new_helper(HelperName, Args, AdminCtx, Insecure, StoragePathType) ->
    {Required, _} = expected_helper_args(HelperName),
    ok = check_args(Required, Args),
    ok = validate_user_ctx(HelperName, AdminCtx),
    {ok, #helper{
        name = HelperName,
        args = Args,
        admin_ctx = AdminCtx,
        insecure = Insecure andalso allow_insecure(HelperName),
        extended_direct_io = extended_direct_io(HelperName),
        storage_path_type = StoragePathType
    }}.


%% @private
-spec extended_direct_io(name()) -> boolean().
extended_direct_io(?POSIX_HELPER_NAME) -> false;
extended_direct_io(_) -> true.


%% @private
-spec allow_insecure(name()) -> boolean().
allow_insecure(?POSIX_HELPER_NAME) -> false;
allow_insecure(_) -> true.


%%--------------------------------------------------------------------
%% @doc
%% Removes unknown keys from helper arguments map.
%% @end
%%--------------------------------------------------------------------
-spec filter_args(name(), #{binary() => term()}) -> #{binary() => term()}.
filter_args(HelperName, Args) ->
    {Required, Optional} = expected_helper_args(HelperName),
    maps:with(Required ++ Optional, Args).


%% @private
-spec expected_helper_args(name()) ->
    {Required :: [binary()], Optional :: [binary()]}.
expected_helper_args(?CEPH_HELPER_NAME) ->
    {
        [<<"monitorHostname">>, <<"clusterName">>, <<"poolName">>],
        [<<"timeout">>]
    };
expected_helper_args(?CEPHRADOS_HELPER_NAME) ->
    {
        [<<"monitorHostname">>, <<"clusterName">>, <<"poolName">>],
        [<<"timeout">>, <<"blockSize">>]
    };
expected_helper_args(?POSIX_HELPER_NAME) ->
    {
        [<<"mountPoint">>],
        [<<"timeout">>]
    };
expected_helper_args(?S3_HELPER_NAME) ->
    {
        [<<"hostname">>, <<"bucketName">>, <<"scheme">>],
        [<<"timeout">>, <<"signatureVersion">>, <<"blockSize">>]
    };
expected_helper_args(?SWIFT_HELPER_NAME) ->
    {
        [<<"authUrl">>, <<"containerName">>, <<"tenantName">>],
        [<<"timeout">>, <<"blockSize">>]
    };
expected_helper_args(?GLUSTERFS_HELPER_NAME) ->
    {
        [<<"volume">>, <<"hostname">>],
        [<<"port">>, <<"mountPoint">>, <<"transport">>,
            <<"xlatorOptions">>, <<"timeout">>, <<"blockSize">>]
    };
expected_helper_args(?WEBDAV_HELPER_NAME) ->
    {
        [<<"endpoint">>],
        [<<"timeout">>, <<"verifyServerCertificate">>,
            <<"authorizationHeader">>, <<"rangeWriteSupport">>,
            <<"connectionPoolSize">>, <<"maximumUploadSize">>]
    };
expected_helper_args(?NULL_DEVICE_HELPER_NAME) ->
    {
        [],
        [<<"timeout">>, <<"latencyMin">>, <<"latencyMax">>,
            <<"timeoutProbability">>, <<"filter">>,
            <<"simulatedFilesystemParameters">>,
            <<"simulatedFilesystemGrowSpeed">>
        ]
    }.


%%--------------------------------------------------------------------
%% @doc
%% Removes unkown keys from helper user ctx parameters.
%% @end
%%--------------------------------------------------------------------
-spec filter_user_ctx(name(), #{binary() => term()}) -> #{binary() => term()}.
filter_user_ctx(HelperName, Params) ->
    Required = expected_user_ctx_params(HelperName, Params),
    Optional = optional_user_ctx_params(HelperName, Params),
    maps:with(Required ++ Optional, Params).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns required fields for the user ctx of given storage type.
%% @end
%%--------------------------------------------------------------------
-spec expected_user_ctx_params(name(), Params :: #{binary() => term()}) -> [binary()].
expected_user_ctx_params(?CEPH_HELPER_NAME, _) ->
    [<<"username">>, <<"key">>];
expected_user_ctx_params(?CEPHRADOS_HELPER_NAME, _) ->
    [<<"username">>, <<"key">>];
expected_user_ctx_params(?POSIX_HELPER_NAME, _) ->
    [<<"uid">>, <<"gid">>];
expected_user_ctx_params(?S3_HELPER_NAME, _) ->
    [<<"accessKey">>, <<"secretKey">>];
expected_user_ctx_params(?SWIFT_HELPER_NAME, _) ->
    [<<"username">>, <<"password">>];
expected_user_ctx_params(?GLUSTERFS_HELPER_NAME, _) ->
    [<<"uid">>, <<"gid">>];
expected_user_ctx_params(?WEBDAV_HELPER_NAME, #{<<"credentialsType">> := <<"none">>}) ->
    [<<"credentialsType">>];
expected_user_ctx_params(?WEBDAV_HELPER_NAME, _) ->
    [<<"credentialsType">>, <<"credentials">>];
expected_user_ctx_params(?NULL_DEVICE_HELPER_NAME, _) ->
    [<<"uid">>, <<"gid">>].


optional_user_ctx_params(?WEBDAV_HELPER_NAME, _) ->
    [<<"credentials">>, <<"adminId">>,
        <<"onedataAccessToken">>, <<"accessToken">>,
        <<"accessTokenTTL">>];

optional_user_ctx_params(_, _) ->
    [].


%%--------------------------------------------------------------------
%% @doc
%% Checks whether user context is valid for the storage helper.
%% @end
%%--------------------------------------------------------------------
-spec validate_user_ctx(storage:helper() | name(), user_ctx()) ->
    ok | {error, Reason :: term()}.
validate_user_ctx(#helper{name = StorageType}, UserCtx) ->
    validate_user_ctx(StorageType, UserCtx);

validate_user_ctx(StorageType, UserCtx) ->
    Required = expected_user_ctx_params(StorageType, UserCtx),
    Optional = [{optional, Field} || Field <-
        optional_user_ctx_params(StorageType, UserCtx) -- Required],
    Fields = Required ++ Optional,
    check_user_or_group_ctx_fields(Fields, UserCtx).


%%--------------------------------------------------------------------
%% @doc
%% Checks whether user context is valid for the storage helper.
%% @end
%%--------------------------------------------------------------------
-spec validate_group_ctx(storage:helper(), group_ctx()) ->
    ok | {error, Reason :: term()}.
validate_group_ctx(#helper{name = ?POSIX_HELPER_NAME}, GroupCtx) ->
    check_user_or_group_ctx_fields([<<"gid">>], GroupCtx);
validate_group_ctx(#helper{name = ?GLUSTERFS_HELPER_NAME}, GroupCtx) ->
    check_user_or_group_ctx_fields([<<"gid">>], GroupCtx);
validate_group_ctx(#helper{name = ?NULL_DEVICE_HELPER_NAME}, GroupCtx) ->
    check_user_or_group_ctx_fields([<<"gid">>], GroupCtx);
validate_group_ctx(#helper{name = ?WEBDAV_HELPER_NAME}, _GroupCtx) ->
    ok;
validate_group_ctx(#helper{name = HelperName}, _GroupCtx) ->
    {error, {group_ctx_not_supported, HelperName}}.

%%--------------------------------------------------------------------
%% @doc
%% Returns helper name.
%% @end
%%--------------------------------------------------------------------
-spec get_name(helpers:helper() | params()) -> name().
get_name(#helper{name = Name}) ->
    Name;
get_name(#helper_params{helper_name = Name}) ->
    Name.

%%--------------------------------------------------------------------
%% @doc
%% Returns helper arguments.
%% @end
%%--------------------------------------------------------------------
-spec get_args(helpers:helper() | params()) -> args().
get_args(#helper{args = Args}) ->
    Args;
get_args(#helper_params{helper_args = Args}) ->
    maps:from_list([{K, V} || #helper_arg{key = K, value = V} <- Args]).

%%--------------------------------------------------------------------
%% @doc
%% Returns helper admin context.
%% @end
%%--------------------------------------------------------------------
-spec get_admin_ctx(helpers:helper()) -> user_ctx().
get_admin_ctx(#helper{admin_ctx = Ctx}) ->
    Ctx.

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
%% Returns helper storage path type.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_path_type(helpers:helper()) -> helper:storage_path_type().
get_storage_path_type(#helper{storage_path_type = StoragePathType}) ->
    StoragePathType.

%%--------------------------------------------------------------------
%% @doc
%% Returns helper parameters.
%% @end
%%--------------------------------------------------------------------
-spec get_params(helpers:helper(), user_ctx()) -> params().
get_params(Helper, UserCtx) ->
    {ok, #helper{name = Name, args = Args, extended_direct_io = DS}} =
        set_user_ctx(Helper, UserCtx),
    #helper_params{
        helper_name = Name,
        helper_args = maps:fold(fun(Key, Value, Acc) ->
            [#helper_arg{key = Key, value = Value} | Acc]
        end, [], Args),
        extended_direct_io = DS
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns proxy helper parameters.
%% @end
%%--------------------------------------------------------------------
-spec get_proxy_params(helpers:helper() | undefined, storage:id()) -> params().
get_proxy_params(Helper, StorageId) ->
    Timeout = get_timeout(Helper),
    {ok, Latency} = application:get_env(?APP_NAME, proxy_helper_latency_milliseconds),
    TimeoutValue = integer_to_binary(Timeout + Latency),
    #helper_params{
        helper_name = ?PROXY_HELPER_NAME,
        helper_args = [
            #helper_arg{key = <<"storageId">>, value = StorageId},
            #helper_arg{key = <<"timeout">>, value = TimeoutValue}
        ],
        extended_direct_io = false
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns timeout for storage helper async operations.
%% @end
%%--------------------------------------------------------------------
-spec get_timeout(helpers:helper() | undefined) -> Timeout :: timeout().
get_timeout(undefined) ->
    {ok, Value} = application:get_env(?APP_NAME,
        helpers_async_operation_timeout_milliseconds),
    Value;
get_timeout(#helper{args = Args}) ->
    case maps:find(<<"timeout">>, Args) of
        {ok, Value} ->
            erlang:binary_to_integer(Value);
        error ->
            get_timeout(undefined)
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


-spec webdav_fill_admin_id(UserCtx :: user_ctx()) -> user_ctx().
webdav_fill_admin_id(UserCtx = #{<<"onedataAccessToken">> := <<>>}) ->
    {ok, UserCtx};
webdav_fill_admin_id(UserCtx = #{<<"onedataAccessToken">> := Token}) ->
    {ok, AdminId} = user_identity:get_or_fetch_user_id(Token),
    {ok, UserCtx#{
        <<"adminId">> => AdminId
    }};

webdav_fill_admin_id(UserCtx) ->
    UserCtx.


%%--------------------------------------------------------------------
%% @doc
%% Translates storage helper name from legacy format.
%% @end
%%--------------------------------------------------------------------
-spec translate_name(OldName :: binary()) -> NewName :: binary().
translate_name(<<"Ceph">>) -> ?CEPH_HELPER_NAME;
translate_name(<<"CephRados">>) -> ?CEPHRADOS_HELPER_NAME;
translate_name(<<"DirectIO">>) -> ?POSIX_HELPER_NAME;
translate_name(<<"ProxyIO">>) -> ?PROXY_HELPER_NAME;
translate_name(<<"AmazonS3">>) -> ?S3_HELPER_NAME;
translate_name(<<"Swift">>) -> ?SWIFT_HELPER_NAME;
translate_name(<<"GlusterFS">>) -> ?GLUSTERFS_HELPER_NAME;
translate_name(<<"WebDAV">>) -> ?WEBDAV_HELPER_NAME;
translate_name(<<"NullDevice">>) -> ?NULL_DEVICE_HELPER_NAME;
translate_name(Name) -> Name.

%%--------------------------------------------------------------------
%% @doc
%% Translates storage helper argument name from legacy format.
%% @end
%%--------------------------------------------------------------------
-spec translate_arg_name(OldName :: binary()) -> NewName :: binary().
translate_arg_name(<<"access_key">>) -> <<"accessKey">>;
translate_arg_name(<<"auth_url">>) -> <<"authUrl">>;
translate_arg_name(<<"block_size">>) -> <<"blockSize">>;
translate_arg_name(<<"bucket_name">>) -> <<"bucketName">>;
translate_arg_name(<<"cluster_name">>) -> <<"clusterName">>;
translate_arg_name(<<"container_name">>) -> <<"containerName">>;
translate_arg_name(<<"host_name">>) -> <<"hostname">>;
translate_arg_name(<<"mon_host">>) -> <<"monitorHostname">>;
translate_arg_name(<<"pool_name">>) -> <<"poolName">>;
translate_arg_name(<<"root_path">>) -> <<"mountPoint">>;
translate_arg_name(<<"secret_key">>) -> <<"secretKey">>;
translate_arg_name(<<"tenant_name">>) -> <<"tenantName">>;
translate_arg_name(<<"user_name">>) -> <<"username">>;
translate_arg_name(<<"xlator_options">>) -> <<"xlatorOptions">>;
translate_arg_name(<<"verify_server_certificate">>) -> <<"verifyServerCertificate">>;
translate_arg_name(<<"credentials_type">>) -> <<"credentialsType">>;
translate_arg_name(<<"authorization_header">>) -> <<"authorizationHeader">>;
translate_arg_name(<<"range_write_support">>) -> <<"rangeWriteSupport">>;
translate_arg_name(<<"connection_pool_size">>) -> <<"connectionPoolSize">>;
translate_arg_name(<<"maximum_upload_size">>) -> <<"maximumUploadSize">>;
translate_arg_name(<<"latency_min">>) -> <<"latencyMin">>;
translate_arg_name(<<"latency_max">>) -> <<"latencyMax">>;
translate_arg_name(<<"timeout_probability">>) -> <<"timeoutProbability">>;
translate_arg_name(<<"simulated_filesystem_parameters">>) ->
    <<"simulatedFilesystemParameters">>;
translate_arg_name(<<"simulated_filesystem_grow_speed">>) ->
    <<"simulatedFilesystemGrowSpeed">>;
translate_arg_name(<<"storage_path_type">>) -> <<"storagePathType">>;
translate_arg_name(Name) -> Name.


%%%===================================================================
%%% Test utils
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Constructs Ceph storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_ceph_user_ctx(binary(), binary()) -> {ok, user_ctx()}.
new_ceph_user_ctx(Username, Key) ->
    {ok, #{
        <<"username">> => Username,
        <<"key">> => Key
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs CephRados storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_cephrados_user_ctx(binary(), binary()) -> {ok, user_ctx()}.
new_cephrados_user_ctx(Username, Key) ->
    {ok, #{
        <<"username">> => Username,
        <<"key">> => Key
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs POSIX storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_posix_user_ctx(integer(), integer()) -> {ok, user_ctx()}.
new_posix_user_ctx(Uid, Gid) ->
    {ok, #{
        <<"uid">> => integer_to_binary(Uid),
        <<"gid">> => integer_to_binary(Gid)
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs S3 storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_s3_user_ctx(binary(), binary()) -> {ok, user_ctx()}.
new_s3_user_ctx(AccessKey, SecretKey) ->
    {ok, #{
        <<"accessKey">> => AccessKey,
        <<"secretKey">> => SecretKey
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs Swift storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_swift_user_ctx(binary(), binary()) -> {ok, user_ctx()}.
new_swift_user_ctx(Username, Password) ->
    {ok, #{
        <<"username">> => Username,
        <<"password">> => Password
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs GlusterFS storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_glusterfs_user_ctx(integer(), integer()) -> {ok, user_ctx()}.
new_glusterfs_user_ctx(Uid, Gid) ->
    {ok, #{
        <<"uid">> => integer_to_binary(Uid),
        <<"gid">> => integer_to_binary(Gid)
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs WebDAV storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_webdav_user_ctx(binary(), binary()) -> {ok, user_ctx()}.
new_webdav_user_ctx(CredentialsType = <<"none">>, _Credentials) ->
    {ok, #{
        <<"credentialsType">> => CredentialsType
    }};
new_webdav_user_ctx(CredentialsType, Credentials) ->
    {ok, #{
        <<"credentialsType">> => CredentialsType,
        <<"credentials">> => Credentials
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs WebDAV storage helper user context record.
%% This function is used by Onepanel to construct admin user_ctx.
%% @end
%%--------------------------------------------------------------------
-spec new_webdav_user_ctx(binary(), binary(), binary()) -> {ok, user_ctx()}.
new_webdav_user_ctx(CredentialsType, Credentials, <<>>) ->
    % todo VFS-5304 verify parameters
    % todo i. e. when type == oauth2 and insecure == true token cannot be empty
    new_webdav_user_ctx(CredentialsType, Credentials);
new_webdav_user_ctx(CredentialsType, Credentials, OnedataAccessToken) ->
    {ok, AdminId} = user_identity:get_or_fetch_user_id(OnedataAccessToken),
    {ok, UserCtx} = new_webdav_user_ctx(CredentialsType, Credentials),
    {ok, UserCtx#{
        <<"onedataAccessToken">> => OnedataAccessToken,
        <<"adminId">> => AdminId
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs Null Device storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_nulldevice_user_ctx(integer(), integer()) -> {ok, user_ctx()}.
new_nulldevice_user_ctx(Uid, Gid) ->
    {ok, #{
        <<"uid">> => integer_to_binary(Uid),
        <<"gid">> => integer_to_binary(Gid)
    }}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether user/group context map contains only provided fields and they
%% have valid type.
%% @end
%%--------------------------------------------------------------------
-spec check_user_or_group_ctx_fields([binary()], user_ctx() | group_ctx()) ->
    ok | {error, Reason :: term()}.
check_user_or_group_ctx_fields([], Ctx) when Ctx == #{} ->
    ok;
check_user_or_group_ctx_fields([], Ctx) ->
    {error, {invalid_additional_fields, Ctx}};
check_user_or_group_ctx_fields([Field | Fields], Ctx) ->
    case check_ctx_field(Field, Ctx) of
        ok -> check_user_or_group_ctx_fields(Fields, Ctx);
        Error -> Error
    end.

%% @private
-spec check_ctx_field(Key, Ctx :: #{binary() := term()}) ->
    ok | {error, Reason :: term()} when
    Key :: binary() | optional_field().
check_ctx_field({optional, Field}, Ctx) ->
    case check_ctx_field(Field, Ctx) of
        {error, {missing_field, _}} -> ok;
        Result -> Result
    end;

check_ctx_field(Field, Ctx) ->
    case Ctx of
        #{Field := Value = <<"null">>} ->
            {error, {invalid_field_value, Field, Value}};
        #{Field := Value} when is_binary(Value) -> ok;
        #{Field := Value} when is_integer(Value) -> ok;
        #{Field := Value} ->
            {error, {invalid_field_value, Field, Value}};
        #{} ->
            {error, {missing_field, Field}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Validates that a storage helper arguments map contains all
%% required fields and all its values are binaries.
%% @end
%%--------------------------------------------------------------------
-spec check_args(RequiredFields :: [binary()], Args :: map()) ->
    ok | {error, {missing_field, Key :: binary()}}
    | {error, {invalid_field_value, Key :: term(), Value :: term ()}}.
check_args(RequiredFields, Args) ->
    Result = lists:foldl(fun
        (_, {error, _} = Error) -> Error;
        (Field, ok) ->
            case maps:find(Field, Args) of
                {ok, _Value} -> ok;
                error -> {error, {missing_field, Field}}
            end
    end, ok, RequiredFields),

    case Result of
        ok ->
            % ensure all values are binaries
            maps:fold(fun
                (Key, Value, ok) when not is_binary(Value) ->
                    {error, {invalid_field_value, Key, Value}};
                (_, _, Result) -> Result
            end, ok, Args);
        Error -> Error
    end.
