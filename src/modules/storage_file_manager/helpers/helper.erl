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
-include_lib("hackney/include/hackney_lib.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([new_helper/5]).
-export([validate_user_ctx/2, validate_group_ctx/2]).
-export([get_name/1, get_args/1, get_admin_ctx/1, is_insecure/1, get_params/2,
    get_proxy_params/2, get_timeout/1, get_storage_path_type/1]).
-export([set_user_ctx/2]).
-export([translate_name/1, translate_arg_name/1]).

%% Onepanel RPC
-export([prepare_helper_args/2, prepare_user_ctx_params/2]).

%% Test utils
-export([new_ceph_user_ctx/2, new_cephrados_user_ctx/2, new_posix_user_ctx/2,
    new_s3_user_ctx/2, new_swift_user_ctx/2, new_glusterfs_user_ctx/2,
    new_webdav_user_ctx/2, new_webdav_user_ctx/3, new_nulldevice_user_ctx/2]).


%% For Onepanel RPC
-export([filter_args/2, filter_user_ctx/2]).

-type name() :: binary().
-type args() :: #{field() => binary()}.
-type params() :: #helper_params{}.

-type user_ctx() :: #{field() => binary()}.
-type group_ctx() :: #{field() => binary()}.
-type ctx() :: user_ctx() | group_ctx().

-type field() :: binary().
-type optional_field() :: {optional, binary()}.

-export_type([name/0, args/0, params/0, user_ctx/0, group_ctx/0]).


% @fixme support creating webdav ctx with adminId

% @fixme move ctx managemetn to other module

%%%===================================================================
%%% API
%%%===================================================================

-spec new_helper(name(), args(), user_ctx(), Insecure :: boolean(),
    storage_path_type()) -> {ok, helpers:helper()}.
new_helper(HelperName, Args, AdminCtx, Insecure, StoragePathType) ->
    Fields = expected_helper_args(HelperName),
    AdminCtx = maps:merge(default_admin_ctx(HelperName), AdminCtx),
    ok = validate_fields(Fields, Args),
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
    Fields = expected_helper_args(HelperName),
    filter_params(Fields, Args).


%%--------------------------------------------------------------------
%% @doc
%% Removes unkown keys from helper user ctx parameters.
%% @end
%%--------------------------------------------------------------------
-spec filter_user_ctx(name(), #{binary() => term()}) -> #{binary() => term()}.
filter_user_ctx(HelperName, Params) ->
    AllowedFields = expected_user_ctx_params(HelperName),
    filter_params(AllowedFields, Params).


%% @private
-spec filter_params(AllowedFields, Params) -> Params when
    AllowedFields :: [field() | optional_field()],
    Params :: args() | ctx().
filter_params(AllowedFields, Params) ->
    Fields = strip_modifiers(AllowedFields),
    maps:with(Fields, Params).


%%--------------------------------------------------------------------
%% @doc
%% Translates storage params as specified by a user into correct
%% helper args.
%% The result will not contain any excessive fields,
%% but may not contain all required fields.
%% @end
%%--------------------------------------------------------------------
-spec prepare_helper_args(name(), args()) -> args().
prepare_helper_args(HelperName = ?S3_HELPER_NAME, Params) ->
    Transformed = maps_flatmap(fun
        (<<"hostname">>, URL) ->
            {ok, HttpOrHttps, Host} = extract_scheme(URL),
            #{
                <<"scheme">> => atom_to_binary(HttpOrHttps, utf8),
                <<"hostname">> => Host
            };
        (Key, Value) -> #{Key => Value}
    end, Params),
    filter_params(expected_helper_args(HelperName), Transformed);

prepare_helper_args(HelperName, Params) ->
    filter_params(expected_helper_args(HelperName), Params).


-spec prepare_user_ctx_params(name(), ctx()) -> ctx().
prepare_user_ctx_params(HelperName = ?WEBDAV_HELPER_NAME, Params) ->
    Transformed = maps_flatmap(fun
        (Key = <<"onedataAccessToken">>, Token) when byte_size(Token) > 0 ->
            {ok, AdminId} = user_identity:get_or_fetch_user_id(Token),
            #{
                <<"adminId">> => AdminId,
                Key => Token
            };
        (Key, Value) -> #{Key => Value}
    end, Params),
    filter_params(expected_user_ctx_params(HelperName), Transformed);

prepare_user_ctx_params(HelperName, Params) ->
    filter_params(expected_user_ctx_params(HelperName), Params).


-spec default_admin_ctx(name()) -> user_ctx().
default_admin_ctx(HelperName) when
    HelperName == ?POSIX_HELPER_NAME;
    HelperName == ?NULL_DEVICE_HELPER_NAME;
    HelperName == ?GLUSTERFS_HELPER_NAME ->
    #{<<"uid">> => <<"0">>, <<"gid">> => <<"0">>};

default_admin_ctx(_) ->
    #{}.


%% @private
-spec extract_scheme(URL :: binary()) -> {ok, Scheme :: http | https, binary()}.
extract_scheme(URL) ->
    #hackney_url{scheme = S3Scheme, host = S3Host, port = S3Port} =
        hackney_url:parse_url(URL),
    Scheme = case S3Scheme of
        https -> https;
        _ -> http
    end,
    {ok, Scheme, str_utils:format_bin("~s:~B", [S3Host, S3Port])}.


-spec strip_modifiers(Fields :: [field() | optional_field()]) -> [field()].
strip_modifiers(Fields) ->
    lists:map(fun
        ({optional, Field}) -> Field;
        (Field) -> Field
    end, Fields).


%% @private
-spec expected_helper_args(name()) ->
    [field() | optional_field()].
expected_helper_args(?CEPH_HELPER_NAME) -> [
    <<"monitorHostname">>, <<"clusterName">>, <<"poolName">>,
    {optional, <<"timeout">>}];
expected_helper_args(?CEPHRADOS_HELPER_NAME) -> [
    <<"monitorHostname">>, <<"clusterName">>, <<"poolName">>,
    {optional, <<"timeout">>}, {optional, <<"blockSize">>}];
expected_helper_args(?POSIX_HELPER_NAME) -> [
    <<"mountPoint">>,
    {optional, <<"timeout">>}];
expected_helper_args(?S3_HELPER_NAME) -> [
    <<"hostname">>, <<"bucketName">>, <<"scheme">>,
    {optional, <<"timeout">>}, {optional, <<"signatureVersion">>},
    {optional, <<"blockSize">>}];
expected_helper_args(?SWIFT_HELPER_NAME) -> [
    <<"authUrl">>, <<"containerName">>, <<"tenantName">>,
    {optional, <<"timeout">>}, {optional, <<"blockSize">>}];
expected_helper_args(?GLUSTERFS_HELPER_NAME) -> [
    <<"volume">>, <<"hostname">>,
    {optional, <<"port">>}, {optional, <<"mountPoint">>},
    {optional, <<"transport">>}, {optional, <<"xlatorOptions">>},
    {optional, <<"timeout">>}, {optional, <<"blockSize">>}];
expected_helper_args(?WEBDAV_HELPER_NAME) -> [
    <<"endpoint">>,
    {optional, <<"timeout">>}, {optional, <<"verifyServerCertificate">>},
    {optional, <<"authorizationHeader">>}, {optional, <<"rangeWriteSupport">>},
    {optional, <<"connectionPoolSize">>}, {optional, <<"maximumUploadSize">>}];
expected_helper_args(?NULL_DEVICE_HELPER_NAME) -> [
    {optional, <<"timeout">>}, {optional, <<"latencyMin">>},
    {optional, <<"latencyMax">>}, {optional, <<"timeoutProbability">>},
    {optional, <<"filter">>},
    {optional, <<"simulatedFilesystemParameters">>},
    {optional, <<"simulatedFilesystemGrowSpeed">>}].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns required fields for the user ctx of given storage type.
%% @end
%%--------------------------------------------------------------------
-spec expected_user_ctx_params(name()) -> [field() | optional_field()].
expected_user_ctx_params(?CEPH_HELPER_NAME) ->
    [<<"username">>, <<"key">>];
expected_user_ctx_params(?CEPHRADOS_HELPER_NAME) ->
    [<<"username">>, <<"key">>];
expected_user_ctx_params(?POSIX_HELPER_NAME) ->
    [<<"uid">>, <<"gid">>];
expected_user_ctx_params(?S3_HELPER_NAME) ->
    [<<"accessKey">>, <<"secretKey">>];
expected_user_ctx_params(?SWIFT_HELPER_NAME) ->
    [<<"username">>, <<"password">>];
expected_user_ctx_params(?GLUSTERFS_HELPER_NAME) ->
    [<<"uid">>, <<"gid">>];
expected_user_ctx_params(?WEBDAV_HELPER_NAME) ->
    [<<"credentialsType">>,
        {optional, <<"credentials">>}, {optional, <<"adminId">>},
        {optional, <<"onedataAccessToken">>}, {optional, <<"accessToken">>},
        {optional, <<"accessTokenTTL">>}
    ];
expected_user_ctx_params(?NULL_DEVICE_HELPER_NAME) ->
    [<<"uid">>, <<"gid">>].


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
    Fields = expected_user_ctx_params(StorageType),
    Fields2 = case {StorageType, UserCtx} of
        {?WEBDAV_HELPER_NAME, #{<<"credentialsType">> := Type}} when
            Type /= <<"none">> ->
            [{<<"credentials">>} | Fields -- [{optional, <<"credentials">>}]];
        _ ->
            Fields
    end,
    validate_fields(Fields2, UserCtx).


%%--------------------------------------------------------------------
%% @doc
%% Checks whether user context is valid for the storage helper.
%% @end
%%--------------------------------------------------------------------
-spec validate_group_ctx(storage:helper(), group_ctx()) ->
    ok | {error, Reason :: term()}.
validate_group_ctx(#helper{name = ?POSIX_HELPER_NAME}, GroupCtx) ->
    validate_fields([<<"gid">>], GroupCtx);
validate_group_ctx(#helper{name = ?GLUSTERFS_HELPER_NAME}, GroupCtx) ->
    validate_fields([<<"gid">>], GroupCtx);
validate_group_ctx(#helper{name = ?NULL_DEVICE_HELPER_NAME}, GroupCtx) ->
    validate_fields([<<"gid">>], GroupCtx);
validate_group_ctx(#helper{name = ?WEBDAV_HELPER_NAME}, _GroupCtx) ->
    ok;
validate_group_ctx(#helper{name = HelperName}, _GroupCtx) ->
    {error, {group_ctx_not_supported, HelperName}}.

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
%%% Getters
%%% Functions for extracting info from helper records
%%%===================================================================

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


%%%===================================================================
%%% Upgrade functions
%%%===================================================================

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
-spec validate_fields([field() | optional_field()], Params :: map()) ->
    ok | {error, Reason :: term()}.
validate_fields([], Params) when Params == #{} ->
    ok;
validate_fields([], Params) ->
    {error, {invalid_additional_fields, Params}};
validate_fields([Field | Fields], Params) ->
    case validate_field(Field, Params) of
        {ok, ParamsRemainder} ->
            validate_fields(Fields, ParamsRemainder);
        Error ->
            Error
    end.


%% @private
-spec validate_field(Key, Params) ->
    {ok, ParamsRemainder :: user_ctx() | group_ctx()} |
    {error, Reason :: term()} when
    Key :: field() | optional_field(),
    Params :: #{binary() := term()}.
validate_field({optional, Field}, Params) ->
    case validate_field(Field, Params) of
        {error, {missing_field, _}} -> {ok, Params};
        Result -> Result
    end;

validate_field(Field, Params) ->
    case Params of
        #{Field := <<Value/binary>>} when Value /= <<"null">> ->
            {ok, maps:remove(Field, Params)};
        #{Field := Value} ->
            {error, {invalid_field_value, Field, Value}};
        #{} ->
            {error, {missing_field, Field}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Invokes Fun for each key-value pair in the map.
%% The Fun must return a map, all of which are merged to create the result.
%% @end
%%--------------------------------------------------------------------
-spec maps_flatmap(Fun, #{K1 => V1}) -> #{K2 => V2} when
    Fun :: fun((K1, V1) -> #{K2 => V2}).
maps_flatmap(Fun, Map) ->
    maps:fold(fun(K, V, Acc) ->
        maps:merge(Acc, Fun(K, V))
    end, #{}, Map).

