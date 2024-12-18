%%%-------------------------------------------------------------------
%%% @author Wojciech Geisler
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module used for creation and validation of helper
%%% argument and user/group ctx maps.
%%% TODO VFS-6312 use middleware_sanitizer to parse helper params instead of custom functions
%%% @end
%%%-------------------------------------------------------------------
-module(helper_params).
-author("Wojciech Geisler").


-include("modules/storage/helpers/helpers.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([validate_args/2, validate_user_ctx/2]).
-export([default_admin_ctx/1]).
-export([redact_confidential_data/2]).

%% Onepanel RPC API
-export([prepare_helper_args/2, prepare_user_ctx_params/2]).

-type name() :: helper:name().
-type args() :: helper:args().
-type user_ctx() :: helper:user_ctx().

-type field() :: binary().
-type optional_field() :: {optional, field()}.
-type field_spec() :: field() | optional_field().

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Translates storage params as specified by the user
%% (i.e. via Onepanel API when creating the storage)
%% into correct helper args.
%% The result will not contain any unknown fields
%% but is not verified to contain all required fields.
%% @end
%%--------------------------------------------------------------------
-spec prepare_helper_args(name(), args()) -> args().
prepare_helper_args(?S3_HELPER_NAME = HelperName, Params) ->
    Args = derive_scheme_from_url(Params),
    filter_fields(expected_helper_args(HelperName), Args);

prepare_helper_args(HelperName, Params) ->
    filter_fields(expected_helper_args(HelperName), Params).


%% @private
-spec derive_scheme_from_url(args()) -> args().
derive_scheme_from_url(#{<<"hostname">> := Hostname, <<"scheme">> := Scheme} = Params) ->
    HostnameWithScheme = str_utils:join_binary([Scheme, <<"://">>, Hostname]),
    {ok, UrlScheme, Host} = get_scheme_and_hostname(HostnameWithScheme),
    Scheme = case UrlScheme of
        https -> <<"https">>;
        _ -> <<"http">>
    end,
    Params#{<<"scheme">> => Scheme, <<"hostname">> => Host};
derive_scheme_from_url(#{<<"hostname">> := HostnameWithScheme} = Params) ->
    {ok, UrlScheme, Host} = get_scheme_and_hostname(HostnameWithScheme),
    Scheme = case UrlScheme of
        https -> <<"https">>;
        _ -> <<"http">>
    end,
    Params#{<<"scheme">> => Scheme, <<"hostname">> => Host};
derive_scheme_from_url(Params) -> Params.


%%--------------------------------------------------------------------
%% @doc
%% Translates storage params as specified by the user
%% (i.e. via Onepanel API when creating the storage)
%% into correct user ctx.
%% The result will not contain any unknown fields
%% but is not verified to contain all required fields.
%% @end
%%--------------------------------------------------------------------
-spec prepare_user_ctx_params(name(), user_ctx()) -> user_ctx().
prepare_user_ctx_params(?WEBDAV_HELPER_NAME = HelperName, Params) ->
    Ctx1 = clear_unused_webdav_credentials(Params),
    Ctx2 = resolve_user_by_token(Ctx1),
    filter_fields(expected_user_ctx_params(HelperName), Ctx2);

prepare_user_ctx_params(HelperName, Params) ->
    MappedParams = maps:fold(
        fun(<<"rootUid">>, Value, Acc) -> Acc#{<<"uid">> => Value};
            (<<"rootGid">>, Value, Acc) -> Acc#{<<"gid">> => Value};
            (Key, Value, Acc) -> Acc#{Key => Value}
        end, #{}, Params),
    filter_fields(expected_user_ctx_params(HelperName), MappedParams).


%% @private
-spec clear_unused_webdav_credentials(user_ctx()) -> user_ctx().
clear_unused_webdav_credentials(#{<<"credentialsType">> := <<"none">>} = Params) ->
    maps:remove(<<"credentials">>, Params);
clear_unused_webdav_credentials(Params) -> Params.


%% @private
-spec resolve_user_by_token(user_ctx()) -> user_ctx().
resolve_user_by_token(#{<<"onedataAccessToken">> := AccessToken} = Params) when
    byte_size(AccessToken) > 0
->
    TokenCredentials = auth_manager:build_token_credentials(
        AccessToken, undefined, undefined,
        undefined, disallow_data_access_caveats
    ),
    {ok, ?USER(UserId), _} = auth_manager:verify_credentials(TokenCredentials),
    Params#{<<"adminId">> => UserId};
resolve_user_by_token(Params) -> Params.


%%--------------------------------------------------------------------
%% @doc
%% Translates storage params as specified by the user
%% (i.e. via Onepanel API when creating the storage)
%% into correct user ctx.
%% The result will not contain any unknown fields
%% but is not verified to contain all required fields.
%% @end
%%--------------------------------------------------------------------
-spec validate_args(name(), args()) ->
    ok | {error, Reason :: term()}.
validate_args(HelperName, Args) ->
    Fields = expected_helper_args(HelperName),
    validate_fields(Fields, Args).


%%--------------------------------------------------------------------
%% @doc
%% Checks whether user context is valid for the storage helper.
%% @end
%%--------------------------------------------------------------------
-spec validate_user_ctx(name(), user_ctx()) ->
    ok | {error, Reason :: term()}.
validate_user_ctx(StorageType = ?WEBDAV_HELPER_NAME, UserCtx) ->
    FieldsBase = expected_user_ctx_params(StorageType),
    Fields = case UserCtx of
        #{<<"credentialsType">> := <<"none">>} ->
            FieldsBase;
        #{<<"credentialsType">> := _} ->
            % make "credentials" required rather than optional
            [<<"credentials">> | remove_field(<<"credentials">>, FieldsBase)];
        _ ->
            FieldsBase
    end,
    validate_fields(Fields, UserCtx);

validate_user_ctx(StorageType = ?HTTP_HELPER_NAME, UserCtx) ->
    FieldsBase = expected_user_ctx_params(StorageType),
    Fields = case UserCtx of
        #{<<"credentialsType">> := <<"none">>} ->
            FieldsBase;
        #{<<"credentialsType">> := _} ->
            % make "credentials" required rather than optional
            [<<"credentials">> | remove_field(<<"credentials">>, FieldsBase)];
        _ ->
            FieldsBase
    end,
    validate_fields(Fields, UserCtx);

validate_user_ctx(StorageType, UserCtx) ->
    Fields = expected_user_ctx_params(StorageType),
    validate_fields(Fields, UserCtx).


-spec default_admin_ctx(name()) -> user_ctx().
default_admin_ctx(HelperName) when
    HelperName == ?POSIX_HELPER_NAME;
    HelperName == ?NULL_DEVICE_HELPER_NAME;
    HelperName == ?NFS_HELPER_NAME;
    HelperName == ?GLUSTERFS_HELPER_NAME ->
    #{<<"uid">> => <<"0">>, <<"gid">> => <<"0">>};

default_admin_ctx(_) ->
    #{}.


-spec redact_confidential_data(name(), args()) -> args().
redact_confidential_data(HelperName, Params) ->
    lists:foldl(fun(ParamName, Acc) ->
        case maps:find(ParamName, Acc) of
            error ->
                Acc;
            {ok, _ConfidentialValue} ->
                maps:put(ParamName, <<"*****">>, Acc)
        end
    end, Params, confidential_params(HelperName)).

%%%===================================================================
%%% Requirements
%%%===================================================================

%% @private
-spec expected_helper_args(name()) ->
    [field() | optional_field()].
expected_helper_args(HelperName) ->
    expected_custom_helper_args(HelperName) ++ expected_generic_helper_args().

%% @private
-spec expected_custom_helper_args(name()) ->
    [field() | optional_field()].
expected_custom_helper_args(?CEPH_HELPER_NAME) -> [
    <<"monitorHostname">>, <<"clusterName">>, <<"poolName">>];
expected_custom_helper_args(?CEPHRADOS_HELPER_NAME) -> [
    <<"monitorHostname">>, <<"clusterName">>, <<"poolName">>,
    {optional, <<"blockSize">>}];
expected_custom_helper_args(?POSIX_HELPER_NAME) -> [
    <<"mountPoint">>];
expected_custom_helper_args(?S3_HELPER_NAME) -> [
    <<"hostname">>, <<"bucketName">>, <<"scheme">>,
    {optional, <<"signatureVersion">>},
    {optional, <<"verifyServerCertificate">>},
    {optional, <<"region">>},
    {optional, <<"maximumCanonicalObjectSize">>},
    {optional, <<"fileMode">>}, {optional, <<"dirMode">>},
    {optional, <<"blockSize">>}];
expected_custom_helper_args(?SWIFT_HELPER_NAME) -> [
    <<"authUrl">>, <<"containerName">>, <<"tenantName">>,
    {optional, <<"blockSize">>}];
expected_custom_helper_args(?GLUSTERFS_HELPER_NAME) -> [
    <<"volume">>, <<"hostname">>,
    {optional, <<"port">>}, {optional, <<"mountPoint">>},
    {optional, <<"transport">>}, {optional, <<"xlatorOptions">>},
    {optional, <<"blockSize">>}];
expected_custom_helper_args(?NFS_HELPER_NAME) -> [
    <<"version">>, <<"host">>, <<"volume">>,
    {optional, <<"readAhead">>}, {optional, <<"tcpSyncnt">>},
    {optional, <<"dirCache">>}, {optional, <<"autoReconnect">>},
    {optional, <<"connectionPoolSize">>}];
expected_custom_helper_args(?WEBDAV_HELPER_NAME) -> [
    <<"endpoint">>,
    {optional, <<"oauth2IdP">>},
    {optional, <<"verifyServerCertificate">>},
    {optional, <<"authorizationHeader">>}, {optional, <<"rangeWriteSupport">>},
    {optional, <<"connectionPoolSize">>}, {optional, <<"maximumUploadSize">>},
    {optional, <<"fileMode">>}, {optional, <<"dirMode">>}];
expected_custom_helper_args(?HTTP_HELPER_NAME) -> [
    <<"endpoint">>,
    {optional, <<"oauth2IdP">>}, {optional, <<"verifyServerCertificate">>},
    {optional, <<"authorizationHeader">>}, {optional, <<"connectionPoolSize">>},
    {optional, <<"maxRequestsPerSession">>},{optional, <<"fileMode">>}];
expected_custom_helper_args(?XROOTD_HELPER_NAME) -> [
    <<"url">>,
    {optional, <<"fileModeMask">>}, {optional, <<"dirModeMask">>}];
expected_custom_helper_args(?NULL_DEVICE_HELPER_NAME) -> [
    {optional, <<"latencyMin">>},
    {optional, <<"latencyMax">>},
    {optional, <<"timeoutProbability">>},
    {optional, <<"filter">>},
    {optional, <<"simulatedFilesystemParameters">>},
    {optional, <<"simulatedFilesystemGrowSpeed">>},
    {optional, <<"enableDataVerification">>}].


-spec expected_generic_helper_args() -> [field() | optional_field()].
expected_generic_helper_args() -> [
    <<"storagePathType">>,
    {optional, <<"timeout">>},
    {optional, <<"archiveStorage">>}
].

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
    [<<"uid">>, {optional, <<"gid">>}];
expected_user_ctx_params(?S3_HELPER_NAME) ->
    [<<"accessKey">>, <<"secretKey">>];
expected_user_ctx_params(?SWIFT_HELPER_NAME) ->
    [<<"username">>, <<"password">>];
expected_user_ctx_params(?GLUSTERFS_HELPER_NAME) ->
    [<<"uid">>, {optional, <<"gid">>}];
expected_user_ctx_params(?NFS_HELPER_NAME) ->
    [<<"uid">>, {optional, <<"gid">>}];
expected_user_ctx_params(?WEBDAV_HELPER_NAME) ->
    [<<"credentialsType">>,
        {optional, <<"credentials">>}, {optional, <<"adminId">>},
        {optional, <<"onedataAccessToken">>}, {optional, <<"accessToken">>},
        {optional, <<"accessTokenTTL">>}
    ];
expected_user_ctx_params(?HTTP_HELPER_NAME) ->
    [<<"credentialsType">>,
        {optional, <<"credentials">>}, {optional, <<"adminId">>},
        {optional, <<"onedataAccessToken">>}, {optional, <<"accessToken">>},
        {optional, <<"accessTokenTTL">>}
    ];
expected_user_ctx_params(?XROOTD_HELPER_NAME) ->
    [<<"credentialsType">>, {optional, <<"credentials">>}];
expected_user_ctx_params(?NULL_DEVICE_HELPER_NAME) ->
    [<<"uid">>, {optional, <<"gid">>}].


%% @private
-spec confidential_params(name()) -> [field()].
confidential_params(?CEPH_HELPER_NAME) ->
    [<<"key">>];
confidential_params(?CEPHRADOS_HELPER_NAME) ->
    [<<"key">>];
confidential_params(?POSIX_HELPER_NAME) ->
    [];
confidential_params(?S3_HELPER_NAME) ->
    [<<"secretKey">>];
confidential_params(?SWIFT_HELPER_NAME) ->
    [<<"password">>];
confidential_params(?GLUSTERFS_HELPER_NAME) ->
    [];
confidential_params(?NFS_HELPER_NAME) ->
    [];
confidential_params(?WEBDAV_HELPER_NAME) ->
    [<<"credentials">>, <<"accessToken">>, <<"onedataAccessToken">>];
confidential_params(?HTTP_HELPER_NAME) ->
    [<<"credentials">>, <<"accessToken">>, <<"onedataAccessToken">>];
confidential_params(?XROOTD_HELPER_NAME) ->
    [<<"credentials">>];
confidential_params(?NULL_DEVICE_HELPER_NAME) ->
    [].

%%%===================================================================
%%% Internal helpers
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes unknown fields from args or ctx map.
%% @end
%%--------------------------------------------------------------------
-spec filter_fields([field_spec()], args() | user_ctx()) -> args() | user_ctx().
filter_fields(AllowedFields, Map) ->
    Fields = strip_optional_modifier(AllowedFields),
    maps:with(Fields, Map).


%% @private
-spec strip_optional_modifier(Fields :: [field() | optional_field()]) -> [field()].
strip_optional_modifier(Fields) ->
    lists:map(fun
        ({optional, Field}) -> Field;
        (Field) -> Field
    end, Fields).


%% @private
-spec remove_field(ToRemove :: field(), [field_spec()]) -> [field_spec()].
remove_field(ToRemove, Fields) ->
    lists:filter(fun(Field) -> case Field of
        ToRemove -> false;
        {optional, ToRemove} -> false;
        _ -> true
    end end, Fields).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given args or ctx map contains all required fields,
%% no unexpected fields and all their values are binaries.
%% @end
%%--------------------------------------------------------------------
-spec validate_fields([field() | optional_field()], Params :: map()) ->
    ok | {error, Reason :: term()}.
validate_fields([], Params) when Params == #{} ->
    ok;
validate_fields([], Params) ->
    {error, {invalid_additional_fields, Params}};
validate_fields([Field | FieldsTail], Params) ->
    case validate_field(Field, Params) of
        {ok, ParamsTail} -> validate_fields(FieldsTail, ParamsTail);
        Error -> Error
    end.


%% @private
-spec validate_field(Field, Params) ->
    {ok, ParamsTail :: Params} | {error, Reason :: term()} when
    Field :: field() | optional_field(),
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


%% @private
-spec get_scheme_and_hostname(binary()) ->
    {ok, Scheme :: http | https, HostAndPort :: binary()} | no_return().
get_scheme_and_hostname(Url) ->
    #{scheme := Scheme, host := Host, port := Port, path := Path} = url_utils:infer_components(Url),
    {ok, Scheme, str_utils:format_bin("~ts:~B~ts", [Host, Port, Path])}.
