%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of helper_config_behaviour for HTTP storage.
%%% HTTP storage is read-only.
%%% @end
%%%-------------------------------------------------------------------
-module(http_helper_config).
-author("Bartosz Walkowicz").

-behaviour(helper_config_behaviour).

-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("op_panel_contracts/include/storage/common.hrl").
-include_lib("op_panel_contracts/include/storage/http.hrl").

%% helper_config_behaviour callbacks
-export([
    build/1,
    validate_user_ctx/1,
    build_args_diff/2,
    build_admin_ctx_diff/2,
    describe/1,

    is_posix_compatible/0,
    is_object_storage/0,
    is_rename_supported/0,
    is_nfs4_acl_supported/0,
    is_oauth2_supported/0,
    is_storage_access_type_supported/1,
    is_auto_import_supported/1,
    is_file_registration_supported/1,
    is_getting_size_supported/1,
    get_block_size/1,

    redact_confidential_credentials/1,
    redact_confidential_credentials_diff/1
]).


%%%===================================================================
%%% helper_config_behaviour callbacks
%%%===================================================================


-spec build(onedata_storage:create_spec()) -> helper_config:t().
build(CreateReq = #storage_create_spec{type = ?HTTP_HELPER_NAME, credentials = Credentials}) ->
    #helper_config{
        name = ?HTTP_HELPER_NAME,
        args = build_args(CreateReq),
        admin_ctx = build_admin_ctx(Credentials)
    }.


-spec validate_user_ctx(helper_config:user_ctx()) -> ok | {error, Reason :: term()}.
validate_user_ctx(UserCtx) ->
    BaseFields = [<<"credentialsType">>],
    OptionalFields = [
        <<"credentials">>, <<"adminId">>, <<"onedataAccessToken">>, <<"oauth2IdP">>,
        <<"accessToken">>, <<"accessTokenTTL">>
    ],

    case UserCtx of
        #{<<"credentialsType">> := Type} when Type /= <<"none">> ->
            %% credentials is required
            RequiredFields = [<<"credentialsType">>, <<"credentials">>],
            RemainingOptionalFields = lists:delete(<<"credentials">>, OptionalFields),
            helper_config_utils:validate_user_ctx(UserCtx, RequiredFields, RemainingOptionalFields);
        _ ->
            helper_config_utils:validate_user_ctx(UserCtx, BaseFields, OptionalFields)
    end.


-spec build_args_diff(helper_config:t(), onedata_storage:update_spec()) -> helper_config:args().
build_args_diff(HelperConfig, UpdateSpec = #storage_update_spec{configuration = undefined}) ->
    build_args_diff(HelperConfig, UpdateSpec#storage_update_spec{
        configuration = #http_helper_configuration_diff{}
    });
build_args_diff(HelperConfig, #storage_update_spec{
    timeout = Timeout,
    configuration = #http_helper_configuration_diff{
        endpoint = Endpoint,
        verify_server_certificate = VerifyServerCertificate,
        authorization_header = AuthorizationHeader,
        connection_pool_size = ConnectionPoolSize,
        max_requests_per_session = MaxRequestsPerSession,
        emulate_range_read = EmulateRangeRead,
        max_emulated_range_read_file_size = MaxEmulatedRangeReadFileSize,
        file_mode = FileMode
    }
}) ->
    helper_config_utils:build_args_diff_from_specs(HelperConfig#helper_config.args, [
        {<<"endpoint">>, Endpoint},
        {<<"verifyServerCertificate">>, VerifyServerCertificate, fun atom_to_binary/1},
        {<<"authorizationHeader">>, AuthorizationHeader},
        {<<"connectionPoolSize">>, ConnectionPoolSize, fun integer_to_binary/1},
        {<<"maxRequestsPerSession">>, MaxRequestsPerSession, fun integer_to_binary/1},
        {<<"emulateRangeRead">>, EmulateRangeRead, fun atom_to_binary/1},
        {<<"maxEmulatedRangeReadFileSize">>, MaxEmulatedRangeReadFileSize, fun integer_to_binary/1},
        {<<"fileMode">>, FileMode},
        {<<"timeout">>, Timeout, fun integer_to_binary/1}
    ]).


-spec build_admin_ctx_diff(helper_config:t(), onedata_storage:update_spec()) ->
    helper_config:user_ctx().
build_admin_ctx_diff(_HelperConfig, #storage_update_spec{credentials = undefined}) ->
    #{};
build_admin_ctx_diff(HelperConfig, #storage_update_spec{
    credentials = #http_helper_credentials_diff{
        credentials_type = CredentialsType,
        credentials = Credentials,
        oauth2_idp = OAuth2IdP,
        onedata_access_token = OnedataAccessToken
    }
}) ->
    helper_config_utils:build_args_diff_from_specs(HelperConfig#helper_config.admin_ctx, [
        {<<"credentialsType">>, CredentialsType, fun credentials_type_to_binary/1},
        {<<"credentials">>, Credentials},
        {<<"oauth2IdP">>, OAuth2IdP},
        {<<"onedataAccessToken">>, OnedataAccessToken}
    ]).


-spec describe(helper_config:t()) -> helper_config:description().
describe(#helper_config{
    name = ?HTTP_HELPER_NAME,
    args = Args,
    admin_ctx = AdminCtx
}) ->
    %% Reconstruct configuration record from args map
    BaseConfiguration = #http_helper_configuration{
        endpoint = maps:get(<<"endpoint">>, Args),
        storage_path_type = helper_config_utils:storage_path_type_from_binary(
            maps:get(<<"storagePathType">>, Args)
        )
    },
    Configuration = helper_config_utils:set_optional_record_fields_if_defined(BaseConfiguration, Args, [
        {<<"verifyServerCertificate">>, #http_helper_configuration.verify_server_certificate, fun utils:to_boolean/1},
        {<<"authorizationHeader">>, #http_helper_configuration.authorization_header},
        {<<"connectionPoolSize">>, #http_helper_configuration.connection_pool_size, fun binary_to_integer/1},
        {<<"maxRequestsPerSession">>, #http_helper_configuration.max_requests_per_session, fun binary_to_integer/1},
        {<<"emulateRangeRead">>, #http_helper_configuration.emulate_range_read, fun utils:to_boolean/1},
        {<<"maxEmulatedRangeReadFileSize">>, #http_helper_configuration.max_emulated_range_read_file_size, fun binary_to_integer/1},
        {<<"fileMode">>, #http_helper_configuration.file_mode}
    ]),

    %% Reconstruct credentials record from admin_ctx (with redaction for security)
    BaseCredentials = #http_helper_credentials{
        credentials_type = credentials_type_from_binary(maps:get(<<"credentialsType">>, AdminCtx))
    },
    Credentials = redact_confidential_credentials(
        helper_config_utils:set_optional_record_fields_if_defined(BaseCredentials, AdminCtx, [
            {<<"credentials">>, #http_helper_credentials.credentials},
            {<<"oauth2IdP">>, #http_helper_credentials.oauth2_idp},
            {<<"onedataAccessToken">>, #http_helper_credentials.onedata_access_token}
        ])
    ),

    #helper_config_description{
        type = ?HTTP_HELPER_NAME,
        credentials = Credentials,
        configuration = Configuration,
        timeout = utils:convert_defined(
            maps:get(<<"timeout">>, Args, undefined),
            fun binary_to_integer/1
        )
    }.


-spec is_posix_compatible() -> boolean().
is_posix_compatible() -> false.


-spec is_object_storage() -> boolean().
is_object_storage() -> false.


-spec is_rename_supported() -> boolean().
is_rename_supported() -> false.


-spec is_nfs4_acl_supported() -> boolean().
is_nfs4_acl_supported() -> false.


-spec is_oauth2_supported() -> boolean().
is_oauth2_supported() -> true.


-spec is_storage_access_type_supported(helper_config:access_type()) -> boolean().
is_storage_access_type_supported(?READWRITE) -> false;  %% HTTP is read-only
is_storage_access_type_supported(?READONLY) -> true.


-spec is_auto_import_supported(#helper_config{}) -> boolean().
is_auto_import_supported(_HelperConfig) ->
    false.


-spec is_file_registration_supported(#helper_config{}) -> boolean().
is_file_registration_supported(HelperConfig) ->
    helper_config_utils:is_canonical(HelperConfig).


-spec is_getting_size_supported(#helper_config{}) -> boolean().
is_getting_size_supported(_HelperConfig) ->
    true.


-spec get_block_size(#helper_config{}) -> non_neg_integer() | undefined.
get_block_size(#helper_config{}) ->
    undefined.


-spec redact_confidential_credentials(#http_helper_credentials{}) -> #http_helper_credentials{}.
redact_confidential_credentials(Credentials) ->
    helper_config_utils:redact_record_fields_if_defined(Credentials, [
        #http_helper_credentials.credentials,
        #http_helper_credentials.onedata_access_token
    ]).


-spec redact_confidential_credentials_diff(#http_helper_credentials_diff{}) -> #http_helper_credentials_diff{}.
redact_confidential_credentials_diff(CredentialsDiff) ->
    helper_config_utils:redact_record_fields_if_defined(CredentialsDiff, [
        #http_helper_credentials_diff.credentials,
        #http_helper_credentials_diff.onedata_access_token
    ]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_args(onedata_storage:create_spec()) -> helper_config:args().
build_args(#storage_create_spec{
    timeout = Timeout,
    configuration = #http_helper_configuration{
        endpoint = Endpoint,
        verify_server_certificate = VerifyServerCertificate,
        authorization_header = AuthorizationHeader,
        connection_pool_size = ConnectionPoolSize,
        max_requests_per_session = MaxRequestsPerSession,
        emulate_range_read = EmulateRangeRead,
        max_emulated_range_read_file_size = MaxEmulatedRangeReadFileSize,
        file_mode = FileMode,
        storage_path_type = StoragePathType
    }
}) ->
    RequiredArgs = #{
        <<"endpoint">> => Endpoint,
        <<"storagePathType">> => helper_config_utils:storage_path_type_to_binary(StoragePathType)
    },
    helper_config_utils:add_optional_args_if_defined(RequiredArgs, [
        {<<"verifyServerCertificate">>, VerifyServerCertificate, fun atom_to_binary/1},
        {<<"authorizationHeader">>, AuthorizationHeader},
        {<<"connectionPoolSize">>, ConnectionPoolSize, fun integer_to_binary/1},
        {<<"maxRequestsPerSession">>, MaxRequestsPerSession, fun integer_to_binary/1},
        {<<"emulateRangeRead">>, EmulateRangeRead, fun atom_to_binary/1},
        {<<"maxEmulatedRangeReadFileSize">>, MaxEmulatedRangeReadFileSize, fun integer_to_binary/1},
        {<<"fileMode">>, FileMode},
        {<<"timeout">>, Timeout, fun integer_to_binary/1}
    ]).


%% @private
-spec build_admin_ctx(#http_helper_credentials{}) -> helper_config:user_ctx().
build_admin_ctx(#http_helper_credentials{
    credentials_type = CredentialsType,
    credentials = Credentials,
    oauth2_idp = OAuth2IdP,
    onedata_access_token = OnedataAccessToken
}) ->
    BaseCtx0 = #{
        <<"credentialsType">> => credentials_type_to_binary(CredentialsType)
    },
    BaseCtx1 = helper_config_utils:add_optional_args_if_defined(BaseCtx0, [
        {<<"credentials">>, Credentials},
        {<<"oauth2IdP">>, OAuth2IdP},
        {<<"onedataAccessToken">>, OnedataAccessToken}
    ]),
    helper_config_utils:resolve_admin_id(case CredentialsType of
        none -> maps:remove(<<"credentials">>, BaseCtx1);
        _ -> BaseCtx1
    end).


%% @private
-spec credentials_type_to_binary(none | basic | token | oauth2) -> binary().
credentials_type_to_binary(none) -> <<"none">>;
credentials_type_to_binary(basic) -> <<"basic">>;
credentials_type_to_binary(token) -> <<"token">>;
credentials_type_to_binary(oauth2) -> <<"oauth2">>.


%% @private
-spec credentials_type_from_binary(binary()) -> none | basic | token | oauth2.
credentials_type_from_binary(<<"none">>) -> none;
credentials_type_from_binary(<<"basic">>) -> basic;
credentials_type_from_binary(<<"token">>) -> token;
credentials_type_from_binary(<<"oauth2">>) -> oauth2.
