%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of helper_spec_behaviour for HTTP storage.
%%% HTTP storage is read-only.
%%% @end
%%%-------------------------------------------------------------------
-module(http_helper_spec).
-author("Bartosz Walkowicz").

-behaviour(helper_spec_behaviour).

-include("modules/storage/helpers/helpers.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").
-include_lib("opw_panel_contracts/include/storage/http.hrl").

%% helper_spec_behaviour callbacks
-export([
    build/1,
    validate_credentials/1,
    build_configuration_diff/2,
    build_credentials_diff/2,
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
%%% helper_spec_behaviour callbacks
%%%===================================================================


-spec build(onedata_storage:create_spec()) -> helper_spec:t().
build(CreateReq = #storage_create_spec{type = ?HTTP_HELPER_NAME, credentials = Credentials}) ->
    #helper_spec{
        name = ?HTTP_HELPER_NAME,
        configuration = build_configuration(CreateReq),
        credentials = build_credentials(Credentials)
    }.


-spec validate_credentials(helper_spec:credentials()) -> ok | {error, Reason :: term()}.
validate_credentials(Credentials) ->
    BaseFields = [<<"credentialsType">>],
    OptionalFields = [
        <<"credentials">>, <<"adminId">>, <<"onedataAccessToken">>, <<"oauth2IdP">>,
        <<"accessToken">>, <<"accessTokenTTL">>
    ],

    case Credentials of
        #{<<"credentialsType">> := Type} when Type /= <<"none">> ->
            %% credentials is required
            RequiredFields = [<<"credentialsType">>, <<"credentials">>],
            RemainingOptionalFields = lists:delete(<<"credentials">>, OptionalFields),
            helper_spec_utils:validate_credentials(Credentials, RequiredFields, RemainingOptionalFields);
        _ ->
            helper_spec_utils:validate_credentials(Credentials, BaseFields, OptionalFields)
    end.


-spec build_configuration_diff(helper_spec:t(), onedata_storage:update_spec()) -> helper_spec:configuration().
build_configuration_diff(HelperSpec, UpdateSpec = #storage_update_spec{configuration = undefined}) ->
    build_configuration_diff(HelperSpec, UpdateSpec#storage_update_spec{
        configuration = #http_helper_configuration_diff{}
    });
build_configuration_diff(HelperSpec, #storage_update_spec{
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
    helper_spec_utils:build_diff_from_specs(HelperSpec#helper_spec.configuration, [
        {<<"endpoint">>, Endpoint},
        {<<"verifyServerCertificate">>, VerifyServerCertificate, fun atom_to_binary/1},
        {<<"authorizationHeader">>, AuthorizationHeader},
        {<<"connectionPoolSize">>, ConnectionPoolSize, fun integer_to_binary/1},
        {<<"maxRequestsPerSession">>, MaxRequestsPerSession, fun integer_to_binary/1},
        {<<"emulateRangeRead">>, EmulateRangeRead, fun atom_to_binary/1},
        {<<"maxEmulatedRangeReadFileSize">>, MaxEmulatedRangeReadFileSize, fun integer_to_binary/1},
        {<<"fileMode">>, FileMode}
    ]).


-spec build_credentials_diff(helper_spec:t(), onedata_storage:update_spec()) ->
    helper_spec:credentials().
build_credentials_diff(_HelperSpec, #storage_update_spec{credentials = undefined}) ->
    #{};
build_credentials_diff(HelperSpec, #storage_update_spec{
    credentials = #http_helper_credentials_diff{
        credentials_type = CredentialsType,
        credentials = Credentials,
        oauth2_idp = OAuth2IdP,
        onedata_access_token = OnedataAccessToken
    }
}) ->
    helper_spec_utils:build_diff_from_specs(HelperSpec#helper_spec.credentials, [
        {<<"credentialsType">>, CredentialsType, fun credentials_type_to_binary/1},
        {<<"credentials">>, Credentials},
        {<<"oauth2IdP">>, OAuth2IdP},
        {<<"onedataAccessToken">>, OnedataAccessToken}
    ]).


-spec describe(helper_spec:t()) ->
    {onedata_storage:helper_configuration(), onedata_storage:helper_credentials()}.
describe(#helper_spec{
    name = ?HTTP_HELPER_NAME,
    configuration = ConfigurationParams,
    credentials = CredentialsParams
}) ->
    %% Reconstruct configuration record from flat params
    BaseConfiguration = #http_helper_configuration{
        endpoint = maps:get(<<"endpoint">>, ConfigurationParams),
        storage_path_type = helper_spec_utils:storage_path_type_from_binary(
            maps:get(<<"storagePathType">>, ConfigurationParams)
        )
    },
    Configuration = helper_spec_utils:set_optional_record_fields_if_defined(BaseConfiguration, ConfigurationParams, [
        {<<"verifyServerCertificate">>, #http_helper_configuration.verify_server_certificate, fun utils:to_boolean/1},
        {<<"authorizationHeader">>, #http_helper_configuration.authorization_header},
        {<<"connectionPoolSize">>, #http_helper_configuration.connection_pool_size, fun binary_to_integer/1},
        {<<"maxRequestsPerSession">>, #http_helper_configuration.max_requests_per_session, fun binary_to_integer/1},
        {<<"emulateRangeRead">>, #http_helper_configuration.emulate_range_read, fun utils:to_boolean/1},
        {<<"maxEmulatedRangeReadFileSize">>, #http_helper_configuration.max_emulated_range_read_file_size, fun binary_to_integer/1},
        {<<"fileMode">>, #http_helper_configuration.file_mode}
    ]),

    %% Reconstruct credentials record from credentials (with redaction for security)
    BaseCredentials = #http_helper_credentials{
        credentials_type = credentials_type_from_binary(maps:get(<<"credentialsType">>, CredentialsParams))
    },
    Credentials = redact_confidential_credentials(
        helper_spec_utils:set_optional_record_fields_if_defined(BaseCredentials, CredentialsParams, [
            {<<"credentials">>, #http_helper_credentials.credentials},
            {<<"oauth2IdP">>, #http_helper_credentials.oauth2_idp},
            {<<"onedataAccessToken">>, #http_helper_credentials.onedata_access_token}
        ])
    ),

    {Configuration, Credentials}.


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


-spec is_storage_access_type_supported(helper_spec:access_type()) -> boolean().
is_storage_access_type_supported(?READWRITE) -> false;  %% HTTP is read-only
is_storage_access_type_supported(?READONLY) -> true.


-spec is_auto_import_supported(#helper_spec{}) -> boolean().
is_auto_import_supported(_HelperSpec) ->
    false.


-spec is_file_registration_supported(#helper_spec{}) -> boolean().
is_file_registration_supported(HelperSpec) ->
    helper_spec_utils:is_canonical(HelperSpec).


-spec is_getting_size_supported(#helper_spec{}) -> boolean().
is_getting_size_supported(_HelperSpec) ->
    true.


-spec get_block_size(#helper_spec{}) -> non_neg_integer() | undefined.
get_block_size(#helper_spec{}) ->
    undefined.


-spec redact_confidential_credentials(#http_helper_credentials{}) -> #http_helper_credentials{}.
redact_confidential_credentials(Credentials) ->
    helper_spec_utils:redact_record_fields_if_defined(Credentials, [
        #http_helper_credentials.credentials,
        #http_helper_credentials.onedata_access_token
    ]).


-spec redact_confidential_credentials_diff(#http_helper_credentials_diff{}) -> #http_helper_credentials_diff{}.
redact_confidential_credentials_diff(CredentialsDiff) ->
    helper_spec_utils:redact_record_fields_if_defined(CredentialsDiff, [
        #http_helper_credentials_diff.credentials,
        #http_helper_credentials_diff.onedata_access_token
    ]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_configuration(onedata_storage:create_spec()) -> helper_spec:configuration().
build_configuration(#storage_create_spec{
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
    RequiredParams = #{
        <<"endpoint">> => Endpoint,
        <<"storagePathType">> => helper_spec_utils:storage_path_type_to_binary(StoragePathType)
    },
    helper_spec_utils:add_optional_entries_if_defined(RequiredParams, [
        {<<"verifyServerCertificate">>, VerifyServerCertificate, fun atom_to_binary/1},
        {<<"authorizationHeader">>, AuthorizationHeader},
        {<<"connectionPoolSize">>, ConnectionPoolSize, fun integer_to_binary/1},
        {<<"maxRequestsPerSession">>, MaxRequestsPerSession, fun integer_to_binary/1},
        {<<"emulateRangeRead">>, EmulateRangeRead, fun atom_to_binary/1},
        {<<"maxEmulatedRangeReadFileSize">>, MaxEmulatedRangeReadFileSize, fun integer_to_binary/1},
        {<<"fileMode">>, FileMode}
    ]).


%% @private
-spec build_credentials(#http_helper_credentials{}) -> helper_spec:credentials().
build_credentials(#http_helper_credentials{
    credentials_type = CredentialsType,
    credentials = Credentials,
    oauth2_idp = OAuth2IdP,
    onedata_access_token = OnedataAccessToken
}) ->
    BaseCredentialsParams0 = #{
        <<"credentialsType">> => credentials_type_to_binary(CredentialsType)
    },
    BaseCredentialsParams1 = helper_spec_utils:add_optional_entries_if_defined(BaseCredentialsParams0, [
        {<<"credentials">>, Credentials},
        {<<"oauth2IdP">>, OAuth2IdP},
        {<<"onedataAccessToken">>, OnedataAccessToken}
    ]),
    helper_spec_utils:resolve_admin_id(case CredentialsType of
        none -> maps:remove(<<"credentials">>, BaseCredentialsParams1);
        _ -> BaseCredentialsParams1
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
