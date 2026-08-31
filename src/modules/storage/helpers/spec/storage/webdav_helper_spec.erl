%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of helper_spec_behaviour for WebDAV storage.
%%% @end
%%%-------------------------------------------------------------------
-module(webdav_helper_spec).
-author("Bartosz Walkowicz").

-behaviour(helper_spec_behaviour).

-include("modules/storage/helpers/helpers.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").
-include_lib("opw_panel_contracts/include/storage/webdav.hrl").

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
build(CreateReq = #storage_create_spec{type = ?WEBDAV_HELPER_NAME, credentials = Credentials}) ->
    #helper_spec{
        name = ?WEBDAV_HELPER_NAME,
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
        configuration = #webdav_helper_configuration_diff{}
    });
build_configuration_diff(HelperSpec, #storage_update_spec{
    configuration = #webdav_helper_configuration_diff{
        endpoint = Endpoint,
        verify_server_certificate = VerifyServerCertificate,
        authorization_header = AuthorizationHeader,
        range_write_support = RangeWriteSupport,
        connection_pool_size = ConnectionPoolSize,
        maximum_upload_size = MaximumUploadSize,
        file_mode = FileMode,
        dir_mode = DirMode
    }
}) ->
    helper_spec_utils:build_diff_from_specs(HelperSpec#helper_spec.configuration, [
        {<<"endpoint">>, Endpoint},
        {<<"verifyServerCertificate">>, VerifyServerCertificate, fun atom_to_binary/1},
        {<<"authorizationHeader">>, AuthorizationHeader},
        {<<"rangeWriteSupport">>, RangeWriteSupport, fun range_write_support_to_binary/1},
        {<<"connectionPoolSize">>, ConnectionPoolSize, fun integer_to_binary/1},
        {<<"maximumUploadSize">>, MaximumUploadSize, fun integer_to_binary/1},
        {<<"fileMode">>, FileMode},
        {<<"dirMode">>, DirMode}
    ]).


-spec build_credentials_diff(helper_spec:t(), onedata_storage:update_spec()) ->
    helper_spec:credentials().
build_credentials_diff(_HelperSpec, #storage_update_spec{credentials = undefined}) ->
    #{};
build_credentials_diff(HelperSpec, #storage_update_spec{
    credentials = #webdav_helper_credentials_diff{
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
    name = ?WEBDAV_HELPER_NAME,
    configuration = ConfigurationParams,
    credentials = CredentialsParams
}) ->
    %% Reconstruct configuration record from flat params
    BaseConfiguration = #webdav_helper_configuration{
        endpoint = maps:get(<<"endpoint">>, ConfigurationParams),
        storage_path_type = helper_spec_utils:storage_path_type_from_binary(
            maps:get(<<"storagePathType">>, ConfigurationParams)
        )
    },
    Configuration = helper_spec_utils:set_optional_record_fields_if_defined(BaseConfiguration, ConfigurationParams, [
        {<<"verifyServerCertificate">>, #webdav_helper_configuration.verify_server_certificate, fun utils:to_boolean/1},
        {<<"authorizationHeader">>, #webdav_helper_configuration.authorization_header},
        {<<"rangeWriteSupport">>, #webdav_helper_configuration.range_write_support, fun range_write_support_from_binary/1},
        {<<"connectionPoolSize">>, #webdav_helper_configuration.connection_pool_size, fun binary_to_integer/1},
        {<<"maximumUploadSize">>, #webdav_helper_configuration.maximum_upload_size, fun binary_to_integer/1},
        {<<"fileMode">>, #webdav_helper_configuration.file_mode},
        {<<"dirMode">>, #webdav_helper_configuration.dir_mode}
    ]),

    %% Reconstruct credentials record from credentials (with redaction for security)
    BaseCredentials = #webdav_helper_credentials{
        credentials_type = credentials_type_from_binary(maps:get(<<"credentialsType">>, CredentialsParams))
    },
    Credentials = redact_confidential_credentials(
        helper_spec_utils:set_optional_record_fields_if_defined(BaseCredentials, CredentialsParams, [
            {<<"credentials">>, #webdav_helper_credentials.credentials},
            {<<"oauth2IdP">>, #webdav_helper_credentials.oauth2_idp},
            {<<"onedataAccessToken">>, #webdav_helper_credentials.onedata_access_token}
        ])
    ),

    {Configuration, Credentials}.


-spec is_posix_compatible() -> boolean().
is_posix_compatible() -> false.


-spec is_object_storage() -> boolean().
is_object_storage() -> false.


-spec is_rename_supported() -> boolean().
is_rename_supported() -> true.


-spec is_nfs4_acl_supported() -> boolean().
is_nfs4_acl_supported() -> false.


-spec is_oauth2_supported() -> boolean().
is_oauth2_supported() -> true.


-spec is_storage_access_type_supported(helper_spec:access_type()) -> boolean().
is_storage_access_type_supported(_) -> true.


-spec is_auto_import_supported(#helper_spec{}) -> boolean().
is_auto_import_supported(HelperSpec) ->
    helper_spec_utils:is_canonical(HelperSpec).


-spec is_file_registration_supported(#helper_spec{}) -> boolean().
is_file_registration_supported(HelperSpec) ->
    helper_spec_utils:is_canonical(HelperSpec).


-spec is_getting_size_supported(#helper_spec{}) -> boolean().
is_getting_size_supported(_HelperSpec) ->
    true.


-spec get_block_size(#helper_spec{}) -> non_neg_integer() | undefined.
get_block_size(#helper_spec{}) ->
    undefined.


-spec redact_confidential_credentials(#webdav_helper_credentials{}) -> #webdav_helper_credentials{}.
redact_confidential_credentials(Credentials) ->
    helper_spec_utils:redact_record_fields_if_defined(Credentials, [
        #webdav_helper_credentials.credentials,
        #webdav_helper_credentials.onedata_access_token
    ]).


-spec redact_confidential_credentials_diff(#webdav_helper_credentials_diff{}) -> #webdav_helper_credentials_diff{}.
redact_confidential_credentials_diff(CredentialsDiff) ->
    helper_spec_utils:redact_record_fields_if_defined(CredentialsDiff, [
        #webdav_helper_credentials_diff.credentials,
        #webdav_helper_credentials_diff.onedata_access_token
    ]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_configuration(onedata_storage:create_spec()) -> helper_spec:configuration().
build_configuration(#storage_create_spec{
    configuration = #webdav_helper_configuration{
        endpoint = Endpoint,
        verify_server_certificate = VerifyServerCertificate,
        authorization_header = AuthorizationHeader,
        range_write_support = RangeWriteSupport,
        connection_pool_size = ConnectionPoolSize,
        maximum_upload_size = MaximumUploadSize,
        file_mode = FileMode,
        dir_mode = DirMode,
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
        {<<"rangeWriteSupport">>, RangeWriteSupport, fun range_write_support_to_binary/1},
        {<<"connectionPoolSize">>, ConnectionPoolSize, fun integer_to_binary/1},
        {<<"maximumUploadSize">>, MaximumUploadSize, fun integer_to_binary/1},
        {<<"fileMode">>, FileMode},
        {<<"dirMode">>, DirMode}
    ]).


%% @private
-spec build_credentials(#webdav_helper_credentials{}) -> helper_spec:credentials().
build_credentials(#webdav_helper_credentials{
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


%% @private
-spec range_write_support_to_binary(none | moddav | sabredav) -> binary().
range_write_support_to_binary(none) -> <<"none">>;
range_write_support_to_binary(moddav) -> <<"moddav">>;
range_write_support_to_binary(sabredav) -> <<"sabredav">>.


%% @private
-spec range_write_support_from_binary(binary()) -> none | moddav | sabredav.
range_write_support_from_binary(<<"none">>) -> none;
range_write_support_from_binary(<<"moddav">>) -> moddav;
range_write_support_from_binary(<<"sabredav">>) -> sabredav.
