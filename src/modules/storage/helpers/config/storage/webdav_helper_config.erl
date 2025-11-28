%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of helper_config_behaviour for WebDAV storage.
%%% @end
%%%-------------------------------------------------------------------
-module(webdav_helper_config).
-author("Bartosz Walkowicz").

-behaviour(helper_config_behaviour).

-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/storage/common.hrl").
-include_lib("ctool/include/storage/webdav.hrl").

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
    get_block_size/1
]).


%%%===================================================================
%%% helper_config_behaviour callbacks
%%%===================================================================


-spec build(onedata_storage:create_spec()) -> helper_config:t().
build(CreateReq = #storage_create_spec{type = ?WEBDAV_HELPER_NAME, credentials = Credentials}) ->
    #helper_config{
        name = ?WEBDAV_HELPER_NAME,
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
        configuration = #webdav_configuration_diff{}
    });
build_args_diff(HelperConfig, #storage_update_spec{
    timeout = Timeout,
    configuration = #webdav_configuration_diff{
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
    helper_config_utils:build_args_diff_from_specs(HelperConfig#helper_config.args, [
        {<<"endpoint">>, Endpoint},
        {<<"verifyServerCertificate">>, VerifyServerCertificate, fun atom_to_binary/1},
        {<<"authorizationHeader">>, AuthorizationHeader},
        {<<"rangeWriteSupport">>, RangeWriteSupport, fun range_write_support_to_binary/1},
        {<<"connectionPoolSize">>, ConnectionPoolSize, fun integer_to_binary/1},
        {<<"maximumUploadSize">>, MaximumUploadSize, fun integer_to_binary/1},
        {<<"fileMode">>, FileMode},
        {<<"dirMode">>, DirMode},
        {<<"timeout">>, Timeout, fun integer_to_binary/1}
    ]).


-spec build_admin_ctx_diff(helper_config:t(), onedata_storage:update_spec()) ->
    helper_config:user_ctx().
build_admin_ctx_diff(_HelperConfig, #storage_update_spec{credentials = undefined}) ->
    #{};
build_admin_ctx_diff(HelperConfig, #storage_update_spec{
    credentials = #webdav_credentials_diff{
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
    name = ?WEBDAV_HELPER_NAME,
    args = Args,
    admin_ctx = AdminCtx
}) ->
    %% Reconstruct configuration record from args map
    BaseConfiguration = #webdav_configuration{
        endpoint = maps:get(<<"endpoint">>, Args),
        storage_path_type = helper_config_utils:storage_path_type_from_binary(
            maps:get(<<"storagePathType">>, Args)
        )
    },
    Configuration = helper_config_utils:set_optional_record_fields_if_defined(BaseConfiguration, Args, [
        {<<"verifyServerCertificate">>, #webdav_configuration.verify_server_certificate, fun utils:to_boolean/1},
        {<<"authorizationHeader">>, #webdav_configuration.authorization_header},
        {<<"rangeWriteSupport">>, #webdav_configuration.range_write_support, fun range_write_support_from_binary/1},
        {<<"connectionPoolSize">>, #webdav_configuration.connection_pool_size, fun binary_to_integer/1},
        {<<"maximumUploadSize">>, #webdav_configuration.maximum_upload_size, fun binary_to_integer/1},
        {<<"fileMode">>, #webdav_configuration.file_mode},
        {<<"dirMode">>, #webdav_configuration.dir_mode}
    ]),

    %% Reconstruct credentials record from admin_ctx (with redaction for security)
    BaseCredentials = #webdav_credentials{
        credentials_type = credentials_type_from_binary(maps:get(<<"credentialsType">>, AdminCtx))
    },
    Credentials = helper_config_utils:set_optional_record_fields_if_defined(BaseCredentials, AdminCtx, [
        {<<"credentials">>, #webdav_credentials.credentials, fun(_) -> ?CONFIDENTIAL_MASK end},
        {<<"oauth2IdP">>, #webdav_credentials.oauth2_idp},
        {<<"onedataAccessToken">>, #webdav_credentials.onedata_access_token, fun(_) -> ?CONFIDENTIAL_MASK end}
    ]),

    #helper_config_description{
        type = ?WEBDAV_HELPER_NAME,
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
is_rename_supported() -> true.


-spec is_nfs4_acl_supported() -> boolean().
is_nfs4_acl_supported() -> false.


-spec is_oauth2_supported() -> boolean().
is_oauth2_supported() -> true.


-spec is_storage_access_type_supported(helper_config:access_type()) -> boolean().
is_storage_access_type_supported(_) -> true.


-spec is_auto_import_supported(#helper_config{}) -> boolean().
is_auto_import_supported(HelperConfig) ->
    helper_config_utils:is_canonical(HelperConfig).


-spec is_file_registration_supported(#helper_config{}) -> boolean().
is_file_registration_supported(HelperConfig) ->
    helper_config_utils:is_canonical(HelperConfig).


-spec is_getting_size_supported(#helper_config{}) -> boolean().
is_getting_size_supported(_HelperConfig) ->
    true.


-spec get_block_size(#helper_config{}) -> non_neg_integer() | undefined.
get_block_size(#helper_config{}) ->
    undefined.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_args(onedata_storage:create_spec()) -> helper_config:args().
build_args(#storage_create_spec{
    timeout = Timeout,
    configuration = #webdav_configuration{
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
    RequiredArgs = #{
        <<"endpoint">> => Endpoint,
        <<"storagePathType">> => helper_config_utils:storage_path_type_to_binary(StoragePathType)
    },
    helper_config_utils:add_optional_args_if_defined(RequiredArgs, [
        {<<"verifyServerCertificate">>, VerifyServerCertificate, fun atom_to_binary/1},
        {<<"authorizationHeader">>, AuthorizationHeader},
        {<<"rangeWriteSupport">>, RangeWriteSupport, fun range_write_support_to_binary/1},
        {<<"connectionPoolSize">>, ConnectionPoolSize, fun integer_to_binary/1},
        {<<"maximumUploadSize">>, MaximumUploadSize, fun integer_to_binary/1},
        {<<"fileMode">>, FileMode},
        {<<"dirMode">>, DirMode},
        {<<"timeout">>, Timeout, fun integer_to_binary/1}
    ]).


%% @private
-spec build_admin_ctx(#webdav_credentials{}) -> helper_config:user_ctx().
build_admin_ctx(#webdav_credentials{
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

    %% Clear unused credentials if type is 'none'
    BaseCtx2 = case CredentialsType of
        none -> maps:remove(<<"credentials">>, BaseCtx1);
        _ -> BaseCtx1
    end,

    %% Resolve user ID by token if onedataAccessToken is present
    case OnedataAccessToken of
        undefined ->
            BaseCtx2;
        AccessToken ->
            TokenCredentials = auth_manager:build_token_credentials(
                AccessToken, undefined, undefined,
                undefined, disallow_data_access_caveats
            ),
            {ok, ?USER(UserId), _} = auth_manager:verify_credentials(TokenCredentials),
            BaseCtx2#{<<"adminId">> => UserId}
    end.


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
