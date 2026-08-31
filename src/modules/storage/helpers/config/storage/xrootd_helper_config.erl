%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of helper_config_behaviour for XRootD storage.
%%% @end
%%%-------------------------------------------------------------------
-module(xrootd_helper_config).
-author("Bartosz Walkowicz").

-behaviour(helper_config_behaviour).

-include("modules/storage/helpers/helpers.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").
-include_lib("opw_panel_contracts/include/storage/xrootd.hrl").

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
build(CreateReq = #storage_create_spec{type = ?XROOTD_HELPER_NAME, credentials = Credentials}) ->
    #helper_config{
        name = ?XROOTD_HELPER_NAME,
        args = build_args(CreateReq),
        admin_ctx = build_admin_ctx(Credentials)
    }.


-spec validate_user_ctx(helper_config:user_ctx()) -> ok | {error, Reason :: term()}.
validate_user_ctx(UserCtx) ->
    {RequiredFields, OptionalFields} = case UserCtx of
        #{<<"credentialsType">> := <<"pwd">>} ->
            {[<<"credentialsType">>, <<"credentials">>], []};
        _ ->
            {[<<"credentialsType">>], [<<"credentials">>]}
    end,
    helper_config_utils:validate_user_ctx(UserCtx, RequiredFields, OptionalFields).


-spec build_args_diff(helper_config:t(), onedata_storage:update_spec()) -> helper_config:args().
build_args_diff(HelperConfig, UpdateSpec = #storage_update_spec{configuration = undefined}) ->
    build_args_diff(HelperConfig, UpdateSpec#storage_update_spec{
        configuration = #xrootd_helper_configuration_diff{}
    });
build_args_diff(HelperConfig, #storage_update_spec{
    timeout = Timeout,
    configuration = #xrootd_helper_configuration_diff{
        url = Url,
        file_mode_mask = FileModeMask,
        dir_mode_mask = DirModeMask
    }
}) ->
    helper_config_utils:build_args_diff_from_specs(HelperConfig#helper_config.args, [
        {<<"url">>, Url},
        {<<"fileModeMask">>, FileModeMask},
        {<<"dirModeMask">>, DirModeMask},
        {<<"timeout">>, Timeout, fun integer_to_binary/1}
    ]).


-spec build_admin_ctx_diff(helper_config:t(), onedata_storage:update_spec()) ->
    helper_config:user_ctx().
build_admin_ctx_diff(_HelperConfig, #storage_update_spec{credentials = undefined}) ->
    #{};
build_admin_ctx_diff(HelperConfig, #storage_update_spec{
    credentials = #xrootd_helper_credentials_diff{
        credentials_type = CredentialsType,
        credentials = Credentials
    }
}) ->
    helper_config_utils:build_args_diff_from_specs(HelperConfig#helper_config.admin_ctx, [
        {<<"credentialsType">>, CredentialsType, fun credentials_type_to_binary/1},
        {<<"credentials">>, Credentials}
    ]).


-spec describe(helper_config:t()) -> helper_config:description().
describe(#helper_config{
    name = ?XROOTD_HELPER_NAME,
    args = Args,
    admin_ctx = AdminCtx
}) ->
    %% Reconstruct configuration record from args map
    BaseConfiguration = #xrootd_helper_configuration{
        url = maps:get(<<"url">>, Args),
        storage_path_type = helper_config_utils:storage_path_type_from_binary(
            maps:get(<<"storagePathType">>, Args)
        )
    },
    Configuration = helper_config_utils:set_optional_record_fields_if_defined(BaseConfiguration, Args, [
        {<<"fileModeMask">>, #xrootd_helper_configuration.file_mode_mask},
        {<<"dirModeMask">>, #xrootd_helper_configuration.dir_mode_mask}
    ]),

    %% Reconstruct credentials record from admin_ctx (with redaction for security)
    BaseCredentials = #xrootd_helper_credentials{
        credentials_type = credentials_type_from_binary(maps:get(<<"credentialsType">>, AdminCtx))
    },
    Credentials = redact_confidential_credentials(
        helper_config_utils:set_optional_record_fields_if_defined(BaseCredentials, AdminCtx, [
            {<<"credentials">>, #xrootd_helper_credentials.credentials}
        ])
    ),

    #helper_config_description{
        type = ?XROOTD_HELPER_NAME,
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
is_oauth2_supported() -> false.


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


-spec redact_confidential_credentials(#xrootd_helper_credentials{}) -> #xrootd_helper_credentials{}.
redact_confidential_credentials(Credentials) ->
    helper_config_utils:redact_record_fields_if_defined(Credentials, [
        #xrootd_helper_credentials.credentials
    ]).


-spec redact_confidential_credentials_diff(#xrootd_helper_credentials_diff{}) -> #xrootd_helper_credentials_diff{}.
redact_confidential_credentials_diff(CredentialsDiff) ->
    helper_config_utils:redact_record_fields_if_defined(CredentialsDiff, [
        #xrootd_helper_credentials_diff.credentials
    ]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_args(onedata_storage:create_spec()) -> helper_config:args().
build_args(#storage_create_spec{
    timeout = Timeout,
    configuration = #xrootd_helper_configuration{
        url = Url,
        file_mode_mask = FileModeMask,
        dir_mode_mask = DirModeMask,
        storage_path_type = StoragePathType
    }
}) ->
    RequiredArgs = #{
        <<"url">> => Url,
        <<"storagePathType">> => helper_config_utils:storage_path_type_to_binary(StoragePathType)
    },
    helper_config_utils:add_optional_args_if_defined(RequiredArgs, [
        {<<"fileModeMask">>, FileModeMask},
        {<<"dirModeMask">>, DirModeMask},
        {<<"timeout">>, Timeout, fun integer_to_binary/1}
    ]).


%% @private
-spec build_admin_ctx(#xrootd_helper_credentials{}) -> helper_config:user_ctx().
build_admin_ctx(#xrootd_helper_credentials{
    credentials_type = CredentialsType,
    credentials = Credentials
}) ->
    BaseCtx = #{
        <<"credentialsType">> => credentials_type_to_binary(CredentialsType)
    },
    helper_config_utils:add_optional_args_if_defined(BaseCtx, [
        {<<"credentials">>, Credentials}
    ]).


%% @private
-spec credentials_type_to_binary(none | pwd) -> binary().
credentials_type_to_binary(none) -> <<"none">>;
credentials_type_to_binary(pwd) -> <<"pwd">>.


%% @private
-spec credentials_type_from_binary(binary()) -> none | pwd.
credentials_type_from_binary(<<"none">>) -> none;
credentials_type_from_binary(<<"pwd">>) -> pwd.
