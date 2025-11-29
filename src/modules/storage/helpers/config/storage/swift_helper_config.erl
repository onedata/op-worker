%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of helper_config_behaviour for Swift storage.
%%% @end
%%%-------------------------------------------------------------------
-module(swift_helper_config).
-author("Bartosz Walkowicz").

-behaviour(helper_config_behaviour).

-include("modules/storage/helpers/helpers.hrl").
-include_lib("op_panel_contracts/include/storage/common.hrl").
-include_lib("op_panel_contracts/include/storage/swift.hrl").

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

-define(DEFAULT_SWIFT_BLOCK_SIZE, 10485760).


%%%===================================================================
%%% helper_config_behaviour callbacks
%%%===================================================================


-spec build(onedata_storage:create_spec()) -> helper_config:t().
build(CreateReq = #storage_create_spec{type = ?SWIFT_HELPER_NAME, credentials = Credentials}) ->
    #helper_config{
        name = ?SWIFT_HELPER_NAME,
        args = build_args(CreateReq),
        admin_ctx = build_admin_ctx(Credentials)
    }.


-spec validate_user_ctx(helper_config:user_ctx()) -> ok | {error, Reason :: term()}.
validate_user_ctx(UserCtx) ->
    helper_config_utils:validate_user_ctx(
        UserCtx,
        [<<"username">>, <<"password">>, <<"projectName">>],
        [<<"userDomainName">>, <<"projectDomainName">>]
    ).


-spec build_args_diff(helper_config:t(), onedata_storage:update_spec()) -> helper_config:args().
build_args_diff(HelperConfig, UpdateSpec = #storage_update_spec{configuration = undefined}) ->
    build_args_diff(HelperConfig, UpdateSpec#storage_update_spec{
        configuration = #swift_configuration_diff{}
    });
build_args_diff(HelperConfig, #storage_update_spec{
    timeout = Timeout,
    configuration = #swift_configuration_diff{
        auth_url = AuthUrl,
        container_name = ContainerName
    }
}) ->
    helper_config_utils:build_args_diff_from_specs(HelperConfig#helper_config.args, [
        {<<"authUrl">>, AuthUrl},
        {<<"containerName">>, ContainerName},
        {<<"timeout">>, Timeout, fun integer_to_binary/1}
    ]).


-spec build_admin_ctx_diff(helper_config:t(), onedata_storage:update_spec()) ->
    helper_config:user_ctx().
build_admin_ctx_diff(_HelperConfig, #storage_update_spec{credentials = undefined}) ->
    #{};
build_admin_ctx_diff(HelperConfig, #storage_update_spec{
    credentials = #swift_credentials_diff{
        username = Username,
        password = Password,
        project_name = ProjectName,
        user_domain_name = UserDomainName,
        project_domain_name = ProjectDomainName
    }
}) ->
    helper_config_utils:build_args_diff_from_specs(HelperConfig#helper_config.admin_ctx, [
        {<<"username">>, Username},
        {<<"password">>, Password},
        {<<"projectName">>, ProjectName},
        {<<"userDomainName">>, UserDomainName},
        {<<"projectDomainName">>, ProjectDomainName}
    ]).


-spec describe(helper_config:t()) -> helper_config:description().
describe(#helper_config{
    name = ?SWIFT_HELPER_NAME,
    args = Args,
    admin_ctx = AdminCtx
}) ->
    %% Reconstruct configuration record from args map
    BaseConfiguration = #swift_configuration{
        auth_url = maps:get(<<"authUrl">>, Args),
        container_name = maps:get(<<"containerName">>, Args),
        storage_path_type = helper_config_utils:storage_path_type_from_binary(
            maps:get(<<"storagePathType">>, Args)
        )
    },
    Configuration = helper_config_utils:set_optional_record_fields_if_defined(BaseConfiguration, Args, [
        {<<"blockSize">>, #swift_configuration.block_size, fun binary_to_integer/1}
    ]),

    %% Reconstruct credentials record from admin_ctx
    BaseCredentials = #swift_credentials{
        username = maps:get(<<"username">>, AdminCtx),
        password = maps:get(<<"password">>, AdminCtx),
        project_name = maps:get(<<"projectName">>, AdminCtx)
    },
    Credentials = redact_confidential_credentials(
        helper_config_utils:set_optional_record_fields_if_defined(BaseCredentials, AdminCtx, [
            {<<"userDomainName">>, #swift_credentials.user_domain_name},
            {<<"projectDomainName">>, #swift_credentials.project_domain_name}
        ])
    ),

    #helper_config_description{
        type = ?SWIFT_HELPER_NAME,
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
is_object_storage() -> true.


-spec is_rename_supported() -> boolean().
is_rename_supported() -> false.


-spec is_nfs4_acl_supported() -> boolean().
is_nfs4_acl_supported() -> false.


-spec is_oauth2_supported() -> boolean().
is_oauth2_supported() -> false.


-spec is_storage_access_type_supported(helper_config:access_type()) -> boolean().
is_storage_access_type_supported(_) -> true.


-spec is_auto_import_supported(#helper_config{}) -> boolean().
is_auto_import_supported(_HelperConfig) ->
    false.


-spec is_file_registration_supported(#helper_config{}) -> boolean().
is_file_registration_supported(HelperConfig) ->
    helper_config_utils:is_canonical(HelperConfig) andalso block_size_equals_0(HelperConfig).


-spec is_getting_size_supported(#helper_config{}) -> boolean().
is_getting_size_supported(HelperConfig) ->
    block_size_equals_0(HelperConfig).


-spec get_block_size(#helper_config{}) -> non_neg_integer() | undefined.
get_block_size(#helper_config{args = Args}) ->
    case maps:get(<<"blockSize">>, Args, ?DEFAULT_SWIFT_BLOCK_SIZE) of
        Bin when is_binary(Bin) -> binary_to_integer(Bin);
        Int when is_integer(Int) -> Int
    end.


-spec redact_confidential_credentials(#swift_credentials{}) -> #swift_credentials{}.
redact_confidential_credentials(Credentials) ->
    helper_config_utils:redact_record_fields_if_defined(Credentials, [#swift_credentials.password]).


-spec redact_confidential_credentials_diff(#swift_credentials_diff{}) -> #swift_credentials_diff{}.
redact_confidential_credentials_diff(CredentialsDiff) ->
    helper_config_utils:redact_record_fields_if_defined(CredentialsDiff, [#swift_credentials_diff.password]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_args(onedata_storage:create_spec()) -> helper_config:args().
build_args(#storage_create_spec{
    timeout = Timeout,
    configuration = #swift_configuration{
        auth_url = AuthUrl,
        container_name = ContainerName,
        block_size = BlockSize,
        storage_path_type = StoragePathType
    }
}) ->
    RequiredArgs = #{
        <<"authUrl">> => AuthUrl,
        <<"containerName">> => ContainerName,
        <<"storagePathType">> => helper_config_utils:storage_path_type_to_binary(StoragePathType)
    },
    helper_config_utils:add_optional_args_if_defined(RequiredArgs, [
        {<<"blockSize">>, BlockSize, fun integer_to_binary/1},
        {<<"timeout">>, Timeout, fun integer_to_binary/1}
    ]).


%% @private
-spec build_admin_ctx(#swift_credentials{}) -> helper_config:user_ctx().
build_admin_ctx(#swift_credentials{
    username = Username,
    password = Password,
    project_name = ProjectName,
    user_domain_name = UserDomainName,
    project_domain_name = ProjectDomainName
}) ->
    BaseCtx = #{
        <<"username">> => Username,
        <<"password">> => Password,
        <<"projectName">> => ProjectName
    },
    helper_config_utils:add_optional_args_if_defined(BaseCtx, [
        {<<"userDomainName">>, UserDomainName},
        {<<"projectDomainName">>, ProjectDomainName}
    ]).


%% @private
-spec block_size_equals_0(helper_config:t()) -> boolean().
block_size_equals_0(HelperConfig) ->
    get_block_size(HelperConfig) =:= 0.
