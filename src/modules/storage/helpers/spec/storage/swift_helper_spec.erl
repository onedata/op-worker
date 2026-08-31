%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of helper_spec_behaviour for Swift storage.
%%% @end
%%%-------------------------------------------------------------------
-module(swift_helper_spec).
-author("Bartosz Walkowicz").

-behaviour(helper_spec_behaviour).

-include("modules/storage/helpers/helpers.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").
-include_lib("opw_panel_contracts/include/storage/swift.hrl").

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

-define(DEFAULT_SWIFT_BLOCK_SIZE, 10485760).


%%%===================================================================
%%% helper_spec_behaviour callbacks
%%%===================================================================


-spec build(onedata_storage:create_spec()) -> helper_spec:t().
build(CreateReq = #storage_create_spec{type = ?SWIFT_HELPER_NAME, credentials = Credentials}) ->
    #helper_spec{
        name = ?SWIFT_HELPER_NAME,
        configuration = build_configuration(CreateReq),
        credentials = build_credentials(Credentials)
    }.


-spec validate_credentials(helper_spec:credentials()) -> ok | {error, Reason :: term()}.
validate_credentials(Credentials) ->
    helper_spec_utils:validate_credentials(
        Credentials,
        [<<"username">>, <<"password">>, <<"projectName">>],
        [<<"userDomainName">>, <<"projectDomainName">>]
    ).


-spec build_configuration_diff(helper_spec:t(), onedata_storage:update_spec()) -> helper_spec:configuration().
build_configuration_diff(HelperSpec, UpdateSpec = #storage_update_spec{configuration = undefined}) ->
    build_configuration_diff(HelperSpec, UpdateSpec#storage_update_spec{
        configuration = #swift_helper_configuration_diff{}
    });
build_configuration_diff(HelperSpec, #storage_update_spec{
    configuration = #swift_helper_configuration_diff{
        auth_url = AuthUrl,
        container_name = ContainerName
    }
}) ->
    helper_spec_utils:build_diff_from_specs(HelperSpec#helper_spec.configuration, [
        {<<"authUrl">>, AuthUrl},
        {<<"containerName">>, ContainerName}
    ]).


-spec build_credentials_diff(helper_spec:t(), onedata_storage:update_spec()) ->
    helper_spec:credentials().
build_credentials_diff(_HelperSpec, #storage_update_spec{credentials = undefined}) ->
    #{};
build_credentials_diff(HelperSpec, #storage_update_spec{
    credentials = #swift_helper_credentials_diff{
        username = Username,
        password = Password,
        project_name = ProjectName,
        user_domain_name = UserDomainName,
        project_domain_name = ProjectDomainName
    }
}) ->
    helper_spec_utils:build_diff_from_specs(HelperSpec#helper_spec.credentials, [
        {<<"username">>, Username},
        {<<"password">>, Password},
        {<<"projectName">>, ProjectName},
        {<<"userDomainName">>, UserDomainName},
        {<<"projectDomainName">>, ProjectDomainName}
    ]).


-spec describe(helper_spec:t()) -> helper_spec:description().
describe(#helper_spec{
    name = ?SWIFT_HELPER_NAME,
    configuration = ConfigurationParams,
    credentials = CredentialsParams
}) ->
    %% Reconstruct configuration record from flat params
    BaseConfiguration = #swift_helper_configuration{
        auth_url = maps:get(<<"authUrl">>, ConfigurationParams),
        container_name = maps:get(<<"containerName">>, ConfigurationParams),
        storage_path_type = helper_spec_utils:storage_path_type_from_binary(
            maps:get(<<"storagePathType">>, ConfigurationParams)
        )
    },
    Configuration = helper_spec_utils:set_optional_record_fields_if_defined(BaseConfiguration, ConfigurationParams, [
        {<<"blockSize">>, #swift_helper_configuration.block_size, fun binary_to_integer/1}
    ]),

    %% Reconstruct credentials record from flat params
    BaseCredentials = #swift_helper_credentials{
        username = maps:get(<<"username">>, CredentialsParams),
        password = maps:get(<<"password">>, CredentialsParams),
        project_name = maps:get(<<"projectName">>, CredentialsParams)
    },
    Credentials = redact_confidential_credentials(
        helper_spec_utils:set_optional_record_fields_if_defined(BaseCredentials, CredentialsParams, [
            {<<"userDomainName">>, #swift_helper_credentials.user_domain_name},
            {<<"projectDomainName">>, #swift_helper_credentials.project_domain_name}
        ])
    ),

    #helper_spec_description{
        type = ?SWIFT_HELPER_NAME,
        configuration = Configuration,
        credentials = Credentials
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


-spec is_storage_access_type_supported(helper_spec:access_type()) -> boolean().
is_storage_access_type_supported(_) -> true.


-spec is_auto_import_supported(#helper_spec{}) -> boolean().
is_auto_import_supported(_HelperSpec) ->
    false.


-spec is_file_registration_supported(#helper_spec{}) -> boolean().
is_file_registration_supported(HelperSpec) ->
    helper_spec_utils:is_canonical(HelperSpec) andalso block_size_equals_0(HelperSpec).


-spec is_getting_size_supported(#helper_spec{}) -> boolean().
is_getting_size_supported(HelperSpec) ->
    block_size_equals_0(HelperSpec).


-spec get_block_size(#helper_spec{}) -> non_neg_integer() | undefined.
get_block_size(#helper_spec{configuration = ConfigurationParams}) ->
    case maps:get(<<"blockSize">>, ConfigurationParams, ?DEFAULT_SWIFT_BLOCK_SIZE) of
        Bin when is_binary(Bin) -> binary_to_integer(Bin);
        Int when is_integer(Int) -> Int
    end.


-spec redact_confidential_credentials(#swift_helper_credentials{}) -> #swift_helper_credentials{}.
redact_confidential_credentials(Credentials) ->
    helper_spec_utils:redact_record_fields_if_defined(Credentials, [#swift_helper_credentials.password]).


-spec redact_confidential_credentials_diff(#swift_helper_credentials_diff{}) -> #swift_helper_credentials_diff{}.
redact_confidential_credentials_diff(CredentialsDiff) ->
    helper_spec_utils:redact_record_fields_if_defined(CredentialsDiff, [#swift_helper_credentials_diff.password]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_configuration(onedata_storage:create_spec()) -> helper_spec:configuration().
build_configuration(#storage_create_spec{
    configuration = #swift_helper_configuration{
        auth_url = AuthUrl,
        container_name = ContainerName,
        block_size = BlockSize,
        storage_path_type = StoragePathType
    }
}) ->
    RequiredParams = #{
        <<"authUrl">> => AuthUrl,
        <<"containerName">> => ContainerName,
        <<"storagePathType">> => helper_spec_utils:storage_path_type_to_binary(StoragePathType)
    },
    helper_spec_utils:add_optional_entries_if_defined(RequiredParams, [
        {<<"blockSize">>, BlockSize, fun integer_to_binary/1}
    ]).


%% @private
-spec build_credentials(#swift_helper_credentials{}) -> helper_spec:credentials().
build_credentials(#swift_helper_credentials{
    username = Username,
    password = Password,
    project_name = ProjectName,
    user_domain_name = UserDomainName,
    project_domain_name = ProjectDomainName
}) ->
    BaseCredentialsParams = #{
        <<"username">> => Username,
        <<"password">> => Password,
        <<"projectName">> => ProjectName
    },
    helper_spec_utils:add_optional_entries_if_defined(BaseCredentialsParams, [
        {<<"userDomainName">>, UserDomainName},
        {<<"projectDomainName">>, ProjectDomainName}
    ]).


%% @private
-spec block_size_equals_0(helper_spec:t()) -> boolean().
block_size_equals_0(HelperSpec) ->
    get_block_size(HelperSpec) =:= 0.
