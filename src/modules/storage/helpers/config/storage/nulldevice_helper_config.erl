%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of helper_config_behaviour for NullDevice storage.
%%% This is a testing/simulation storage backend.
%%% @end
%%%-------------------------------------------------------------------
-module(nulldevice_helper_config).
-author("Bartosz Walkowicz").

-behaviour(helper_config_behaviour).

-include("modules/storage/helpers/helpers.hrl").
-include_lib("op_panel_contracts/include/storage/common.hrl").
-include_lib("op_panel_contracts/include/storage/nulldevice.hrl").

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
build(CreateReq = #storage_create_spec{type = ?NULL_DEVICE_HELPER_NAME, credentials = Credentials}) ->
    #helper_config{
        name = ?NULL_DEVICE_HELPER_NAME,
        args = build_args(CreateReq),
        admin_ctx = build_admin_ctx(Credentials)
    }.


-spec validate_user_ctx(helper_config:user_ctx()) -> ok | {error, Reason :: term()}.
validate_user_ctx(UserCtx) ->
    helper_config_utils:validate_user_ctx(UserCtx, [<<"uid">>], [<<"gid">>]).


-spec build_args_diff(helper_config:t(), onedata_storage:update_spec()) -> helper_config:args().
build_args_diff(HelperConfig, UpdateSpec = #storage_update_spec{configuration = undefined}) ->
    build_args_diff(HelperConfig, UpdateSpec#storage_update_spec{
        configuration = #nulldevice_configuration_diff{}
    });
build_args_diff(HelperConfig, #storage_update_spec{
    timeout = Timeout,
    configuration = #nulldevice_configuration_diff{
        latency_min = LatencyMin,
        latency_max = LatencyMax,
        timeout_probability = TimeoutProbability,
        filter = Filter,
        simulated_filesystem_parameters = SimulatedFilesystemParameters,
        simulated_filesystem_grow_speed = SimulatedFilesystemGrowSpeed,
        enable_data_verification = EnableDataVerification
    }
}) ->
    helper_config_utils:build_args_diff_from_specs(HelperConfig#helper_config.args, [
        {<<"latencyMin">>, LatencyMin, fun integer_to_binary/1},
        {<<"latencyMax">>, LatencyMax, fun integer_to_binary/1},
        {<<"timeoutProbability">>, TimeoutProbability, fun float_to_binary/1},
        {<<"filter">>, Filter},
        {<<"simulatedFilesystemParameters">>, SimulatedFilesystemParameters},
        {<<"simulatedFilesystemGrowSpeed">>, SimulatedFilesystemGrowSpeed, fun float_to_binary/1},
        {<<"enableDataVerification">>, EnableDataVerification, fun atom_to_binary/1},
        {<<"timeout">>, Timeout, fun integer_to_binary/1}
    ]).


-spec build_admin_ctx_diff(helper_config:t(), onedata_storage:update_spec()) ->
    helper_config:user_ctx().
build_admin_ctx_diff(_HelperConfig, #storage_update_spec{credentials = undefined}) ->
    #{};
build_admin_ctx_diff(HelperConfig, #storage_update_spec{
    credentials = #nulldevice_credentials_diff{
        uid = Uid,
        gid = Gid
    }
}) ->
    helper_config_utils:build_args_diff_from_specs(HelperConfig#helper_config.admin_ctx, [
        {<<"uid">>, Uid, fun integer_to_binary/1},
        {<<"gid">>, Gid, fun integer_to_binary/1}
    ]).


-spec describe(helper_config:t()) -> helper_config:description().
describe(#helper_config{
    name = ?NULL_DEVICE_HELPER_NAME,
    args = Args,
    admin_ctx = AdminCtx
}) ->
    %% Reconstruct configuration record from args map
    BaseConfiguration = #nulldevice_configuration{
        storage_path_type = helper_config_utils:storage_path_type_from_binary(
            maps:get(<<"storagePathType">>, Args)
        )
    },
    Configuration = helper_config_utils:set_optional_record_fields_if_defined(BaseConfiguration, Args, [
        {<<"latencyMin">>, #nulldevice_configuration.latency_min, fun binary_to_integer/1},
        {<<"latencyMax">>, #nulldevice_configuration.latency_max, fun binary_to_integer/1},
        {<<"timeoutProbability">>, #nulldevice_configuration.timeout_probability, fun binary_to_float/1},
        {<<"filter">>, #nulldevice_configuration.filter},
        {<<"simulatedFilesystemParameters">>, #nulldevice_configuration.simulated_filesystem_parameters},
        {<<"simulatedFilesystemGrowSpeed">>, #nulldevice_configuration.simulated_filesystem_grow_speed, fun binary_to_float/1},
        {<<"enableDataVerification">>, #nulldevice_configuration.enable_data_verification, fun utils:to_boolean/1}
    ]),

    %% Reconstruct credentials record from admin_ctx
    BaseCredentials = #nulldevice_credentials{
        uid = binary_to_integer(maps:get(<<"uid">>, AdminCtx))
    },
    Credentials = redact_confidential_credentials(
        helper_config_utils:set_optional_record_fields_if_defined(BaseCredentials, AdminCtx, [
            {<<"gid">>, #nulldevice_credentials.gid, fun binary_to_integer/1}
        ])
    ),

    #helper_config_description{
        type = ?NULL_DEVICE_HELPER_NAME,
        credentials = Credentials,
        configuration = Configuration,
        timeout = utils:convert_defined(
            maps:get(<<"timeout">>, Args, undefined),
            fun binary_to_integer/1
        )
    }.


-spec is_posix_compatible() -> boolean().
is_posix_compatible() -> true.


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


-spec redact_confidential_credentials(#nulldevice_credentials{}) -> #nulldevice_credentials{}.
redact_confidential_credentials(Credentials) ->
    Credentials.


-spec redact_confidential_credentials_diff(#nulldevice_credentials_diff{}) -> #nulldevice_credentials_diff{}.
redact_confidential_credentials_diff(CredentialsDiff) ->
    CredentialsDiff.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_args(onedata_storage:create_spec()) -> helper_config:args().
build_args(#storage_create_spec{
    timeout = Timeout,
    configuration = #nulldevice_configuration{
        latency_min = LatencyMin,
        latency_max = LatencyMax,
        timeout_probability = TimeoutProbability,
        filter = Filter,
        simulated_filesystem_parameters = SimulatedFilesystemParameters,
        simulated_filesystem_grow_speed = SimulatedFilesystemGrowSpeed,
        enable_data_verification = EnableDataVerification,
        storage_path_type = StoragePathType
    }
}) ->
    RequiredArgs = #{
        <<"storagePathType">> => helper_config_utils:storage_path_type_to_binary(StoragePathType)
    },
    helper_config_utils:add_optional_args_if_defined(RequiredArgs, [
        {<<"latencyMin">>, LatencyMin, fun integer_to_binary/1},
        {<<"latencyMax">>, LatencyMax, fun integer_to_binary/1},
        {<<"timeoutProbability">>, TimeoutProbability, fun float_to_binary/1},
        {<<"filter">>, Filter},
        {<<"simulatedFilesystemParameters">>, SimulatedFilesystemParameters},
        {<<"simulatedFilesystemGrowSpeed">>, SimulatedFilesystemGrowSpeed, fun float_to_binary/1},
        {<<"enableDataVerification">>, EnableDataVerification, fun atom_to_binary/1},
        {<<"timeout">>, Timeout, fun integer_to_binary/1}
    ]).


%% @private
-spec build_admin_ctx(#nulldevice_credentials{}) -> helper_config:user_ctx().
build_admin_ctx(#nulldevice_credentials{
    uid = Uid,
    gid = Gid
}) ->
    BaseCtx = #{
        <<"uid">> => integer_to_binary(Uid)
    },
    helper_config_utils:add_optional_args_if_defined(BaseCtx, [
        {<<"gid">>, Gid, fun integer_to_binary/1}
    ]).
