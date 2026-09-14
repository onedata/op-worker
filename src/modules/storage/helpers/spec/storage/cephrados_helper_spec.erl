%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of helper_spec_behaviour for CephRados storage.
%%% @end
%%%-------------------------------------------------------------------
-module(cephrados_helper_spec).
-author("Bartosz Walkowicz").

-behaviour(helper_spec_behaviour).

-include("modules/storage/helpers/helpers.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").
-include_lib("opw_panel_contracts/include/storage/cephrados.hrl").

%% helper_spec_behaviour callbacks
-export([
    build/1,
    validate_credentials/1,
    build_configuration_diff/2,
    build_credentials_diff/2,
    describe/1,

    redact_confidential_credentials/1,
    redact_confidential_credentials_diff/1
]).


%%%===================================================================
%%% helper_spec_behaviour callbacks
%%%===================================================================


-spec build(onedata_storage:create_spec()) -> helper_spec:t().
build(CreateReq = #storage_create_spec{type = ?CEPHRADOS_HELPER_NAME, credentials = Credentials}) ->
    #helper_spec{
        name = ?CEPHRADOS_HELPER_NAME,
        configuration = build_configuration(CreateReq),
        credentials = build_credentials(Credentials)
    }.


-spec validate_credentials(helper_spec:credentials()) -> ok | {error, Reason :: term()}.
validate_credentials(Credentials) ->
    helper_spec_utils:validate_credentials(Credentials, [<<"username">>, <<"key">>]).


-spec build_configuration_diff(helper_spec:t(), onedata_storage:update_spec()) -> helper_spec:configuration().
build_configuration_diff(HelperSpec, UpdateSpec = #storage_update_spec{configuration = undefined}) ->
    build_configuration_diff(HelperSpec, UpdateSpec#storage_update_spec{
        configuration = #cephrados_helper_configuration_diff{}
    });
build_configuration_diff(HelperSpec, #storage_update_spec{
    configuration = #cephrados_helper_configuration_diff{
        monitor_hostname = MonitorHostname,
        cluster_name = ClusterName,
        pool_name = PoolName
    }
}) ->
    helper_spec_utils:build_diff_from_specs(HelperSpec#helper_spec.configuration, [
        {<<"monitorHostname">>, MonitorHostname},
        {<<"clusterName">>, ClusterName},
        {<<"poolName">>, PoolName}
    ]).


-spec build_credentials_diff(helper_spec:t(), onedata_storage:update_spec()) ->
    helper_spec:credentials().
build_credentials_diff(_HelperSpec, #storage_update_spec{credentials = undefined}) ->
    #{};
build_credentials_diff(HelperSpec, #storage_update_spec{
    credentials = #cephrados_helper_credentials_diff{
        username = Username,
        key = Key
    }
}) ->
    helper_spec_utils:build_diff_from_specs(HelperSpec#helper_spec.credentials, [
        {<<"username">>, Username},
        {<<"key">>, Key}
    ]).


-spec describe(helper_spec:t()) ->
    {onedata_storage:helper_configuration(), onedata_storage:helper_credentials()}.
describe(#helper_spec{
    name = ?CEPHRADOS_HELPER_NAME,
    configuration = ConfigurationParams,
    credentials = CredentialsParams
}) ->
    %% Reconstruct configuration record from flat params
    BaseConfiguration = #cephrados_helper_configuration{
        monitor_hostname = maps:get(<<"monitorHostname">>, ConfigurationParams),
        cluster_name = maps:get(<<"clusterName">>, ConfigurationParams),
        pool_name = maps:get(<<"poolName">>, ConfigurationParams),
        storage_path_type = helper_spec_utils:storage_path_type_from_binary(
            maps:get(<<"storagePathType">>, ConfigurationParams)
        )
    },
    Configuration = helper_spec_utils:set_optional_record_fields_if_defined(BaseConfiguration, ConfigurationParams, [
        {<<"blockSize">>, #cephrados_helper_configuration.block_size, fun binary_to_integer/1}
    ]),

    %% Reconstruct credentials record from flat params
    Credentials = redact_confidential_credentials(#cephrados_helper_credentials{
        username = maps:get(<<"username">>, CredentialsParams),
        key = maps:get(<<"key">>, CredentialsParams)
    }),

    {Configuration, Credentials}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_configuration(onedata_storage:create_spec()) -> helper_spec:configuration().
build_configuration(#storage_create_spec{
    configuration = #cephrados_helper_configuration{
        monitor_hostname = MonitorHostname,
        cluster_name = ClusterName,
        pool_name = PoolName,
        block_size = BlockSize,
        storage_path_type = StoragePathType
    }
}) ->
    RequiredParams = #{
        <<"monitorHostname">> => MonitorHostname,
        <<"clusterName">> => ClusterName,
        <<"poolName">> => PoolName,
        <<"storagePathType">> => helper_spec_utils:storage_path_type_to_binary(StoragePathType)
    },
    helper_spec_utils:add_optional_entries_if_defined(RequiredParams, [
        {<<"blockSize">>, BlockSize, fun integer_to_binary/1}
    ]).


%% @private
-spec build_credentials(#cephrados_helper_credentials{}) -> helper_spec:credentials().
build_credentials(#cephrados_helper_credentials{
    username = Username,
    key = Key
}) ->
    #{
        <<"username">> => Username,
        <<"key">> => Key
    }.


-spec redact_confidential_credentials(#cephrados_helper_credentials{}) -> #cephrados_helper_credentials{}.
redact_confidential_credentials(Credentials = #cephrados_helper_credentials{}) ->
    helper_spec_utils:redact_record_fields_if_defined(Credentials, [#cephrados_helper_credentials.key]).


-spec redact_confidential_credentials_diff(#cephrados_helper_credentials_diff{}) -> #cephrados_helper_credentials_diff{}.
redact_confidential_credentials_diff(CredentialsDiff = #cephrados_helper_credentials_diff{}) ->
    helper_spec_utils:redact_record_fields_if_defined(CredentialsDiff, [
        #cephrados_helper_credentials_diff.key
    ]).
