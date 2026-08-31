%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of helper_spec_behaviour for Ceph storage.
%%% @end
%%%-------------------------------------------------------------------
-module(ceph_helper_spec).
-author("Bartosz Walkowicz").

-behaviour(helper_spec_behaviour).

-include("modules/storage/helpers/helpers.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").
-include_lib("opw_panel_contracts/include/storage/ceph.hrl").

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
build(CreateReq = #storage_create_spec{type = ?CEPH_HELPER_NAME, credentials = Credentials}) ->
    #helper_spec{
        name = ?CEPH_HELPER_NAME,
        configuration = build_configuration(CreateReq),
        credentials = build_credentials(Credentials)
    }.


-spec validate_credentials(helper_spec:credentials()) -> ok | {error, Reason :: term()}.
validate_credentials(Credentials) ->
    helper_spec_utils:validate_credentials(Credentials, [<<"username">>, <<"key">>]).


-spec build_configuration_diff(helper_spec:t(), onedata_storage:update_spec()) -> helper_spec:configuration().
build_configuration_diff(HelperSpec, UpdateSpec = #storage_update_spec{configuration = undefined}) ->
    build_configuration_diff(HelperSpec, UpdateSpec#storage_update_spec{
        configuration = #ceph_helper_configuration_diff{}
    });
build_configuration_diff(HelperSpec, #storage_update_spec{
    timeout = Timeout,
    configuration = #ceph_helper_configuration_diff{
        monitor_hostname = MonitorHostname,
        cluster_name = ClusterName,
        pool_name = PoolName
    }
}) ->
    helper_spec_utils:build_diff_from_specs(HelperSpec#helper_spec.configuration, [
        {<<"monitorHostname">>, MonitorHostname},
        {<<"clusterName">>, ClusterName},
        {<<"poolName">>, PoolName},
        {<<"timeout">>, Timeout, fun integer_to_binary/1}
    ]).


-spec build_credentials_diff(helper_spec:t(), onedata_storage:update_spec()) ->
    helper_spec:credentials().
build_credentials_diff(_HelperSpec, #storage_update_spec{credentials = undefined}) ->
    #{};
build_credentials_diff(HelperSpec, #storage_update_spec{
    credentials = #ceph_helper_credentials_diff{
        username = Username,
        key = Key
    }
}) ->
    helper_spec_utils:build_diff_from_specs(HelperSpec#helper_spec.credentials, [
        {<<"username">>, Username},
        {<<"key">>, Key}
    ]).


-spec describe(helper_spec:t()) -> helper_spec:description().
describe(#helper_spec{
    name = ?CEPH_HELPER_NAME,
    configuration = ConfigurationParams,
    credentials = CredentialsParams
}) ->
    %% Reconstruct configuration record from flat params
    Configuration = #ceph_helper_configuration{
        monitor_hostname = maps:get(<<"monitorHostname">>, ConfigurationParams),
        cluster_name = maps:get(<<"clusterName">>, ConfigurationParams),
        pool_name = maps:get(<<"poolName">>, ConfigurationParams),
        storage_path_type = helper_spec_utils:storage_path_type_from_binary(
            maps:get(<<"storagePathType">>, ConfigurationParams)
        )
    },

    %% Reconstruct credentials record from flat params
    Credentials = redact_confidential_credentials(#ceph_helper_credentials{
        username = maps:get(<<"username">>, CredentialsParams),
        key = maps:get(<<"key">>, CredentialsParams)
    }),

    #helper_spec_description{
        type = ?CEPH_HELPER_NAME,
        credentials = Credentials,
        configuration = Configuration,
        timeout = utils:convert_defined(
            maps:get(<<"timeout">>, ConfigurationParams, undefined),
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


-spec is_storage_access_type_supported(helper_spec:access_type()) -> boolean().
is_storage_access_type_supported(_) -> true.


-spec is_auto_import_supported(#helper_spec{}) -> boolean().
is_auto_import_supported(_HelperSpec) ->
    false.


-spec is_file_registration_supported(#helper_spec{}) -> boolean().
is_file_registration_supported(_HelperSpec) ->
    false.


-spec is_getting_size_supported(#helper_spec{}) -> boolean().
is_getting_size_supported(_HelperSpec) ->
    false.


-spec get_block_size(#helper_spec{}) -> non_neg_integer() | undefined.
get_block_size(#helper_spec{}) ->
    undefined.


-spec redact_confidential_credentials(#ceph_helper_credentials{}) -> #ceph_helper_credentials{}.
redact_confidential_credentials(Credentials = #ceph_helper_credentials{}) ->
    helper_spec_utils:redact_record_fields_if_defined(Credentials, [#ceph_helper_credentials.key]).


-spec redact_confidential_credentials_diff(#ceph_helper_credentials_diff{}) -> #ceph_helper_credentials_diff{}.
redact_confidential_credentials_diff(CredentialsDiff = #ceph_helper_credentials_diff{}) ->
    helper_spec_utils:redact_record_fields_if_defined(CredentialsDiff, [
        #ceph_helper_credentials_diff.key
    ]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_configuration(onedata_storage:create_spec()) -> helper_spec:configuration().
build_configuration(#storage_create_spec{
    timeout = Timeout,
    configuration = #ceph_helper_configuration{
        monitor_hostname = MonitorHostname,
        cluster_name = ClusterName,
        pool_name = PoolName,
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
        {<<"timeout">>, Timeout, fun integer_to_binary/1}
    ]).


%% @private
-spec build_credentials(#ceph_helper_credentials{}) -> helper_spec:credentials().
build_credentials(#ceph_helper_credentials{
    username = Username,
    key = Key
}) ->
    #{
        <<"username">> => Username,
        <<"key">> => Key
    }.
