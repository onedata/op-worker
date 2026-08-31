%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Central dispatcher module for storage helper specs.
%%% Routes calls to storage-specific implementation modules and provides
%%% a unified API for creating, updating, and querying helper specs.
%%% @end
%%%-------------------------------------------------------------------
-module(helper_spec).
-author("Bartosz Walkowicz").

-include("modules/storage/helpers/helpers.hrl").

-include("global_definitions.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("hackney/include/hackney_lib.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").

%% API
-export([
    build/1,
    build_helper_params/2,
    validate_credentials/2,
    update/2,
    describe/1,

    %% Property queries
    is_posix_compatible/1,
    is_object_storage/1,
    is_rename_supported/1,
    is_nfs4_acl_supported/1,
    is_oauth2_supported/1,
    is_storage_access_type_supported/2,
    is_auto_import_supported/1,
    is_file_registration_supported/1,
    is_import_supported/1,
    is_getting_size_supported/1,

    get_name/1,
    get_configuration/1,
    get_credentials/1,
    get_timeout/1,
    get_block_size/1,
    get_storage_path_type/1,
    get_params/2,
    get_proxy_params/2,

    redact_confidential_credentials/2,
    redact_confidential_credentials_diff/2
]).

-type t() :: #helper_spec{}.

-type name() :: onedata_storage:type().
%% The two halves of the helper's parameter set, each a flat binary map:
%% configuration is fixed for the storage, credentials are substituted per user
%% by LUMA. Merged together they form the params handed to the storage helper.
-type configuration() :: #{binary() => binary()}.
-type credentials() :: #{binary() => binary()}.
-type helper_params() :: #{binary() => binary()}.
-type access_type() :: ?READONLY | ?READWRITE.

-type description() :: #helper_spec_description{}.

-export_type([t/0, name/0, configuration/0, credentials/0, helper_params/0, access_type/0, description/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(onedata_storage:create_spec()) -> t().
build(CreateReq = #storage_create_spec{type = Type}) ->
    Module = get_module(Type),
    Module:build(CreateReq).


-spec build_helper_params(t(), credentials()) ->
    {ok, helper_params()} | {error, Reason :: term()}.
build_helper_params(HelperSpec, CredentialsParams) ->
    case validate_credentials(HelperSpec, CredentialsParams) of
        ok -> {ok, maps:merge(HelperSpec#helper_spec.configuration, CredentialsParams)};
        Error -> Error
    end.


-spec validate_credentials(t() | name(), credentials()) -> ok | {error, Reason :: term()}.
validate_credentials(HelperSpecOrName, CredentialsParams) ->
    Module = get_module(HelperSpecOrName),
    Module:validate_credentials(CredentialsParams).


-spec update(t(), onedata_storage:update_spec()) ->
    {ok, t()} | {error, no_change}.
update(
    HelperSpec = #helper_spec{name = StorageType},
    UpdateSpec = #storage_update_spec{type = StorageType}
) ->
    Module = get_module(HelperSpec),

    ConfigurationDiff = Module:build_configuration_diff(HelperSpec, UpdateSpec),
    CredentialsDiff = Module:build_credentials_diff(HelperSpec, UpdateSpec),

    case {maps_utils:is_empty(ConfigurationDiff), maps_utils:is_empty(CredentialsDiff)} of
        {true, true} ->
            {error, no_change};
        {IsEmptyConfigurationDiff, IsEmptyCredentialsDiff} ->
            CurrentConfiguration = HelperSpec#helper_spec.configuration,
            CurrentCredentials = HelperSpec#helper_spec.credentials,

            {ok, HelperSpec#helper_spec{
                configuration = case IsEmptyConfigurationDiff of
                    true -> CurrentConfiguration;
                    false -> maps:merge(CurrentConfiguration, ConfigurationDiff)
                end,
                credentials = case IsEmptyCredentialsDiff of
                    true -> CurrentCredentials;
                    false -> maps:merge(CurrentCredentials, CredentialsDiff)
                end
            }}
    end.


-spec describe(t()) -> description().
describe(HelperSpec) ->
    Module = get_module(HelperSpec),
    Module:describe(HelperSpec).


-spec is_posix_compatible(t() | helper_spec:name()) -> boolean().
is_posix_compatible(HelperSpecOrName) ->
    Module = get_module(HelperSpecOrName),
    Module:is_posix_compatible().


-spec is_object_storage(t() | helper_spec:name()) -> boolean().
is_object_storage(HelperSpecOrName) ->
    Module = get_module(HelperSpecOrName),
    Module:is_object_storage().


-spec is_rename_supported(t() | helper_spec:name()) -> boolean().
is_rename_supported(HelperSpecOrName) ->
    Module = get_module(HelperSpecOrName),
    Module:is_rename_supported().


-spec is_nfs4_acl_supported(t() | helper_spec:name()) -> boolean().
is_nfs4_acl_supported(HelperSpecOrName) ->
    Module = get_module(HelperSpecOrName),
    Module:is_nfs4_acl_supported().


-spec is_oauth2_supported(t() | helper_spec:name()) -> boolean().
is_oauth2_supported(HelperSpecOrName) ->
    Module = get_module(HelperSpecOrName),
    Module:is_oauth2_supported().


-spec is_storage_access_type_supported(t() | helper_spec:name(), helper_spec:access_type()) -> boolean().
is_storage_access_type_supported(HelperSpecName, AccessType) ->
    Module = get_module(HelperSpecName),
    Module:is_storage_access_type_supported(AccessType).


-spec is_auto_import_supported(t()) -> boolean().
is_auto_import_supported(HelperSpec) ->
    Module = get_module(HelperSpec),
    Module:is_auto_import_supported(HelperSpec).


-spec is_file_registration_supported(t()) -> boolean().
is_file_registration_supported(HelperSpec) ->
    Module = get_module(HelperSpec),
    Module:is_file_registration_supported(HelperSpec).


-spec is_import_supported(t()) -> boolean().
is_import_supported(HelperSpec) ->
    is_auto_import_supported(HelperSpec) orelse
        is_file_registration_supported(HelperSpec).


-spec is_getting_size_supported(t()) -> boolean().
is_getting_size_supported(HelperSpec) ->
    Module = get_module(HelperSpec),
    Module:is_getting_size_supported(HelperSpec).


-spec get_name(t()) -> name().
get_name(#helper_spec{name = Name}) -> Name.


-spec get_configuration(t()) -> configuration().
get_configuration(#helper_spec{configuration = ConfigurationParams}) -> ConfigurationParams.


-spec get_credentials(t()) -> credentials().
get_credentials(#helper_spec{credentials = CredentialsParams}) -> CredentialsParams.


-spec get_timeout(t() | undefined) -> integer().
get_timeout(undefined) ->
    {ok, Value} = application:get_env(?APP_NAME, helpers_async_operation_timeout_milliseconds),
    Value;
get_timeout(#helper_spec{configuration = ConfigurationParams}) ->
    case maps:find(<<"timeout">>, ConfigurationParams) of
        {ok, Value} ->
            erlang:binary_to_integer(Value);
        error ->
            get_timeout(undefined)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns the block size used by the storage.
%% Returns undefined for non-object storage types.
%% @end
%%--------------------------------------------------------------------
-spec get_block_size(t()) -> non_neg_integer() | undefined.
get_block_size(HelperSpec) ->
    Module = get_module(HelperSpec),
    Module:get_block_size(HelperSpec).


-spec get_storage_path_type(t()) -> binary().
get_storage_path_type(HelperSpec) ->
    helper_spec_utils:get_storage_path_type(HelperSpec).


-spec get_params(t(), credentials()) -> #helper_params{}.
get_params(HelperSpec, CredentialsParams) ->
    {ok, HelperParams} = build_helper_params(HelperSpec, CredentialsParams),
    #helper_params{
        helper_name = HelperSpec#helper_spec.name,
        helper_args = [
            #helper_arg{key = Key, value = Value} || {Key, Value} <- maps:to_list(HelperParams)
        ]
    }.


-spec get_proxy_params(timeout(), storage:id()) -> #helper_params{}.
get_proxy_params(Timeout, StorageId) ->
    Latency = op_worker:get_env(proxy_helper_latency_milliseconds),
    TimeoutValue = integer_to_binary(Timeout + Latency),
    #helper_params{
        helper_name = ?PROXY_HELPER_NAME,
        helper_args = [
            #helper_arg{key = <<"storageId">>, value = StorageId},
            #helper_arg{key = <<"timeout">>, value = TimeoutValue}
        ]
    }.


-spec redact_confidential_credentials(t() | name(), onedata_storage:helper_credentials()) ->
    onedata_storage:helper_credentials().
redact_confidential_credentials(HelperSpecOrName, Credentials) ->
    Module = get_module(HelperSpecOrName),
    Module:redact_confidential_credentials(Credentials).


-spec redact_confidential_credentials_diff(t() | name(), onedata_storage:helper_credentials_diff()) ->
    onedata_storage:helper_credentials_diff().
redact_confidential_credentials_diff(HelperSpecOrName, CredentialsDiff) ->
    Module = get_module(HelperSpecOrName),
    Module:redact_confidential_credentials_diff(CredentialsDiff).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_module(t() | name()) -> module().
get_module(#helper_spec{name = HelperName}) ->
    get_module(HelperName);
get_module(?CEPH_HELPER_NAME) -> ceph_helper_spec:module_info(module);
get_module(?CEPHRADOS_HELPER_NAME) -> cephrados_helper_spec:module_info(module);
get_module(?GLUSTERFS_HELPER_NAME) -> glusterfs_helper_spec:module_info(module);
get_module(?HTTP_HELPER_NAME) -> http_helper_spec:module_info(module);
get_module(?NFS_HELPER_NAME) -> nfs_helper_spec:module_info(module);
get_module(?NULL_DEVICE_HELPER_NAME) -> nulldevice_helper_spec:module_info(module);
get_module(?POSIX_HELPER_NAME) -> posix_helper_spec:module_info(module);
get_module(?S3_HELPER_NAME) -> s3_helper_spec:module_info(module);
get_module(?SWIFT_HELPER_NAME) -> swift_helper_spec:module_info(module);
get_module(?WEBDAV_HELPER_NAME) -> webdav_helper_spec:module_info(module);
get_module(?XROOTD_HELPER_NAME) -> xrootd_helper_spec:module_info(module).
