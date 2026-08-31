%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of helper_spec_behaviour for POSIX storage.
%%% @end
%%%-------------------------------------------------------------------
-module(posix_helper_spec).
-author("Bartosz Walkowicz").

-behaviour(helper_spec_behaviour).

-include("modules/storage/helpers/helpers.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").
-include_lib("opw_panel_contracts/include/storage/posix.hrl").

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
build(CreateReq = #storage_create_spec{type = ?POSIX_HELPER_NAME, credentials = Credentials}) ->
    #helper_spec{
        name = ?POSIX_HELPER_NAME,
        configuration = build_configuration(CreateReq),
        credentials = build_credentials(Credentials)
    }.


-spec validate_credentials(helper_spec:credentials()) -> ok | {error, Reason :: term()}.
validate_credentials(Credentials) ->
    helper_spec_utils:validate_credentials(Credentials, [<<"uid">>], [<<"gid">>]).


-spec build_configuration_diff(helper_spec:t(), onedata_storage:update_spec()) -> helper_spec:configuration().
build_configuration_diff(HelperSpec, UpdateSpec = #storage_update_spec{configuration = undefined}) ->
    build_configuration_diff(HelperSpec, UpdateSpec#storage_update_spec{
        configuration = #posix_helper_configuration_diff{}
    });
build_configuration_diff(HelperSpec, #storage_update_spec{
    configuration = #posix_helper_configuration_diff{
        mount_point = MountPoint
    }
}) ->
    helper_spec_utils:build_diff_from_specs(HelperSpec#helper_spec.configuration, [
        {<<"mountPoint">>, MountPoint}
    ]).


-spec build_credentials_diff(helper_spec:t(), onedata_storage:update_spec()) ->
    helper_spec:credentials().
build_credentials_diff(_HelperSpec, #storage_update_spec{credentials = undefined}) ->
    #{};
build_credentials_diff(HelperSpec, #storage_update_spec{
    credentials = #posix_helper_credentials_diff{
        uid = Uid,
        gid = Gid
    }
}) ->
    helper_spec_utils:build_diff_from_specs(HelperSpec#helper_spec.credentials, [
        {<<"uid">>, Uid, fun integer_to_binary/1},
        {<<"gid">>, Gid, fun integer_to_binary/1}
    ]).


-spec describe(helper_spec:t()) -> helper_spec:description().
describe(#helper_spec{
    name = ?POSIX_HELPER_NAME,
    configuration = ConfigurationParams,
    credentials = CredentialsParams
}) ->
    %% Reconstruct configuration record from flat params
    BaseConfiguration = #posix_helper_configuration{
        mount_point = maps:get(<<"mountPoint">>, ConfigurationParams),
        storage_path_type = helper_spec_utils:storage_path_type_from_binary(
            maps:get(<<"storagePathType">>, ConfigurationParams)
        )
    },

    %% Reconstruct credentials record from flat params
    BaseCredentials = #posix_helper_credentials{
        uid = binary_to_integer(maps:get(<<"uid">>, CredentialsParams))
    },
    Credentials = redact_confidential_credentials(
        helper_spec_utils:set_optional_record_fields_if_defined(BaseCredentials, CredentialsParams, [
            {<<"gid">>, #posix_helper_credentials.gid, fun binary_to_integer/1}
        ])
    ),

    #helper_spec_description{
        type = ?POSIX_HELPER_NAME,
        configuration = BaseConfiguration,
        credentials = Credentials
    }.


-spec is_posix_compatible() -> boolean().
is_posix_compatible() -> true.


-spec is_object_storage() -> boolean().
is_object_storage() -> false.


-spec is_rename_supported() -> boolean().
is_rename_supported() -> true.


-spec is_nfs4_acl_supported() -> boolean().
is_nfs4_acl_supported() -> true.


-spec is_oauth2_supported() -> boolean().
is_oauth2_supported() -> false.


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


-spec redact_confidential_credentials(#posix_helper_credentials{}) -> #posix_helper_credentials{}.
redact_confidential_credentials(Credentials) ->
    Credentials.


-spec redact_confidential_credentials_diff(#posix_helper_credentials_diff{}) -> #posix_helper_credentials_diff{}.
redact_confidential_credentials_diff(CredentialsDiff) ->
    CredentialsDiff.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_configuration(onedata_storage:create_spec()) -> helper_spec:configuration().
build_configuration(#storage_create_spec{
    configuration = #posix_helper_configuration{
        mount_point = MountPoint,
        storage_path_type = StoragePathType
    }
}) ->
    RequiredParams = #{
        <<"mountPoint">> => MountPoint,
        <<"storagePathType">> => helper_spec_utils:storage_path_type_to_binary(StoragePathType)
    },
    RequiredParams.


%% @private
-spec build_credentials(#posix_helper_credentials{}) -> helper_spec:credentials().
build_credentials(#posix_helper_credentials{
    uid = Uid,
    gid = Gid
}) ->
    BaseCredentialsParams = #{
        <<"uid">> => integer_to_binary(Uid)
    },
    helper_spec_utils:add_optional_entries_if_defined(BaseCredentialsParams, [
        {<<"gid">>, Gid, fun integer_to_binary/1}
    ]).
