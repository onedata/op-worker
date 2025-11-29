%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of helper_config_behaviour for S3 storage.
%%% @end
%%%-------------------------------------------------------------------
-module(s3_helper_config).
-author("Bartosz Walkowicz").

-behaviour(helper_config_behaviour).

-include("modules/storage/helpers/helpers.hrl").
-include_lib("op_panel_contracts/include/storage/common.hrl").
-include_lib("op_panel_contracts/include/storage/s3.hrl").

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

-define(DEFAULT_S3_BLOCK_SIZE, 10485760).


%%%===================================================================
%%% helper_config_behaviour callbacks
%%%===================================================================


-spec build(onedata_storage:create_spec()) -> helper_config:t().
build(CreateReq = #storage_create_spec{type = ?S3_HELPER_NAME, credentials = Credentials}) ->
    #helper_config{
        name = ?S3_HELPER_NAME,
        args = build_args(CreateReq),
        admin_ctx = build_admin_ctx(Credentials)
    }.


-spec validate_user_ctx(helper_config:user_ctx()) -> ok | {error, Reason :: term()}.
validate_user_ctx(UserCtx) ->
    helper_config_utils:validate_user_ctx(UserCtx, [<<"accessKey">>, <<"secretKey">>]).


-spec build_args_diff(helper_config:t(), onedata_storage:update_spec()) -> helper_config:args().
build_args_diff(HelperConfig, UpdateSpec = #storage_update_spec{configuration = undefined}) ->
    build_args_diff(HelperConfig, UpdateSpec#storage_update_spec{
        configuration = #s3_configuration_diff{}
    });
build_args_diff(HelperConfig, #storage_update_spec{
    timeout = Timeout,
    configuration = #s3_configuration_diff{
        scheme = Scheme,
        hostname = Hostname,
        bucket_name = BucketName,
        signature_version = SignatureVersion,
        verify_server_certificate = VerifyServerCertificate,
        region = Region,
        maximum_canonical_object_size = MaxCanonicalObjectSize,
        file_mode = FileMode,
        dir_mode = DirMode
    }
}) ->
    helper_config_utils:build_args_diff_from_specs(HelperConfig#helper_config.args, [
        {<<"scheme">>, Scheme},
        {<<"hostname">>, Hostname},
        {<<"bucketName">>, BucketName},
        {<<"signatureVersion">>, SignatureVersion, fun integer_to_binary/1},
        {<<"verifyServerCertificate">>, VerifyServerCertificate, fun atom_to_binary/1},
        {<<"region">>, Region},
        {<<"maximumCanonicalObjectSize">>, MaxCanonicalObjectSize, fun integer_to_binary/1},
        {<<"fileMode">>, FileMode},
        {<<"dirMode">>, DirMode},
        {<<"timeout">>, Timeout, fun integer_to_binary/1}
    ]).


-spec build_admin_ctx_diff(helper_config:t(), onedata_storage:update_spec()) ->
    helper_config:user_ctx().
build_admin_ctx_diff(_HelperConfig, #storage_update_spec{credentials = undefined}) ->
    #{};
build_admin_ctx_diff(HelperConfig, #storage_update_spec{
    credentials = #s3_credentials_diff{
        access_key = AccessKey,
        secret_key = SecretKey
    }
}) ->
    helper_config_utils:build_args_diff_from_specs(HelperConfig#helper_config.admin_ctx, [
        {<<"accessKey">>, AccessKey},
        {<<"secretKey">>, SecretKey}
    ]).


-spec describe(helper_config:t()) -> helper_config:description().
describe(#helper_config{
    name = ?S3_HELPER_NAME,
    args = Args,
    admin_ctx = AdminCtx
}) ->
    %% Reconstruct configuration record from args map
    BaseConfiguration = #s3_configuration{
        scheme = maps:get(<<"scheme">>, Args),
        hostname = maps:get(<<"hostname">>, Args),
        bucket_name = maps:get(<<"bucketName">>, Args),
        storage_path_type = helper_config_utils:storage_path_type_from_binary(
            maps:get(<<"storagePathType">>, Args)
        )
    },
    Configuration = helper_config_utils:set_optional_record_fields_if_defined(BaseConfiguration, Args, [
        {<<"signatureVersion">>, #s3_configuration.signature_version, fun binary_to_integer/1},
        {<<"verifyServerCertificate">>, #s3_configuration.verify_server_certificate, fun utils:to_boolean/1},
        {<<"region">>, #s3_configuration.region},
        {<<"blockSize">>, #s3_configuration.block_size, fun binary_to_integer/1},
        {<<"maximumCanonicalObjectSize">>, #s3_configuration.maximum_canonical_object_size, fun binary_to_integer/1},
        {<<"fileMode">>, #s3_configuration.file_mode},
        {<<"dirMode">>, #s3_configuration.dir_mode}
    ]),

    %% Reconstruct credentials record from admin_ctx
    Credentials = redact_confidential_credentials(#s3_credentials{
        access_key = maps:get(<<"accessKey">>, AdminCtx),
        secret_key = maps:get(<<"secretKey">>, AdminCtx)
    }),

    #helper_config_description{
        type = ?S3_HELPER_NAME,
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
is_auto_import_supported(HelperConfig) ->
    helper_config_utils:is_canonical(HelperConfig) andalso block_size_equals_0(HelperConfig).


-spec is_file_registration_supported(#helper_config{}) -> boolean().
is_file_registration_supported(HelperConfig) ->
    helper_config_utils:is_canonical(HelperConfig) andalso block_size_equals_0(HelperConfig).


-spec is_getting_size_supported(#helper_config{}) -> boolean().
is_getting_size_supported(HelperConfig) ->
    block_size_equals_0(HelperConfig).


-spec get_block_size(#helper_config{}) -> non_neg_integer() | undefined.
get_block_size(#helper_config{args = Args}) ->
    case maps:get(<<"blockSize">>, Args, ?DEFAULT_S3_BLOCK_SIZE) of
        Bin when is_binary(Bin) -> binary_to_integer(Bin);
        Int when is_integer(Int) -> Int
    end.


-spec redact_confidential_credentials(#s3_credentials{}) -> #s3_credentials{}.
redact_confidential_credentials(Credentials) ->
    helper_config_utils:redact_record_fields_if_defined(Credentials, [#s3_credentials.secret_key]).


-spec redact_confidential_credentials_diff(#s3_credentials_diff{}) -> #s3_credentials_diff{}.
redact_confidential_credentials_diff(CredentialsDiff) ->
    helper_config_utils:redact_record_fields_if_defined(CredentialsDiff, [
        #s3_credentials_diff.secret_key
    ]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_args(onedata_storage:create_spec()) -> helper_config:args().
build_args(#storage_create_spec{
    timeout = Timeout,
    configuration = #s3_configuration{
        scheme = Scheme,
        hostname = Hostname,
        bucket_name = BucketName,
        signature_version = SignatureVersion,
        verify_server_certificate = VerifyServerCertificate,
        region = Region,
        block_size = BlockSize,
        maximum_canonical_object_size = MaxCanonicalObjectSize,
        file_mode = FileMode,
        dir_mode = DirMode,
        storage_path_type = StoragePathType
    }
}) ->
    RequiredArgs = #{
        <<"scheme">> => Scheme,
        <<"hostname">> => Hostname,
        <<"bucketName">> => BucketName,
        <<"storagePathType">> => helper_config_utils:storage_path_type_to_binary(StoragePathType)
    },
    helper_config_utils:add_optional_args_if_defined(RequiredArgs, [
        {<<"signatureVersion">>, SignatureVersion, fun integer_to_binary/1},
        {<<"verifyServerCertificate">>, VerifyServerCertificate, fun atom_to_binary/1},
        {<<"region">>, Region},
        {<<"blockSize">>, BlockSize, fun integer_to_binary/1},
        {<<"maximumCanonicalObjectSize">>, MaxCanonicalObjectSize, fun integer_to_binary/1},
        {<<"fileMode">>, FileMode},
        {<<"dirMode">>, DirMode},
        {<<"timeout">>, Timeout, fun integer_to_binary/1}
    ]).


%% @private
-spec build_admin_ctx(#s3_credentials{}) -> helper_config:user_ctx().
build_admin_ctx(#s3_credentials{
    access_key = AccessKey,
    secret_key = SecretKey
}) ->
    #{
        <<"accessKey">> => AccessKey,
        <<"secretKey">> => SecretKey
    }.


%% @private
-spec block_size_equals_0(helper_config:t()) -> boolean().
block_size_equals_0(HelperConfig) ->
    get_block_size(HelperConfig) =:= 0.
