%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of helper_config_behaviour for NFS storage.
%%% @end
%%%-------------------------------------------------------------------
-module(nfs_helper_config).
-author("Bartosz Walkowicz").

-behaviour(helper_config_behaviour).

-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/storage/common.hrl").
-include_lib("ctool/include/storage/nfs.hrl").

%% helper_config_behaviour callbacks
-export([
    build/1,
    validate_user_ctx/1,
    build_args_diff/2,
    build_admin_ctx_diff/2,
    describe/1,

    is_posix_compatible/0,
    is_object/0,
    is_rename_supported/0,
    is_nfs4_acl_supported/0,
    supports_storage_access_type/1,
    is_auto_import_supported/1,
    is_file_registration_supported/1,
    is_getting_size_supported/1,
    get_block_size/1
]).


%%%===================================================================
%%% helper_config_behaviour callbacks
%%%===================================================================


-spec build(onedata_storage:create_spec()) -> helper_config:t().
build(CreateReq = #storage_create_spec{type = ?NFS_HELPER_NAME, credentials = Credentials}) ->
    #helper_config{
        name = ?NFS_HELPER_NAME,
        args = build_args(CreateReq),
        admin_ctx = build_admin_ctx(Credentials)
    }.


-spec validate_user_ctx(helper_config:user_ctx()) -> ok | {error, Reason :: term()}.
validate_user_ctx(UserCtx) ->
    helper_config_utils:validate_user_ctx(UserCtx, [<<"uid">>], [<<"gid">>]).


-spec build_args_diff(helper_config:t(), onedata_storage:update_spec()) -> helper_config:args().
build_args_diff(HelperConfig, UpdateSpec = #storage_update_spec{configuration = undefined}) ->
    build_args_diff(HelperConfig, UpdateSpec#storage_update_spec{
        configuration = #nfs_configuration_diff{}
    });
build_args_diff(HelperConfig, #storage_update_spec{
    timeout = Timeout,
    archive = Archive,
    configuration = #nfs_configuration_diff{
        version = Version,
        host = Host,
        volume = Volume,
        read_ahead = ReadAhead,
        dir_cache = DirCache,
        auto_reconnect = AutoReconnect,
        connection_pool_size = ConnectionPoolSize
    }
}) ->
    helper_config_utils:build_args_diff_from_specs(HelperConfig#helper_config.args, [
        {<<"version">>, Version, fun integer_to_binary/1},
        {<<"host">>, Host},
        {<<"volume">>, Volume},
        {<<"readAhead">>, ReadAhead, fun integer_to_binary/1},
        {<<"dirCache">>, DirCache, fun atom_to_binary/1},
        {<<"autoReconnect">>, AutoReconnect, fun integer_to_binary/1},
        {<<"connectionPoolSize">>, ConnectionPoolSize, fun integer_to_binary/1},
        {<<"timeout">>, Timeout, fun integer_to_binary/1},
        {<<"archiveStorage">>, Archive, fun atom_to_binary/1}
    ]).


-spec build_admin_ctx_diff(helper_config:t(), onedata_storage:update_spec()) ->
    helper_config:user_ctx().
build_admin_ctx_diff(_HelperConfig, #storage_update_spec{credentials = undefined}) ->
    #{};
build_admin_ctx_diff(HelperConfig, #storage_update_spec{
    credentials = #nfs_credentials_diff{
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
    name = ?NFS_HELPER_NAME,
    args = Args,
    admin_ctx = AdminCtx
}) ->
    %% Reconstruct configuration record from args map
    BaseConfiguration = #nfs_configuration{
        version = binary_to_integer(maps:get(<<"version">>, Args)),
        host = maps:get(<<"host">>, Args),
        volume = maps:get(<<"volume">>, Args),
        storage_path_type = helper_config_utils:storage_path_type_from_binary(
            maps:get(<<"storagePathType">>, Args)
        )
    },
    Configuration = helper_config_utils:set_optional_record_fields_if_defined(BaseConfiguration, Args, [
        {<<"readAhead">>, #nfs_configuration.read_ahead, fun binary_to_integer/1},
        {<<"dirCache">>, #nfs_configuration.dir_cache, fun utils:to_boolean/1},
        {<<"autoReconnect">>, #nfs_configuration.auto_reconnect, fun binary_to_integer/1},
        {<<"connectionPoolSize">>, #nfs_configuration.connection_pool_size, fun binary_to_integer/1}
    ]),

    %% Reconstruct credentials record from admin_ctx
    BaseCredentials = #nfs_credentials{
        uid = binary_to_integer(maps:get(<<"uid">>, AdminCtx))
    },
    Credentials = helper_config_utils:set_optional_record_fields_if_defined(BaseCredentials, AdminCtx, [
        {<<"gid">>, #nfs_credentials.gid, fun binary_to_integer/1}
    ]),

    #helper_config_description{
        type = ?NFS_HELPER_NAME,
        credentials = Credentials,
        configuration = Configuration,
        timeout = utils:convert_defined(
            maps:get(<<"timeout">>, Args, undefined),
            fun binary_to_integer/1
        ),
        archive = utils:to_boolean(maps:get(<<"archiveStorage">>, Args, false))
    }.


-spec is_posix_compatible() -> boolean().
is_posix_compatible() -> true.


-spec is_object() -> boolean().
is_object() -> false.


-spec is_rename_supported() -> boolean().
is_rename_supported() -> true.


-spec is_nfs4_acl_supported() -> boolean().
is_nfs4_acl_supported() -> false.  % TODO is that true ?


-spec supports_storage_access_type(helper_config:access_type()) -> boolean().
supports_storage_access_type(_) -> true.


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
    archive = Archive,
    configuration = #nfs_configuration{
        version = Version,
        host = Host,
        volume = Volume,
        read_ahead = ReadAhead,
        dir_cache = DirCache,
        auto_reconnect = AutoReconnect,
        connection_pool_size = ConnectionPoolSize,
        storage_path_type = StoragePathType
    }
}) ->
    RequiredArgs = #{
        <<"version">> => Version,
        <<"host">> => Host,
        <<"volume">> => Volume,
        <<"storagePathType">> => helper_config_utils:storage_path_type_to_binary(StoragePathType)
    },
    helper_config_utils:add_optional_args_if_defined(RequiredArgs, [
        {<<"readAhead">>, ReadAhead, fun integer_to_binary/1},
        {<<"dirCache">>, DirCache, fun atom_to_binary/1},
        {<<"autoReconnect">>, AutoReconnect, fun integer_to_binary/1},
        {<<"connectionPoolSize">>, ConnectionPoolSize, fun integer_to_binary/1},
        {<<"timeout">>, Timeout, fun integer_to_binary/1},
        {<<"archiveStorage">>, Archive, fun atom_to_binary/1}
    ]).


%% @private
-spec build_admin_ctx(#nfs_credentials{}) -> helper_config:user_ctx().
build_admin_ctx(#nfs_credentials{
    uid = Uid,
    gid = Gid
}) ->
    BaseCtx = #{
        <<"uid">> => integer_to_binary(Uid)
    },
    helper_config_utils:add_optional_args_if_defined(BaseCtx, [
        {<<"gid">>, Gid, fun integer_to_binary/1}
    ]).
