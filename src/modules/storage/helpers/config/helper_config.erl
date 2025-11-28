%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Central dispatcher module for storage helper configurations.
%%% Routes calls to storage-specific implementation modules and provides
%%% a unified API for creating, updating, and querying helper configurations.
%%% @end
%%%-------------------------------------------------------------------
-module(helper_config).
-author("Bartosz Walkowicz").

-include("modules/storage/helpers/helpers.hrl").

-include("global_definitions.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("hackney/include/hackney_lib.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/storage/common.hrl").

%% API
-export([
    build/1,
    build_helper_nif_args/2,
    validate_user_ctx/2,
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
    get_args/1,
    get_admin_ctx/1,
    get_timeout/1,
    get_block_size/1,
    get_storage_path_type/1,
    get_params/2,
    get_proxy_params/2
]).

-type t() :: #helper_config{}.

-type name() :: onedata_storage:type().
-type args() :: #{binary() => binary()}.
-type user_ctx() :: #{binary() => binary()}.
-type nif_args() :: #{binary() => binary()}.
-type params() :: #helper_params{}.
-type access_type() :: ?READONLY | ?READWRITE.

-type description() :: #helper_config_description{}.

-export_type([t/0, name/0, args/0, user_ctx/0, params/0, access_type/0, description/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(onedata_storage:create_spec()) -> t().
build(CreateReq = #storage_create_spec{type = Type}) ->
    Module = get_module(Type),
    Module:build(CreateReq).


-spec build_helper_nif_args(t(), user_ctx()) ->
    {ok, nif_args()} | {error, Reason :: term()}.
build_helper_nif_args(HelperConfig, UserCtx) ->
    case validate_user_ctx(HelperConfig, UserCtx) of
        ok -> {ok, maps:merge(HelperConfig#helper_config.args, UserCtx)};
        Error -> Error
    end.


-spec validate_user_ctx(t() | name(), user_ctx()) -> ok | {error, Reason :: term()}.
validate_user_ctx(HelperConfigOrName, UserCtx) ->
    Module = get_module(HelperConfigOrName),
    Module:validate_user_ctx(UserCtx).


-spec update(t(), onedata_storage:update_spec()) ->
    {ok, t()} | {error, no_change}.
update(
    HelperConfig = #helper_config{name = StorageType},
    UpdateSpec = #storage_update_spec{type = StorageType}
) ->
    Module = get_module(HelperConfig),

    ArgsDiff = Module:build_args_diff(HelperConfig, UpdateSpec),
    AdminCtxDiff = Module:build_admin_ctx_diff(HelperConfig, UpdateSpec),

    case {maps_utils:is_empty(ArgsDiff), maps_utils:is_empty(AdminCtxDiff)} of
        {true, true} ->
            {error, no_change};
        {IsEmptyArgsDiff, IsEmptyAdminCtxDiff} ->
            CurrentArgs = HelperConfig#helper_config.args,
            CurrentAdminCtx = HelperConfig#helper_config.admin_ctx,

            {ok, HelperConfig#helper_config{
                args = case IsEmptyArgsDiff of
                    true -> CurrentArgs;
                    false -> maps:merge(CurrentArgs, ArgsDiff)
                end,
                admin_ctx = case IsEmptyAdminCtxDiff of
                    true -> CurrentAdminCtx;
                    false -> maps:merge(CurrentAdminCtx, AdminCtxDiff)
                end
            }}
    end.


-spec describe(t()) -> description().
describe(HelperConfig) ->
    Module = get_module(HelperConfig),
    Module:describe(HelperConfig).


-spec is_posix_compatible(t() | helper_config:name()) -> boolean().
is_posix_compatible(HelperConfigOrName) ->
    Module = get_module(HelperConfigOrName),
    Module:is_posix_compatible().


-spec is_object_storage(t() | helper_config:name()) -> boolean().
is_object_storage(HelperConfigOrName) ->
    Module = get_module(HelperConfigOrName),
    Module:is_object_storage().


-spec is_rename_supported(t() | helper_config:name()) -> boolean().
is_rename_supported(HelperConfigOrName) ->
    Module = get_module(HelperConfigOrName),
    Module:is_rename_supported().


-spec is_nfs4_acl_supported(t() | helper_config:name()) -> boolean().
is_nfs4_acl_supported(HelperConfigOrName) ->
    Module = get_module(HelperConfigOrName),
    Module:is_nfs4_acl_supported().


-spec is_oauth2_supported(t() | helper_config:name()) -> boolean().
is_oauth2_supported(HelperConfigOrName) ->
    Module = get_module(HelperConfigOrName),
    Module:is_oauth2_supported().


-spec is_storage_access_type_supported(t() | helper_config:name(), helper_config:access_type()) -> boolean().
is_storage_access_type_supported(HelperConfigName, AccessType) ->
    Module = get_module(HelperConfigName),
    Module:is_storage_access_type_supported(AccessType).


-spec is_auto_import_supported(t()) -> boolean().
is_auto_import_supported(HelperConfig) ->
    Module = get_module(HelperConfig),
    Module:is_auto_import_supported(HelperConfig).


-spec is_file_registration_supported(t()) -> boolean().
is_file_registration_supported(HelperConfig) ->
    Module = get_module(HelperConfig),
    Module:is_file_registration_supported(HelperConfig).


-spec is_import_supported(t()) -> boolean().
is_import_supported(HelperConfig) ->
    is_auto_import_supported(HelperConfig) orelse
        is_file_registration_supported(HelperConfig).


-spec is_getting_size_supported(t()) -> boolean().
is_getting_size_supported(HelperConfig) ->
    Module = get_module(HelperConfig),
    Module:is_getting_size_supported(HelperConfig).


-spec get_name(t()) -> name().
get_name(#helper_config{name = Name}) -> Name.


-spec get_args(t()) -> args().
get_args(#helper_config{args = Args}) -> Args.


-spec get_admin_ctx(t()) -> user_ctx().
get_admin_ctx(#helper_config{admin_ctx = Ctx}) -> Ctx.


-spec get_timeout(t() | undefined) -> integer().
get_timeout(undefined) ->
    {ok, Value} = application:get_env(?APP_NAME, helpers_async_operation_timeout_milliseconds),
    Value;
get_timeout(#helper_config{args = Args}) ->
    case maps:find(<<"timeout">>, Args) of
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
get_block_size(HelperConfig) ->
    Module = get_module(HelperConfig),
    Module:get_block_size(HelperConfig).


-spec get_storage_path_type(t()) -> binary().
get_storage_path_type(HelperConfig) ->
    helper_config_utils:get_storage_path_type(HelperConfig).


-spec get_params(t(), user_ctx()) -> params().
get_params(HelperConfig, UserCtx) ->
    {ok, Args} = build_helper_nif_args(HelperConfig, UserCtx),
    #helper_params{
        helper_name = HelperConfig#helper_config.name,
        helper_args = [#helper_arg{key = Key, value = Value} || {Key, Value} <- maps:to_list(Args)]
    }.


-spec get_proxy_params(timeout(), storage:id()) -> params().
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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_module(t() | name()) -> module().
get_module(#helper_config{name = HelperName}) ->
    get_module(HelperName);
get_module(?CEPH_HELPER_NAME) -> ceph_helper_config:module_info(module);
get_module(?CEPHRADOS_HELPER_NAME) -> cephrados_helper_config:module_info(module);
get_module(?GLUSTERFS_HELPER_NAME) -> glusterfs_helper_config:module_info(module);
get_module(?HTTP_HELPER_NAME) -> http_helper_config:module_info(module);
get_module(?NFS_HELPER_NAME) -> nfs_helper_config:module_info(module);
get_module(?NULL_DEVICE_HELPER_NAME) -> nulldevice_helper_config:module_info(module);
get_module(?POSIX_HELPER_NAME) -> posix_helper_config:module_info(module);
get_module(?S3_HELPER_NAME) -> s3_helper_config:module_info(module);
get_module(?SWIFT_HELPER_NAME) -> swift_helper_config:module_info(module);
get_module(?WEBDAV_HELPER_NAME) -> webdav_helper_config:module_info(module);
get_module(?XROOTD_HELPER_NAME) -> xrootd_helper_config:module_info(module).
