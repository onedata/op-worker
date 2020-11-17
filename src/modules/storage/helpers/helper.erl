%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides a synchronous interface to the helpers NIF library.
%%% It wraps {@link helpers_nif} module by calling its functions and awaiting
%%% results.
%%% @end
%%%-------------------------------------------------------------------
-module(helper).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("hackney/include/hackney_lib.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([new_helper/3]).
-export([update_args/2, update_admin_ctx/2]).
-export([
    get_name/1, get_args/1, get_admin_ctx/1, get_redacted_admin_ctx/1,
    get_params/2, get_proxy_params/2, get_timeout/1,
    get_storage_path_type/1, get_block_size/1, get_mount_point/1
]).
-export([
    is_posix_compatible/1, is_getting_size_supported/1, is_rename_supported/1,
    is_import_supported/1, is_auto_import_supported/1, is_file_registration_supported/1,
    is_nfs4_acl_supported/1, should_skip_storage_detection/1, supports_storage_access_type/2
]).
-export([get_args_with_user_ctx/2]).
-export([translate_name/1, translate_arg_name/1]).

-type name() :: binary().

-type args() :: #{binary() => binary()}.
-type user_ctx() :: #{binary() => binary()}.
-type params() :: #helper_params{}.
-type type() :: object_storage | block_storage.
-type access_type() :: ?READONLY | ?READWRITE.

-export_type([name/0, args/0, params/0, user_ctx/0, type/0, access_type/0]).

-define(DEFAULT_CEPHRADOS_BLOCK_SIZE, 4194304).
-define(DEFAULT_SWIFT_BLOCK_SIZE, 10485760).
-define(DEFAULT_S3_BLOCK_SIZE, 10485760).

%%%===================================================================
%%% API
%%%===================================================================

-spec new_helper(name(), args(), user_ctx()) -> {ok, helpers:helper()}.
new_helper(HelperName, Args, AdminCtx) ->
    BaseAdminCtx = helper_params:default_admin_ctx(HelperName),
    FullAdminCtx = maps:merge(BaseAdminCtx, AdminCtx),
    ok = helper_params:validate_args(HelperName, Args),
    ok = helper_params:validate_user_ctx(HelperName, FullAdminCtx),
    {ok, #helper{
        name = HelperName,
        args = Args,
        admin_ctx = FullAdminCtx
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Returns helper arguments with user ctx added.
%% @end
%%--------------------------------------------------------------------
-spec get_args_with_user_ctx(helpers:helper(), user_ctx()) ->
    {ok, args()} | {error, Reason :: term()}.
get_args_with_user_ctx(#helper{args = Args} = Helper, UserCtx) ->
    case helper_params:validate_user_ctx(Helper#helper.name, UserCtx) of
        ok -> {ok, maps:merge(Args, UserCtx)};
        Error -> Error
    end.


-spec update_args(helpers:helper(), args()) ->
    {ok, helpers:helper()} | {error, Reason :: term()}.
update_args(#helper{args = Args} = Helper, Changes) ->
    NewArgs = maps:merge(Args, Changes),
    case helper_params:validate_args(Helper#helper.name, NewArgs) of
        ok -> {ok, Helper#helper{args = NewArgs}};
        Error -> Error
    end.


-spec update_admin_ctx(helpers:helper(), user_ctx()) ->
    {ok, helpers:helper()} | {error, Reason :: term()}.
update_admin_ctx(#helper{admin_ctx = OldCtx} = Helper, Changes) ->
    NewCtx = maps:merge(OldCtx, Changes),
    case helper_params:validate_user_ctx(Helper#helper.name, NewCtx) of
        ok -> {ok, Helper#helper{admin_ctx = NewCtx}};
        Error -> Error
    end.


%%%===================================================================
%%% Getters
%%% Functions for extracting info from helper records
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns helper name.
%% @end
%%--------------------------------------------------------------------
-spec get_name(helpers:helper()) -> name().
get_name(#helper{name = Name}) ->
    Name.

%%--------------------------------------------------------------------
%% @doc
%% Returns helper arguments.
%% @end
%%--------------------------------------------------------------------
-spec get_args(helpers:helper()) -> args().
get_args(#helper{args = Args}) ->
    Args.

%%--------------------------------------------------------------------
%% @doc
%% Returns helper admin context.
%% @end
%%--------------------------------------------------------------------
-spec get_admin_ctx(helpers:helper()) -> user_ctx().
get_admin_ctx(#helper{admin_ctx = Ctx}) ->
    Ctx.


%%--------------------------------------------------------------------
%% @doc
%% Returns part of admin context without sensitive information
%% (passwords, access codes).
%% @end
%%--------------------------------------------------------------------
-spec get_redacted_admin_ctx(helpers:helper()) -> user_ctx().
get_redacted_admin_ctx(Helper) ->
    maps:with([<<"username">>, <<"accessKey">>, <<"credentialsType">>],
        get_admin_ctx(Helper)).


%%--------------------------------------------------------------------
%% @doc
%% Returns helper storage path type.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_path_type(helpers:helper()) -> helper:storage_path_type().
get_storage_path_type(#helper{args = Args}) ->
    maps:get(<<"storagePathType">>, Args).


%%--------------------------------------------------------------------
%% @doc
%% Returns size of block used by underlying object storage. For posix-compatible
%% ones 'undefined' is returned.
%% @end
%%--------------------------------------------------------------------
-spec get_block_size(helpers:helper()) -> non_neg_integer() | undefined.
get_block_size(#helper{name = ?CEPHRADOS_HELPER_NAME, args = Args}) ->
    sanitize_int(maps:get(<<"blockSize">>, Args, ?DEFAULT_CEPHRADOS_BLOCK_SIZE));
get_block_size(#helper{name = ?SWIFT_HELPER_NAME, args = Args}) ->
    sanitize_int(maps:get(<<"blockSize">>, Args, ?DEFAULT_SWIFT_BLOCK_SIZE));
get_block_size(#helper{name = ?S3_HELPER_NAME, args = Args}) ->
    sanitize_int(maps:get(<<"blockSize">>, Args, ?DEFAULT_S3_BLOCK_SIZE));
get_block_size(_) ->
    undefined.


-spec get_mount_point(helpers:helper()) -> undefined | binary().
get_mount_point(#helper{args = #{<<"mountPoint">> := MountPoint}}) ->
    MountPoint;
get_mount_point(_) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% Returns helper parameters.
%% @end
%%--------------------------------------------------------------------
-spec get_params(helpers:helper(), user_ctx()) -> params().
get_params(#helper{name = Name} = Helper, UserCtx) ->
    {ok, Args} = get_args_with_user_ctx(Helper, UserCtx),
    #helper_params{
        helper_name = Name,
        helper_args = [#helper_arg{key = Key, value = Value} || {Key, Value} <- maps:to_list(Args)]
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns proxy helper parameters.
%% @end
%%--------------------------------------------------------------------
-spec get_proxy_params(helpers:helper() | undefined, storage:id()) -> params().
get_proxy_params(Helper, StorageId) ->
    Timeout = get_timeout(Helper),
    {ok, Latency} = application:get_env(?APP_NAME, proxy_helper_latency_milliseconds),
    TimeoutValue = integer_to_binary(Timeout + Latency),
    #helper_params{
        helper_name = ?PROXY_HELPER_NAME,
        helper_args = [
            #helper_arg{key = <<"storageId">>, value = StorageId},
            #helper_arg{key = <<"timeout">>, value = TimeoutValue}
        ]
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns timeout for storage helper async operations.
%% @end
%%--------------------------------------------------------------------
-spec get_timeout(helpers:helper() | undefined) -> Timeout :: timeout().
get_timeout(undefined) ->
    {ok, Value} = application:get_env(?APP_NAME,
        helpers_async_operation_timeout_milliseconds),
    Value;
get_timeout(#helper{args = Args}) ->
    case maps:find(<<"timeout">>, Args) of
        {ok, Value} ->
            erlang:binary_to_integer(Value);
        error ->
            get_timeout(undefined)
    end.

-spec should_skip_storage_detection(helpers:helper()) -> boolean().
should_skip_storage_detection(#helper{args = Args}) ->
    case maps:get(<<"skipStorageDetection">>, Args, false) of
        false -> false;
        <<"false">> -> false;
        <<"true">> -> true
    end.

-spec is_posix_compatible(helpers:helper() | name()) -> boolean().
is_posix_compatible(?POSIX_HELPER_NAME) -> true;
is_posix_compatible(?GLUSTERFS_HELPER_NAME) -> true;
is_posix_compatible(?NULL_DEVICE_HELPER_NAME) -> true;
is_posix_compatible(#helper{name = HelperName}) -> is_posix_compatible(HelperName);
is_posix_compatible(_) -> false.


-spec is_import_supported(helpers:helper()) -> boolean().
is_import_supported(Helper) ->
    is_auto_import_supported(Helper) orelse is_file_registration_supported(Helper).


-spec is_auto_import_supported(helpers:helper()) -> boolean().
is_auto_import_supported(Helper = #helper{name = HelperName})
    when HelperName =:= ?POSIX_HELPER_NAME
    orelse HelperName =:= ?GLUSTERFS_HELPER_NAME
    orelse HelperName =:= ?NULL_DEVICE_HELPER_NAME
    orelse HelperName =:= ?WEBDAV_HELPER_NAME
    orelse HelperName =:= ?XROOTD_HELPER_NAME
->
    is_canonical_helper(Helper);
is_auto_import_supported(Helper = #helper{name = ?S3_HELPER_NAME}) ->
    is_canonical_helper(Helper) andalso block_size_equals_0(Helper);
is_auto_import_supported(_) -> false.


-spec is_file_registration_supported(helpers:helper()) -> boolean().
is_file_registration_supported(Helper = #helper{name = HelperName})
    when HelperName =:= ?POSIX_HELPER_NAME
    orelse HelperName =:= ?GLUSTERFS_HELPER_NAME
    orelse HelperName =:= ?NULL_DEVICE_HELPER_NAME
    orelse HelperName =:= ?WEBDAV_HELPER_NAME
    orelse HelperName =:= ?XROOTD_HELPER_NAME
    orelse HelperName =:= ?HTTP_HELPER_NAME
->
    is_canonical_helper(Helper);
is_file_registration_supported(Helper = #helper{name = HelperName})
    when HelperName =:= ?S3_HELPER_NAME
    orelse HelperName =:= ?SWIFT_HELPER_NAME
    orelse HelperName =:= ?CEPH_HELPER_NAME
    orelse HelperName =:= ?CEPHRADOS_HELPER_NAME
->
    is_canonical_helper(Helper) andalso block_size_equals_0(Helper);
is_file_registration_supported(_) -> false.


-spec is_nfs4_acl_supported(helpers:helper() | name()) -> boolean().
is_nfs4_acl_supported(?POSIX_HELPER_NAME) -> true;
is_nfs4_acl_supported(?GLUSTERFS_HELPER_NAME) -> true;
is_nfs4_acl_supported(#helper{name = HelperName}) -> is_nfs4_acl_supported(HelperName);
is_nfs4_acl_supported(_) -> false.


-spec is_rename_supported(helpers:helper() | name()) -> boolean().
is_rename_supported(?POSIX_HELPER_NAME) -> true;
is_rename_supported(?GLUSTERFS_HELPER_NAME) -> true;
is_rename_supported(?NULL_DEVICE_HELPER_NAME) -> true;
is_rename_supported(?WEBDAV_HELPER_NAME) -> true;
is_rename_supported(?XROOTD_HELPER_NAME) -> true;
is_rename_supported(#helper{name = HelperName}) -> is_rename_supported(HelperName);
is_rename_supported(_) -> false.

%% @private
-spec is_object(helpers:helper() | name()) -> boolean().
is_object(#helper{name = Name}) ->
    is_object(Name);
is_object(HelperName) ->
    lists:member(HelperName, ?OBJECT_HELPERS).

-spec is_getting_size_supported(helpers:helper()) -> boolean().
is_getting_size_supported(Helper) ->
    not is_object(Helper) orelse block_size_equals_0(Helper).

-spec supports_storage_access_type(helpers:helper() | name(), access_type()) -> boolean().
supports_storage_access_type(#helper{name = HelperName}, AccessType) ->
    supports_storage_access_type(HelperName, AccessType);
supports_storage_access_type(?HTTP_HELPER_NAME, ?READWRITE) ->
    false;
supports_storage_access_type(_HelperName, _StorageAccessType) ->
    true.


-spec is_canonical_helper(helpers:helper()) -> boolean().
is_canonical_helper(Helper) ->
    get_storage_path_type(Helper) =:= ?CANONICAL_STORAGE_PATH.

-spec block_size_equals_0(helpers:helper()) -> boolean().
block_size_equals_0(#helper{args = Args}) ->
    (<<"0">> =:= maps:get(<<"blockSize">>, Args, undefined)).

%%%===================================================================
%%% Upgrade functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Translates storage helper name from legacy format.
%% @end
%%--------------------------------------------------------------------
-spec translate_name(OldName :: binary()) -> NewName :: binary().
translate_name(<<"Ceph">>) -> ?CEPH_HELPER_NAME;
translate_name(<<"CephRados">>) -> ?CEPHRADOS_HELPER_NAME;
translate_name(<<"DirectIO">>) -> ?POSIX_HELPER_NAME;
translate_name(<<"ProxyIO">>) -> ?PROXY_HELPER_NAME;
translate_name(<<"AmazonS3">>) -> ?S3_HELPER_NAME;
translate_name(<<"Swift">>) -> ?SWIFT_HELPER_NAME;
translate_name(<<"GlusterFS">>) -> ?GLUSTERFS_HELPER_NAME;
translate_name(<<"WebDAV">>) -> ?WEBDAV_HELPER_NAME;
translate_name(<<"XRootD">>) -> ?XROOTD_HELPER_NAME;
translate_name(<<"HTTP">>) -> ?HTTP_HELPER_NAME;
translate_name(<<"NullDevice">>) -> ?NULL_DEVICE_HELPER_NAME;
translate_name(Name) -> Name.

%%--------------------------------------------------------------------
%% @doc
%% Translates storage helper argument name from legacy format.
%% @end
%%--------------------------------------------------------------------
-spec translate_arg_name(OldName :: binary()) -> NewName :: binary().
translate_arg_name(<<"access_key">>) -> <<"accessKey">>;
translate_arg_name(<<"auth_url">>) -> <<"authUrl">>;
translate_arg_name(<<"block_size">>) -> <<"blockSize">>;
translate_arg_name(<<"bucket_name">>) -> <<"bucketName">>;
translate_arg_name(<<"cluster_name">>) -> <<"clusterName">>;
translate_arg_name(<<"container_name">>) -> <<"containerName">>;
translate_arg_name(<<"host_name">>) -> <<"hostname">>;
translate_arg_name(<<"mon_host">>) -> <<"monitorHostname">>;
translate_arg_name(<<"pool_name">>) -> <<"poolName">>;
translate_arg_name(<<"root_path">>) -> <<"mountPoint">>;
translate_arg_name(<<"secret_key">>) -> <<"secretKey">>;
translate_arg_name(<<"tenant_name">>) -> <<"tenantName">>;
translate_arg_name(<<"user_name">>) -> <<"username">>;
translate_arg_name(<<"xlator_options">>) -> <<"xlatorOptions">>;
translate_arg_name(<<"verify_server_certificate">>) -> <<"verifyServerCertificate">>;
translate_arg_name(<<"credentials_type">>) -> <<"credentialsType">>;
translate_arg_name(<<"authorization_header">>) -> <<"authorizationHeader">>;
translate_arg_name(<<"range_write_support">>) -> <<"rangeWriteSupport">>;
translate_arg_name(<<"connection_pool_size">>) -> <<"connectionPoolSize">>;
translate_arg_name(<<"maximum_upload_size">>) -> <<"maximumUploadSize">>;
translate_arg_name(<<"latency_min">>) -> <<"latencyMin">>;
translate_arg_name(<<"latency_max">>) -> <<"latencyMax">>;
translate_arg_name(<<"timeout_probability">>) -> <<"timeoutProbability">>;
translate_arg_name(<<"simulated_filesystem_parameters">>) ->
    <<"simulatedFilesystemParameters">>;
translate_arg_name(<<"simulated_filesystem_grow_speed">>) ->
    <<"simulatedFilesystemGrowSpeed">>;
translate_arg_name(<<"storage_path_type">>) -> <<"storagePathType">>;
translate_arg_name(Name) -> Name.


%% @private
-spec sanitize_int(integer() | binary()) -> integer().
sanitize_int(Int) when is_integer(Int) ->
    Int;
sanitize_int(Bin) when is_binary(Bin) ->
    binary_to_integer(Bin).
