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
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("hackney/include/hackney_lib.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([new_helper/5]).
-export([update_args/2, update_admin_ctx/2, update_insecure/2]).
-export([get_name/1, get_args/1, get_admin_ctx/1, is_insecure/1, get_params/2,
    get_proxy_params/2, get_timeout/1, get_storage_path_type/1]).
-export([insert_user_ctx/2]).
-export([translate_name/1, translate_arg_name/1]).

%% Test utils
-export([new_ceph_user_ctx/2, new_cephrados_user_ctx/2, new_posix_user_ctx/2,
    new_s3_user_ctx/2, new_swift_user_ctx/2, new_glusterfs_user_ctx/2,
    new_webdav_user_ctx/2, new_webdav_user_ctx/3, new_nulldevice_user_ctx/2]).

-type name() :: binary().
-type field() :: binary().

-type args() :: #{field() => binary()}.
-type params() :: #helper_params{}.

-type user_ctx() :: #{field() => binary()}.
-type group_ctx() :: #{field() => binary()}.

-export_type([name/0, args/0, params/0, user_ctx/0, group_ctx/0]).


% @fixme support creating webdav ctx with adminId

% @fixme move ctx managemetn to other module

%%%===================================================================
%%% API
%%%===================================================================

-spec new_helper(name(), args(), user_ctx(), Insecure :: boolean(),
    storage_path_type()) -> {ok, helpers:helper()}.
new_helper(HelperName, Args, AdminCtx, Insecure, StoragePathType) ->
    BaseAdminCtx = helper_params:default_admin_ctx(HelperName),
    FullAdminCtx = maps:merge(BaseAdminCtx, AdminCtx),
    ok = helper_params:validate_args(HelperName, Args),
    ok = helper_params:validate_user_ctx(HelperName, FullAdminCtx),
    {ok, #helper{
        name = HelperName,
        args = Args,
        admin_ctx = FullAdminCtx,
        insecure = Insecure andalso allow_insecure(HelperName),
        extended_direct_io = extended_direct_io(HelperName),
        storage_path_type = StoragePathType
    }}.


%%--------------------------------------------------------------------
%% @doc
%% Injects user context into helper parameters.
%% @end
%%--------------------------------------------------------------------
-spec insert_user_ctx(helpers:helper(), user_ctx()) ->
    {ok, helpers:helper()} | {error, Reason :: term()}.
insert_user_ctx(#helper{args = Args} = Helper, UserCtx) ->
    case helper_params:validate_user_ctx(Helper, UserCtx) of
        ok -> {ok, Helper#helper{args = maps:merge(Args, UserCtx)}};
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


-spec update_insecure(helpers:helper(), NewInsecure :: boolean()) ->
    {ok, helpers:helper()} | {error, Reason :: term()}.
update_insecure(#helper{name = Name} = Helper, NewInsecure) ->
    case {allow_insecure(Name), NewInsecure} of
        {false, true} ->
            {error, {invalid_field_value, <<"insecure">>, NewInsecure}};
        {_, _} ->
            {ok, Helper#helper{insecure = NewInsecure}}
    end.


%%%===================================================================
%%% Validators
%%%===================================================================

%% @private
-spec extended_direct_io(name()) -> boolean().
extended_direct_io(?POSIX_HELPER_NAME) -> false;
extended_direct_io(_) -> true.


%% @private
-spec allow_insecure(name()) -> boolean().
allow_insecure(?POSIX_HELPER_NAME) -> false;
allow_insecure(_) -> true.


%%%===================================================================
%%% Getters
%%% Functions for extracting info from helper records
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns helper name.
%% @end
%%--------------------------------------------------------------------
-spec get_name(helpers:helper() | params()) -> name().
get_name(#helper{name = Name}) ->
    Name;
get_name(#helper_params{helper_name = Name}) ->
    Name.

%%--------------------------------------------------------------------
%% @doc
%% Returns helper arguments.
%% @end
%%--------------------------------------------------------------------
-spec get_args(helpers:helper() | params()) -> args().
get_args(#helper{args = Args}) ->
    Args;
get_args(#helper_params{helper_args = Args}) ->
    maps:from_list([{K, V} || #helper_arg{key = K, value = V} <- Args]).

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
%% Returns helper insecure status.
%% @end
%%--------------------------------------------------------------------
-spec is_insecure(helpers:helper()) -> boolean().
is_insecure(#helper{insecure = Insecure}) ->
    Insecure.

%%--------------------------------------------------------------------
%% @doc
%% Returns helper storage path type.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_path_type(helpers:helper()) -> helper:storage_path_type().
get_storage_path_type(#helper{storage_path_type = StoragePathType}) ->
    StoragePathType.

%%--------------------------------------------------------------------
%% @doc
%% Returns helper parameters.
%% @end
%%--------------------------------------------------------------------
-spec get_params(helpers:helper(), user_ctx()) -> params().
get_params(Helper, UserCtx) ->
    {ok, #helper{name = Name, args = Args, extended_direct_io = DS}} =
        insert_user_ctx(Helper, UserCtx),
    #helper_params{
        helper_name = Name,
        helper_args = maps:fold(fun(Key, Value, Acc) ->
            [#helper_arg{key = Key, value = Value} | Acc]
        end, [], Args),
        extended_direct_io = DS
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
        ],
        extended_direct_io = false
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


%%%===================================================================
%%% Test utils
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Constructs Ceph storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_ceph_user_ctx(binary(), binary()) -> {ok, user_ctx()}.
new_ceph_user_ctx(Username, Key) ->
    {ok, #{
        <<"username">> => Username,
        <<"key">> => Key
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs CephRados storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_cephrados_user_ctx(binary(), binary()) -> {ok, user_ctx()}.
new_cephrados_user_ctx(Username, Key) ->
    {ok, #{
        <<"username">> => Username,
        <<"key">> => Key
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs POSIX storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_posix_user_ctx(integer(), integer()) -> {ok, user_ctx()}.
new_posix_user_ctx(Uid, Gid) ->
    {ok, #{
        <<"uid">> => integer_to_binary(Uid),
        <<"gid">> => integer_to_binary(Gid)
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs S3 storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_s3_user_ctx(binary(), binary()) -> {ok, user_ctx()}.
new_s3_user_ctx(AccessKey, SecretKey) ->
    {ok, #{
        <<"accessKey">> => AccessKey,
        <<"secretKey">> => SecretKey
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs Swift storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_swift_user_ctx(binary(), binary()) -> {ok, user_ctx()}.
new_swift_user_ctx(Username, Password) ->
    {ok, #{
        <<"username">> => Username,
        <<"password">> => Password
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs GlusterFS storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_glusterfs_user_ctx(integer(), integer()) -> {ok, user_ctx()}.
new_glusterfs_user_ctx(Uid, Gid) ->
    {ok, #{
        <<"uid">> => integer_to_binary(Uid),
        <<"gid">> => integer_to_binary(Gid)
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs WebDAV storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_webdav_user_ctx(binary(), binary()) -> {ok, user_ctx()}.
new_webdav_user_ctx(CredentialsType = <<"none">>, _Credentials) ->
    {ok, #{
        <<"credentialsType">> => CredentialsType
    }};
new_webdav_user_ctx(CredentialsType, Credentials) ->
    {ok, #{
        <<"credentialsType">> => CredentialsType,
        <<"credentials">> => Credentials
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs WebDAV storage helper user context record.
%% This function is used by Onepanel to construct admin user_ctx.
%% @end
%%--------------------------------------------------------------------
-spec new_webdav_user_ctx(binary(), binary(), binary()) -> {ok, user_ctx()}.
new_webdav_user_ctx(CredentialsType, Credentials, <<>>) ->
    % todo VFS-5304 verify parameters
    % todo i. e. when type == oauth2 and insecure == true token cannot be empty
    new_webdav_user_ctx(CredentialsType, Credentials);
new_webdav_user_ctx(CredentialsType, Credentials, OnedataAccessToken) ->
    {ok, AdminId} = user_identity:get_or_fetch_user_id(OnedataAccessToken),
    {ok, UserCtx} = new_webdav_user_ctx(CredentialsType, Credentials),
    {ok, UserCtx#{
        <<"onedataAccessToken">> => OnedataAccessToken,
        <<"adminId">> => AdminId
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs Null Device storage helper user context record.
%% @end
%%--------------------------------------------------------------------
-spec new_nulldevice_user_ctx(integer(), integer()) -> {ok, user_ctx()}.
new_nulldevice_user_ctx(Uid, Gid) ->
    {ok, #{
        <<"uid">> => integer_to_binary(Uid),
        <<"gid">> => integer_to_binary(Gid)
    }}.
