%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module with default in-provider user mapping.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_provider).
-author("Michal Wrona").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([new_user_ctx/3, get_posix_user_ctx/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context based on given helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_user_ctx(HelperInit :: helpers:init(), SessionId :: session:id(),
    SpaceUUID :: file_meta:uuid()) -> helpers_user:ctx().
new_user_ctx(#helper_init{name = ?CEPH_HELPER_NAME}, SessionId, SpaceUUID) ->
    get_or_create_user(ceph_user, SessionId, SpaceUUID);
new_user_ctx(#helper_init{name = ?DIRECTIO_HELPER_NAME}, SessionId, SpaceUUID) ->
    get_or_create_user(posix_user, SessionId, SpaceUUID);
new_user_ctx(#helper_init{name = ?S3_HELPER_NAME}, SessionId, SpaceUUID) ->
    get_or_create_user(s3_user, SessionId, SpaceUUID);
new_user_ctx(#helper_init{name = ?SWIFT_HELPER_NAME}, SessionId, SpaceUUID) ->
    get_or_create_user(swift_user, SessionId, SpaceUUID).


%%--------------------------------------------------------------------
%% @doc
%% Returns posix user ctx for file attrs
%% @end
%%--------------------------------------------------------------------
-spec get_posix_user_ctx(StorageType :: helpers:name(),
    SessionIdOrIdentity :: session:id() | session:identity(),
    SpaceUUID :: file_meta:uuid()) -> #posix_user_ctx{}.
get_posix_user_ctx(_, SessionIdOrIdentity, SpaceUUID) ->
    get_or_create_user(posix_user, SessionIdOrIdentity, SpaceUUID).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns user ctx. If user ctx is missing is created.
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_user(UserModel :: helpers_user:model(),
    SessionIdOrIdentity :: session:id() | session:identity(),
    SpaceUUID :: file_meta:uuid()) -> UserCtx :: helpers_user:ctx().
get_or_create_user(posix_user, SessionIdOrIdentity, SpaceUUID) ->
    UserId = luma_utils:get_user_id(SessionIdOrIdentity),
    {ok, #document{value = #file_meta{name = SpaceName}}} =
        file_meta:get({uuid, SpaceUUID}),
    UID = luma_utils:gen_storage_uid(UserId),
    GID = luma_utils:gen_storage_gid(SpaceName, SpaceUUID),
    #posix_user_ctx{uid = UID, gid = GID};
get_or_create_user(swift_user, _SessionIdOrIdentity, SpaceUUID) ->
    StorageId = luma_utils:get_storage_id(SpaceUUID),
    Args = luma_utils:get_helper_args(StorageId),

    #swift_user_ctx{
        user_name = maps:get(<<"user_name">>, Args),
        password = maps:get(<<"password">>, Args)
    };
get_or_create_user(UserModel, SessionIdOrIdentity, SpaceUUID) ->
    UserId = luma_utils:get_user_id(SessionIdOrIdentity),
    StorageId = luma_utils:get_storage_id(SpaceUUID),

    case UserModel:get_ctx(UserId, StorageId) of
        undefined ->
            {ok, UserCtx} = create_user(UserModel, UserId, StorageId),
            UserModel:add(UserId, StorageId, UserCtx),
            UserCtx;
        UserCtx -> UserCtx
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates user.
%% @end
%%--------------------------------------------------------------------
-spec create_user(UserModel :: helpers_user:model(), UserId :: binary(),
    StorageId :: storage:id()) -> {ok, UserCtx :: helpers_user:ctx()}.
create_user(ceph_user, ?ROOT_USER_ID, StorageId) ->
    Args = luma_utils:get_helper_args(StorageId),
    {ok, #ceph_user_ctx{user_name = maps:get(<<"user_name">>, Args),
        user_key = maps:get(<<"user_key">>, Args)}};
create_user(ceph_user, UserId, StorageId) ->
    Args = luma_utils:get_helper_args(StorageId),

    {ok, {UserName, UserKey}} = luma_nif:create_ceph_user(UserId,
        maps:get(<<"mon_host">>, Args),
        maps:get(<<"cluster_name">>, Args, <<"Ceph">>),
        maps:get(<<"pool_name">>, Args),
        maps:get(<<"user_name">>, Args),
        maps:get(<<"user_key">>, Args)
    ),

    {ok, #ceph_user_ctx{user_name = UserName, user_key = UserKey}};
create_user(s3_user, ?ROOT_USER_ID, StorageId) ->
    Args = luma_utils:get_helper_args(StorageId),
    {ok, #s3_user_ctx{access_key = maps:get(<<"access_key">>, Args),
        secret_key = maps:get(<<"secret_key">>, Args)}};
create_user(s3_user, UserId, StorageId) ->
    Args = luma_utils:get_helper_args(StorageId),
    AdminAccessKey = maps:get(<<"access_key">>, Args),
    AdminSecretKey = maps:get(<<"secret_key">>, Args),
    BucketName = maps:get(<<"bucket_name">>, Args),
    IAMRequestScheme = maps:get(<<"iam_request_scheme">>, Args, <<"https">>),
    IAMHost = maps:get(<<"iam_host">>, Args, <<"iam.amazonaws.com">>),
    Region = maps:get(<<"region">>, Args, <<"us-east-1">>),

    ok = amazonaws_iam:create_user(AdminAccessKey, AdminSecretKey, IAMRequestScheme,
        IAMHost, Region, UserId),
    {ok, {AccessKey, SecretKey}} =
        amazonaws_iam:create_access_key(AdminAccessKey, AdminSecretKey,
            IAMRequestScheme, IAMHost, Region, UserId),
    ok = amazonaws_iam:allow_access_to_bucket(AdminAccessKey, AdminSecretKey,
        IAMRequestScheme, IAMHost, Region, UserId, BucketName),

    {ok, #s3_user_ctx{access_key = AccessKey, secret_key = SecretKey}}.