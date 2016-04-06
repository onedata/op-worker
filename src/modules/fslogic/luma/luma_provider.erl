%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module with default in-provider user mapping.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_provider).
-author("Michal Wrona").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([new_user_ctx/3, get_posix_user_ctx/3, new_posix_user_ctx/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context based on given helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_user_ctx(StorageType :: helpers:init(), SessionId :: session:id(),
    SpaceUUID :: file_meta:uuid()) -> helpers:user_ctx().
new_user_ctx(#helper_init{name = ?CEPH_HELPER_NAME}, SessionId, SpaceUUID) ->
    new_ceph_user_ctx(SessionId, SpaceUUID);
new_user_ctx(#helper_init{name = ?DIRECTIO_HELPER_NAME}, SessionId, SpaceUUID) ->
    new_posix_user_ctx(SessionId, SpaceUUID);
new_user_ctx(#helper_init{name = ?S3_HELPER_NAME}, SessionId, SpaceUUID) ->
    new_s3_user_ctx(SessionId, SpaceUUID).


%%--------------------------------------------------------------------
%% @doc Returns posix user ctx for file attrs
%% @end
%%--------------------------------------------------------------------
-spec get_posix_user_ctx(StorageType :: helpers:name(), SessionId :: session:id(),
    SpaceUUID :: file_meta:uuid()) -> #posix_user_ctx{}.
get_posix_user_ctx(_, SessionId, SpaceUUID) ->
    new_posix_user_ctx(SessionId, SpaceUUID).


%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context for all posix-compilant helpers.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_posix_user_ctx(SessionIdOrIdentity :: session:id() | session:identity(),
    SpaceUUID :: file_meta:uuid()) -> helpers:user_ctx().
new_posix_user_ctx(SessionIdOrIdentity, SpaceUUID) ->
    UserId = luma_utils:get_user_id(SessionIdOrIdentity),
    {ok, #document{value = #file_meta{name = SpaceName}}} =
        file_meta:get({uuid, SpaceUUID}),

    UID = luma_utils:gen_storage_uid(UserId),
    GID = luma_utils:gen_storage_gid(SpaceName, SpaceUUID),

    #posix_user_ctx{uid = UID, gid = GID}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context for Ceph storage helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_ceph_user_ctx(SessionId :: session:id(),
    SpaceUUID :: file_meta:uuid()) -> helpers:user_ctx().
new_ceph_user_ctx(SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} =
        session:get(SessionId),
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),
    {ok, #document{value = #space_storage{storage_ids = [StorageId | _]}}} =
        space_storage:get(SpaceId),

    case luma_utils:get_ceph_user(UserId, StorageId) of
        {ok, Credentials} ->
            #ceph_user_ctx{
                user_name = ceph_user:name(Credentials),
                user_key = ceph_user:key(Credentials)
            };
        undefined ->
            {ok, #ceph_user_ctx{user_name = UserName, user_key = UserKey} =
                Credentials} = create_ceph_user(UserId, StorageId),
            ceph_user:add(UserId, StorageId, UserName, UserKey),
            Credentials
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context for Amazon S3 storage helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_s3_user_ctx(SessionId :: session:id(),
    SpaceUUID :: file_meta:uuid()) -> helpers:user_ctx().
new_s3_user_ctx(SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} =
        session:get(SessionId),
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),
    {ok, #document{value = #space_storage{storage_ids = [StorageId | _]}}} =
        space_storage:get(SpaceId),

    case luma_utils:get_s3_user(UserId, StorageId) of
        {ok, Credentials} ->
            #s3_user_ctx{
                access_key = s3_user:access_key(Credentials),
                secret_key = s3_user:secret_key(Credentials)
            };
        undefined ->
            {ok, #s3_user_ctx{access_key = AccessKey, secret_key = SecretKey} =
                Credentials} = create_s3_user(UserId, StorageId),
            s3_user:add(UserId, StorageId, AccessKey, SecretKey),
            Credentials
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates Ceph user credentials.
%% @end
%%--------------------------------------------------------------------
-spec create_ceph_user(UserId :: binary(), StorageId :: storage:id()) ->
    {ok, #ceph_user_ctx{}}.
create_ceph_user(?ROOT_USER_ID, StorageId) ->
    {ok, #document{value = #storage{helpers = [#helper_init{args = Args} | _]}}} =
        storage:get(StorageId),
    {ok, #ceph_user_ctx{user_name = maps:get(<<"user_name">>, Args),
        user_key = maps:get(<<"user_key">>, Args)}};
create_ceph_user(UserId, StorageId) ->
    {ok, #document{value = #storage{helpers = [#helper_init{args = Args} | _]}}} =
        storage:get(StorageId),

    {ok, {UserName, UserKey}} = luma_nif:create_ceph_user(UserId,
        maps:get(<<"mon_host">>, Args),
        maps:get(<<"cluster_name">>, Args, <<"Ceph">>),
        maps:get(<<"pool_name">>, Args),
        maps:get(<<"user_name">>, Args),
        maps:get(<<"user_key">>, Args)
    ),

    {ok, #ceph_user_ctx{user_name = UserName, user_key = UserKey}}.


%%--------------------------------------------------------------------
%% @doc
%% Creates S3 user credentials.
%% @end
%%--------------------------------------------------------------------
-spec create_s3_user(UserId :: binary(), StorageId :: storage:id()) ->
    {ok, #s3_user_ctx{}}.
create_s3_user(?ROOT_USER_ID, StorageId) ->
    {ok, #document{value = #storage{helpers = [#helper_init{args = Args} | _]}}} =
        storage:get(StorageId),
    {ok, #s3_user_ctx{access_key = maps:get(<<"access_key">>, Args),
        secret_key = maps:get(<<"secret_key">>, Args)}};
create_s3_user(UserId, StorageId) ->
    {ok, #document{value = #storage{helpers = [#helper_init{args = Args} | _]}}} =
        storage:get(StorageId),

    AdminAccessKey = maps:get(<<"access_key">>, Args),
    AdminSecretKey = maps:get(<<"secret_key">>, Args),
    BucketName = maps:get(<<"bucket_name">>, Args),
    IAMHost = maps:get(<<"iam_host">>, Args, <<"iam.amazonaws.com">>),
    Region = maps:get(<<"region">>, Args, <<"us-east-1">>),

    ok = amazonaws_iam:create_user(AdminAccessKey, AdminSecretKey, IAMHost,
        Region, UserId),
    {ok, {AccessKey, SecretKey}} =
        amazonaws_iam:create_access_key(AdminAccessKey, AdminSecretKey,
            IAMHost, Region, UserId),
    ok = amazonaws_iam:allow_access_to_bucket(AdminAccessKey, AdminSecretKey,
        IAMHost, Region, UserId, BucketName),

    {ok, #s3_user_ctx{access_key = AccessKey, secret_key = SecretKey}}.