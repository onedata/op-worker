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
-spec new_user_ctx(StorageType :: helpers:init(), SessionId :: session:id(), SpaceUUID :: file_meta:uuid()) ->
  helpers:user_ctx().
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


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context for Ceph storage helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_ceph_user_ctx(SessionId :: session:id(), SpaceUUID :: file_meta:uuid()) -> helpers:user_ctx().
new_ceph_user_ctx(SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    {ok, #document{value = #ceph_user{credentials = CredentialsMap}}} = ceph_user:get(UserId),
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),
    {ok, #document{value = #space_storage{storage_ids = [StorageId | _]}}} = space_storage:get(SpaceId),
    {ok, Credentials} = maps:find(StorageId, CredentialsMap),
    #ceph_user_ctx{
      user_name = ceph_user:name(Credentials),
      user_key = ceph_user:key(Credentials)
    }.


%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context for all posix-compilant helpers.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_posix_user_ctx(SessionId :: session:id(), SpaceUUID :: file_meta:uuid()) -> helpers:user_ctx().
new_posix_user_ctx(SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    {ok, #document{value = #file_meta{name = SpaceName}}} = file_meta:get({uuid, SpaceUUID}),
    FinalGID = fslogic_utils:gen_storage_gid(SpaceName, SpaceUUID),
    FinalUID = fslogic_utils:gen_storage_uid(UserId),
    #posix_user_ctx{uid = FinalUID, gid = FinalGID}.


%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context for Amazon S3 storage helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_s3_user_ctx(SessionId :: session:id(), SpaceUUID :: file_meta:uuid()) -> helpers:user_ctx().
new_s3_user_ctx(SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    {ok, #document{value = #s3_user{credentials = CredentialsMap}}} = s3_user:get(UserId),
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),
    {ok, #document{value = #space_storage{storage_ids = [StorageId | _]}}} = space_storage:get(SpaceId),
    {ok, Credentials} = maps:find(StorageId, CredentialsMap),
    #s3_user_ctx{
      access_key = s3_user:access_key(Credentials),
      secret_key = s3_user:secret_key(Credentials)
    }.
