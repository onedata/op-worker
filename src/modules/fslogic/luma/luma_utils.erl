%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Util functions for luma.
%%% @end
%%%--------------------------------------------------------------------
-module(luma_utils).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").

%% API
-export([get_user_id/1, get_s3_user/2, get_ceph_user/2, get_posix_user/2,
    gen_storage_uid/1, gen_storage_gid/2, get_storage_id/1, get_storage_type/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Gets S3 credentials from datastore.
%% @end
%%--------------------------------------------------------------------
-spec get_s3_user(UserId :: binary(), StorageId :: storage:id()) ->
    {ok, s3_user:credentials()} | undefined.
get_s3_user(UserId, StorageId) ->
    case s3_user:get(UserId) of
        {ok, #document{value = #s3_user{credentials = CredentialsMap}}} ->
            case maps:find(StorageId, CredentialsMap) of
                {ok, Credentials} ->
                    {ok, Credentials};
                _ ->
                    undefined
            end;
        _ ->
            undefined
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets Ceph credentials from datastore.
%% @end
%%--------------------------------------------------------------------
-spec get_ceph_user(UserId :: binary(), StorageId :: storage:id()) ->
    {ok, ceph_user:credentials()} | undefined.
get_ceph_user(UserId, StorageId) ->
    case ceph_user:get(UserId) of
        {ok, #document{value = #ceph_user{credentials = CredentialsMap}}} ->
            case maps:find(StorageId, CredentialsMap) of
                {ok, Credentials} ->
                    {ok, Credentials};
                _ ->
                    undefined
            end;
        _ ->
            undefined
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets POSIX credentials from datastore.
%% @end
%%--------------------------------------------------------------------
-spec get_posix_user(UserId :: binary(), StorageId :: storage:id()) ->
    {ok, posix_user:credentials()} | undefined.
get_posix_user(UserId, StorageId) ->
    case posix_user:get(UserId) of
        {ok, #document{value = #posix_user{credentials = CredentialsMap}}} ->
            case maps:find(StorageId, CredentialsMap) of
                {ok, Credentials} ->
                    {ok, Credentials};
                _ ->
                    undefined
            end;
        _ ->
            undefined
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get user_id from identity or session_id
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(session:id() | session:identity()) -> onedata_user:id().
get_user_id(#identity{user_id = UserId}) ->
    UserId;
get_user_id(SessionId) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} =
        session:get(SessionId),
    UserId.

%%--------------------------------------------------------------------
%% @doc Generates storage UID/GID based arbitrary binary (e.g. user's global id, space id, etc)
%% @end
%%--------------------------------------------------------------------
-spec gen_storage_uid(ID :: binary()) -> non_neg_integer().
gen_storage_uid(?ROOT_USER_ID) ->
    0;
gen_storage_uid(ID) ->
    <<UID0:16/big-unsigned-integer-unit:8>> = crypto:hash(md5, ID),
    {ok, LowestUID} = application:get_env(?APP_NAME,
        lowest_generated_storage_uid),
    {ok, HighestUID} = application:get_env(?APP_NAME,
        highest_generated_storage_uid),
    LowestUID + UID0 rem HighestUID.

%%--------------------------------------------------------------------
%% @doc Generates storage GID based on SpaceName or SpaceUUID
%% @end
%%--------------------------------------------------------------------
-spec gen_storage_gid(SpaceName :: file_meta:name(),
    SpaceUUID :: file_meta:uuid()) -> non_neg_integer().
gen_storage_gid(SpaceName, SpaceUUID) ->
    case helpers_nif:groupname_to_gid(SpaceName) of
        {ok, GID} ->
            GID;
        {error, _} ->
            luma_utils:gen_storage_uid(SpaceUUID)
    end.

%%--------------------------------------------------------------------
%% @doc Returns StorageType for given StorageId
%% @end
%%--------------------------------------------------------------------
-spec get_storage_type(storage:id()) -> helpers:name().
get_storage_type(StorageId) ->
    {ok, Doc} = storage:get(StorageId),
    {ok, #helper_init{name = StorageType}} = fslogic_storage:select_helper(Doc),
    StorageType.

%%--------------------------------------------------------------------
%% @doc Returns StorageId for given SpaceUUID
%% @end
%%--------------------------------------------------------------------
-spec get_storage_id(SpaceUUID :: file_meta:uuid()) -> storage:id().
get_storage_id(SpaceUUID) ->
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),
    {ok, #document{value = #space_storage{storage_ids = [StorageId | _]}}} =
        space_storage:get(SpaceId),
    StorageId.

%%%===================================================================
%%% Internal functions
%%%===================================================================