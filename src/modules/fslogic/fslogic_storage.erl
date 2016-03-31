%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module for storage management.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_storage).
-author("Rafal Slota").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/helpers.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([select_helper/1, select_storage/1, new_storage/2, new_helper_init/2]).
-export([new_user_ctx/3, new_posix_user_ctx/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context based on given helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_user_ctx(helpers:init(), session:id() | #identity{}, SpaceUUID :: file_meta:uuid()) ->
    helpers:user_ctx().
new_user_ctx(HelperInit, SessionId, SpaceUUID) when is_binary(SessionId) ->
    {ok, #document{value = #session{identity = Identity}}} = session:get(SessionId),
    new_user_ctx(HelperInit, Identity, SpaceUUID);
new_user_ctx(#helper_init{name = ?CEPH_HELPER_NAME}, Identity, SpaceUUID) ->
    new_ceph_user_ctx(Identity, SpaceUUID);
new_user_ctx(#helper_init{name = ?DIRECTIO_HELPER_NAME}, Identity, SpaceUUID) ->
    new_posix_user_ctx(Identity, SpaceUUID);
new_user_ctx(#helper_init{name = ?S3_HELPER_NAME}, Identity, SpaceUUID) ->
    new_s3_user_ctx(Identity, SpaceUUID).


%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context for Ceph storage helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_ceph_user_ctx(Identity :: #identity{}, SpaceUUID :: file_meta:uuid()) ->
    helpers:user_ctx().
new_ceph_user_ctx(#identity{user_id = UserId}, SpaceUUID) ->
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
-spec new_posix_user_ctx(Identity :: #identity{}, SpaceUUID :: file_meta:uuid()) ->
    helpers:user_ctx().
new_posix_user_ctx(#identity{user_id = UserId}, SpaceUUID) ->
    {ok, #document{value = #space_info{name = SpaceName}}} = space_info:get(SpaceUUID),
    FinalGID =
        case helpers_nif:groupname_to_gid(SpaceName) of
            {ok, GID} ->
                GID;
            {error, _} ->
                fslogic_utils:gen_storage_uid(SpaceUUID)
        end,

    FinalUID = fslogic_utils:gen_storage_uid(UserId),
    #posix_user_ctx{uid = FinalUID, gid = FinalGID}.


%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context for Amazon S3 storage helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_s3_user_ctx(Identity :: #identity{}, SpaceUUID :: file_meta:uuid()) ->
    helpers:user_ctx().
new_s3_user_ctx(#identity{user_id = UserId}, SpaceUUID) ->
    {ok, #document{value = #s3_user{credentials = CredentialsMap}}} = s3_user:get(UserId),
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),
    {ok, #document{value = #space_storage{storage_ids = [StorageId | _]}}} = space_storage:get(SpaceId),
    {ok, Credentials} = maps:find(StorageId, CredentialsMap),
    #s3_user_ctx{
        access_key = s3_user:access_key(Credentials),
        secret_key = s3_user:secret_key(Credentials)
    }.


%%--------------------------------------------------------------------
%% @doc
%% Returns any available storage for given fslogic ctx.
%% @end
%%--------------------------------------------------------------------
-spec select_helper(datastore:document() | #storage{}) -> {ok, #helper_init{}} | {error, Reason :: term()}.
select_helper(#document{value = Storage}) ->
    select_helper(Storage);
select_helper(#storage{helpers = []} = Storage) ->
    {error, {no_helper_available, Storage}};
select_helper(#storage{helpers = [Helper | _]}) ->
    {ok, Helper}.


%%--------------------------------------------------------------------
%% @doc
%% Returns any available storage for given fslogic ctx.
%% @end
%%--------------------------------------------------------------------
-spec select_storage(SpaceId :: binary()) ->
    {ok, datastore:document()} | {error, Reason :: term()}.
select_storage(SpaceId) ->
    case space_storage:get(SpaceId) of
        {ok, #document{value = #space_storage{storage_ids = [StorageId | _]}}} ->
            case storage:get(StorageId) of
                {ok, #document{} = Storage} -> {ok, Storage};
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates new helper_init structure.
%% @end
%%--------------------------------------------------------------------
-spec new_helper_init(HelperName :: helpers:name(), HelperArgs :: helpers:args()) -> #helper_init{}.
new_helper_init(HelperName, HelperArgs) ->
    #helper_init{name = HelperName, args = HelperArgs}.


%%--------------------------------------------------------------------
%% @doc
%% Creates new storage structure.
%% @end
%%--------------------------------------------------------------------
-spec new_storage(Name :: storage:name(), [#helper_init{}]) -> #storage{}.
new_storage(Name, Helpers) ->
    #storage{name = Name, helpers = Helpers}.


%%%===================================================================
%%% Internal functions
%%%===================================================================