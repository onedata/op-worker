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
-export([new_user_ctx/3, new_posix_user_ctx/3]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context based on given helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_user_ctx(helpers:init(), SessionId :: session:id(), SpaceUUID :: file_meta:uuid()) ->
    helpers:user_ctx().
new_user_ctx(#helper_init{name = StorageType = ?CEPH_HELPER_NAME}, SessionId, SpaceUUID) ->
    new_ceph_user_ctx(StorageType, SessionId, SpaceUUID);
new_user_ctx(#helper_init{name = StorageType = ?DIRECTIO_HELPER_NAME}, SessionId, SpaceUUID) ->
    new_posix_user_ctx(StorageType, SessionId, SpaceUUID);
new_user_ctx(#helper_init{name = StorageType = ?S3_HELPER_NAME}, SessionId, SpaceUUID) ->
    new_s3_user_ctx(StorageType, SessionId, SpaceUUID).


%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context for Ceph storage helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_ceph_user_ctx(StorageType :: helpers:name(), SessionId :: session:id(), SpaceUUID :: file_meta:uuid()) ->
    helpers:user_ctx().
new_ceph_user_ctx(StorageType, SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    StorageId = fslogic_utils:get_storage_id(SpaceUUID),
    {ok, Response} = fslogic_utils:get_credentials_from_luma(<<"global_id=",UserId/binary,
        "&storage_id=",StorageId/binary,"&storage_type=",StorageType/binary>>),
    #ceph_user_ctx{
        user_name = proplists:get_value(<<"user_name">>, Response),
        user_key = proplists:get_value(<<"user_key">>, Response)
    }.


%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context for all posix-compilant helpers.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_posix_user_ctx(StorageType :: helpers:name(), SessionId :: session:id(), SpaceUUID :: file_meta:uuid()) ->
    helpers:user_ctx().
new_posix_user_ctx(StorageType, SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    StorageId = fslogic_utils:get_storage_id(SpaceUUID),
    case UserId of
        ?ROOT_USER_ID ->
            ?ROOT_POSIX_CTX;
        _ ->
            {ok, Response} = fslogic_utils:get_credentials_from_luma(<<"global_id=",UserId/binary,
                "&storage_id=",StorageId/binary,"&storage_type=",StorageType/binary>>),
            #posix_user_ctx{uid = proplists:get_value(<<"uid">>, Response),
                gid = proplists:get_value(<<"gid">>, Response)}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context for Amazon S3 storage helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_s3_user_ctx(StorageType :: helpers:name(), SessionId :: session:id(), SpaceUUID :: file_meta:uuid()) ->
    helpers:user_ctx().
new_s3_user_ctx(StorageType, SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    StorageId = fslogic_utils:get_storage_id(SpaceUUID),
    {ok, Response} = fslogic_utils:get_credentials_from_luma(<<"global_id=",UserId/binary,
        "&storage_id=",StorageId/binary,"&storage_type=",StorageType/binary>>),
    #s3_user_ctx{
        access_key = proplists:get_value(<<"access_key">>, Response),
        secret_key = proplists:get_value(<<"secret_key">>, Response)
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
-spec select_storage(fslogic_worker:ctx()) -> {ok, datastore:document()} | {error, Reason :: term()}.
select_storage(#fslogic_ctx{space_id = SpaceId}) ->
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