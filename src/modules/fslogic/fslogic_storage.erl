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

-include("global_definitions.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/helpers.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([select_helper/1, select_storage/1, new_storage/2, new_helper_init/2]).
-export([new_user_ctx/3, get_posix_user_ctx/4]).

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
new_user_ctx(#helper_init{name = ?CEPH_HELPER_NAME}, SessionId, SpaceUUID) ->
    {ok, Enable_LUMA} = application:get_env(?APP_NAME, enable_luma),
    new_ceph_user_ctx(SessionId, SpaceUUID, Enable_LUMA);
new_user_ctx(#helper_init{name = ?DIRECTIO_HELPER_NAME}, SessionId, SpaceUUID) ->
    {ok, Enable_LUMA} = application:get_env(?APP_NAME, enable_luma),
    new_posix_user_ctx(SessionId, SpaceUUID, Enable_LUMA);
new_user_ctx(#helper_init{name = ?S3_HELPER_NAME}, SessionId, SpaceUUID) ->
    {ok, Enable_LUMA} = application:get_env(?APP_NAME, enable_luma),
    new_s3_user_ctx(SessionId, SpaceUUID, Enable_LUMA).


%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context for Ceph storage helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_ceph_user_ctx(SessionId :: session:id(), SpaceUUID :: file_meta:uuid(), Enable_LUMA :: boolean())
        -> helpers:user_ctx().
new_ceph_user_ctx(SessionId, SpaceUUID, true) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    StorageId = fslogic_utils:get_storage_id(SpaceUUID),
    case luma_response:get(UserId, StorageId) of
        {ok, Credentials} ->
            Credentials;
        _ ->
            StorageType = fslogic_utils:get_storage_type(StorageId),
            {ok, Response} = fslogic_utils:get_credentials_from_luma(UserId, StorageType, StorageId),
            User_ctx = #ceph_user_ctx{
                user_name = proplists:get_value(<<"user_name">>, Response),
                user_key = proplists:get_value(<<"user_key">>, Response)
            },
            luma_response:save(UserId, StorageId, User_ctx),
            User_ctx
    end;
new_ceph_user_ctx(SessionId, SpaceUUID, false) ->
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
-spec new_posix_user_ctx(SessionId :: session:id(), SpaceUUID :: file_meta:uuid(), Enable_LUMA :: boolean())
        -> helpers:user_ctx().
new_posix_user_ctx(SessionId, SpaceUUID, true) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    StorageId = fslogic_utils:get_storage_id(SpaceUUID),
    case luma_response:get(UserId, StorageId) of
        {ok, Credentials} ->
            Credentials;
        _ ->
            StorageType = fslogic_utils:get_storage_type(StorageId),
            {ok, Response} = fslogic_utils:get_credentials_from_luma(UserId, StorageType, StorageId),
            User_ctx = fslogic_utils:parse_posix_ctx_from_luma(Response, SpaceUUID),
            luma_response:save(UserId, StorageId, User_ctx),
            User_ctx
    end;
new_posix_user_ctx(SessionId, SpaceUUID, false) ->
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
-spec new_s3_user_ctx(SessionId :: session:id(), SpaceUUID :: file_meta:uuid(), Enable_LUMA :: boolean())
        -> helpers:user_ctx().
new_s3_user_ctx(SessionId, SpaceUUID, true) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    StorageId = fslogic_utils:get_storage_id(SpaceUUID),
    case luma_response:get(UserId, StorageId) of
        {ok, Credentials} ->
            Credentials;
        _ ->
            StorageType = fslogic_utils:get_storage_type(StorageId),
            {ok, Response} = fslogic_utils:get_credentials_from_luma(UserId, StorageType, StorageId),
            User_ctx = #s3_user_ctx{
                access_key = proplists:get_value(<<"access_key">>, Response),
                secret_key = proplists:get_value(<<"secret_key">>, Response)
            },
            luma_response:save(UserId, StorageId, User_ctx),
            User_ctx
    end;
new_s3_user_ctx(SessionId, SpaceUUID, false) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    {ok, #document{value = #s3_user{credentials = CredentialsMap}}} = s3_user:get(UserId),
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),
    {ok, #document{value = #space_storage{storage_ids = [StorageId | _]}}} = space_storage:get(SpaceId),
    {ok, Credentials} = maps:find(StorageId, CredentialsMap),
    #s3_user_ctx{
        access_key = s3_user:access_key(Credentials),
        secret_key = s3_user:secret_key(Credentials)
    }.


%%--------------------------------------------------------------------
%% @doc Retrieves posix user ctx for file attrs
%% @end
%%--------------------------------------------------------------------
-spec get_posix_user_ctx(Enable_LUMA :: boolean(), StorageType :: helpers:name(), SessionId :: session:id(),
    SpaceUUID :: file_meta:uuid()) -> #posix_user_ctx{}.
get_posix_user_ctx(false, _, SessionId, SpaceUUID) ->
    new_posix_user_ctx(SessionId, SpaceUUID, false);
get_posix_user_ctx(true, ?DIRECTIO_HELPER_NAME, SessionId, SpaceUUID) ->
    new_posix_user_ctx(SessionId, SpaceUUID, true);
get_posix_user_ctx(true, _, SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    case luma_response:get(UserId, ?DIRECTIO_HELPER_NAME) of
        {ok, Credentials} ->
            Credentials;
        _ ->
            {ok, Response} = fslogic_utils:get_credentials_from_luma(UserId, ?DIRECTIO_HELPER_NAME, ?DIRECTIO_HELPER_NAME),
            User_ctx = fslogic_utils:parse_posix_ctx_from_luma(Response, SpaceUUID),
            luma_response:save(UserId, ?DIRECTIO_HELPER_NAME, User_ctx),
            User_ctx
    end.


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