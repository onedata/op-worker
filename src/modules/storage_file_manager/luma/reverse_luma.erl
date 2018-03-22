%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for mapping onedata users to storage users.
%%% @end
%%%-------------------------------------------------------------------
-module(reverse_luma).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([
    get_user_id/2, get_user_id/3,
    get_user_id_by_name/2, get_user_id_by_name/3,
    get_group_id/3, get_group_id/4,
    get_group_id_by_name/3, get_group_id_by_name/4
]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns od_user:id() for storage user associated with given Uid, Gid.
%% which is appropriate for the local server operations.
%% Ids are cached for timeout defined in #luma_config{} record.
%% If reverse LUMA is disabled, function returns ?ROOT USER_ID.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(integer(), storage:id(), storage:model()) ->
    {ok, od_user:id()} | {error, Reason :: term()}.
get_user_id(Uid, StorageId, Storage = #storage{}) ->
    case storage:is_luma_enabled(Storage) of
        false ->
            {ok, ?ROOT_USER_ID};
        true ->
            case is_storage_supported(Storage) of
                false ->
                    {error, not_supported_storage_type};
                true ->
                    get_user_id_from_supported_storage_credentials(Uid,
                        StorageId, Storage)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_user_id(Uid, Gid, StorageId, Storage = #storage{}).
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(integer(), storage:id() | storage:doc()) ->
    {ok, od_user:id()} | {error, Reason :: term()}.
get_user_id(Uid,  #document{key = StorageId, value = Storage = #storage{}}) ->
    get_user_id(Uid, StorageId, Storage);
get_user_id(Uid, StorageId) ->
    {ok, StorageDoc} = storage:get(StorageId),
    get_user_id(Uid, StorageDoc).

%%--------------------------------------------------------------------
%% @doc
%% Returns od_user:id() for storage user associated with given
%% NFSv4 ACL name which is appropriate for the local server operations.
%% Ids are cached for timeout defined in #luma_config{} record.
%% If reverse LUMA is disabled, function returns ?ROOT USER_ID.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id_by_name(binary(), storage:id(), storage:model()) ->
    {ok, od_user:id()} | {error, Reason :: term()}.
get_user_id_by_name(Name, StorageId, Storage = #storage{}) ->
    case storage:is_luma_enabled(Storage) of
        false ->
            {error, luma_disabled};
        true ->
            case is_storage_supported(Storage) of
                false ->
                    {error, not_supported_storage_type};
                true ->
                    get_user_id_from_supported_storage_acl_name(Name,
                        StorageId, Storage)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_user_id_by_name(Name, StorageId, Storage = #storage{}).
%% @end
%%--------------------------------------------------------------------
-spec get_user_id_by_name(binary(), storage:id() | storage:doc()) ->
    {ok, od_user:id()} | {error, Reason :: term()}.
get_user_id_by_name(Name, #document{key = StorageId, value = Storage = #storage{}}) ->
    get_user_id_by_name(Name, StorageId, Storage);
get_user_id_by_name(Name, StorageId) ->
    {ok, StorageDoc} = storage:get(StorageId),
    get_user_id_by_name(Name, StorageDoc).

%%--------------------------------------------------------------------
%% @doc
%% Returns od_group:id() for storage group associated with given Gid.
%% which is appropriate for the local server operations.
%% Ids are cached for timeout defined in #luma_config{} record.
%% If reverse LUMA is disabled, function returns error.
%% @end
%%--------------------------------------------------------------------
-spec get_group_id(integer(), od_space:id(), storage:id(), storage:model()) ->
    {ok, od_group:id() | undefined} | {error, Reason :: term()}.
get_group_id(Gid, SpaceId, StorageId, Storage = #storage{}) ->
    case storage:is_luma_enabled(Storage) of
        false ->
            {ok, undefined};
        true ->
            case is_storage_supported(Storage) of
                false ->
                    {error, not_supported_storage_type};
                true ->
                    get_group_id_from_supported_storage_credentials(Gid, SpaceId, StorageId, Storage)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_group_id(Gid, StorageId, Storage = #storage{}).
%% @end
%%--------------------------------------------------------------------
-spec get_group_id(integer(), od_space:id(), storage:doc() | storage:id()) ->
    {ok, od_group:id() | undefined} | {error, Reason :: term()}.
get_group_id(Gid, SpaceId, #document{key = StorageId, value = Storage = #storage{}}) ->
    get_group_id(Gid, SpaceId, StorageId, Storage);
get_group_id(Gid, SpaceId, StorageId) ->
    {ok, StorageDoc} = storage:get(StorageId),
    get_group_id(Gid, SpaceId, StorageDoc).

%%--------------------------------------------------------------------
%% @doc
%% Returns od_group:id() for storage group associated with given
%% NFSv4 ACL name which is appropriate for the local server operations.
%% Ids are cached for timeout defined in #luma_config{} record.
%% If reverse LUMA is disabled, function returns ?ROOT USER_ID.
%% @end
%%--------------------------------------------------------------------
-spec get_group_id_by_name(binary(), od_space:id(), storage:id(), storage:model()) ->
    {ok, od_group:id() | undefined} | {error, Reason :: term()}.
get_group_id_by_name(Name, SpaceId, StorageId, Storage = #storage{}) ->
    case storage:is_luma_enabled(Storage) of
        false ->
            {error, luma_disabled};
        true ->
            case is_storage_supported(Storage) of
                false ->
                    {error, not_supported_storage_type};
                true ->
                    get_group_id_from_supported_storage_acl_name(Name, SpaceId,
                        StorageId, Storage)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_group_id_by_name(Name, StorageId, Storage = #storage{}).
%% @end
%%--------------------------------------------------------------------
-spec get_group_id_by_name(binary(), od_space:id(), storage:id() | storage:doc()) ->
    {ok, od_group:id() | undefined} | {error, Reason :: term()}.
get_group_id_by_name(Name, SpaceId, #document{key = StorageId, value = Storage = #storage{}}) ->
    get_group_id_by_name(Name, SpaceId, StorageId, Storage);
get_group_id_by_name(Name, SpaceId, StorageId) ->
    {ok, StorageDoc} = storage:get(StorageId),
    get_group_id_by_name(Name, SpaceId, StorageDoc).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Maps user credentials on supported storage to onedata user id.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id_from_supported_storage_credentials(integer(), storage:id(),
    storage:model()) -> {ok, od_user:id()} | {error,  term()}.
get_user_id_from_supported_storage_credentials(Uid, StorageId, #storage{
    name = StorageName,
    luma_config = LumaConfig
}) ->
    Result = luma_cache:get_user_id(Uid, StorageId,  fun() ->
        reverse_luma_proxy:get_user_id(Uid, StorageId, StorageName, LumaConfig)
    end),
    case Result of
        {error, Reason} ->
            {error, {luma_server, Reason}};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Maps user credentials on supported storage to onedata user id.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id_from_supported_storage_acl_name(binary(), storage:id(),
    storage:model()) -> {ok, od_user:id()} | {error,  term()}.
get_user_id_from_supported_storage_acl_name(Name, StorageId, #storage{
    name = StorageName,
    luma_config = LumaConfig
}) ->
    Result = luma_cache:get_user_id(Name, StorageId, fun() ->
        reverse_luma_proxy:get_user_id_by_name(Name, StorageId, StorageName,
            LumaConfig)
    end),
    case Result of
        {error, Reason} ->
            {error, {luma_server, Reason}};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Maps user credentials on supported storage to onedata user id.
%% @end
%%--------------------------------------------------------------------
-spec get_group_id_from_supported_storage_credentials(integer(), od_space:id(),
    storage:id(), storage:model()) -> {ok, od_group:id()} | {error,  term()}.
get_group_id_from_supported_storage_credentials(Gid, SpaceId, StorageId, #storage{
    name = StorageName,
    luma_config = LumaConfig
}) ->
    Result = luma_cache:get_group_id(Gid, StorageId, fun() ->
        reverse_luma_proxy:get_group_id(Gid, SpaceId, StorageId, StorageName,
            LumaConfig)
    end),
    case Result of
        {error, Reason} ->
            {error, {luma_server, Reason}};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Maps user credentials on supported storage to onedata user id.
%% @end
%%--------------------------------------------------------------------
-spec get_group_id_from_supported_storage_acl_name(binary(), od_space:id(),
    storage:id(), storage:model()) -> {ok, od_group:id()} | {error,  term()}.
get_group_id_from_supported_storage_acl_name(Name, SpaceId, StorageId, #storage{
    name = StorageName,
    luma_config = LumaConfig
}) ->
    Result = luma_cache:get_group_id(Name, StorageId, fun() ->
        reverse_luma_proxy:get_group_id_by_name(Name, SpaceId, StorageId,
            StorageName, LumaConfig)
    end),
    case Result of
        {error, Reason} ->
            {error, {luma_server, Reason}};
        Other ->
            Other
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given storage is supported.
%% @end
%%-------------------------------------------------------------------
-spec is_storage_supported(storage:model()) -> boolean().
is_storage_supported(#storage{
    helpers = [#helper{name = HelperName} | _]
}) ->
    lists:member(HelperName, supported_storages()).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% List of storages supported by reverse luma
%% @end
%%-------------------------------------------------------------------
-spec supported_storages() -> [helper:name()].
supported_storages() -> [
    ?POSIX_HELPER_NAME,
    ?GLUSTERFS_HELPER_NAME,
    ?NULL_DEVICE_HELPER_NAME
].