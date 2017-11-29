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

-define(KEY_SEPARATOR, <<"::">>).


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
                    get_user_id_from_supported_storage_credentials(Uid, StorageId, Storage)
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
                    get_user_id_from_supported_storage_acl_name(Name, StorageId, Storage)
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
                    get_group_id_from_supported_storage_acl_name(Name, SpaceId, StorageId, Storage)
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
    storage:model()) -> {ok, od_user:id()}.
get_user_id_from_supported_storage_credentials(Uid, StorageId,
    #storage{
        name = StorageName,
        luma_config = LumaConfig = #luma_config{
            cache_timeout = CacheTimeout
}}) ->

    Key = to_user_key(StorageId, Uid),
    luma_cache:get(Key,
        fun reverse_luma_proxy:get_user_id/4,
        [Uid, StorageId, StorageName, LumaConfig],
        CacheTimeout
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Maps user credentials on supported storage to onedata user id.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id_from_supported_storage_acl_name(binary(), storage:id(),
    storage:model()) -> {ok, od_user:id()}.
get_user_id_from_supported_storage_acl_name(Name, StorageId,
    #storage{
        name = StorageName,
        luma_config = LumaConfig = #luma_config{
            cache_timeout = CacheTimeout
        }}) ->

    Key = to_user_key(StorageId, Name),
    luma_cache:get(Key,
        fun reverse_luma_proxy:get_user_id_by_name/4,
        [Name, StorageId, StorageName, LumaConfig],
        CacheTimeout
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Maps user credentials on supported storage to onedata user id.
%% @end
%%--------------------------------------------------------------------
-spec get_group_id_from_supported_storage_credentials(integer(), od_space:id(),
    storage:id(), storage:model()) -> {ok, od_group:id()}.
get_group_id_from_supported_storage_credentials(Gid, SpaceId, StorageId,
    #storage{
        name = StorageName,
        luma_config = LumaConfig = #luma_config{
            cache_timeout = CacheTimeout
        }}) ->

    Key = to_group_key(StorageId, Gid),
    luma_cache:get(Key,
        fun reverse_luma_proxy:get_group_id/5,
        [Gid, SpaceId, StorageId, StorageName, LumaConfig],
        CacheTimeout
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Maps user credentials on supported storage to onedata user id.
%% @end
%%--------------------------------------------------------------------
-spec get_group_id_from_supported_storage_acl_name(binary(), od_space:id(),
    storage:id(), storage:model()) -> {ok, od_group:id()}.
get_group_id_from_supported_storage_acl_name(Name, SpaceId, StorageId,
    #storage{
        name = StorageName,
        luma_config = LumaConfig = #luma_config{
            cache_timeout = CacheTimeout
        }}) ->

    Key = to_group_key(StorageId, Name),
    luma_cache:get(Key,
        fun reverse_luma_proxy:get_group_id_by_name/5,
        [Name, SpaceId, StorageId, StorageName, LumaConfig],
        CacheTimeout
    ).

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
    ?GLUSTERFS_HELPER_NAME
].

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Concatenates helper name uid, and guid to create unique key for given user
%% @end
%%-------------------------------------------------------------------
-spec to_user_key(storage:id(), integer() | binary()) -> binary().
to_user_key(StorageId, UidOrName) ->
    Args = [<<"user">>, StorageId, UidOrName],
    Binaries = [str_utils:to_binary(E) || E <- Args],
    str_utils:join_binary(Binaries, ?KEY_SEPARATOR).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Concatenates helper name and guid to create unique key for given group
%% @end
%%-------------------------------------------------------------------
-spec to_group_key(storage:id(), integer() | binary()) -> binary().
to_group_key(StorageId, GidOrName) ->
    Args = [<<"group">>, StorageId, GidOrName],
    Binaries = [str_utils:to_binary(E) || E <- Args],
    str_utils:join_binary(Binaries, ?KEY_SEPARATOR).