%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for mapping storage users to onedata users.
%%% @end
%%%-------------------------------------------------------------------
-module(reverse_luma).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([
    get_user_id/2,
    get_user_id_by_name/2,
    get_group_id/3,
    get_group_id_by_name/3
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
-spec get_user_id(integer(), od_storage:id() | storage:record()) ->
    {ok, od_user:id()} | {error, Reason :: term()}.
get_user_id(Uid, StorageId) when is_binary(StorageId) ->
    {ok, StorageRecord} = storage:get(StorageId),
    get_user_id(Uid, StorageRecord);
get_user_id(Uid, StorageRecord) ->
    StorageId = storage:get_id(StorageRecord),
    luma_cache:get_user_id(Uid, StorageId, fun() ->
        case storage:is_luma_enabled(StorageRecord) of
            false ->
                {ok, ?ROOT_USER_ID};
            true ->
                case is_storage_supported(StorageRecord) of
                    false ->
                        {error, not_supported_storage_type};
                    true ->
                        get_user_id_from_supported_storage_credentials(Uid,
                            StorageId, StorageRecord)
                end
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Returns od_user:id() for storage user associated with given
%% NFSv4 ACL name which is appropriate for the local server operations.
%% Ids are cached for timeout defined in #luma_config{} record.
%% If reverse LUMA is disabled, function returns ?ROOT USER_ID.
%% @end
-spec get_user_id_by_name(binary(), od_storage:id() | storage:record()) ->
    {ok, od_user:id()} | {error, Reason :: term()}.
get_user_id_by_name(Name, StorageId) when is_binary(StorageId) ->
    {ok, StorageRecord} = storage:get(StorageId),
    get_user_id_by_name(Name, StorageRecord);
get_user_id_by_name(Name, StorageRecord) ->
    StorageId = storage:get_id(StorageRecord),
    luma_cache:get_user_id(Name, StorageId, fun() ->
        case storage:is_luma_enabled(StorageRecord) of
            false ->
                {error, luma_disabled};
            true ->
                case is_storage_supported(StorageRecord) of
                    false ->
                        {error, not_supported_storage_type};
                    true ->
                        get_user_id_from_supported_storage_acl_name(Name,
                            StorageId, StorageRecord)
                end
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Returns od_group:id() for storage group associated with given Gid.
%% which is appropriate for the local server operations.
%% Ids are cached for timeout defined in #luma_config{} record.
%% If reverse LUMA is disabled, function returns error.
%% @end
-spec get_group_id(integer(), od_space:id(), storage:record() | od_storage:id()) ->
    {ok, od_group:id() | undefined} | {error, Reason :: term()}.
get_group_id(Gid, SpaceId, StorageId) when is_binary(StorageId) ->
    {ok, StorageRecord} = storage:get(StorageId),
    get_group_id(Gid, SpaceId, StorageRecord);
get_group_id(Gid, SpaceId, StorageRecord) ->
    StorageId = storage:get_id(StorageRecord),
    luma_cache:get_group_id(Gid, StorageId, fun() ->
        case storage:is_luma_enabled(StorageRecord) of
            false ->
                {ok, undefined};
            true ->
                case is_storage_supported(StorageRecord) of
                    false ->
                        {error, not_supported_storage_type};
                    true ->
                        get_group_id_from_supported_storage_credentials(Gid, SpaceId, StorageId, StorageRecord)
                end
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Returns od_group:id() for storage group associated with given
%% NFSv4 ACL name which is appropriate for the local server operations.
%% Ids are cached for timeout defined in #luma_config{} record.
%% If reverse LUMA is disabled, function returns ?ROOT USER_ID.
%% @end
-spec get_group_id_by_name(binary(), od_space:id(), od_storage:id() | storage:record()) ->
    {ok, od_group:id() | undefined} | {error, Reason :: term()}.
get_group_id_by_name(Name, SpaceId, StorageId) when is_binary(StorageId) ->
    {ok, StorageRecord} = storage:get(StorageId),
    get_group_id_by_name(Name, SpaceId, StorageRecord);
get_group_id_by_name(Name, SpaceId, StorageRecord) ->
    StorageId = storage:get_id(StorageRecord),
    luma_cache:get_group_id(Name, StorageId, fun() ->
        case storage:is_luma_enabled(StorageRecord) of
            false ->
                {error, luma_disabled};
            true ->
                case is_storage_supported(StorageRecord) of
                    false ->
                        {error, not_supported_storage_type};
                    true ->
                        get_group_id_from_supported_storage_acl_name(Name, SpaceId,
                            StorageId, StorageRecord)
                end
        end
    end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Maps user credentials on supported storage to onedata user id.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id_from_supported_storage_credentials(integer(), od_storage:id(),
    storage:record()) -> {ok, od_user:id()} | {error, term()}.
get_user_id_from_supported_storage_credentials(Uid, StorageId, StorageRecord) ->
    StorageName = storage:get_name(StorageRecord),
    LumaConfig = storage:get_luma_config(StorageRecord),
    case reverse_luma_proxy:get_user_id(Uid, StorageId, StorageName, LumaConfig) of
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
-spec get_user_id_from_supported_storage_acl_name(binary(), od_storage:id(),
    storage:record()) -> {ok, od_user:id()} | {error, term()}.
get_user_id_from_supported_storage_acl_name(Name, StorageId, StorageRecord) ->
    StorageName = storage:get_name(StorageRecord),
    LumaConfig = storage:get_luma_config(StorageRecord),
    case reverse_luma_proxy:get_user_id_by_name(Name, StorageId, StorageName,
        LumaConfig)
    of
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
    od_storage:id(), storage:record()) -> {ok, od_group:id()} | {error, term()}.
get_group_id_from_supported_storage_credentials(Gid, SpaceId, StorageId, StorageRecord) ->
    StorageName = storage:get_name(StorageRecord),
    LumaConfig = storage:get_luma_config(StorageRecord),
    case reverse_luma_proxy:get_group_id(Gid, SpaceId, StorageId, StorageName,
        LumaConfig)
    of
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
    od_storage:id(), storage:record()) -> {ok, od_group:id()} | {error, term()}.
get_group_id_from_supported_storage_acl_name(Name, SpaceId, StorageId, StorageRecord) ->
    StorageName = storage:get_name(StorageRecord),
    LumaConfig = storage:get_luma_config(StorageRecord),
    case reverse_luma_proxy:get_group_id_by_name(Name, SpaceId, StorageId,
        StorageName, LumaConfig)
    of
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
-spec is_storage_supported(storage:record()) -> boolean().
is_storage_supported(StorageRecord) ->
    Helper = storage:get_helper(StorageRecord),
    HelperName = helper:get_name(Helper),
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
    ?WEBDAV_HELPER_NAME,
    ?NULL_DEVICE_HELPER_NAME
].
