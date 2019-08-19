%%%-------------------------------------------------------------------
%%% @author Wojciech Geisler
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module grouping all functions rpc called by Onepanel
%%% to simplify detecting their usage.
%%% @end
%%%-------------------------------------------------------------------
-module(rpc_api).
-author("Wojciech Geisler").

-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([
    storage_new/4, storage_create/1, storage_safe_remove/1,
    storage_supports_any_space/1, storage_list_ids/0, get_storage/1,
    get_space_storage/1, space_storage_get_storage_ids/1,
    space_storage_get_mounted_in_root/1, file_popularity_api_configure/2,
    file_popularity_api_get_configuration/1, autocleaning_api_configure/2,
    autocleaning_api_get_configuration/1, invalidate_luma_cache/1,
    new_helper/5
]).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec storage_new(storage:name(), [storage:helper()], boolean(),
    undefined | luma_config:config()) -> storage:doc().
storage_new(Name, Helpers, ReadOnly, LumaConfig) ->
    storage:new(Name, Helpers, ReadOnly, LumaConfig).


-spec storage_create(storage:doc()) -> ok | {error, term()}.
storage_create(StorageDoc) ->
    case storage:create(StorageDoc) of
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.


-spec storage_safe_remove(storage:id()) -> ok | {error, storage_in_use | term()}.
storage_safe_remove(StorageId) ->
    storage:safe_remove(StorageId).


-spec storage_supports_any_space(StorageId :: storage:id()) -> boolean().
storage_supports_any_space(StorageId) ->
    storage:supports_any_space(StorageId).


-spec storage_list_ids() -> {ok, [storage:doc()]} | {error, term()}.
storage_list_ids() ->
    case storage:list() of
        {ok, Docs} -> {ok, lists:map(fun storage:get_id/1, Docs)};
        Error -> Error
    end.


-spec get_storage(storage:id()) -> {ok, storage:doc()} | {error, term()}.
get_storage(Key) ->
    storage:get(Key).


-spec get_space_storage(space_storage:id()) ->
    {ok, space_storage:doc()} | {error, term()}.
get_space_storage(Key) ->
    space_storage:get(Key).


-spec space_storage_get_storage_ids(space_storage:id()) -> [storage:id()].
space_storage_get_storage_ids(SpaceId) ->
    space_storage:get_storage_ids(SpaceId).


-spec space_storage_get_mounted_in_root(space_storage:id()) -> [storage:id()].
space_storage_get_mounted_in_root(SpaceId) ->
    space_storage:get_mounted_in_root(SpaceId).


-spec file_popularity_api_configure(file_popularity_config:id(), map()) ->
    ok | {error, term()}.
file_popularity_api_configure(SpaceId, NewConfiguration) ->
    file_popularity_api:configure(SpaceId, NewConfiguration).


-spec file_popularity_api_get_configuration(file_popularity_config:id()) ->
    {ok, map()} | {error, term()}.
file_popularity_api_get_configuration(SpaceId) ->
    file_popularity_api:get_configuration(SpaceId).


-spec autocleaning_api_configure(od_space:id(), map()) -> ok | {error, term()}.
autocleaning_api_configure(SpaceId, Configuration) ->
    autocleaning_api:configure(SpaceId, Configuration).


-spec autocleaning_api_get_configuration(od_space:id()) -> map().
autocleaning_api_get_configuration(SpaceId) ->
    autocleaning_api:get_configuration(SpaceId).


-spec invalidate_luma_cache(storage:id()) -> ok.
invalidate_luma_cache(StorageId) ->
    luma_cache:invalidate(StorageId).


-spec new_helper(helper:name(), helper:args(), helper:user_ctx(), Insecure :: boolean(),
    helper:storage_path_type()) -> {ok, helpers:helper()}.
new_helper(HelperName, Args, AdminCtx, Insecure, StoragePathType) ->
    helper:new_helper(HelperName, Args, AdminCtx, Insecure, StoragePathType).


new_luma_config() ->
    luma_config:new()

