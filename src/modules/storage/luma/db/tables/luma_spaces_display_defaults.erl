%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements LUMA DB table that associates space with
%%% default display credentials used in LUMA DB mappings.
%%% The default credentials are represented by #luma_posix_credentials record.
%%%
%%% A separate table is created for each storage
%%% so the mappings are actually associated with
%%% pair (storage:id(), od_space:id()).
%%%
%%% For more info please read the docs of luma.erl module.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_spaces_display_defaults).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").


%% API
-export([get_or_acquire/2, store/3, delete/2, clear_all/1, get_and_describe/2, delete_if_auto_feed/2]).

-type key() :: od_space:id().
-type record() :: luma_posix_credentials:credentials().

-export_type([key/0, record/0]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec get_or_acquire(storage:data(), key()) -> {ok, record()} | {error, term()}.
get_or_acquire(StorageData, SpaceId) ->
    luma_db:get_or_acquire(StorageData, SpaceId, ?MODULE, fun() ->
        acquire(StorageData, SpaceId)
    end).


-spec store(storage:data(), key(), luma_posix_credentials:credentials_map()) -> ok | {error, term()}.
store(StorageData, SpaceId, DisplayDefaultsMap) ->
    case luma_sanitizer:sanitize_posix_credentials(DisplayDefaultsMap) of
        {ok, DisplayDefaultsMap2} ->
            DisplayDefaultsMap3 = ensure_all_fields_are_defined(DisplayDefaultsMap2, StorageData, SpaceId),
            Record = luma_posix_credentials:new(DisplayDefaultsMap3),
            luma_db:store(StorageData, SpaceId, ?MODULE, Record, ?LOCAL_FEED);
        Error ->
            Error
    end.


-spec delete(storage:data(), key()) -> ok.
delete(StorageData, SpaceId) ->
    luma_db:delete(StorageData, SpaceId, ?MODULE).


-spec delete_if_auto_feed(storage:data(), key()) -> ok.
delete_if_auto_feed(StorageData, SpaceId) ->
    luma_db:delete_if_auto_feed(StorageData, SpaceId, ?MODULE).


-spec clear_all(storage:data()) -> ok | {error, term()}.
clear_all(StorageData) ->
    luma_db:clear_all(StorageData, ?MODULE).


-spec get_and_describe(storage:data(), key()) ->
    {ok, luma_posix_credentials:credentials_map()} | {error, term()}.
get_and_describe(StorageData, SpaceId) ->
    luma_db:get_and_describe(StorageData, SpaceId, ?MODULE).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec acquire(storage:data(), key()) -> {luma_db:cache_policy(), record(), luma:feed()}.
acquire(StorageData, SpaceId) ->
    LumaFeed = storage:get_luma_feed(StorageData),
    % Feed returned from acquire_ functions can be different from the feed set in #luma_config because
    % endpoint for configuring display credentials is optional
    case LumaFeed of
        ?EXTERNAL_FEED -> acquire_from_external_feed(StorageData, SpaceId);
        ?AUTO_FEED -> acquire_from_auto_feed(StorageData, SpaceId);
        ?LOCAL_FEED -> acquire_from_auto_feed(StorageData, SpaceId)
    end.


%% @private
-spec acquire_from_auto_feed(storage:data(), od_space:id()) ->
    {luma_db:cache_policy(), record(), luma:feed()}.
acquire_from_auto_feed(StorageData, SpaceId) ->
    {ok, DisplayDefaults} = luma_auto_feed:acquire_default_display_credentials(StorageData, SpaceId),
    {nocache, DisplayDefaults, ?AUTO_FEED}.


%% @private
-spec acquire_from_external_feed(storage:data(), od_space:id()) ->
    {luma_db:cache_policy(), record(), luma:feed()}.
acquire_from_external_feed(StorageData, SpaceId) ->
    DisplayDefaultsMap0 = fetch_display_credentials(StorageData, SpaceId),
    RealFeed = case map_size(DisplayDefaultsMap0) =:= 0 of
        true -> ?AUTO_FEED; % if returned map was empty, none of the fields were set by external feed
        false -> ?EXTERNAL_FEED
    end,
    DisplayDefaultsMap1 = ensure_all_fields_are_defined(DisplayDefaultsMap0, StorageData, SpaceId),
    {cache, luma_posix_credentials:new(DisplayDefaultsMap1), RealFeed}.


%% @private
-spec fetch_display_credentials(storage:data(), key()) ->
    luma_posix_credentials:credentials_map().
fetch_display_credentials(StorageData, SpaceId) ->
    case luma_external_feed:fetch_default_display_credentials(SpaceId, StorageData) of
        {ok, DisplayCredentials} -> DisplayCredentials;
        {error, not_found} -> #{};
        {error, Reason} -> throw(Reason)
    end.


%% @private
-spec ensure_all_fields_are_defined(luma_posix_credentials:credentials_map(), storage:data(), od_space:id()) ->
    luma_posix_credentials:credentials_map().
ensure_all_fields_are_defined(DisplayDefaultsMap, StorageData, SpaceId) ->
    case luma_posix_credentials:all_fields_defined(DisplayDefaultsMap) of
        true ->
            DisplayDefaultsMap;
        false ->
            case storage:is_posix_compatible(StorageData) of
                true ->
                    set_missing_fields_posix(DisplayDefaultsMap, StorageData, SpaceId);
                false ->
                    set_missing_fields_non_posix(DisplayDefaultsMap, StorageData, SpaceId)
            end
    end.


%% @private
-spec set_missing_fields_posix(luma_posix_credentials:credentials_map(), storage:data(), od_space:id()) ->
    luma_posix_credentials:credentials_map().
set_missing_fields_posix(DisplayDefaultsMap, StorageData, SpaceId) ->
    {ok, PosixDefaults} = luma_spaces_posix_storage_defaults:get_or_acquire(StorageData, SpaceId),
    PosixDefaultsMap = luma_posix_credentials:to_json(PosixDefaults),
    maps:merge(PosixDefaultsMap, DisplayDefaultsMap).


%% @private
-spec set_missing_fields_non_posix(luma_posix_credentials:credentials_map(), storage:data(), od_space:id()) ->
    luma_posix_credentials:credentials_map().
set_missing_fields_non_posix(DisplayDefaultsMap, StorageData, SpaceId) ->
    {ok, FallbackDefaults} = luma_auto_feed:acquire_default_display_credentials(StorageData, SpaceId),
    FallbackDefaultsJson = luma_posix_credentials:to_json(FallbackDefaults),
    maps:merge(FallbackDefaultsJson, DisplayDefaultsMap).
