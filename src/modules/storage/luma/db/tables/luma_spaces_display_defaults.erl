%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
% TODO update
%%% This module implements LUMA DB table that associates space with
%%% default credentials used in LUMA DB mappings for users.
%%% The default credentials are represented by #luma_space_default record.
%%%
%%% A separate table is created for each storage
%%% so the mappings are actually associated with
%%% pair (storage:id(), od_space:id()).
%%%
%%% For more info on luma_space_defaults:defaults() structure please see
%%% luma_space_defaults.erl module.
%%%
%%% Mappings may be set in 3 ways:
%%%  * filled by default algorithm in case NO_LUMA mode is set for given
%%%    storage (see luma_space_defaults:ensure_all_fields_defined/3 function)
%%%  * preconfigured using REST API in case EMBEDDED_LUMA
%%%    is set for given storage
%%%  * cached after querying external, 3rd party LUMA server in case
%%%    EXTERNAL_LUMA mode is set for given storage
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
-type storage() :: storage:id() | storage:data().

-export_type([key/0, record/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec get_or_acquire(storage(), key()) -> {ok, record()} | {error, term()}.
get_or_acquire(Storage, SpaceId) ->
    luma_db:get_or_acquire(Storage, SpaceId, ?MODULE, fun() ->
        acquire(Storage, SpaceId)
    end).

-spec store(storage(), key(), luma_posix_credentials:credentials_map()) -> ok | {error, term()}.
store(Storage, SpaceId, DisplayDefaultsMap) ->
    case luma_sanitizer:sanitize_posix_credentials(DisplayDefaultsMap) of
        {ok, DisplayDefaultsMap2} ->
            Record = luma_posix_credentials:new(DisplayDefaultsMap2),
            luma_db:store(Storage, SpaceId, ?MODULE, Record, ?LOCAL_FEED);
        Error ->
            Error
    end.

-spec delete(storage:id(), key()) -> ok.
delete(StorageId, SpaceId) ->
    % todo usuwac ale tylko jak jest dobry feed ustaiowny , sprawdzic !!!1
    luma_db:delete(StorageId, SpaceId, ?MODULE).

delete_if_auto_feed(Storage, SpaceId) ->
    luma_db:delete_if_auto_feed(Storage, SpaceId, ?MODULE).

-spec clear_all(storage:id()) -> ok | {error, term()}.
clear_all(StorageId) ->
    luma_db:clear_all(StorageId, ?MODULE).

-spec get_and_describe(storage(), key()) ->
    {ok, json_utils:json_map()} | {error, term()}.
get_and_describe(Storage, SpaceId) ->
    luma_db:get_and_describe(Storage, SpaceId, ?MODULE).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec acquire(storage:data(), key()) -> {ok, record(), luma:feed()}.
acquire(Storage, SpaceId) ->
    LumaFeed = storage:get_luma_feed(Storage),
    % Feed returned from acquire_ functions can be different from the feed set in #luma_config because
    % endpoint for configuring display credentials is optional
    case LumaFeed of
        ?EXTERNAL_FEED -> acquire_from_external_feed(Storage, SpaceId);
        ?AUTO_FEED -> acquire_from_auto_feed(Storage, SpaceId);
        ?LOCAL_FEED -> acquire_from_auto_feed(Storage, SpaceId)
    end.

-spec acquire_from_auto_feed(storage(), od_space:id()) ->
    {ok, record(), luma:feed()}.
acquire_from_auto_feed(Storage, SpaceId) ->
    DisplayDefaults = luma_auto_feed:acquire_default_display_credentials(Storage, SpaceId),
    {ok, DisplayDefaults, ?AUTO_FEED}.

-spec acquire_from_external_feed(storage(), od_space:id()) ->
    {ok, record(), luma:feed()}.
acquire_from_external_feed(Storage, SpaceId) ->
    DisplayDefaultsMap0 = fetch_display_credentials(Storage, SpaceId),
    RealFeed = case map_size(DisplayDefaultsMap0) =:= 0 of
        true -> ?AUTO_FEED; % if returned map was empty, none of the fields were set by external feed
        false -> ?EXTERNAL_FEED
    end,
    DisplayDefaultsMap1 = ensure_all_fields_are_defined(DisplayDefaultsMap0, Storage, SpaceId),
    {ok, luma_posix_credentials:new(DisplayDefaultsMap1), RealFeed}.


-spec fetch_display_credentials(storage:data(), key()) ->
    luma_posix_credentials:credentials_map().
fetch_display_credentials(Storage, SpaceId) ->
    case luma_external_feed:fetch_default_display_credentials(SpaceId, Storage) of
        {ok, DisplayCredentials} -> DisplayCredentials;
        {error, not_found} -> #{};
        {error, Reason} -> throw(Reason)
    end.

-spec ensure_all_fields_are_defined(luma_posix_credentials:credentials_map(), storage(), od_space:id()) ->
    luma_posix_credentials:credentials_map().
ensure_all_fields_are_defined(DisplayDefaultsMap, Storage, SpaceId) ->
    case luma_posix_credentials:all_fields_defined(DisplayDefaultsMap) of
        true ->
            DisplayDefaultsMap;
        false ->
            case storage:is_posix_compatible(Storage) of
                true ->
                    set_missing_fields_posix(DisplayDefaultsMap, Storage, SpaceId);
                false ->
                    set_missing_fields_non_posix(DisplayDefaultsMap, Storage, SpaceId)
            end
    end.

-spec set_missing_fields_posix(luma_posix_credentials:credentials_map(), storage(), od_space:id()) ->
    luma_posix_credentials:credentials_map().
set_missing_fields_posix(DisplayDefaultsMap, Storage, SpaceId) ->
    {ok, PosixDefaults} = luma_spaces_posix_storage_defaults:get_or_acquire(Storage, SpaceId),
    PosixDefaultsMap = luma_posix_credentials:to_json(PosixDefaults),
    maps:merge(PosixDefaultsMap, DisplayDefaultsMap).

-spec set_missing_fields_non_posix(luma_posix_credentials:credentials_map(), storage(), od_space:id()) ->
    luma_posix_credentials:credentials_map().
set_missing_fields_non_posix(DisplayDefaultsMap, Storage, SpaceId) ->
    FallbackDefaults = luma_auto_feed:acquire_default_display_credentials(Storage, SpaceId),
    FallbackDefaultsJson = luma_posix_credentials:to_json(FallbackDefaults),
    maps:merge(FallbackDefaultsJson, DisplayDefaultsMap).