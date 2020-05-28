%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO UPDATE
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
-module(luma_spaces_posix_storage_defaults).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/luma/luma.hrl").

%% API
-export([get_or_acquire/2, delete/2, clear_all/1, get_and_describe/2, store/3]).

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
store(Storage, SpaceId, PosixDefaultsMap) ->
    case storage:is_imported_storage(Storage) of
        true ->
            % TODO handle this error in onepanel, add to ctol errors
          {error, imported_storage};
        false ->
            case luma_sanitizer:sanitize_posix_credentials(PosixDefaultsMap) of
                {ok, PosixDefaultsMap} ->
                    Record = luma_posix_credentials:new(PosixDefaultsMap),
                    case luma_db:store(Storage, SpaceId, ?MODULE, Record, ?LOCAL_FEED) of
                        ok ->
                            luma_spaces_display_defaults:delete_if_auto_feed(Storage, SpaceId);
                        Error ->
                            Error
                    end;
                Error2 ->
                    Error2
            end
    end.

-spec delete(storage:id(), key()) -> ok.
delete(StorageId, SpaceId) ->
    ok = luma_db:delete(StorageId, SpaceId, ?MODULE),
    luma_spaces_display_defaults:delete_if_auto_feed(StorageId, SpaceId).

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
    IsImportedStorage = storage:is_imported_storage(Storage),
    LumaFeed = storage:get_luma_feed(Storage),
    case IsImportedStorage orelse LumaFeed =/= ?EXTERNAL_FEED of
        true ->
            % On imported storages this record is always taken from ?AUTO_FEED
            % even if LumaFeed == ?EXTERNAL_FEED
            acquire_from_auto_feed(Storage, SpaceId);
        false ->
           acquire_from_external_feed(Storage, SpaceId)
    end.

-spec acquire_from_auto_feed(storage(), od_space:id()) ->
    {ok, record(), luma:feed()}.
acquire_from_auto_feed(Storage, SpaceId) ->
    PosixDefaults = luma_auto_feed:acquire_default_posix_storage_credentials(Storage, SpaceId),
    {ok, PosixDefaults, ?AUTO_FEED}.

-spec acquire_from_external_feed(storage(), od_space:id()) ->
    {ok, record(), luma:feed()}.
acquire_from_external_feed(Storage, SpaceId) ->
    PosixDefaultsMap0 = fetch_default_posix_credentials(Storage, SpaceId),
    RealFeed = case map_size(PosixDefaultsMap0) =:= 0 of
        true -> ?AUTO_FEED; % if returned map was empty, none of the fields were set by external feed
        false -> ?EXTERNAL_FEED
    end,
    PosixDefaultsMap1 = ensure_all_fields_are_defined(PosixDefaultsMap0, Storage, SpaceId),
    {ok, luma_posix_credentials:new(PosixDefaultsMap1), RealFeed}.

-spec fetch_default_posix_credentials(storage:data(), key()) -> 
    luma_posix_credentials:credentials_map().
fetch_default_posix_credentials(Storage, SpaceId) ->
    case luma_external_feed:fetch_default_posix_credentials(SpaceId, Storage) of
        {ok, DefaultCredentials} -> DefaultCredentials;
        {error, not_found} -> #{};
        {error, Reason} -> throw(Reason)
    end.


-spec ensure_all_fields_are_defined(luma_posix_credentials:credentials_map(), storage(), od_space:id()) ->
    luma_posix_credentials:credentials_map().
ensure_all_fields_are_defined(FetchedPosixDefaultsMap, Storage, SpaceId) ->
    case luma_posix_credentials:all_fields_defined(FetchedPosixDefaultsMap) of
        true ->
            FetchedPosixDefaultsMap;
        false ->
            FallbackDefaults = luma_auto_feed:acquire_default_posix_storage_credentials(Storage, SpaceId),
            FallbackDefaultsJson = luma_posix_credentials:to_json(FallbackDefaults),
            maps:merge(FallbackDefaultsJson, FetchedPosixDefaultsMap)
    end.