%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
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
-module(luma_spaces_defaults).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([get/2, delete/2, clear_all/1]).

-type key() :: od_space:id().
-type record() :: luma_space_defaults:defaults().
-type storage() :: storage:id() | storage:data().

-export_type([key/0, record/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec get(storage(), key()) -> {ok, record()} | {error, term()}.
get(Storage, SpaceId) ->
    luma_db:get(Storage, SpaceId, ?MODULE, fun() ->
        acquire(Storage, SpaceId)
    end).

-spec clear_all(storage:id()) -> ok | {error, term()}.
clear_all(StorageId) ->
    luma_db:clear_all(StorageId, ?MODULE).

-spec delete(storage:id(), key()) -> ok.
delete(StorageId, SpaceId) ->
    luma_db:delete(StorageId, SpaceId, ?MODULE).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec acquire(storage:data(), key()) -> {ok, record()}.
acquire(Storage, SpaceId) ->
    IsNotPosix = not storage:is_posix_compatible(Storage),
    % default credentials are ignored on:
    % - posix incompatible storages
    % - synced storages (storage mountpoint credentials are used as default credentials)
    IgnoreLumaDefaultCreds = IsNotPosix orelse storage:is_imported_storage(Storage),
    {DefaultPosixCredentials, DisplayCredentials} = case
        {storage:is_luma_enabled(Storage), IgnoreLumaDefaultCreds}
    of
        {true, false} ->
            {ok, DefaultCreds} = fetch_default_posix_credentials(Storage, SpaceId),
            {ok, DisplayCreds} = fetch_display_credentials(Storage, SpaceId),
            {DefaultCreds, DisplayCreds};
        {true, true} ->
            {ok, DisplayCreds} = fetch_display_credentials(Storage, SpaceId),
            {#{}, DisplayCreds};
        {false, _} ->
            {#{}, #{}}
    end,
    {ok, luma_space_defaults:new(DefaultPosixCredentials, DisplayCredentials, SpaceId, Storage)}.


-spec fetch_default_posix_credentials(storage:data(), key()) ->
    {ok, external_luma:posix_compatible_credentials()} | {error, term()}.
fetch_default_posix_credentials(Storage, SpaceId) ->
    case external_luma:fetch_default_posix_credentials(SpaceId, Storage) of
        {ok, DefaultCredentials} -> {ok, DefaultCredentials};
        {error, not_found} -> {ok, #{}};
        {error, Reason} -> throw(Reason)
    end.

-spec fetch_display_credentials(storage:data(), key()) ->
    {ok, external_luma:posix_compatible_credentials()} | {error, term()}.
fetch_display_credentials(Storage, SpaceId) ->
    case external_luma:fetch_default_display_credentials(SpaceId, Storage) of
        {ok, DisplayCredentials} -> {ok, DisplayCredentials};
        {error, not_found} -> {ok, #{}};
        {error, Reason} -> throw(Reason)
    end.
