%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model that stores list of storage IDs attached to the space.
%%% @TODO VFS-5856 deprecated, included for upgrade procedure. Remove in next major release.
%%% @end
%%%-------------------------------------------------------------------
-module(space_storage).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("global_definitions.hrl").

%% API
-export([get/1, delete/1]).
-export([get_storage_ids/1, get_mounted_in_root/1]).

%% datastore_model callbacks
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type id() :: od_space:id().
-type record() :: #space_storage{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-export_type([id/0, record/0, doc/0, diff/0]).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns space storage.
%% @end
%%--------------------------------------------------------------------
-spec get(undefined | id()) -> {ok, doc()} | {error, term()}.
get(undefined) ->
    {error, not_found};
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes space storage.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).


%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage IDs attached to the space.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_ids(record() | doc() | id()) -> [od_storage:id()].
get_storage_ids(#space_storage{storage_ids = StorageIds}) ->
    StorageIds;
get_storage_ids(#document{value = #space_storage{} = Value}) ->
    get_storage_ids(Value);
get_storage_ids(SpaceId) ->
    {ok, Doc} = ?MODULE:get(SpaceId),
    get_storage_ids(Doc).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage IDs attached to the space that have been mounted in
%% storage root.
%% @end
%%--------------------------------------------------------------------
-spec get_mounted_in_root(record() | doc() | id()) -> [od_storage:id()].
get_mounted_in_root(#space_storage{mounted_in_root = StorageIds}) ->
    StorageIds;
get_mounted_in_root(#document{value = #space_storage{} = Value}) ->
    get_mounted_in_root(Value);
get_mounted_in_root(SpaceId) ->
    {ok, Doc} = ?MODULE:get(SpaceId),
    get_mounted_in_root(Doc).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    5.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {storage_ids, [string]}
    ]};
get_record_struct(2) ->
    {record, [
        {storage_ids, [string]},
        {mounted_in_root, [string]}
    ]};
get_record_struct(3) ->
    {record, [
        {storage_ids, [string]},
        {mounted_in_root, [string]},
        {cleanup_enabled, boolean}
    ]};
get_record_struct(4) ->
    {record, [
        {storage_ids, [string]},
        {mounted_in_root, [string]},
        {file_popularity_enabled, boolean},
        {cleanup_enabled, boolean},
        {cleanup_in_progress, string},
        {autocleaning_config, {record, [
            {lower_file_size_limit, integer},
            {upper_file_size_limit, integer},
            {max_file_not_opened_hours, integer},
            {target, integer},
            {threshold, integer}
        ]}}
    ]};
get_record_struct(5) ->
    {record, [
        {storage_ids, [string]},
        {mounted_in_root, [string]}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, StorageIds}) ->
    {2, {?MODULE, StorageIds, []}};
upgrade_record(2, {?MODULE, StorageIds, MountedInRoot}) ->
    {3, {?MODULE, StorageIds, MountedInRoot, false}};
upgrade_record(3, {?MODULE, StorageIds, MountedInRoot, CleanupEnabled}) ->
    {4, {?MODULE, StorageIds, MountedInRoot, false, CleanupEnabled, undefined, undefined}};
upgrade_record(4, {?MODULE, StorageIds, MountedInRoot, _FilePopularityEnabled,
    _CleanupEnabled, _CleanupInProgress, _AutocleaningConfig}) ->
    {5, {?MODULE, StorageIds, MountedInRoot}}.