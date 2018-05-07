%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model that stores list of storage IDs attached to the space.
%%% @end
%%%-------------------------------------------------------------------
-module(space_storage).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("global_definitions.hrl").

%% API
-export([add/2, add/3]).
-export([get/1, delete/1, update/2]).
-export([get_storage_ids/1, get_mounted_in_root/1,
    is_file_popularity_enabled/1, enable_file_popularity/1,
    disable_file_popularity/1, get_autocleaning_config/1,
    get_cleanup_in_progress/1, mark_cleanup_finished/1,
    maybe_mark_cleanup_in_progress/2,
    get_file_popularity_details/1, get_soft_quota/0]).

%% datastore_model callbacks
-export([get_record_version/0, get_record_struct/1, upgrade_record/2, get_posthooks/0]).

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
%% Updates space storage.
%% @end
%%--------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, id()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

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
    space_strategies:delete(Key),
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Adds storage to the space.
%% @end
%%--------------------------------------------------------------------
-spec add(od_space:id(), storage:id()) ->
    {ok, od_space:id()} | {error, Reason :: term()}.
add(SpaceId, StorageId) ->
    add(SpaceId, StorageId, false).

%%--------------------------------------------------------------------
%% @doc
%% Adds storage to the space.
%% @end
%%--------------------------------------------------------------------
-spec add(od_space:id(), storage:id(), boolean()) ->
    {ok, od_space:id()} | {error, Reason :: term()}.
add(SpaceId, StorageId, MountInRoot) ->
    Diff = fun(#space_storage{
        storage_ids = StorageIds,
        mounted_in_root = MountedInRoot
    } = Model) ->
        case lists:member(StorageId, StorageIds) of
            true -> {error, already_exists};
            false ->
                SpaceStorage = Model#space_storage{
                    storage_ids = [StorageId | StorageIds]
                },
                case MountInRoot of
                    true ->
                        {ok, SpaceStorage#space_storage{
                            mounted_in_root = [StorageId | MountedInRoot]
                        }};
                    _ ->
                        {ok, SpaceStorage}
                end
        end
    end,
    #document{value = Default} = new(SpaceId, StorageId, MountInRoot),

    case datastore_model:update(?CTX, SpaceId, Diff, Default) of
        {ok, _} ->
            ok = space_strategies:add_storage(SpaceId, StorageId),
            {ok, SpaceId};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage IDs attached to the space.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_ids(record() | doc() | id()) -> [storage:id()].
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
-spec get_mounted_in_root(record() | doc() | id()) -> [storage:id()].
get_mounted_in_root(#space_storage{mounted_in_root = StorageIds}) ->
    StorageIds;
get_mounted_in_root(#document{value = #space_storage{} = Value}) ->
    get_mounted_in_root(Value);
get_mounted_in_root(SpaceId) ->
    {ok, Doc} = ?MODULE:get(SpaceId),
    get_mounted_in_root(Doc).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether automatic cleanup is enabled for storage supporting
%% given space.
%% @end
%%--------------------------------------------------------------------
-spec is_file_popularity_enabled(od_space:id()) -> boolean().
is_file_popularity_enabled(SpaceId) ->
    case space_storage:get(SpaceId) of
        {ok, Doc} ->
            Doc#document.value#space_storage.file_popularity_enabled;
        _Error ->
            false
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns file_popularity details for given space.
%% @end
%%-------------------------------------------------------------------
-spec get_file_popularity_details(od_space:id()) -> proplists:proplist().
get_file_popularity_details(SpaceId) -> [
    {enabled, is_file_popularity_enabled(SpaceId)},
    {restUrl, file_popularity_view:rest_url(SpaceId)}
].

%%-------------------------------------------------------------------
%% @doc
%% Enables gathering file popularity statistics for storage
%% supporting given space.
%% @end
%%-------------------------------------------------------------------
-spec enable_file_popularity(od_space:id()) -> {ok, od_space:id()}.
enable_file_popularity(SpaceId) ->
    update_file_popularity(SpaceId, true).

%%-------------------------------------------------------------------
%% @doc
%% Disables gathering file popularity statistics for storage
%% supporting given space.
%% @end
%%-------------------------------------------------------------------
-spec disable_file_popularity(od_space:id()) -> {ok, od_space:id()}.
disable_file_popularity(SpaceId) ->
    update_file_popularity(SpaceId, false),
    space_cleanup_api:disable_autocleaning(SpaceId).

%%-------------------------------------------------------------------
%% @doc
%% getter for cleanup_in_progress field
%% @end
%%-------------------------------------------------------------------
-spec get_cleanup_in_progress(record() | doc() | od_space:id()) -> autocleaning:id() | undefined.
get_cleanup_in_progress(#space_storage{cleanup_in_progress = CleanupInProgress}) ->
    CleanupInProgress;
get_cleanup_in_progress(#document{value = SS}) ->
    get_cleanup_in_progress(SS);
get_cleanup_in_progress(SpaceId) ->
    {ok, SpaceStorageDoc} = ?MODULE:get(SpaceId),
    get_cleanup_in_progress(SpaceStorageDoc).


%%-------------------------------------------------------------------
%% @doc
%% Returns autocleaning_config of storage supporting given space.
%% @end
%%-------------------------------------------------------------------
-spec get_autocleaning_config(record() | doc() | od_space:id()) -> undefined | autocleaning_config:config().
get_autocleaning_config(#document{value = SS = #space_storage{}}) ->
    get_autocleaning_config(SS);
get_autocleaning_config(SpaceStorage = #space_storage{}) ->
    SpaceStorage#space_storage.autocleaning_config;
get_autocleaning_config(SpaceId) ->
    {ok, Doc} = ?MODULE:get(SpaceId),
    get_autocleaning_config(Doc#document.value).


%%-------------------------------------------------------------------
%% @doc
%% Sets given AutocleaningId as currently in progress if
%% cleanup_in_progress field is undefined. Otherwise does nothing.
%% @end
%%-------------------------------------------------------------------
-spec maybe_mark_cleanup_in_progress(od_space:id(), autocleaning:id()) ->
    {ok, od_space:id()}.
maybe_mark_cleanup_in_progress(SpaceId, AutocleaningId) ->
    update(SpaceId, fun
        (SS = #space_storage{cleanup_in_progress = undefined}) ->
            {ok, SS#space_storage{cleanup_in_progress = AutocleaningId}};
        (SS) ->
            {ok, SS}
    end).

%%-------------------------------------------------------------------
%% @doc
%% Sets cleanup_in_progress field to undefined.
%% @end
%%-------------------------------------------------------------------
-spec mark_cleanup_finished(od_space:id()) ->
    {ok, od_space:id()}.
mark_cleanup_finished(SpaceId) ->
    update(SpaceId, fun(SS) ->
        {ok, SS#space_storage{cleanup_in_progress = undefined}}
    end).

%%-------------------------------------------------------------------
%% @doc
%% Returns soft_quota limit currently set in app.config
%% @end
%%-------------------------------------------------------------------
-spec get_soft_quota() -> undefined | non_neg_integer().
get_soft_quota() ->
    application:get_env(?APP_NAME, soft_quota_limit_size, undefined).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns space_storage document.
%% @end
%%--------------------------------------------------------------------
-spec(new(od_space:id(), storage:id(), boolean()) -> doc()).
new(SpaceId, StorageId, true) ->
    #document{key = SpaceId, value = #space_storage{
        storage_ids = [StorageId],
        mounted_in_root = [StorageId]}};
new(SpaceId, StorageId, _) ->
    #document{key = SpaceId, value = #space_storage{storage_ids = [StorageId]}}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Helper function for changing file_popularity setting.
%% @end
%%-------------------------------------------------------------------
-spec update_file_popularity(od_space:id(), boolean()) -> {ok, od_space:id()}.
update_file_popularity(SpaceId, Enable) ->
    update(SpaceId, fun(SpaceStorage) ->
        {ok, SpaceStorage#space_storage{file_popularity_enabled = Enable}}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Space storage create/update posthook.
%% @end
%%--------------------------------------------------------------------
-spec run_after(atom(), list(), term()) -> term().
run_after(create, _, {ok, #document{key = SpaceId}}) ->
    file_popularity:initialize(SpaceId),
    {ok, SpaceId};
run_after(update, [_, _, _], {ok, #document{key = SpaceId}}) ->
    file_popularity:initialize(SpaceId),
    {ok, SpaceId};
run_after(update, [_, _, _, _], {ok, #document{key = SpaceId}}) ->
    file_popularity:initialize(SpaceId),
    {ok, SpaceId};
run_after(save, _, {ok, #document{key = SpaceId}}) ->
    file_popularity:initialize(SpaceId),
    {ok, SpaceId};
run_after(_Function, _Args, Result) ->
    Result.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns list of callbacks which will be called after each operation
%% on datastore model.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks() -> [datastore_hooks:posthook()].
get_posthooks() ->
    [fun run_after/3].

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    4.

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
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, StorageIds}) ->
    {2, #space_storage{storage_ids = StorageIds}};
upgrade_record(2, {?MODULE, StorageIds, MountedInRoot}) ->
    {3, #space_storage{storage_ids = StorageIds, mounted_in_root = MountedInRoot}};
upgrade_record(3, {?MODULE, StorageIds, MountedInRoot, CleanupEnabled}) ->
    {4, #space_storage{
        storage_ids = StorageIds,
        mounted_in_root = MountedInRoot,
        cleanup_enabled = CleanupEnabled,
        autocleaning_config = undefined
    }}.