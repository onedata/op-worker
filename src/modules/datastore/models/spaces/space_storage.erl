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
-behaviour(model_behaviour).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

%% API
-export([add/2, add/3]).
-export([get_storage_ids/1, get_mounted_in_root/1,
    is_file_popularity_enabled/1, is_cleanup_enabled/1, enable_file_popularity/1,
    disable_file_popularity/1, update_autocleaning/2, get_autocleaning_config/1,
    get_cleanup_in_progress/1, mark_cleanup_finished/1,
    maybe_mark_cleanup_in_progress/2, disable_autocleaning/1,
    get_file_popularity_details/1, get_autocleaning_details/1, get_soft_quota/0]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).
-export([record_struct/1, record_upgrade/2]).

-type id() :: od_space:id().
-type model() :: #space_storage{}.
-type doc() :: #document{value :: model()}.

-export_type([id/0, model/0, doc/0]).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) ->
    datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {storage_ids, [string]}
    ]};
record_struct(2) ->
    {record, [
        {storage_ids, [string]},
        {mounted_in_root, [string]}
    ]};
record_struct(3) ->
    {record, [
        {storage_ids, [string]},
        {mounted_in_root, [string]},
        {cleanup_enabled, boolean}
    ]};
record_struct(4) ->
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
%% Upgrades record from specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_upgrade(datastore_json:record_version(), tuple()) ->
    {datastore_json:record_version(), tuple()}.
record_upgrade(1, {?MODEL_NAME, StorageIds}) ->
    {2, #space_storage{storage_ids = StorageIds}};
record_upgrade(2, {?MODEL_NAME, StorageIds, MountedInRoot}) ->
    {3, #space_storage{storage_ids = StorageIds, mounted_in_root = MountedInRoot}};
record_upgrade(3, {?MODEL_NAME, StorageIds, MountedInRoot, CleanupEnabled}) ->
    {4, #space_storage{
        storage_ids = StorageIds,
        mounted_in_root = MountedInRoot,
        cleanup_enabled = CleanupEnabled,
        autocleaning_config = undefined
    }}.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    model:execute_with_default_context(?MODULE, create, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
%TODO - luma gets undefined storage
get(undefined) ->
    {error, {not_found, ?MODULE}};
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    Config = ?MODEL_CONFIG(space_storage_bucket, [{?MODULE, create}, {?MODULE, save},
        {?MODULE, create_or_update}, {?MODULE, update}], ?GLOBALLY_CACHED_LEVEL),
    Config#model_config{version = 4}.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(model_behaviour:model_type(), model_behaviour:model_action(),
    datastore:store_level(), Context :: term(), ReturnValue :: term()) -> ok.
'after'(?MODULE, create, _, _, {ok, SpaceId}) ->
    file_popularity:initialize(SpaceId);
'after'(?MODULE, create_or_update, _, _, {ok, SpaceId}) ->
    file_popularity:initialize(SpaceId);
'after'(?MODULE, save, _, _, {ok, SpaceId}) ->
    file_popularity:initialize(SpaceId);
'after'(?MODULE, update, _, _, {ok, SpaceId}) ->
    file_popularity:initialize(SpaceId);
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(model_behaviour:model_type(), model_behaviour:model_action(),
    datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% API
%%%===================================================================

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
    Doc = new(SpaceId, StorageId, MountInRoot),
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

    case model:execute_with_default_context(?MODULE, create_or_update, [Doc, Diff]) of
        {ok, SpaceId} ->
            ok = space_strategies:add_storage(SpaceId, StorageId, MountInRoot),
            {ok, SpaceId};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage IDs attached to the space.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_ids(model() | doc()) -> [storage:id()].
get_storage_ids(#space_storage{storage_ids = StorageIds}) ->
    StorageIds;
get_storage_ids(#document{value = #space_storage{} = Value}) ->
    get_storage_ids(Value).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage IDs attached to the space that have been mounted in
%% storage root.
%% @end
%%--------------------------------------------------------------------
-spec get_mounted_in_root(model() | doc()) -> [storage:id()].
get_mounted_in_root(#space_storage{mounted_in_root = StorageIds}) ->
    StorageIds;
get_mounted_in_root(#document{value = #space_storage{} = Value}) ->
    get_mounted_in_root(Value).

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
    disable_autocleaning(SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether automatic cleanup is enabled for storage supporting
%% given space.
%% @end
%%--------------------------------------------------------------------
-spec is_cleanup_enabled(od_space:id() | model() | doc()) -> boolean().
is_cleanup_enabled(#document{value = SS = #space_storage{}}) ->
    is_cleanup_enabled(SS);
is_cleanup_enabled(SpaceStorage = #space_storage{}) ->
    SpaceStorage#space_storage.cleanup_enabled;
is_cleanup_enabled(SpaceId) ->
    case space_storage:get(SpaceId) of
        {ok, Doc} ->
            is_cleanup_enabled(Doc#document.value);
        _Error ->
            false
    end.

%%-------------------------------------------------------------------
%% @doc
%% Disables autocleaning.
%% @end
%%-------------------------------------------------------------------
-spec disable_autocleaning(od_space:id()) -> {ok, id()}.
disable_autocleaning(SpaceId) ->
    update_autocleaning(SpaceId, #{enabled => false}).

%%-------------------------------------------------------------------
%% @doc
%% Helper function for changing auto_cleaning settings.
%% @end
%%-------------------------------------------------------------------
-spec update_autocleaning(od_space:id(), maps:map()) -> {ok, od_space:id()}.
update_autocleaning(SpaceId, Settings) ->
    UpdateResult = update(SpaceId, fun(SpaceStorage) ->
        Enabled = maps:get(enabled, Settings, undefined),
        update_autocleaning(SpaceStorage, Enabled, Settings)
    end),
    autocleaning:maybe_start(SpaceId),
    UpdateResult.

%%-------------------------------------------------------------------
%% @doc
%% getter for cleanup_in_progress field
%% @end
%%-------------------------------------------------------------------
-spec get_cleanup_in_progress(model() | doc() | od_space:id()) -> autocleaning:id() | undefined.
get_cleanup_in_progress(#space_storage{cleanup_in_progress = CleanupInProgress}) ->
    CleanupInProgress;
get_cleanup_in_progress(#document{value = SS}) ->
    get_cleanup_in_progress(SS);
get_cleanup_in_progress(SpaceId) ->
    {ok, SpaceStorageDoc} = get(SpaceId),
    get_cleanup_in_progress(SpaceStorageDoc).


%%-------------------------------------------------------------------
%% @doc
%% Returns autocleaning_config of storage supporting given space.
%% @end
%%-------------------------------------------------------------------
-spec get_autocleaning_config(model() | doc() | od_space:id()) -> undefined | autocleaning_config:config().
get_autocleaning_config(#document{value = SS = #space_storage{}}) ->
    get_autocleaning_config(SS);
get_autocleaning_config(SpaceStorage = #space_storage{}) ->
    SpaceStorage#space_storage.autocleaning_config;
get_autocleaning_config(SpaceId) ->
    {ok, Doc} = get(SpaceId),
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
%% Returns autocleaning details.
%% @end
%%-------------------------------------------------------------------
-spec get_autocleaning_details(od_space:id()) -> proplists:proplist().
get_autocleaning_details(SpaceId) -> [
    {enabled, is_cleanup_enabled(SpaceId)},
    {settings, get_autocleaning_settings(SpaceId)}
].

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

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns current autocleaning_settings.
%% @end
%%-------------------------------------------------------------------
-spec get_autocleaning_settings(od_space:id()) -> proplists:proplist() | undefined.
get_autocleaning_settings(SpaceId) ->
    case get_autocleaning_config(SpaceId) of
        undefined ->
            undefined;
        Config ->
        [
            {lowerFileSizeLimit, autocleaning_config:get_lower_size_limit(Config)},
            {upperFileSizeLimit, autocleaning_config:get_upper_size_limit(Config)},
            {maxFileNotOpenedHours, autocleaning_config:get_max_inactive_limit(Config)},
            {threshold, autocleaning_config:get_threshold(Config)},
            {target, autocleaning_config:get_target(Config)}
        ]
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Updates autocleaning_config.
%% @end
%%-------------------------------------------------------------------
-spec update_autocleaning(model(), boolean(), maps:map()) ->
    {ok, model()} | {error, term()}.
update_autocleaning(#space_storage{cleanup_enabled = false}, undefined, _) ->
    {error, autocleaning_disabled};
update_autocleaning(SS = #space_storage{cleanup_enabled = _Enabled}, false, _) ->
    {ok, SS#space_storage{cleanup_enabled = false}};
update_autocleaning(SS = #space_storage{autocleaning_config = OldConfig}, _, Settings) ->
    case SS#space_storage.file_popularity_enabled of
        true ->
            case autocleaning_config:create_or_update(OldConfig, Settings) of
                {error, Reason} ->
                    {error, Reason};
                NewConfig ->
                    {ok, SS#space_storage{
                        cleanup_enabled = true,
                        autocleaning_config = NewConfig
                    }}
            end;
        _ ->
            {error, file_popularity_disabled}
    end.

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