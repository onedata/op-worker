%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @todo: write me!
%%% @end
%%%-------------------------------------------------------------------
-module(space_strategies).
-author("Rafal Slota").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").


-define(DEFAULT_FILENAME_MAPPING_STRATEGY, {simple, #{}}).
-define(DEFAULT_STORAGE_IMPORT_STRATEGY, {bfs_scan, #{scan_interval => 10}}).
-define(DEFAULT_STORAGE_UPDATE_STRATEGIES, [{bfs_scan, #{scan_interval => 60}}]).
-define(DEFAULT_FILE_CONFLICT_RESOLUTION_STRATEGY, {ignore_conflicts, #{}}).
-define(DEFAULT_FILE_CACHING_STRATEGY, {no_cache, #{}}).
-define(DEFAULT_ENOENT_HANDLING_STRATEGY, {check_globally, #{}}).

%% API
-export([new/1, add_storage/2]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).
-export([record_struct/1]).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {storage_strategies, #{string => {record, 1, [
            {filename_mapping, {atom, #{atom => term}}},
            {storage_import, {atom, #{atom => term}}},
            {storage_update, [ {atom, #{atom => term}} ]}, %% List of strategies
            {last_import_time, integer}
        ]}}},
        {file_conflict_resolution, {atom, #{atom => term}}},
        {file_caching, {atom, #{atom => term}}},
        {enoent_handling, {atom, #{atom => term}}}
    ]}.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:ext_key()} | datastore:generic_error().
save(Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(space_strategies_bucket, [], ?GLOBALLY_CACHED_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns datastore document for space-strategies mapping.
%% @end
%%--------------------------------------------------------------------
-spec new(od_space:id()) -> Doc :: #document{}.
new(SpaceId) ->
    #document{key = SpaceId, value = #space_strategies{
        file_conflict_resolution = ?DEFAULT_FILE_CONFLICT_RESOLUTION_STRATEGY,
        file_caching = ?DEFAULT_FILE_CACHING_STRATEGY,
        enoent_handling = ?DEFAULT_ENOENT_HANDLING_STRATEGY
    }}.

add_storage(SpaceId, StorageId) ->
    #document{value = Value = #space_strategies{storage_strategies = StorageStrategies}} = Doc =
        case get(SpaceId) of
            {error, {not_found, _}} ->
                new(SpaceId);
            {ok, Doc0} ->
                Doc0
        end,

    StorageStrategie = #storage_strategies{
        filename_mapping = ?DEFAULT_FILENAME_MAPPING_STRATEGY,
        storage_import = ?DEFAULT_STORAGE_IMPORT_STRATEGY,
        storage_update = ?DEFAULT_STORAGE_UPDATE_STRATEGIES
    },
    {ok, _} = save(Doc#document{value = Value#space_strategies{storage_strategies = maps:put(StorageId, StorageStrategie, StorageStrategies)}}),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

