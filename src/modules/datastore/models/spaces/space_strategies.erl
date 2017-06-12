%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing strategies for operations related to storage <-> space sync
%%% process.
%%% @end
%%%-------------------------------------------------------------------
-module(space_strategies).
-author("Rafal Slota").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([new/1, add_storage/2, add_storage/3]).
-export([set_strategy/4, set_strategy/5, update_last_import_time/3,
    get_storage_import_details/2, get_storage_update_details/2]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).
-export([record_struct/1, record_upgrade/2]).

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
    ]};
record_struct(2) ->
    {record, [
        {storage_strategies, #{string => {record, 1, [
            {filename_mapping, {atom, #{atom => term}}},
            {storage_import, {atom, #{atom => term}}},
            {storage_update, {atom, #{atom => term}}},
            {last_import_time, integer}
        ]}}},
        {file_conflict_resolution, {atom, #{atom => term}}},
        {file_caching, {atom, #{atom => term}}},
        {enoent_handling, {atom, #{atom => term}}}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades record from specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_upgrade(datastore_json:record_version(), tuple()) ->
    {datastore_json:record_version(), tuple()}.
record_upgrade(1, R = {?MODEL_NAME, StorageStrategies, _,  _, _}) ->
    NewStorageStrategies = maps:map(fun(_, {storage_strategies,
        {filename_mapping, FilenameMappingStrategy},
        {storage_import, StorageImportStrategy},
        {storage_update, StorageUpdateStrategies},
        {last_import_time, LastImportTime}
    }) ->
        #storage_strategies{
            filename_mapping = FilenameMappingStrategy,
            storage_import = StorageImportStrategy,
            storage_update = hd(StorageUpdateStrategies),
            last_import_time = LastImportTime
        }
    end, StorageStrategies),
    {2, R#space_strategies{storage_strategies = NewStorageStrategies}}.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(Document) ->
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    model:execute_with_default_context(?MODULE, create, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    Config = ?MODEL_CONFIG(space_strategies_bucket, [], ?GLOBALLY_CACHED_LEVEL),
    Config#model_config{version = 2}.

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
    #document{key = SpaceId, value = #space_strategies{}}.

%%--------------------------------------------------------------------
%% @doc
%% @equiv add_storage(SpaceId, StorageId, false).
%% @end
%%--------------------------------------------------------------------
-spec add_storage(od_space:id(), storage:id()) -> ok | no_return().
add_storage(SpaceId, StorageId) ->
    add_storage(SpaceId, StorageId, false).

%%--------------------------------------------------------------------
%% @doc
%% Adds default strategies for new storage in this space.
%% @end
%%--------------------------------------------------------------------
-spec add_storage(od_space:id(), storage:id(), boolean()) -> ok | no_return().
add_storage(SpaceId, StorageId, MountInRoot) ->
    #document{value = Value = #space_strategies{
            storage_strategies = StorageStrategies
        }} = Doc = case get(SpaceId) of
            {error, {not_found, _}} ->
                new(SpaceId);
            {ok, Doc0} ->
                Doc0
        end,
    StorageStrategy = case MountInRoot of
        true -> #storage_strategies{filename_mapping = {root, #{}}};
        _ -> #storage_strategies{}
    end,
    {ok, _} = save(Doc#document{
        value = Value#space_strategies{
            storage_strategies = maps:put(StorageId, StorageStrategy, StorageStrategies)
        }}),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Sets strategy of given type in this space.
%% @end
%%--------------------------------------------------------------------
-spec set_strategy(od_space:id(), space_strategy:type(), space_strategy:name(),
    space_strategy:arguments()) -> {ok, datastore:ext_key()} | datastore:update_error().
set_strategy(SpaceId, StrategyType, StrategyName, StrategyArgs) ->
    update(SpaceId, #{StrategyType => {StrategyName, StrategyArgs}}).

%%--------------------------------------------------------------------
%% @doc
%% Sets strategy of given type for the storage in this space.
%% @end
%%--------------------------------------------------------------------
-spec set_strategy(od_space:id(), storage:id(), space_strategy:type(),
    space_strategy:name(), space_strategy:arguments()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
set_strategy(SpaceId, StorageId, StrategyType, StrategyName, StrategyArgs) ->
    update(SpaceId, fun(#space_strategies{storage_strategies = Strategies} = OldValue) ->
        OldSS = #storage_strategies{} = maps:get(StorageId, Strategies, #storage_strategies{}),

        NewSS = case StrategyType of
            filename_mapping ->
                OldSS#storage_strategies{filename_mapping = {StrategyName, StrategyArgs}};
            storage_import ->
                OldSS#storage_strategies{storage_import = {StrategyName, StrategyArgs}};
            storage_update ->
                OldSS#storage_strategies{storage_update = {StrategyName, StrategyArgs}}
        end,

        {ok, OldValue#space_strategies{storage_strategies = maps:put(StorageId, NewSS, Strategies)}}
    end).


%%--------------------------------------------------------------------
%% @doc
%% Returns current configuration of storage_ import.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_import_details(od_space:id(), storage:id()) -> space_strategy:config().
get_storage_import_details(SpaceId, StorageId) ->
    {ok, Doc} = get(SpaceId),
    get_storage_strategy_config(Doc, storage_import, StorageId).

%%--------------------------------------------------------------------
%% @doc
%% Returns current configuration of storage_update.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_update_details(od_space:id(), storage:id()) -> space_strategy:config().
get_storage_update_details(SpaceId, StorageId) ->
    {ok, Doc} = get(SpaceId),
    get_storage_strategy_config(Doc, storage_update, StorageId).


%%--------------------------------------------------------------------
%% @doc
%% Sets last_import_time to new value.
%% @end
%%--------------------------------------------------------------------
-spec update_last_import_time(od_space:id(), storage:id(), integer() | undefined) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update_last_import_time(SpaceId, StorageId, NewLastImportTime) ->
    update(SpaceId, fun(#space_strategies{storage_strategies = Strategies} = OldValue) ->
        OldSS = maps:get(StorageId, Strategies),
        NewSS = OldSS#storage_strategies{last_import_time = NewLastImportTime},
        {ok, OldValue#space_strategies{storage_strategies = maps:put(StorageId, NewSS, Strategies)}}
    end).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Returns space_strategy:config() for given StrategyType.
%%% @end
%%%-------------------------------------------------------------------
-spec get_storage_strategy_config(#space_strategies{}, space_strategy:type(),
    storage:id()) -> space_strategy:config().
get_storage_strategy_config(#space_strategies{
    storage_strategies = Strategies
}, storage_import, StorageId
) ->
    #storage_strategies{storage_import = Import} = maps:get(StorageId, Strategies),
    Import;
get_storage_strategy_config(#space_strategies{
    storage_strategies = Strategies
}, storage_update, StorageId
) ->
    #storage_strategies{storage_update = Update} = maps:get(StorageId, Strategies),
    Update;
get_storage_strategy_config(#document{value=Value}, StrategyType, StorageId) ->
    get_storage_strategy_config(Value, StrategyType, StorageId).
