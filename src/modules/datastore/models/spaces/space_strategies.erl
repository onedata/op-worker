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

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([new/1, add_storage/2]).
-export([set_strategy/4, set_strategy/5,
    get_storage_import_details/2, get_storage_update_details/2,
    update_import_start_time/3, update_import_finish_time/3,
    get_import_finish_time/2, get_import_start_time/2,
    update_last_update_start_time/3, update_last_update_finish_time/3,
    get_last_update_finish_time/2, get_last_update_start_time/2, is_import_on/1]).
-export([save/1, get/1, exists/1, delete/1, update/2, create/1]).

%% datastore_model callbacks
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type key() :: datastore:key().
-type record() :: #space_strategies{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves space strategies.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, key()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates space strategies.
%% @end
%%--------------------------------------------------------------------
-spec update(key(), diff()) -> {ok, key()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Creates space strategies.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, key()} | {error, term()}.
create(Doc) ->
    ?extract_key(datastore_model:create(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Returns space strategies.
%% @end
%%--------------------------------------------------------------------
-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes space strategies.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    storage_sync_monitoring:ensure_all_metrics_stopped(Key),
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether space strategies exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(key()) -> boolean().
exists(Key) ->
    {ok, Exists} = datastore_model:exists(?CTX, Key),
    Exists.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks if any storage is imported for a space.
%% @end
%%--------------------------------------------------------------------
-spec is_import_on(od_space:id()) -> boolean().
is_import_on(SpaceId) ->
    {ok, Doc} = space_storage:get(SpaceId),
    StorageIds = space_storage:get_storage_ids(Doc),
    lists:foldl(fun
        (_StorageId, true) ->
            true;
        (StorageId, _) ->
            case get_storage_import_details(SpaceId, StorageId) of
                {no_import, _} -> false;
                _ -> true
            end
    end, false, StorageIds).

%%--------------------------------------------------------------------
%% @doc
%% Returns datastore document for space-strategies mapping.
%% @end
%%--------------------------------------------------------------------
-spec new(od_space:id()) -> Doc :: #document{}.
new(SpaceId) ->
    #document{key = SpaceId, value = #space_strategies{}}.

%%--------------------------------------------------------------------
%% @doc
%% Adds default strategies for new storage in this space.
%% @end
%%--------------------------------------------------------------------
-spec add_storage(od_space:id(), storage:id()) -> ok | no_return().
add_storage(SpaceId, StorageId) ->
    #document{value = Value = #space_strategies{
        storage_strategies = StorageStrategies
    }} = Doc = case space_strategies:get(SpaceId) of
        {error, not_found} ->
            new(SpaceId);
        {ok, Doc0} ->
            Doc0
    end,
    {ok, _} = save(Doc#document{
        value = Value#space_strategies{
            storage_strategies = StorageStrategies#{StorageId => #storage_strategies{}}
    }}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Sets strategy of given type in this space.
%% @end
%%--------------------------------------------------------------------
-spec set_strategy(od_space:id(), space_strategy:type(), space_strategy:name(),
    space_strategy:arguments()) -> {ok, key()} | {error, term()}.
set_strategy(SpaceId, _StrategyType, StrategyName, StrategyArgs) ->
    update(SpaceId, fun(SpaceStrategy = #space_strategies{}) ->
        {ok, SpaceStrategy#space_strategies{
            enoent_handling = {StrategyName, StrategyArgs}
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Sets strategy of given type for the storage in this space.
%% @end
%%--------------------------------------------------------------------
-spec set_strategy(od_space:id(), storage:id(), space_strategy:type(),
    space_strategy:name(), space_strategy:arguments()) ->
    {ok, key()} | {error, term()}.
set_strategy(SpaceId, StorageId, StrategyType, StrategyName, StrategyArgs) ->
    update(SpaceId, fun(#space_strategies{storage_strategies = Strategies} = OldValue) ->
        OldSS = #storage_strategies{} = maps:get(StorageId, Strategies, #storage_strategies{}),

        NewSS = case StrategyType of
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
    {ok, Doc} = space_strategies:get(SpaceId),
    get_storage_strategy_config(Doc, storage_import, StorageId).

%%--------------------------------------------------------------------
%% @doc
%% Returns current configuration of storage_update.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_update_details(od_space:id(), storage:id()) -> space_strategy:config().
get_storage_update_details(SpaceId, StorageId) ->
    {ok, Doc} = space_strategies:get(SpaceId),
    get_storage_strategy_config(Doc, storage_update, StorageId).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of import_finish_time for given SpaceId and StorageId.
%% @end
%%-------------------------------------------------------------------
-spec get_import_finish_time(od_space:id() | maps:map(), storage:id()) -> space_strategy:timestamp().
get_import_finish_time(StorageStrategies, StorageId) when is_map(StorageStrategies) ->
    storage_strategies:get_import_finish_time(StorageId, StorageStrategies);
get_import_finish_time(SpaceId, StorageId) ->
    {ok, #document{
        value = #space_strategies{
            storage_strategies = Strategies
        }}} = space_strategies:get(SpaceId),
    storage_strategies:get_import_finish_time(StorageId, Strategies).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of import_start_time for given SpaceId and StorageId.
%% @end
%%-------------------------------------------------------------------
-spec get_import_start_time(od_space:id() | maps:map(), storage:id()) -> space_strategy:timestamp().
get_import_start_time(StorageStrategies, StorageId) when is_map(StorageStrategies) ->
    storage_strategies:get_import_start_time(StorageId, StorageStrategies);
get_import_start_time(SpaceId, StorageId) ->
    {ok, #document{
        value = #space_strategies{
            storage_strategies = Strategies
        }}} = space_strategies:get(SpaceId),
    storage_strategies:get_import_start_time(StorageId, Strategies).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of last_update_start_time for given SpaceId and StorageId.
%% @end
%%-------------------------------------------------------------------
-spec get_last_update_start_time(od_space:id() | maps:map(), storage:id()) -> space_strategy:timestamp().
get_last_update_start_time(StorageStrategies, StorageId) when is_map(StorageStrategies) ->
    storage_strategies:get_last_update_start_time(StorageId, StorageStrategies);
get_last_update_start_time(SpaceId, StorageId) ->
    {ok, #document{
        value = #space_strategies{
            storage_strategies = Strategies
        }}} = space_strategies:get(SpaceId),
    storage_strategies:get_last_update_start_time(StorageId, Strategies).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of last_update_finish_time for given SpaceId and StorageId.
%% @end
%%-------------------------------------------------------------------
-spec get_last_update_finish_time(od_space:id() | maps:map(), storage:id()) -> space_strategy:timestamp().
get_last_update_finish_time(StorageStrategies, StorageId) when is_map(StorageStrategies) ->
    storage_strategies:get_last_update_finish_time(StorageId, StorageStrategies);
get_last_update_finish_time(SpaceId, StorageId) ->
    {ok, #document{
        value = #space_strategies{
            storage_strategies = Strategies
        }}} = space_strategies:get(SpaceId),
    storage_strategies:get_last_update_finish_time(StorageId, Strategies).

%%--------------------------------------------------------------------
%% @doc
%% Sets import_finish_time to new value.
%% @end
%%--------------------------------------------------------------------
-spec update_import_finish_time(od_space:id(), storage:id(), integer() | undefined) ->
    {ok, key()} | {error, term()}.
update_import_finish_time(SpaceId, StorageId, NewImportFinishTime) ->
    update(SpaceId, fun(#space_strategies{
        storage_strategies = Strategies} = OldValue
    ) ->
        NewStrategies = storage_strategies:update_import_finish_time(StorageId,
            NewImportFinishTime, Strategies),
        {ok, OldValue#space_strategies{storage_strategies = NewStrategies}}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Sets import_start_time to new value.
%% @end
%%--------------------------------------------------------------------
-spec update_import_start_time(od_space:id(), storage:id(), integer() | undefined) ->
    {ok, key()} | {error, term()}.
update_import_start_time(SpaceId, StorageId, NewStartImportTime) ->
    {ok, _} = update(SpaceId, fun(#space_strategies{
        storage_strategies = Strategies} = OldValue
    ) ->
        NewStrategies = storage_strategies:update_import_start_time(
            StorageId, NewStartImportTime, Strategies),
        {ok, OldValue#space_strategies{storage_strategies = NewStrategies}}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Sets update_start_time to new value.
%% @end
%%--------------------------------------------------------------------
-spec update_last_update_start_time(od_space:id(), storage:id(), integer() | undefined) ->
    {ok, key()} | {error, term()}.
update_last_update_start_time(SpaceId, StorageId, NewStartUpdateTime) ->
    {ok, _} = update(SpaceId, fun(#space_strategies{
        storage_strategies = Strategies} = OldValue
    ) ->
        NewStrategies = storage_strategies:update_last_update_start_time(
            StorageId, NewStartUpdateTime, Strategies),
        {ok, OldValue#space_strategies{storage_strategies = NewStrategies}}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Sets update_start_time to new value.
%% @end
%%--------------------------------------------------------------------
-spec update_last_update_finish_time(od_space:id(), storage:id(), integer() | undefined) ->
    {ok, key()} | {error, term()}.
update_last_update_finish_time(SpaceId, StorageId, NewStartUpdateTime) ->
    {ok, _} = update(SpaceId, fun(#space_strategies{
        storage_strategies = Strategies} = OldValue
    ) ->
        NewStrategies = storage_strategies:update_last_update_finish_time(
            StorageId, NewStartUpdateTime, Strategies),
        {ok, OldValue#space_strategies{storage_strategies = NewStrategies}}
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
-spec get_storage_strategy_config(#space_strategies{} | doc(),
    space_strategy:type(), storage:id()) -> space_strategy:config().
get_storage_strategy_config(#space_strategies{
    storage_strategies = Strategies
}, storage_import, StorageId
) ->
    #storage_strategies{storage_import = Import} =
        maps:get(StorageId, Strategies, #storage_strategies{}),
    Import;
get_storage_strategy_config(#space_strategies{
    storage_strategies = Strategies
}, storage_update, StorageId
) ->
    #storage_strategies{storage_update = Update} =
        maps:get(StorageId, Strategies, #storage_strategies{}),
    Update;
get_storage_strategy_config(#document{value = Value}, StrategyType, StorageId) ->
    get_storage_strategy_config(Value, StrategyType, StorageId).

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
        {storage_strategies, #{string => {record, [
            {filename_mapping, {atom, #{atom => term}}},
            {storage_import, {atom, #{atom => term}}},
            {storage_update, [{atom, #{atom => term}}]}, %% List of strategies
            {last_import_time, integer}
        ]}}},
        {file_conflict_resolution, {atom, #{atom => term}}},
        {file_caching, {atom, #{atom => term}}},
        {enoent_handling, {atom, #{atom => term}}}
    ]};
get_record_struct(2) ->
    {record, [
        {storage_strategies, #{string => {record, [
            {filename_mapping, {atom, #{atom => term}}},
            {storage_import, {atom, #{atom => term}}},
            {storage_update, {atom, #{atom => term}}},
            {last_import_time, integer}
        ]}}},
        {file_conflict_resolution, {atom, #{atom => term}}},
        {file_caching, {atom, #{atom => term}}},
        {enoent_handling, {atom, #{atom => term}}}
    ]};
get_record_struct(3) ->
    {record, [
        {storage_strategies, #{string => {record, [
            {filename_mapping, {atom, #{atom => term}}},
            {storage_import, {atom, #{atom => term}}},
            {storage_update, {atom, #{atom => term}}},
            {import_start_time, integer},
            {import_finish_time, integer},
            {last_update_start_time, integer},
            {last_update_finish_time, integer}
        ]}}},
        {file_conflict_resolution, {atom, #{atom => term}}},
        {file_caching, {atom, #{atom => term}}},
        {enoent_handling, {atom, #{atom => term}}}
    ]};
get_record_struct(4) ->
    {record, [
        {storage_strategies, #{string => {record, [
            {storage_import, {atom, #{atom => term}}},
            {storage_update, {atom, #{atom => term}}},
            {import_start_time, integer},
            {import_finish_time, integer},
            {last_update_start_time, integer},
            {last_update_finish_time, integer}
        ]}}},
        {file_conflict_resolution, {atom, #{atom => term}}},
        {file_caching, {atom, #{atom => term}}},
        {enoent_handling, {atom, #{atom => term}}}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, R = {?MODULE, StorageStrategies, _, _, _}) ->
    NewStorageStrategies = maps:map(fun(_, {storage_strategies,
        {filename_mapping, FilenameMappingStrategy},
        {storage_import, StorageImportStrategy},
        {storage_update, StorageUpdateStrategies},
        {last_import_time, LastImportTime}
    }) ->
        {storage_strategies,
            {filename_mapping, FilenameMappingStrategy},
            {storage_import, StorageImportStrategy},
            {storage_update, hd(StorageUpdateStrategies)},
            {last_import_time, LastImportTime}
        }
    end, StorageStrategies),
    {2, R#space_strategies{storage_strategies = NewStorageStrategies}};
upgrade_record(2, R = {?MODULE, StorageStrategies, _, _, _}) ->
    NewStorageStrategies = maps:map(fun(_, {storage_strategies,
        {filename_mapping, FilenameMappingStrategy},
        {storage_import, StorageImportStrategy},
        {storage_update, StorageUpdateStrategy},
        {last_import_time, LastImportTime}
    }) ->
        {storage_strategies,
            FilenameMappingStrategy,
            StorageImportStrategy,
            StorageUpdateStrategy,
            undefined,
            LastImportTime,
            undefined,
            undefined
        }
    end, StorageStrategies),
    {3, R#space_strategies{storage_strategies = NewStorageStrategies}};
upgrade_record(3, R = {?MODULE, StorageStrategies, _, _, _}) ->
    NewStorageStrategies = maps:map(fun(_, {storage_strategies,
        {filename_mapping, _FilenameMappingStrategy},
        {storage_import, StorageImportStrategy},
        {storage_update, StorageUpdateStrategy},
        {import_start_time, ImportStartTime},
        {import_finish_time, ImportFinishTime},
        {last_update_start_time, LastUpdateStartTime},
        {last_update_finish_time, LastImportFinishTime}
    }) ->
        {storage_strategies,
            StorageImportStrategy,
            StorageUpdateStrategy,
            ImportStartTime,
            ImportFinishTime,
            LastUpdateStartTime,
            LastImportFinishTime
        }
    end, StorageStrategies),
    {4, R#space_strategies{storage_strategies = NewStorageStrategies}}.
