%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model that holds information about qos defined for given file.
%%% It contains two fields:
%%%     - qos_list - holds IDs of all qos_entrys defined for this file,
%%%     - target_storages - holds mapping storage_id to list of all qos_entry IDs that
%%%       requires file replica on this storage.
%%% file_qos is updated in two cases:
%%%     1. When new qos is defined for file or directory. In this case,
%%%        qos_entry ID is added to qos_list and to target storage mapping for
%%%        each storage that should store file replica.
%%%     2. When target_storages for file/directory differs from the target_storages
%%%        calculated for parent directory.
%%% @end
%%%-------------------------------------------------------------------
-module(file_qos).
-author("Michal Cwiertnia").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include("modules/datastore/qos.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% functions operating on record using datastore model API
-export([create_or_update/2, delete/1]).

%% higher-level functions operating on file_qos record.
-export([get_effective/1, remove_qos_id/2,
    add_qos/4
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

%% test API
-export([get/1]).


-type key() :: datastore:key().
-type record() :: #file_qos{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type qos_list() :: [qos_entry:id()].
-type target_storages() :: #{storage:id() => qos_list()}.

-export_type([qos_list/0, target_storages/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    mutator => oneprovider:get_id_or_undefined()
}).


%%%===================================================================
%%% Functions operating on record using datastore_model API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Updates file_qos document.
%% @end
%%--------------------------------------------------------------------
-spec update(key(), diff()) -> {ok, key()} | {error, term()}.
update(Key, Diff) ->
    datastore_model:update(?CTX, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% Updates file_qos document. If document with specified Key does not exist,
%% new file_qos document is created.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(doc(), diff()) ->
    {ok, key()} | {error, term()}.
create_or_update(#document{key = Key} = Doc, Diff) ->
    datastore_model:update(?CTX, Key, Diff, Doc).

%%--------------------------------------------------------------------
%% @doc
%% Returns file_qos doc for file.
%% @end
%%--------------------------------------------------------------------
-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes file_qos document.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    case datastore_model:delete(?CTX, Key) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, _} = Error -> Error
    end.


%%%===================================================================
%%% Higher-level functions operating on file_qos record.
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns effective file_qos for file.
%% @end
%%--------------------------------------------------------------------
-spec get_effective(file_meta:doc() | file_meta:uuid()) -> record() | undefined.
get_effective(FileMeta = #document{value = #file_meta{}, scope = SpaceId}) ->
    Callback = fun([#document{key = Uuid}, ParentEffQos, CalculationInfo]) ->
        case {file_qos:get(Uuid), ParentEffQos} of
            {{error, not_found}, _} ->
                {ok, ParentEffQos, CalculationInfo};
            {{ok, #document{value = FileQos}}, undefined} ->
                {ok, FileQos, CalculationInfo};
            {{ok, #document{value = FileQos}}, _} ->
                EffQos = merge_file_qos(ParentEffQos, FileQos),
                {ok, EffQos, CalculationInfo}
        end
    end,

    CacheTableName = ?CACHE_TABLE_NAME(SpaceId),
    {ok, EffQos, _} = effective_value:get_or_calculate(CacheTableName, FileMeta, Callback, [], []),
    EffQos;
get_effective(FileUuid) ->
    {ok, FileMeta} = file_meta:get(FileUuid),
    get_effective(FileMeta).

%%--------------------------------------------------------------------
%% @doc
%% Removes QosId from file_qos document.
%% @end
%%--------------------------------------------------------------------
-spec remove_qos_id(file_meta:uuid(), qos_entry:id()) -> ok.
remove_qos_id(FileUuid, QosId) ->
    Diff = fun(FileQos = #file_qos{qos_list = QosList, target_storages = TS}) ->
        UpdatedQosList = lists:delete(QosId, QosList),
        UpdatedTS = maps:fold(fun(StorageId, QosList, UpdatedTSPartial) ->
            case lists:delete(QosId, QosList) of
                [] ->
                    UpdatedTSPartial;
                List ->
                    UpdatedTSPartial#{StorageId => List}
            end
        end, #{}, TS),
        {ok, FileQos#file_qos{qos_list = UpdatedQosList, target_storages = UpdatedTS}}
    end,

    {ok, _} = update(FileUuid, Diff),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Calls file_qos:create_or_update/2 fun. Document and diff function are
%% created using QoS ID and list of target storages for that QoS.
%% @end
%%--------------------------------------------------------------------
-spec add_qos(file_meta:uuid(), od_space:id(), qos_entry:id(), target_storages()) -> ok.
add_qos(FileUuid, SpaceId, QosId, TargetStoragesList) ->
    NewTargetStorages = merge_storage_list_to_target_storages(
        QosId, TargetStoragesList, #{}
    ),
    NewDoc = #document{
        key = FileUuid,
        scope = SpaceId,
        value = #file_qos{
            qos_list = [QosId],
            target_storages = NewTargetStorages
        }
    },

    Diff = fun(#file_qos{qos_list = CurrFileQos, target_storages = CurrTS}) ->
        UpdatedTS = merge_storage_list_to_target_storages(
            QosId, TargetStoragesList, CurrTS
        ),
        {ok, #file_qos{qos_list = [QosId | CurrFileQos], target_storages = UpdatedTS}}
    end,
    {ok, _} = create_or_update(NewDoc, Diff),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Merge parent's file_qos with child's file_qos. Used when calculating
%% effective file_qos.
%% @end
%%--------------------------------------------------------------------
-spec merge_file_qos(record(), record()) -> record().
merge_file_qos(#file_qos{qos_list = ParentQosList, target_storages = ParentStorages},
    #file_qos{qos_list = ChildQosList, target_storages = ChildStorages}) ->
    MergedQosList = remove_duplicates_from_list(ParentQosList ++ ChildQosList),
    MergedStorages = merge_target_storages(ParentStorages, ChildStorages),
    #file_qos{qos_list = MergedQosList, target_storages = MergedStorages}.

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Merges parent's target_storages with child's target_storages. If qos_entry
%% ID is present in both parent's and child's target storages, only that from
%% child's document should be preserved.
%% @end
%%--------------------------------------------------------------------
-spec merge_target_storages(target_storages(), target_storages()) -> target_storages().
merge_target_storages(ParentStorages, ChildStorages) when map_size(ChildStorages) == 0 ->
    ParentStorages;
merge_target_storages(ParentStorages, ChildStorages) ->
    ChildQosEntrySet = sets:from_list(lists:flatten(maps:values(ChildStorages))),

    ParentStoragesWithoutCommonQos = maps:fold(fun(StorageId, QosList, Acc) ->
        NewQosList = sets:to_list(sets:subtract(sets:from_list(QosList), ChildQosEntrySet)),
        case NewQosList of
            [] -> Acc;
            _ -> Acc#{StorageId => NewQosList}
        end
    end, #{}, ParentStorages),

    maps:fold(fun(StorageId, ChildQosList, Acc) ->
        maps:update_with(
            StorageId, fun(ExistingQosList) -> ExistingQosList ++ ChildQosList end,
            ChildQosList, Acc
        )
    end, ParentStoragesWithoutCommonQos, ChildStorages).

-spec remove_duplicates_from_list([term()]) -> [term()].
remove_duplicates_from_list(List) ->
    sets:to_list(sets:from_list(List)).

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Returns target storages map updated with QoS ID according to
%% list of storages on which file should be replicated according to this QoS.
%% @end
%%--------------------------------------------------------------------
-spec merge_storage_list_to_target_storages(qos_entry:id(), [storage:id()] | undefined,
    target_storages()) -> target_storages().
merge_storage_list_to_target_storages(_QosId, undefined, TargetStorages) ->
    TargetStorages;
merge_storage_list_to_target_storages(QosId, StorageList, TargetStorages) ->
    lists:foldl(fun(StorageId, TargetStoragesForFile) ->
        QosEntryList = maps:get(StorageId, TargetStoragesForFile, []),
        TargetStoragesForFile#{StorageId => [QosId | QosEntryList]}
    end, TargetStorages, StorageList).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {file_qos, [string]},
        {target_storages, #{string => [string]}}
    ]}.
