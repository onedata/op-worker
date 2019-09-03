%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc The file_qos item contains aggregated information about QoS defined
%%% for file or directory. It contains:
%%%     - qos_list - holds IDs of all qos_entries defined for this file,
%%%     - target_storages - holds mapping storage_id to list of qos_entry IDs.
%%%       When new QoS is added for file or directory, storages on which replicas
%%%       should be stored are calculated using QoS expression. Then this mapping
%%%       is appropriately updated with the calculated storages.
%%% Each file or directory can be associated with at most one such document.
%%% It is created/updated when new qos_entry document is created for file or
%%% directory. In this case target storages are chosen according to QoS expression
%%% and number of required replicas defined in qos_entry document.
%%% Then file_qos document is updated - qos_entry ID is added to QoS list and
%%% target storages mapping.
%%% According to this getting full information about QoS defined for file or
%%% directory requires calculating effective file_qos as file_qos document
%%% is not created for each file separately.
%%%
%%% NOTE!!!
%%% If you introduce any changes in this module, please ensure that
%%% docs in qos.hrl are up to date.
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
-export([
    get_effective/1, get_effective/2,
    remove_qos_id/2, add_qos/4,
    is_replica_protected/2, get_qos_to_update/2,
    get_qos_list/1, get_target_storages/1
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

%% test API
-export([get/1]).


-type key() :: file_meta:uuid().
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

-spec update(key(), diff()) -> {ok, key()} | {error, term()}.
update(Key, Diff) ->
    datastore_model:update(?CTX, Key, Diff).


-spec create_or_update(doc(), diff()) ->
    {ok, key()} | {error, term()}.
create_or_update(#document{key = Key} = Doc, Diff) ->
    datastore_model:update(?CTX, Key, Diff, Doc).


-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).


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

-spec get_effective(file_meta:doc() | file_meta:uuid()) -> record() | undefined.
get_effective(FileUuidOrDoc) ->
    get_effective(FileUuidOrDoc, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Returns effective file_qos for file. If effective value cannot be calculated
%% because of file_meta documents not being synchronized yet registers
%% DelayedHook for this file to be run when document is finally synchronized.
%% @end
%%--------------------------------------------------------------------
-spec get_effective(file_meta:doc() | file_meta:uuid(), delayed_hooks:hook() | undefined) ->
    record() | undefined.
get_effective(FileMeta = #document{value = #file_meta{}, scope = SpaceId}, DelayedHook) ->
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
    case effective_value:get_or_calculate(CacheTableName, FileMeta, Callback, [], [], DelayedHook) of
        {ok, EffQos, _} -> EffQos;
        _ -> undefined % documents are not synchronized yet
    end;

get_effective(FileUuid, DelayedHook) ->
    case file_meta:get(FileUuid) of
        {ok, FileMeta} ->
            get_effective(FileMeta);
        _ ->
            case DelayedHook of
                undefined -> ok;
                _ -> delayed_hooks:add_hook(FileUuid, DelayedHook)
            end,
            undefined
    end.


%%--------------------------------------------------------------------
%% @doc
%% Removes given Qos ID from both qos_list and target_storages in file_qos doc.
%% @end
%%--------------------------------------------------------------------
-spec remove_qos_id(file_meta:uuid(), qos_entry:id()) -> ok | {error, term()}.
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

    case update(FileUuid, Diff) of
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Calls file_qos:create_or_update/2 fun. Document and diff function are
%% created using QoS ID and list of target storages for that QoS.
%% @end
%%--------------------------------------------------------------------
-spec add_qos(file_meta:uuid(), od_space:id(),
    qos_entry:id(), [storage:id()]) -> ok.
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
        {ok, #file_qos{qos_list = lists:usort([QosId | CurrFileQos]), target_storages = UpdatedTS}}
    end,

    case create_or_update(NewDoc, Diff) of
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks whether given file is protected on given storage by QoS.
%% @end
%%--------------------------------------------------------------------
-spec is_replica_protected(file_meta:uuid(), storage:id()) -> boolean().
is_replica_protected(FileUuid, StorageId) ->
    QosStorages = case file_qos:get_effective(FileUuid) of
        undefined -> #{};
        #file_qos{target_storages = TS} -> TS
    end,
    maps:is_key(StorageId, QosStorages).


-spec get_qos_list(record()) -> qos_list().
get_qos_list(FileQos) ->
    FileQos#file_qos.qos_list.

-spec get_target_storages(record()) -> target_storages().
get_target_storages(FileQos) ->
    FileQos#file_qos.target_storages.

-spec get_qos_to_update(storage:id(), record()) -> [qos_entry:id()].
get_qos_to_update(StorageId, EffectiveFileQos) ->
    maps:get(StorageId, EffectiveFileQos#file_qos.target_storages, []).


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
    MergedQosList = lists:usort(ParentQosList ++ ChildQosList),
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
        maps:update_with(StorageId,
            fun(ExistingQosList) ->
                ExistingQosList ++ ChildQosList
            end, ChildQosList, Acc)
    end, ParentStoragesWithoutCommonQos, ChildStorages).


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

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {file_qos, [string]},
        {target_storages, #{string => [string]}}
    ]}.
