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

%% API
-export([update/2, create_or_update/2, get/1, delete/1]).

-export([
    get_effective/1, add_to_target_storages/3, remove_from_target_storages/2,
    merge_storage_list_to_target_storages/3, remove_qos_id/2
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

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
%%% API
%%%===================================================================

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
get(FileGuid) ->
    datastore_model:get(?CTX, FileGuid).

%%%===================================================================
%%% Higher-level functions operating on file_qos record.
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns effective file_qos for file.
%% @end
%%--------------------------------------------------------------------
-spec get_effective(file_meta:doc()) -> record() | undefined.
get_effective(FileMeta = #document{value = #file_meta{}, scope = SpaceId}) ->
    Callback = fun([#document{key = Uuid, scope = SpaceId}, ParentEffQos, CalculationInfo]) ->
        Guid = file_id:pack_guid(Uuid, SpaceId),
        case {file_qos:get(Guid), ParentEffQos} of
            {{error, not_found}, _} ->
                {ok, ParentEffQos, CalculationInfo};
            {{ok, #document{value = FileQos}}, undefined} ->
                {ok, FileQos, CalculationInfo};
            {{ok, #document{value = FileQos}}, _} ->
                EffQos = merge_file_qos(ParentEffQos, FileQos),
                {ok, EffQos, CalculationInfo}
        end
    end,

    CacheTableName = binary_to_atom(SpaceId, utf8),
    {ok, EffQos, _} = effective_value:get_or_calculate(CacheTableName, FileMeta, Callback, [], []),
    EffQos;
get_effective(FileGuid) ->
    FileUuid = file_id:guid_to_uuid(FileGuid),
    {ok, FileMeta} = file_meta:get(FileUuid),
    get_effective(FileMeta).

%%--------------------------------------------------------------------
%% @doc
%% Deletes file_qos document.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(FileGuid) ->
    case datastore_model:delete(?CTX, FileGuid) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, _} = Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Adds storages from given list to target storages in file_qos document.
%% @end
%%--------------------------------------------------------------------
-spec add_to_target_storages(fslogic_worker:file_guid(), [storage:id()],
    qos_entry:id()) -> {ok, key()} | {error, term}.
add_to_target_storages(FileGuid, StoragesList, QosId) ->
    Diff = fun(FileQos = #file_qos{target_storages = TS}) ->
        UpdatedTS = merge_storage_list_to_target_storages(QosId, StoragesList, TS),
        {ok, FileQos#file_qos{target_storages = UpdatedTS}}
    end,

    NewTargetStorages = lists:foldl(fun (StorageId, TargetStorages) ->
        TargetStorages#{StorageId => [QosId]}
    end, #{}, StoragesList),
    DocToCreate = #document{
        key = FileGuid,
        scope = file_id:guid_to_space_id(FileGuid),
        value = #file_qos{qos_list = [QosId], target_storages = NewTargetStorages}
    },

    create_or_update(DocToCreate, Diff).

%%--------------------------------------------------------------------
%% @doc
%% Removes QosId from target storages in file_qos document.
%% @end
%%--------------------------------------------------------------------
-spec remove_from_target_storages(fslogic_worker:file_guid(), qos_entry:id()) ->
    {ok, key()} | {error, term}.
remove_from_target_storages(FileGuid, QosId) ->
    Diff = fun(FileQos = #file_qos{target_storages = TS}) ->
        UpdatedTS = lists:foldl(
            fun (StorageId, PartialTargetStorages) ->
                QosList = maps:get(StorageId, PartialTargetStorages, []),
                case lists:delete(QosId, QosList) of
                    [] ->
                        maps:remove(StorageId, PartialTargetStorages);
                    List ->
                        PartialTargetStorages#{StorageId => List}
                end
            end, TS, maps:keys(TS)),
        {ok, FileQos#file_qos{target_storages = UpdatedTS}}
    end,

    update(FileGuid, Diff).

%%--------------------------------------------------------------------
%% @doc
%% Removes QosId from file_qos document.
%% @end
%%--------------------------------------------------------------------
-spec remove_qos_id(fslogic_worker:file_guid(), qos_entry:id()) ->
    {ok, key()} | {error, term}.
remove_qos_id(FileGuid, QosId) ->
    Diff = fun(FileQos = #file_qos{qos_list = QosList, target_storages = TS}) ->
        UpdatedQosList = lists:delete(QosId, QosList),
        UpdatedTS = lists:foldl(
            fun (StorageId, PartialTargetStorages) ->
                QosList = maps:get(StorageId, PartialTargetStorages, []),
                case lists:delete(QosId, QosList) of
                    [] ->
                        maps:remove(StorageId, PartialTargetStorages);
                    List ->
                        PartialTargetStorages#{StorageId => List}
                end
            end, TS, maps:keys(TS)),
        {ok, FileQos#file_qos{qos_list = UpdatedQosList, target_storages = UpdatedTS}}
    end,

    update(FileGuid, Diff).

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
-spec merge_file_qos(record(), target_storages()) -> record().
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
-spec merge_target_storages(target_storages(), target_storages() | undefined) -> target_storages().
merge_target_storages(ParentStorages, undefined) ->
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
        maps:update_with(StorageId, fun(ExistingQosList) -> ExistingQosList ++ ChildQosList end, ChildQosList, Acc)
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
