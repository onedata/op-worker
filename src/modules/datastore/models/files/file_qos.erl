%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc The file_qos document contains aggregated information about QoS defined
%%% for file or directory. It contains:
%%%     - qos_entries - holds IDs of all qos_entries defined for this file (
%%%       including qos_entries which demands cannot be satisfied),
%%%     - target_storages - holds mapping storage_id to list of qos_entry IDs.
%%%       When new QoS is added for file or directory, storages on which replicas
%%%       should be stored are calculated using QoS expression. Then traverse
%%%       requests are added to qos_entry document. When provider notices change
%%%       in qos_entry, it checks whether there is traverse request defining
%%%       its storage. If yes, provider updates target_storages and
%%%       starts traverse.
%%% Each file or directory can be associated with at most one such document.
%%% Getting full information about QoS defined for file or directory requires
%%% calculating effective file_qos as it is inherited from all parents.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(file_qos).
-author("Michal Cwiertnia").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/qos.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% functions operating on record using datastore model API
-export([delete/1]).

%% higher-level functions operating on file_qos document
-export([
    get_effective/1, remove_qos_entry_id/2,
    add_qos_entry_id/3, add_qos_entry_id/4, is_replica_protected/2
]).

%% higher-level functions operating on effective_file_qos record.
-export([
    get_qos_to_update/2, get_qos_entries/1, get_target_storages/1
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

%% test API
-export([get/1]).


-type key() :: file_meta:uuid().
-type record() :: #file_qos{}.
-type effective_file_qos() :: #effective_file_qos{}.
-type target_storages() :: #{storage:id() => [qos_entry:id()]}.

-export_type([target_storages/0]).

-define(CTX, #{
    model => ?MODULE
}).


%%%===================================================================
%%% Functions operating on record using datastore_model API
%%%===================================================================

-spec get(key()) -> {ok, datastore:doc()} | {error, term()}.
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
%%% Higher-level functions operating on file_qos document.
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns effective file_qos for file. If effective value cannot be calculated
%% because of file_meta documents not being synchronized yet, ErrorCallback is executed.
%% @end
%%--------------------------------------------------------------------
-spec get_effective(file_meta:uuid()) -> {ok, effective_file_qos()} | {error, term()} | undefined.
get_effective(FileUuid) ->
    case file_meta:get(FileUuid) of
        {ok, #document{scope = SpaceId} = FileMeta} ->
            get_effective_internal(FileMeta, SpaceId);
        {error, not_found} ->
            {error, {file_meta_missing, FileUuid}};
        _ ->
            undefined
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes given Qos ID from both qos_entries and target_storages in file_qos doc.
%% @end
%%--------------------------------------------------------------------
-spec remove_qos_entry_id(file_meta:uuid(), qos_entry:id()) -> ok | {error, term()}.
remove_qos_entry_id(FileUuid, QosEntryId) ->
    Diff = fun(FileQos = #file_qos{qos_entries = QosEntries, target_storages = TS}) ->
        UpdatedQosEntries = lists:delete(QosEntryId, QosEntries),
        UpdatedTS = maps:fold(fun(StorageId, QosEntries, UpdatedTSPartial) ->
            case lists:delete(QosEntryId, QosEntries) of
                [] ->
                    UpdatedTSPartial;
                List ->
                    UpdatedTSPartial#{StorageId => List}
            end
        end, #{}, TS),
        {ok, FileQos#file_qos{qos_entries = UpdatedQosEntries, target_storages = UpdatedTS}}
    end,

    ?extract_ok(datastore_model:update(?CTX, FileUuid, Diff)).


%%--------------------------------------------------------------------
%% @doc
%% @equiv
%% add_qos(FileUuid, SpaceId, QosEntryId, undefined)
%% @end
%%--------------------------------------------------------------------
-spec add_qos_entry_id(file_meta:uuid(), od_space:id(), qos_entry:id()) -> ok.
add_qos_entry_id(FileUuid, SpaceId, QosEntryId) ->
    add_qos_entry_id(FileUuid, SpaceId, QosEntryId, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Creates or updates file_qos document. Document and diff function are
%% created using QoS entry ID and storage ID for that QoS.
%% @end
%%--------------------------------------------------------------------
-spec add_qos_entry_id(file_meta:uuid(), od_space:id(), qos_entry:id(), storage:id()) -> ok.
add_qos_entry_id(FileUuid, SpaceId, QosEntryId, TargetStorage) ->
    NewDoc = #document{
        key = FileUuid,
        scope = SpaceId,
        value = #file_qos{
            qos_entries = [QosEntryId],
            target_storages = case TargetStorage of
                undefined -> #{};
                _ -> #{TargetStorage => [QosEntryId]}
            end
        }
    },

    Diff = fun(#file_qos{qos_entries = CurrQosEntries, target_storages = CurrTS}) ->
        UpdatedTS = case TargetStorage of
            undefined ->
                CurrTS;
            _ ->
                maps:update_with(TargetStorage, fun(QosEntries) ->
                    lists:usort([QosEntryId | QosEntries])
                end, [QosEntryId], CurrTS)
        end,
        {ok, #file_qos{qos_entries = lists:usort([QosEntryId | CurrQosEntries]), target_storages = UpdatedTS}}
    end,

    ?extract_ok(datastore_model:update(?CTX, FileUuid, Diff, NewDoc)).


%%--------------------------------------------------------------------
%% @doc
%% Checks whether given file is protected on given storage by QoS.
%% @end
%%--------------------------------------------------------------------
-spec is_replica_protected(file_meta:uuid(), storage:id()) -> boolean().
is_replica_protected(FileUuid, StorageId) ->
    QosStorages = case get_effective(FileUuid) of
        {ok, #effective_file_qos{target_storages = TS}} -> TS;
        _ -> #{}
    end,
    maps:is_key(StorageId, QosStorages).


%%%===================================================================
%%% Functions operating on effective_file_qos record.
%%%===================================================================

-spec get_qos_entries(effective_file_qos()) -> [qos_entry:id()].
get_qos_entries(FileQos) ->
    FileQos#effective_file_qos.qos_entries.


-spec get_target_storages(effective_file_qos()) -> target_storages().
get_target_storages(FileQos) ->
    FileQos#effective_file_qos.target_storages.


-spec get_qos_to_update(storage:id(), effective_file_qos()) -> [qos_entry:id()].
get_qos_to_update(StorageId, EffectiveFileQos) ->
    maps:get(StorageId, EffectiveFileQos#effective_file_qos.target_storages, []).


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
merge_file_qos(ParentQos, ChildQos) ->
    #file_qos{
        qos_entries = lists:usort(
            ParentQos#file_qos.qos_entries ++ ChildQos#file_qos.qos_entries
        ),
        target_storages = merge_target_storages(
            ParentQos#file_qos.target_storages,
            ChildQos#file_qos.target_storages
        )
    }.

-spec merge_target_storages(target_storages(), target_storages()) -> target_storages().
merge_target_storages(ParentTargetStorages, ChildTargetStorages) ->
    maps:fold(fun(StorageId, StorageQosEntries, Acc) ->
        maps:update_with(StorageId, fun(ParentStorageQosEntries) ->
            ParentStorageQosEntries ++ StorageQosEntries
        end, StorageQosEntries, Acc)
    end, ParentTargetStorages, ChildTargetStorages).

-spec get_effective_internal(file_meta:doc(), od_space:id()) ->
    {ok, effective_file_qos()} | {error, term()}| undefined.
get_effective_internal(FileMeta, SpaceId) ->
    Callback = fun([#document{key = Uuid}, ParentEffQos, CalculationInfo]) ->
        case {datastore_model:get(?CTX, Uuid), ParentEffQos} of
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
    case effective_value:get_or_calculate(CacheTableName, FileMeta, Callback, [], []) of
        {ok, undefined, _} ->
            undefined;
        {ok, EffQosAsFileQos, _} ->
            {ok, #effective_file_qos{
                qos_entries = EffQosAsFileQos#file_qos.qos_entries,
                target_storages = EffQosAsFileQos#file_qos.target_storages
            }};
        {error, {file_meta_missing, _MissingUuid}} = Error ->
            % documents are not synchronized yet
            Error
    end.

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
        {qos_entries, [string]},
        {target_storages, #{string => [string]}}
    ]}.
