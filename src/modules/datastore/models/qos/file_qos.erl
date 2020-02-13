%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc The file_qos document contains information about QoS entries defined
%%% for file or directory. It contains:
%%%     - qos_entries - holds IDs of all qos_entries defined for this file (
%%%       including qos_entries which demands cannot be satisfied). This list
%%%       is updated on change of qos_entry document (see qos_hooks.erl),
%%%     - assigned_entries - holds mapping storage_id to list of qos_entry IDs.
%%%       When new QoS is added for file or directory, storages on which replicas
%%%       should be stored are calculated using QoS expression. Then traverse
%%%       requests are added to qos_entry document. When provider notices change
%%%       in qos_entry, it checks whether there is traverse request defining
%%%       its storage. If yes, provider updates assigned_entries and
%%%       starts traverse.
%%% Each file or directory can be associated with zero or one such document.
%%% Getting information about all QoS entries that influences file or directory
%%% requires calculating effective_file_qos as file_qos is inherited from all
%%% parents.
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

%% functions operating on file_qos document using datastore_model API
-export([delete/1]).

%% higher-level functions operating on file_qos document
-export([
    get_effective/1, 
    add_qos_entry_id/3, add_qos_entry_id/4, remove_qos_entry_id/3,
    is_replica_required_on_storage/2, is_effective_qos_of_file/2,
    clean_up/1
]).

%% higher-level functions operating on effective_file_qos record.
-export([
    get_assigned_entries_for_storage/2, get_qos_entries/1, get_assigned_entries/1
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type key() :: file_meta:uuid().
-type record() :: #file_qos{}.
-type effective_file_qos() :: #effective_file_qos{}.
-type assigned_entries() :: #{storage:id() => [qos_entry:id()]}.

-export_type([assigned_entries/0]).

-define(CTX, #{
    model => ?MODULE
}).


%%%===================================================================
%%% Functions operating on file_qos document using datastore_model API
%%%===================================================================

-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    ?ok_if_not_found(datastore_model:delete(?CTX, Key)).

%%%===================================================================
%%% Higher-level functions operating on file_qos document.
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns effective_file_qos for file. If effective value cannot be calculated
%% because of file_meta documents not being synchronized yet, appropriate error
%% is returned.
%% @end
%%--------------------------------------------------------------------
-spec get_effective(file_meta:doc() | file_meta:uuid()) -> 
    {ok, effective_file_qos()} | {error, term()} | undefined.
get_effective(FileUuid) when is_binary(FileUuid) ->
    case file_meta:get(FileUuid) of
        {ok, FileDoc} -> get_effective(FileDoc);
        ?ERROR_NOT_FOUND -> {error, {file_meta_missing, FileUuid}};
        _ -> undefined
    end;
get_effective(#document{scope = SpaceId} = FileDoc) ->
    Callback = fun([#document{key = Uuid}, ParentEffQos, CalculationInfo]) ->
        case {datastore_model:get(?CTX, Uuid), ParentEffQos} of
            {?ERROR_NOT_FOUND, _} ->
                {ok, ParentEffQos, CalculationInfo};
            {{ok, #document{value = FileQos}}, _} ->
                EffQos = merge_file_qos(ParentEffQos, FileQos),
                {ok, EffQos, CalculationInfo}
        end
    end,
    
    CacheTableName = ?CACHE_TABLE_NAME(SpaceId),
    case effective_value:get_or_calculate(CacheTableName, FileDoc, Callback, [], []) of
        {ok, undefined, _} ->
            undefined;
        {ok, EffQosAsFileQos, _} ->
            {ok, #effective_file_qos{
                qos_entries = EffQosAsFileQos#file_qos.qos_entries,
                assigned_entries = EffQosAsFileQos#file_qos.assigned_entries
            }};
        {error, {file_meta_missing, _MissingUuid}} = Error ->
            % documents are not synchronized yet
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% @equiv
%% add_qos_entry_id(FileUuid, SpaceId, QosEntryId, undefined)
%% @end
%%--------------------------------------------------------------------
-spec add_qos_entry_id(od_space:id(), file_meta:uuid(), qos_entry:id()) -> ok.
add_qos_entry_id(SpaceId, FileUuid, QosEntryId) ->
    add_qos_entry_id(SpaceId, FileUuid, QosEntryId, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Adds new QoS entry to file_qos document associated with file.
%% Creates file_qos document if needed.
%% @end
%%--------------------------------------------------------------------
-spec add_qos_entry_id(od_space:id(), file_meta:uuid(), qos_entry:id(), storage:id() | undefined) -> ok.
add_qos_entry_id(SpaceId, FileUuid, QosEntryId, Storage) ->
    NewDoc = #document{
        key = FileUuid,
        scope = SpaceId,
        value = #file_qos{
            qos_entries = [QosEntryId],
            assigned_entries = case Storage of
                undefined -> #{};
                _ -> #{Storage => [QosEntryId]}
            end
        }
    },

    UpdateFun = fun(#file_qos{qos_entries = CurrQosEntries, assigned_entries = CurrAssignedEntries}) ->
        UpdatedAssignedEntries = case Storage of
            undefined ->
                CurrAssignedEntries;
            _ ->
                maps:update_with(Storage, fun(QosEntries) ->
                    lists:usort([QosEntryId | QosEntries])
                end, [QosEntryId], CurrAssignedEntries)
        end,

        {ok, #file_qos{
            qos_entries = lists:usort([QosEntryId | CurrQosEntries]),
            assigned_entries = UpdatedAssignedEntries}
        }
    end,
    case datastore_model:update(?CTX, FileUuid, UpdateFun, NewDoc) of
        {ok, _} -> ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId);
        {error, _} = Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Removes given QoS entry ID from both qos_entries and assigned_entries.
%% @end
%%--------------------------------------------------------------------
-spec remove_qos_entry_id(od_space:id(), file_meta:uuid(), qos_entry:id()) -> ok | {error, term()}.
remove_qos_entry_id(SpaceId, FileUuid, QosEntryId) ->
    UpdateFun = fun(FileQos = #file_qos{qos_entries = QosEntries, assigned_entries = AssignedEntries}) ->
        UpdatedQosEntries = lists:delete(QosEntryId, QosEntries),
        UpdatedAssignedEntries = maps:fold(fun(StorageId, EntriesForStorage, UpdatedAssignedEntriesPartial) ->
            case lists:delete(QosEntryId, EntriesForStorage) of
                [] ->
                    UpdatedAssignedEntriesPartial;
                List ->
                    UpdatedAssignedEntriesPartial#{StorageId => List}
            end
        end, #{}, AssignedEntries),
        
        {ok, FileQos#file_qos{
            qos_entries = UpdatedQosEntries,
            assigned_entries = UpdatedAssignedEntries
        }}
    end,
    case datastore_model:update(?CTX, FileUuid, UpdateFun) of
        {ok, _} -> ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId);
        {error, _} = Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks whether given file is required on given storage by QoS.
%% @end
%%--------------------------------------------------------------------
-spec is_replica_required_on_storage(file_meta:uuid(), storage:id()) -> boolean().
is_replica_required_on_storage(FileUuid, StorageId) ->
    QosStorages = case get_effective(FileUuid) of
        {ok, #effective_file_qos{assigned_entries = AssignedEntries}} -> AssignedEntries;
        _ -> #{}
    end,
    maps:is_key(StorageId, QosStorages).


-spec is_effective_qos_of_file(file_meta:doc() | file_meta:uuid(), qos_entry:id()) -> 
    boolean() | {error, term()}.
is_effective_qos_of_file(FileUuidOrDoc, QosEntryId) ->
    case get_effective(FileUuidOrDoc) of
        undefined -> false;
        {ok, EffectiveFileQos} -> lists:member(QosEntryId, get_qos_entries(EffectiveFileQos));
        {error, _} = Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes all QoS documents related to given file.
%% @end
%%--------------------------------------------------------------------
-spec clean_up(file_ctx:ctx()) -> ok.
clean_up(FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {FileDoc, FileCtx1} = file_ctx:get_file_doc_including_deleted(FileCtx),
    case get_effective(FileDoc) of
        undefined -> ok;
        {ok, #effective_file_qos{qos_entries = EffectiveQosEntries}} ->
            lists:foreach(fun(EffectiveQosEntryId) ->
                qos_status:report_file_deleted(FileCtx1, EffectiveQosEntryId)
            end, EffectiveQosEntries);
        {error, _} = Error ->
            ?warning("Error during QoS clean up procedure:~p", [Error]),
            ok
    end,
    Uuid = file_ctx:get_uuid_const(FileCtx1),
    case datastore_model:get(?CTX, Uuid) of
        {ok, #document{value = #file_qos{qos_entries = QosEntries}}} ->
            lists:foreach(fun(QosEntryId) ->
                ok = qos_entry:remove_from_impossible_list(SpaceId, QosEntryId),
                ok = qos_status:report_entry_deleted(SpaceId, QosEntryId),
                ok = qos_traverse:report_entry_deleted(QosEntryId),
                ok = qos_entry:delete(QosEntryId)
            end, QosEntries);
        ?ERROR_NOT_FOUND -> ok
    end,
    ok = delete(Uuid).

%%%===================================================================
%%% Functions operating on effective_file_qos record.
%%%===================================================================

-spec get_qos_entries(effective_file_qos()) -> [qos_entry:id()].
get_qos_entries(EffectiveFileQos) ->
    EffectiveFileQos#effective_file_qos.qos_entries.


-spec get_assigned_entries(effective_file_qos()) -> assigned_entries().
get_assigned_entries(EffectiveFileQos) ->
    EffectiveFileQos#effective_file_qos.assigned_entries.


-spec get_assigned_entries_for_storage(effective_file_qos(), storage:id()) ->
    [qos_entry:id()].
get_assigned_entries_for_storage(EffectiveFileQos, StorageId) ->
    maps:get(StorageId, get_assigned_entries(EffectiveFileQos), []).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Merge parent's file_qos with child's file_qos. Used when calculating
%% effective_file_qos.
%% @end
%%--------------------------------------------------------------------
-spec merge_file_qos(record(), record()) -> record().
merge_file_qos(undefined, ChildQos) ->
    ChildQos;
merge_file_qos(ParentQos, ChildQos) ->
    #file_qos{
        qos_entries = lists:usort(
            ParentQos#file_qos.qos_entries ++ ChildQos#file_qos.qos_entries
        ),
        assigned_entries = merge_assigned_entries(
            ParentQos#file_qos.assigned_entries,
            ChildQos#file_qos.assigned_entries
        )
    }.


-spec merge_assigned_entries(assigned_entries(), assigned_entries()) -> assigned_entries().
merge_assigned_entries(ParentAssignedEntries, ChildAssignedEntries) ->
    maps:fold(fun(StorageId, StorageQosEntries, Acc) ->
        maps:update_with(StorageId, fun(ParentStorageQosEntries) ->
            ParentStorageQosEntries ++ StorageQosEntries
        end, StorageQosEntries, Acc)
    end, ParentAssignedEntries, ChildAssignedEntries).


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
        {assigned_entries, #{string => [string]}}
    ]}.
