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
%%% Only one file_qos document exists per inode i.e when QoS entry is added to 
%%% hardlink it is saved in inode file_qos document.
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
    qos_membership/1, has_any_qos_entry/2, 
    cleanup_reference_related_documents/1, cleanup_reference_related_documents/2, 
    cleanup_on_no_reference/1,
    delete_associated_entries_on_no_references/1
]).

%% higher-level functions operating on effective_file_qos record.
-export([
    get_assigned_entries_for_storage/2,
    get_qos_entries/1,
    get_assigned_entries/1,
    is_in_trash/1
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type key() :: file_meta:uuid().
-type record() :: #file_qos{}.
-type doc() :: datastore_doc:doc(record()).
-type pred() :: datastore_doc:pred(record()).
-type effective_file_qos() :: #effective_file_qos{}.
-type assigned_entries() :: #{storage:id() => [qos_entry:id()]}.
-type membership() :: ?NONE_QOS_MEMBERSHIP | ?DIRECT_QOS_MEMBERSHIP | ?ANCESTOR_QOS_MEMBERSHIP.

-export_type([assigned_entries/0, membership/0]).

-define(CTX, #{
    model => ?MODULE
}).

-compile({no_auto_import, [get/1]}).


%%%===================================================================
%%% Functions operating on file_qos document using datastore_model API
%%%===================================================================

-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

-spec delete(key(), pred()) -> ok | {error, term()}.
delete(Key, Pred) ->
    case datastore_model:delete(?CTX, Key, Pred) of
        {error, {not_satisfied, _}} -> ok;
        Other -> Other
    end.

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
    {ok, effective_file_qos()} | {error, {file_meta_missing, binary()}} | undefined.
get_effective(FileUuid) when is_binary(FileUuid) ->
    case file_meta:get(FileUuid) of
        {ok, FileDoc} -> get_effective(FileDoc);
        ?ERROR_NOT_FOUND -> {error, {file_meta_missing, FileUuid}}
    end;
get_effective(FileDoc) ->
    get_effective(FileDoc, undefined).


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
    % critical section to avoid adding new entry for file between update and delete
    case datastore_model:update(?CTX, FileUuid, UpdateFun) of
        {ok, #document{value = #file_qos{qos_entries = []}}} -> 
            ok = delete(FileUuid, fun(#file_qos{qos_entries = QosEntries}) -> QosEntries =:= [] end),
            ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId);
        {ok, _} ->
            ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId);
        {error, _} = Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks whether given file is required on given storage by QoS.
%% @end
%%--------------------------------------------------------------------
-spec is_replica_required_on_storage(file_meta:uuid(), storage:id()) -> boolean().
is_replica_required_on_storage(FileUuid, StorageId) ->
    case get_effective(FileUuid) of
        {ok, #effective_file_qos{
            in_trash = InTrash,
            assigned_entries = AssignedEntries
        }} ->
            not InTrash andalso maps:is_key(StorageId, AssignedEntries);
        _ ->
            false
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks whether given entry was added to given file or to any of file's ancestor.
%% @end
%%--------------------------------------------------------------------
-spec is_effective_qos_of_file(file_meta:doc() | file_meta:uuid(), qos_entry:id()) -> 
    boolean() | {error, term()}.
is_effective_qos_of_file(FileUuidOrDoc, QosEntryId) ->
    case get_effective(FileUuidOrDoc) of
        undefined -> false;
        {ok, EffectiveFileQos} ->
            lists:member(QosEntryId, get_qos_entries(EffectiveFileQos))
                andalso not is_in_trash(EffectiveFileQos);
        {error, _} = Error -> Error
    end.


-spec qos_membership(file_meta:uuid() | file_meta:doc()) -> membership().
qos_membership(FileUuidOrDoc) ->
    case has_any_qos_entry(FileUuidOrDoc, direct) of
        true ->
            ?DIRECT_QOS_MEMBERSHIP;
        false ->
            case has_any_qos_entry(FileUuidOrDoc, effective) of
                true -> ?ANCESTOR_QOS_MEMBERSHIP;
                false -> ?NONE_QOS_MEMBERSHIP
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks whether given file has any QoS entry (effective or direct).
%% @end
%%--------------------------------------------------------------------
-spec has_any_qos_entry(file_meta:uuid() | file_meta:doc(), direct | effective) -> boolean().
has_any_qos_entry(#document{key = Uuid}, direct) ->
    has_any_qos_entry(Uuid, direct);
has_any_qos_entry(Key, direct) ->
    has_any_qos_entry(get(Key));
has_any_qos_entry(UuidOrDoc, effective) ->
    has_any_qos_entry(get_effective(UuidOrDoc)).


%%--------------------------------------------------------------------
%% @doc
%% Performs cleanup necessary for each file reference deletion.
%% @end
%%--------------------------------------------------------------------
cleanup_reference_related_documents(FileCtx) ->
    cleanup_reference_related_documents(FileCtx, undefined).

cleanup_reference_related_documents(FileCtx, OriginalParentCtx) ->
    {FileDoc, FileCtx1} = file_ctx:get_file_doc_including_deleted(FileCtx),
    %% This is used when directory is being moved to trash. In such case, to 
    %% calculate effective QoS before deletion, QoS for given directory needs to be 
    %% merged with effective QoS for parent before deletion (OriginalParent)
    %% TODO VFS-7133 take original parent uuid from file_meta doc
    ParentDoc = get_original_parent_doc(OriginalParentCtx),
    case get_effective(FileDoc, ParentDoc) of
        undefined -> ok;
        % clean up all potential documents related to status calculation
        {ok, #effective_file_qos{qos_entries = EffectiveQosEntries}} ->
            lists:foreach(fun(EffectiveQosEntryId) ->
                qos_status:report_file_deleted(FileCtx1, EffectiveQosEntryId, OriginalParentCtx)
            end, EffectiveQosEntries);
        {error, _} = Error ->
            ?warning("Error during QoS clean up procedure:~p", [Error]),
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Performs necessary cleanup on last file reference deletion.
%% @end
%%--------------------------------------------------------------------
-spec cleanup_on_no_reference(file_ctx:ctx()) -> ok.
cleanup_on_no_reference(FileCtx) ->
    ok = delete(file_ctx:get_referenced_uuid_const(FileCtx)).


%%--------------------------------------------------------------------
%% @doc
%% Deletes all QoS entries added to given file if all references were deleted.
%% @end
%%--------------------------------------------------------------------
-spec delete_associated_entries_on_no_references(file_ctx:ctx()) -> ok.
delete_associated_entries_on_no_references(FileCtx) ->
    {ok, ReferencesCount} = file_ctx:count_references_const(FileCtx),
    InodeUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    case {get(InodeUuid), ReferencesCount} of
        {{ok, #document{value = #file_qos{qos_entries = QosEntries}}}, 0} ->
            lists:foreach(fun(QosEntryId) ->
                ok = qos_hooks:handle_entry_delete(QosEntryId),
                ok = qos_entry:delete(QosEntryId)
            end, QosEntries);
        {_, _} -> ok
    end.

%%%===================================================================
%%% Functions operating on effective_file_qos record.
%%%===================================================================

-spec get_qos_entries(effective_file_qos()) -> [qos_entry:id()].
get_qos_entries(EffectiveFileQos) ->
    EffectiveFileQos#effective_file_qos.qos_entries.


-spec get_assigned_entries(effective_file_qos()) -> assigned_entries().
get_assigned_entries(EffectiveFileQos) ->
    EffectiveFileQos#effective_file_qos.assigned_entries.


-spec is_in_trash(effective_file_qos()) -> boolean().
is_in_trash(EffectiveFileQos) ->
    EffectiveFileQos#effective_file_qos.in_trash.


-spec get_assigned_entries_for_storage(effective_file_qos(), storage:id()) ->
    [qos_entry:id()].
get_assigned_entries_for_storage(EffectiveFileQos, StorageId) ->
    maps:get(StorageId, get_assigned_entries(EffectiveFileQos), []).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_effective(file_meta:doc(), undefined | file_meta:doc()) ->
    {ok, effective_file_qos()} | {error, {file_meta_missing, binary()}} | undefined.
get_effective(#document{key = FileUuid} = FileDoc, OriginalParentDoc) ->
    Callback = fun([#document{key = Uuid, value = #file_meta{}}, ParentEffQos, CalculationInfo]) ->
        case fslogic_uuid:is_trash_dir_uuid(Uuid) of
            true ->
                % qos cannot be set on trash directory
                {ok, #effective_file_qos{in_trash = true}, CalculationInfo};
            false ->
                case get(Uuid) of
                    ?ERROR_NOT_FOUND ->
                        {ok, ParentEffQos, CalculationInfo};
                    {ok, #document{value = FileQos}} ->
                        EffQos = merge_file_qos(ParentEffQos, FileQos),
                        {ok, EffQos, CalculationInfo}
                end
        end
    end,
    MergeCallback = fun(NewEntry, Acc, _EntryCalculationInfo, CalculationInfoAcc) ->
        {ok, merge_file_qos(NewEntry, Acc), CalculationInfoAcc}
    end,
    Options = #{merge_callback => MergeCallback, use_referenced_key => true, force_execution_on_referenced_key => true},

    merge_eff_qos_for_files([OriginalParentDoc, FileDoc], Callback, Options).


%% @private
-spec merge_eff_qos_for_files([file_meta:doc()], effective_value:callback(), effective_value:get_or_calculate_options()) ->
    effective_file_qos() | undefined.
merge_eff_qos_for_files(FileDocs, Callback, Options) ->
    MergedEffQos = lists_utils:foldl_while(fun(FileDoc, Acc) ->
        case get_effective_qos_for_single_file(FileDoc, Callback, Options) of
            {ok, EffQos} -> {cont, merge_file_qos(Acc, EffQos)};
            {error, _} = Error -> {halt, Error}
        end
    end, undefined, FileDocs),
    case MergedEffQos of
        {error, _} = Error -> Error;
        undefined -> undefined;
        _ -> {ok, MergedEffQos}
    end.
    

%% @private
-spec get_effective_qos_for_single_file(undefined | file_meta:doc(), effective_value:callback(),
    effective_value:get_or_calculate_options()) -> effective_file_qos() | undefined | {error, term()}.
get_effective_qos_for_single_file(undefined, _Callback, _Options) -> {ok, undefined};
get_effective_qos_for_single_file(#document{scope = SpaceId} = FileDoc, Callback, Options) ->
    case effective_value:get_or_calculate(?CACHE_TABLE_NAME(SpaceId), FileDoc, Callback, Options) of
        {ok, EffQos, _} ->
            {ok, EffQos};
        {error, {file_meta_missing, _MissingUuid}} = Error ->
            % documents are not synchronized yet
            Error
    end.


%% @private
-spec file_qos_to_eff_file_qos(record()) -> effective_file_qos().
file_qos_to_eff_file_qos(#file_qos{
    qos_entries = QosEntries,
    assigned_entries = AssignedEntries
}) ->
    #effective_file_qos{
        qos_entries = QosEntries,
        assigned_entries = AssignedEntries
    }.


%% @private
-spec merge_file_qos(
    undefined | effective_file_qos() | record(), 
    undefined | effective_file_qos() | record()
) -> 
    effective_file_qos() | undefined.
merge_file_qos(FileQos = #file_qos{}, SecondEffQos) ->
    merge_file_qos(file_qos_to_eff_file_qos(FileQos), SecondEffQos);
merge_file_qos(FirstEffQos, FileQos = #file_qos{}) ->
    merge_file_qos(FirstEffQos, file_qos_to_eff_file_qos(FileQos));
merge_file_qos(undefined, SecondEffQos) ->
    SecondEffQos;
merge_file_qos(FirstEffQos, undefined) ->
    FirstEffQos;
merge_file_qos(FirstEffQos, SecondEffQos) ->
    #effective_file_qos{
        qos_entries = lists:usort(
            FirstEffQos#effective_file_qos.qos_entries ++ SecondEffQos#effective_file_qos.qos_entries
        ),
        assigned_entries = merge_assigned_entries(
            FirstEffQos#effective_file_qos.assigned_entries,
            SecondEffQos#effective_file_qos.assigned_entries
        ),
        in_trash = FirstEffQos#effective_file_qos.in_trash orelse SecondEffQos#effective_file_qos.in_trash
    }.


%% @private
-spec merge_assigned_entries(assigned_entries(), assigned_entries()) -> assigned_entries().
merge_assigned_entries(FirstAssignedEntries, SecondAssignedEntries) ->
    maps:fold(fun(StorageId, StorageQosEntries, Acc) ->
        maps:update_with(StorageId, fun(ParentStorageQosEntries) ->
            % usort to remove duplicated entries
            lists:usort(ParentStorageQosEntries ++ StorageQosEntries)
        end, StorageQosEntries, Acc)
    end, FirstAssignedEntries, SecondAssignedEntries).


%% @private
-spec has_any_qos_entry({ok, doc()} | {ok, effective_file_qos()} | undefined | {error, term()}) ->
    boolean().
has_any_qos_entry({ok, #document{value = #file_qos{qos_entries = [_ | _]}}}) -> true;
has_any_qos_entry({ok, #effective_file_qos{in_trash = true}}) -> false;
has_any_qos_entry({ok, #effective_file_qos{qos_entries = [_|_]}}) -> true;
has_any_qos_entry(_) -> false.


%% @private
-spec get_original_parent_doc(undefined | file_ctx:ctx()) -> undefined | file_meta:doc().
get_original_parent_doc(undefined) -> undefined;
get_original_parent_doc(OriginalParentCtx) ->
    {ParentDoc, _} = file_ctx:get_file_doc(OriginalParentCtx),
    ParentDoc.


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
