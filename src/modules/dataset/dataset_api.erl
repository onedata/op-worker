%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for performing operations on datasets.
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_api).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/dataset/archivisation_tree.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([establish/2, establish/3, update/4, remove/1, remove/2, move_if_applicable/2]).
-export([get_info/1, get_effective_inheritance_path_and_protection_flags/1, get_effective_summary/1]).
-export([list_top_datasets/4, list_children_datasets/3]).
-export([handle_file_deleted/1]).
-export([handle_remote_change/2]).

%% Utils
-export([get_associated_file_ctx/1]).

-type error() :: {error, term()}.

-type info() :: #dataset_info{}.
-type basic_entry() :: datasets_structure:entry().
-type basic_entries() :: [basic_entry()].
-type extended_entries() :: [info()].
-type entries() :: basic_entries() | extended_entries().
-type listing_opts() :: datasets_structure:opts().
-type listing_mode() :: ?BASIC_INFO | ?EXTENDED_INFO.
-type index() :: datasets_structure:index().
-type dataset_type() :: internal | user_defined.
-type file_eff_summary() :: #file_eff_dataset_summary{}.

-export_type([info/0, entries/0, listing_opts/0, index/0, listing_mode/0, file_eff_summary/0]).

% TODO VFS-7518 how should we handle race on creating dataset on the same file in 2 providers?
% TODO VFS-7533 handle conflicts on remote modification of file-meta and dataset models
% TODO VFS-7563 add tests concerning datasets to permissions test suites
% TODO VFS-7849 does not allow to establish dataset on files in archives or in trash

-define(CRITICAL_SECTION(DatasetId, Function), critical_section:run({dataset, DatasetId}, Function)).

-define(MAX_LIST_EXTENDED_DATASET_INFO_PROCS,
    op_worker:get_env(max_list_extended_dataset_info_procs, 20)).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec establish(file_ctx:ctx(), data_access_control:bitmask()) -> {ok, dataset:id()}.
establish(FileCtx, ProtectionFlags) ->
    establish(FileCtx, ProtectionFlags, user_defined).

-spec establish(file_ctx:ctx(), data_access_control:bitmask(), dataset_type()) -> {ok, dataset:id()}.
establish(FileCtx, ProtectionFlags, DatasetType) ->
    ok = file_meta:validate_protection_flags(ProtectionFlags),
    ?CRITICAL_SECTION(file_ctx:get_logical_uuid_const(FileCtx), fun() ->
        SpaceId = file_ctx:get_space_id_const(FileCtx),
        Uuid = file_ctx:get_logical_uuid_const(FileCtx),
        {ok, DatasetId} = dataset:create(Uuid, SpaceId),
        {DatasetName, _FileCtx2} = file_ctx:get_aliased_name(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
        ok = file_meta_dataset:establish(Uuid, ProtectionFlags),
        case DatasetType of
            internal -> ok;
            user_defined -> ok = attached_datasets:add(SpaceId, Uuid, DatasetName)
        end,
        InvalidateDatasetsOnly = not ?has_any_flags(ProtectionFlags, ?DATA_PROTECTION bor ?METADATA_PROTECTION),
        dataset_eff_cache:invalidate_on_all_nodes(SpaceId, InvalidateDatasetsOnly),
        {ok, DatasetId}
    end).


-spec update(dataset:doc(), undefined | dataset:state(), data_access_control:bitmask(), data_access_control:bitmask()) ->
    ok | error().
update(DatasetDoc, NewState, FlagsToSet, FlagsToUnset) ->
    {ok, DatasetId} = dataset:get_id(DatasetDoc),
    ?CRITICAL_SECTION(DatasetId, fun() ->
        {ok, DatasetId} = dataset:get_id(DatasetDoc),
        {ok, CurrentState} = dataset:get_state(DatasetDoc),
        case {CurrentState, NewState, FlagsToSet, FlagsToUnset} of
            {_, undefined, ?no_flags_mask, ?no_flags_mask} ->
                ok;
            {SameState, SameState, _, _} ->
                ?ERROR_ALREADY_EXISTS;
            {?DETACHED_DATASET, ?ATTACHED_DATASET, _, _} ->
                reattach(DatasetId, FlagsToSet, FlagsToUnset);
            {?DETACHED_DATASET, undefined, _, _} ->
                throw(?ERROR_BAD_DATA(state, <<"Detached dataset cannot be modified.">>));
            {?ATTACHED_DATASET, ?DETACHED_DATASET, ?no_flags_mask, ?no_flags_mask} ->
                {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
                {ok, Uuid} = dataset:get_root_file_uuid(DatasetDoc),
                FileCtx = file_ctx:new_by_uuid(Uuid, SpaceId),
                {FilePath, _FileCtx2} = file_ctx:get_logical_path(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
                % it's forbidden to change flags while detaching dataset
                detach_insecure(DatasetId, FilePath, ?DATASET_USER_TRIGGERED_DETACHMENT);
            {?ATTACHED_DATASET, ?DETACHED_DATASET, _, _} ->
                {error, ?EINVAL};
            {?ATTACHED_DATASET, undefined, _, _} ->
                update_protection_flags(DatasetId, FlagsToSet, FlagsToUnset)
        end
    end).


-spec remove(dataset:id() | dataset:doc()) -> ok | error().
remove(DocOrId) ->
    remove(DocOrId, user_defined).


-spec remove(dataset:id() | dataset:doc(), dataset_type()) -> ok | error().
remove(#document{key = DatasetId}, DatasetType) ->
    % fetch doc again in critical section to avoid races with dataset state change
    remove(DatasetId, DatasetType);
remove(DatasetId, DatasetType) when is_binary(DatasetId) ->
    ?CRITICAL_SECTION(DatasetId, fun() ->
        case archives_list:is_empty(DatasetId) of
            true ->
                case dataset:get(DatasetId) of
                    {ok, Doc} -> remove_unsafe(Doc, DatasetType);
                    {error, not_found} -> ok
                end;
            false ->
                {error, ?ENOTEMPTY}
        end
    end).


-spec move_if_applicable(file_meta:doc(), file_meta:doc()) -> ok.
move_if_applicable(SourceDoc, TargetDoc) ->
    {ok, Uuid} = file_meta:get_uuid(SourceDoc),
    ?CRITICAL_SECTION(Uuid, fun() ->
        case file_meta_dataset:is_attached(TargetDoc) of
            false ->
                ok;
            true ->
                {ok, SpaceId} = file_meta:get_scope_id(SourceDoc),
                {ok, SourceParentUuid} = file_meta:get_parent_uuid(SourceDoc),
                {ok, TargetParentUuid} = file_meta:get_parent_uuid(TargetDoc),
                TargetName = file_meta:get_name(TargetDoc),
                attached_datasets:move(SpaceId, Uuid, SourceParentUuid, TargetParentUuid, TargetName)
        end
    end).


-spec handle_file_deleted(file_ctx:ctx()) -> file_ctx:ctx().
handle_file_deleted(FileCtx) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),
    DatasetId = file_ctx:get_logical_uuid_const(FileCtx),
    ?CRITICAL_SECTION(DatasetId, fun() ->
        case file_meta_dataset:get_state(FileDoc) of
            ?ATTACHED_DATASET ->
                {Path, FileCtx3} = file_ctx:get_logical_path(FileCtx2, user_ctx:new(?ROOT_SESS_ID)),
                PathBeforeDeletion = case file_ctx:get_path_before_deletion(FileCtx3) of
                    undefined -> Path;
                    P -> P
                end,
                detach_insecure(DatasetId, PathBeforeDeletion, ?DATASET_ROOT_FILE_DELETED);
            ?DETACHED_DATASET ->
                dataset:mark_root_file_deleted(DatasetId);
            undefined ->
                ok
        end,
        FileCtx2
    end).


-spec get_info(dataset:id()) -> {ok, #dataset_info{}}.
get_info(DatasetId) ->
    {ok, collect_state_dependant_info(DatasetId)}.


-spec get_effective_inheritance_path_and_protection_flags(file_ctx:ctx()) ->
    {ok, dataset:inheritance_path(), data_access_control:bitmask(), data_access_control:bitmask(), file_ctx:ctx()}.
get_effective_inheritance_path_and_protection_flags(FileCtx) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, EffCacheEntry} = dataset_eff_cache:get(FileDoc),
    {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(EffCacheEntry),
    {ok, EffProtectionFlags} = dataset_eff_cache:get_eff_protection_flags(EffCacheEntry),
    {ok, EffDatasetProtectionFlags} = dataset_eff_cache:get_eff_dataset_protection_flags(EffCacheEntry),
    IsDirectAttached = file_meta_dataset:is_attached(FileDoc),
    EffInheritancePath = case {IsDirectAttached, length(EffAncestorDatasets) =/= 0} of
        {true, true} -> ?direct_and_ancestor_inheritance_path;
        {true, false} -> ?direct_inheritance_path;
        {false, true} -> ?ancestor_inheritance;
        {false, false} -> ?none_inheritance_path
    end,
    {ok, EffInheritancePath, EffProtectionFlags, EffDatasetProtectionFlags, FileCtx2}.


-spec get_effective_summary(file_ctx:ctx()) -> {ok, file_eff_summary()}.
get_effective_summary(FileCtx) ->
    %% @TODO VFS-8972 - remove filtration of datasets established on archives after 
    %% dummy dataset is no longer needed to add data protection flags to archive
    {CanonicalPath, FileCtx2} = file_ctx:get_canonical_path(FileCtx),
    case archivisation_tree:is_in_archive(CanonicalPath) of
        true ->
            % ignore datasets for files in archives 
            % (datasets there are only established to enable protection)
            {ok, #file_eff_dataset_summary{
                direct_dataset = undefined,
                eff_ancestor_datasets = []
            }};
        false ->
            {FileDoc, _FileCtx3} = file_ctx:get_file_doc(FileCtx2),
            {ok, EffCacheEntry} = dataset_eff_cache:get(FileDoc),
            {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(EffCacheEntry),
            {ok, EffProtectionFlags} = dataset_eff_cache:get_eff_protection_flags(EffCacheEntry),
            {ok, #file_eff_dataset_summary{
                direct_dataset = file_meta_dataset:get_id(FileDoc),
                eff_ancestor_datasets = EffAncestorDatasets,
                eff_protection_flags = EffProtectionFlags
            }}
    end.


-spec list_top_datasets(od_space:id(), dataset:state(), listing_opts(), listing_mode()) ->
    {ok, {entries(), boolean()}}.
list_top_datasets(SpaceId, State, Opts, ListingMode) ->
    {ok, DatasetEntries, IsLast} = list_top_datasets_internal(SpaceId, State, Opts),
    case ListingMode of
        ?BASIC_INFO ->
            {ok, {DatasetEntries, IsLast}};
        ?EXTENDED_INFO ->
            {ok, {extend_with_info(DatasetEntries), IsLast}}
    end.


-spec list_children_datasets(dataset:id(), listing_opts(), listing_mode()) ->
    {ok, {entries(), boolean()}}.
list_children_datasets(DatasetId, Opts, ListingMode) ->
    {ok, DatasetEntries, IsLast} = list_children_datasets_internal(DatasetId, Opts),
    case ListingMode of
        ?BASIC_INFO ->
            {ok, {DatasetEntries, IsLast}};
        ?EXTENDED_INFO ->
            {ok, {extend_with_info(DatasetEntries), IsLast}}
    end.


-spec handle_remote_change(od_space:id(), dataset:doc()) -> ok.
handle_remote_change(SpaceId, #document{deleted = true, key = DatasetId}) ->
    ok = file_meta_forest:delete(fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId), SpaceId,
        ?DATASET_ARCHIVES_DIR_NAME(DatasetId), ?DATASET_ARCHIVES_DIR_UUID(DatasetId));
handle_remote_change(SpaceId, #document{deleted = false, key = DatasetId}) ->
    archivisation_tree:ensure_dataset_archives_dir_exists(DatasetId, SpaceId),
    ok.


%%%===================================================================
%%% Util functions
%%%===================================================================

-spec get_associated_file_ctx(dataset:doc()) -> file_ctx:ctx().
get_associated_file_ctx(DatasetDoc) ->
    {ok, Uuid} = dataset:get_root_file_uuid(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    file_ctx:new_by_uuid(Uuid, SpaceId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec remove_unsafe(dataset:doc(), dataset_type()) -> ok.
remove_unsafe(#document{key = DatasetId} = Doc, DatasetType) ->
    % Internal datasets are not added to datasets structure so there is no need to remove them from it.
    case DatasetType of
        internal ->
            ok;
        user_defined ->
            ok = remove_from_datasets_structure(Doc)
    end,
    {ok, SpaceId} = dataset:get_space_id(Doc),
    ok = dataset:delete(DatasetId),
    {ok, Uuid} = dataset:get_root_file_uuid(Doc),
    InvalidateDatasetsOnly = case file_meta:get(Uuid) of
        {ok, FileMetaDoc} ->
            ProtectionFlags = file_meta:get_protection_flags(FileMetaDoc),
            not ?has_any_flags(ProtectionFlags, ?DATA_PROTECTION bor ?METADATA_PROTECTION);
        _ ->
            false
    end,
    ok = file_meta_dataset:remove(Uuid),
    ok = file_meta:delete(?DATASET_ARCHIVES_DIR_NAME(DatasetId)),
    dataset_eff_cache:invalidate_on_all_nodes(SpaceId, InvalidateDatasetsOnly).


-spec reattach(dataset:id(), data_access_control:bitmask(), data_access_control:bitmask()) -> ok | error().
reattach(DatasetId, FlagsToSet, FlagsToUnset) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    case file_meta_dataset:reattach(DatasetId, FlagsToSet, FlagsToUnset) of
        {ok, NewProtectionFlags} ->
            FileCtx = get_associated_file_ctx(Doc),
            {DatasetName, _} = file_ctx:get_aliased_name(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
            ok = dataset:mark_reattached(DatasetId),
            attached_datasets:add(SpaceId, DatasetId, DatasetName),
            detached_datasets:delete(Doc),
            % Dataset was detached so prev flags have been ignored - check if any flag appeared
            InvalidateDatasetsOnly = not ?has_any_flags(NewProtectionFlags, ?DATA_PROTECTION bor ?METADATA_PROTECTION),
            dataset_eff_cache:invalidate_on_all_nodes(SpaceId, InvalidateDatasetsOnly);
        {error, _} = Error ->
            Error
    end.


-spec detach_insecure(dataset:id(), file_meta:path(), dataset:detachment_reason()) -> ok | error().
detach_insecure(DatasetId, RootFilePath, Reason) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, FileDoc} = file_meta:get_including_deleted(DatasetId),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    CurrProtectionFlags = file_meta:get_protection_flags(FileDoc),
    {ok, DatasetPath} = dataset_path:get(SpaceId, DatasetId),
    FileType = file_meta:get_effective_type(FileDoc),
    DatasetName = filename:basename(RootFilePath),
    ok = dataset:mark_detached(DatasetId, DatasetPath, RootFilePath, FileType, CurrProtectionFlags, Reason),
    detached_datasets:add(SpaceId, DatasetPath, DatasetName),
    attached_datasets:delete(SpaceId, DatasetPath),
    case file_meta_dataset:detach(DatasetId) of
        ok ->
            InvalidateDatasetsOnly = not ?has_any_flags(CurrProtectionFlags, ?DATA_PROTECTION bor ?METADATA_PROTECTION),
            dataset_eff_cache:invalidate_on_all_nodes(SpaceId, InvalidateDatasetsOnly);
        Error ->
            Error
    end.


-spec update_protection_flags(dataset:id(), data_access_control:bitmask(), data_access_control:bitmask()) -> ok.
update_protection_flags(DatasetId, FlagsToSet, FlagsToUnset) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, Uuid} = dataset:get_root_file_uuid(Doc),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    InvalidateDatasetsOnly = case file_meta:update_protection_flags(Uuid, FlagsToSet, FlagsToUnset) of
        ok -> false;
        {error, nothing_changed} -> true
    end,
    dataset_eff_cache:invalidate_on_all_nodes(SpaceId, InvalidateDatasetsOnly).


-spec remove_from_datasets_structure(dataset:doc()) -> ok.
remove_from_datasets_structure(Doc) ->
    {ok, State} = dataset:get_state(Doc),
    case State of
        ?ATTACHED_DATASET -> attached_datasets:delete(Doc);
        ?DETACHED_DATASET -> detached_datasets:delete(Doc)
    end.


-spec collect_state_dependant_info(dataset:id()) -> info().
collect_state_dependant_info(DatasetId) ->
    collect_state_dependant_info(DatasetId, undefined).


-spec collect_state_dependant_info(dataset:id(), index() | undefined) -> info().
collect_state_dependant_info(DatasetId, IndexOrUndefined) ->
    {ok, DatasetDoc} = dataset:get(DatasetId),
    {ok, State} = dataset:get_state(DatasetDoc),
    case State of
        ?ATTACHED_DATASET -> collect_attached_info(DatasetDoc, IndexOrUndefined);
        ?DETACHED_DATASET -> collect_detached_info(DatasetDoc, IndexOrUndefined)
    end.


-spec collect_attached_info(dataset:doc(), index() | undefined) -> info().
collect_attached_info(DatasetDoc, IndexOrUndefined) ->
    {ok, DatasetId} = dataset:get_id(DatasetDoc),
    {ok, CreationTime} = dataset:get_creation_time(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, Uuid} = dataset:get_root_file_uuid(DatasetDoc),
    {ok, FileDoc} = file_meta:get(Uuid),
    FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId),
    {FilePath, _FileCtx2} = file_ctx:get_logical_path(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
    FileType = file_meta:get_effective_type(FileDoc),
    {ok, EffCacheEntry} = dataset_eff_cache:get(FileDoc),
    {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(EffCacheEntry),
    {ok, EffProtectionFlags} = dataset_eff_cache:get_eff_dataset_protection_flags(EffCacheEntry),
    FinalIndex = case IndexOrUndefined of
        undefined -> entry_index(DatasetId, FilePath);
        Index -> Index
    end,
    #dataset_info{
        id = DatasetId,
        root_file_guid = file_ctx:get_logical_guid_const(FileCtx),
        creation_time = CreationTime,
        state = ?ATTACHED_DATASET,
        root_file_path = FilePath,
        root_file_type = FileType,
        protection_flags = file_meta:get_protection_flags(FileDoc),
        eff_protection_flags = EffProtectionFlags,
        parent = case length(EffAncestorDatasets) == 0 of
            true -> undefined;
            false -> hd(EffAncestorDatasets)
        end,
        archive_count = archives_list:length(DatasetId),
        index = FinalIndex
    }.


-spec collect_detached_info(dataset:doc(), index() | undefined) -> info().
collect_detached_info(DatasetDoc, IndexOrUndefined) ->
    {ok, DatasetId} = dataset:get_id(DatasetDoc),
    {ok, Uuid} = dataset:get_root_file_uuid(DatasetDoc),
    {ok, CreationTime} = dataset:get_creation_time(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, DetachedInfo} = dataset:get_detached_info(DatasetDoc),
    RootFilePath = detached_dataset_info:get_root_file_path(DetachedInfo),
    RootFileType = detached_dataset_info:get_root_file_type(DetachedInfo),
    DetachedDatasetPath = detached_dataset_info:get_path(DetachedInfo),
    ProtectionFlags = detached_dataset_info:get_protection_flags(DetachedInfo),
    DetachmentReason = detached_dataset_info:get_detachment_reason(DetachedInfo),
    FinalIndex = case IndexOrUndefined of
        undefined -> entry_index(DatasetId, RootFilePath);
        Index -> Index
    end,
    #dataset_info{
        id = DatasetId,
        root_file_guid = file_id:pack_guid(Uuid, SpaceId),
        creation_time = CreationTime,
        state = ?DETACHED_DATASET,
        root_file_path = RootFilePath,
        root_file_type = RootFileType,
        root_file_deleted = DetachmentReason =:= ?DATASET_ROOT_FILE_DELETED, 
        protection_flags = ProtectionFlags,
        eff_protection_flags = ?no_flags_mask,
        parent = detached_datasets:get_parent(SpaceId, DetachedDatasetPath),
        archive_count = archives_list:length(DatasetId),
        index = FinalIndex
    }.


-spec list_top_datasets_internal(od_space:id(), dataset:state(), listing_opts()) ->
    {ok, basic_entries(), boolean()}.
list_top_datasets_internal(SpaceId, ?ATTACHED_DATASET, Opts) ->
    attached_datasets:list_top_datasets(SpaceId, Opts);
list_top_datasets_internal(SpaceId, ?DETACHED_DATASET, Opts) ->
    detached_datasets:list_top_datasets(SpaceId, Opts).


-spec list_children_datasets_internal(dataset:id(), listing_opts()) ->
    {ok, basic_entries(), boolean()}.
list_children_datasets_internal(DatasetId, Opts) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, State} = dataset:get_state(Doc),
    case State of
        ?ATTACHED_DATASET -> attached_datasets:list_children_datasets(Doc, Opts);
        ?DETACHED_DATASET -> detached_datasets:list_children_datasets(Doc, Opts)
    end.


-spec extend_with_info(basic_entries()) -> extended_entries().
extend_with_info(DatasetEntries) ->
    FilterMapFun = fun({DatasetId, _DatasetName, Index}) ->
        try
            {true, collect_state_dependant_info(DatasetId, Index)}
        catch _:_ ->
            % Dataset can be not synchronized with other provider
            false
        end
    end,
    lists_utils:pfiltermap(FilterMapFun, DatasetEntries, ?MAX_LIST_EXTENDED_DATASET_INFO_PROCS).


-spec entry_index(dataset:id(), file_meta:path()) -> index().
entry_index(DatasetId, RootFilePath) ->
    DatasetName = filename:basename(RootFilePath),
    datasets_structure:pack_entry_index(DatasetName, DatasetId).