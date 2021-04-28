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
-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([establish/2, update/4, detach/1, remove/1, move_if_applicable/2]).
-export([get_info/1, get_effective_membership_and_protection_flags/1, get_effective_summary/1]).
-export([list_top_datasets/4, list_children_datasets/3]).

%% Archives API
-export([archive/3, update_archive/2, get_archive_info/1, list_archives/3, remove_archive/1]).

%% Utils
-export([get_associated_file_ctx/1]).

-type error() :: {error, term()}.

-type info() :: #dataset_info{}.
-type basic_entry() :: datasets_structure:entry().
-type basic_entries() :: [basic_entry()].
-type extended_entries() :: [info()].
-type entries() :: basic_entries() | extended_entries().

-type archive_info() :: #archive_info{}.
-type basic_archive_entries() :: [archives_list:entry()].
-type extended_archive_entries() :: [archive_info()].
-type archive_entries() :: basic_archive_entries() | extended_archive_entries().
-type archive_index() :: archives_list:index().

-type listing_opts() :: datasets_structure:opts().
-type listing_mode() :: ?BASIC_INFO | ?EXTENDED_INFO.
-type index() :: datasets_structure:index().

-export_type([entries/0, listing_opts/0, index/0, listing_mode/0, archive_entries/0, archive_index/0]).

% Datasets
% TODO VFS-7518 how should we handle race on creating dataset on the same file in 2 providers?
% TODO VFS-7533 handle conflicts on remote modification of file-meta and dataset models
% TODO VFS-7563 add tests concerning datasets to permissions test suites

% Archives
% TODO VFS-7548 API for archives
% TODO VFS-7601 implement archivisation procedure
% TODO VFS-7613 use datastore function for getting number of links in forest to acquire number of archives per dataset
% TODO VFS-7616 refine archives' attributes
% TODO VFS-7617 implement recall operation of archives
% TODO VFS-7619 add tests concerning archives to permissions test suites

-define(CRITICAL_SECTION(DatasetId, Function), critical_section:run({dataset, DatasetId}, Function)).

-define(MAX_LIST_EXTENDED_DATASET_INFO_PROCS,
    application:get_env(?APP_NAME, max_list_extended_dataset_info_procs, 20)).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec establish(file_ctx:ctx(), data_access_control:bitmask()) -> {ok, dataset:id()}.
establish(FileCtx, ProtectionFlags) ->
    ?CRITICAL_SECTION(file_ctx:get_logical_uuid_const(FileCtx), fun() ->
        SpaceId = file_ctx:get_space_id_const(FileCtx),
        Uuid = file_ctx:get_logical_uuid_const(FileCtx),
        ok = dataset:create(Uuid, SpaceId),
        {DatasetName, _FileCtx2} = file_ctx:get_aliased_name(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
        ok = file_meta_dataset:establish(Uuid, ProtectionFlags),
        ok = attached_datasets:add(SpaceId, Uuid, DatasetName),
        dataset_eff_cache:invalidate_on_all_nodes(SpaceId),
        {ok, Uuid}
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
                {error, ?EINVAL};
                %% TODO VFS-7208 uncomment after introducing API errors to fslogic
                % throw(?ERROR_BAD_DATA(state, <<"Detached dataset cannot be modified.">>));
            {?ATTACHED_DATASET, ?DETACHED_DATASET, ?no_flags_mask, ?no_flags_mask} ->
                % it's forbidden to change flags while detaching dataset
                detach(DatasetId);
            {?ATTACHED_DATASET, ?DETACHED_DATASET, _, _} ->
                {error, ?EINVAL};
            {?ATTACHED_DATASET, undefined, _, _} ->
                update_protection_flags(DatasetId, FlagsToSet, FlagsToUnset)
        end
    end).


-spec detach(dataset:id()) -> ok | error().
detach(DatasetId) ->
    ?CRITICAL_SECTION(DatasetId, fun() -> detach_internal(DatasetId) end).


-spec remove(dataset:id() | dataset:doc()) -> ok | error().
remove(Doc = #document{key = DatasetId})->
    ?CRITICAL_SECTION(DatasetId, fun() ->
        case archives_list:is_empty(DatasetId) of
            true ->
                ok = remove_from_datasets_structure(Doc),
                {ok, SpaceId} = dataset:get_space_id(Doc),
                ok = dataset:delete(DatasetId),
                ok = file_meta_dataset:remove(DatasetId),
                dataset_eff_cache:invalidate_on_all_nodes(SpaceId);
            false ->
                {error, ?ENOTEMPTY}
        end
    end);
remove(DatasetId) when is_binary(DatasetId) ->
    {ok, Doc} = dataset:get(DatasetId),
    remove(Doc).


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


-spec get_info(dataset:id()) -> {ok, #dataset_info{}}.
get_info(DatasetId) ->
    {ok, collect_state_dependant_info(DatasetId)}.


-spec get_effective_membership_and_protection_flags(file_ctx:ctx()) ->
    {ok, dataset:membership(), data_access_control:bitmask(), file_ctx:ctx()}.
get_effective_membership_and_protection_flags(FileCtx) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, EffCacheEntry} = dataset_eff_cache:get(FileDoc),
    {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(EffCacheEntry),
    {ok, EffProtectionFlags} = dataset_eff_cache:get_eff_file_protection_flags(EffCacheEntry),
    IsDirectAttached = file_meta_dataset:is_attached(FileDoc),
    EffMembership = case {IsDirectAttached, length(EffAncestorDatasets) =/= 0} of
        {true, _} -> ?DIRECT_DATASET_MEMBERSHIP;
        {false, true} -> ?ANCESTOR_DATASET_MEMBERSHIP;
        {false, false} -> ?NONE_DATASET_MEMBERSHIP
    end,
    {ok, EffMembership, EffProtectionFlags, FileCtx2}.


-spec get_effective_summary(file_ctx:ctx()) -> {ok, #file_eff_dataset_summary{}}.
get_effective_summary(FileCtx) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, EffCacheEntry} = dataset_eff_cache:get(FileDoc),
    {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(EffCacheEntry),
    {ok, EffProtectionFlags} = dataset_eff_cache:get_eff_file_protection_flags(EffCacheEntry),
    DatasetState = file_meta_dataset:get_state(FileDoc),
    DirectDataset = case DatasetState =:= ?ATTACHED_DATASET orelse DatasetState =:= ?DETACHED_DATASET of
        true -> file_ctx:get_logical_uuid_const(FileCtx2);
        false -> undefined
    end,
    {ok, #file_eff_dataset_summary{
        direct_dataset = DirectDataset,
        eff_ancestor_datasets = EffAncestorDatasets,
        eff_protection_flags = EffProtectionFlags
    }}.


-spec list_top_datasets(od_space:id(), dataset:state(), listing_opts(), listing_mode()) ->
    {ok, entries(), boolean()}.
list_top_datasets(SpaceId, State, Opts, ListingMode) ->
    {ok, DatasetEntries, IsLast} = list_top_datasets_internal(SpaceId, State, Opts),
    case ListingMode of
        ?BASIC_INFO ->
            {ok, DatasetEntries, IsLast};
        ?EXTENDED_INFO ->
            {ok, extend_with_info(DatasetEntries), IsLast}
    end.


-spec list_children_datasets(dataset:id(), listing_opts(), listing_mode()) ->
    {ok, entries(), boolean()}.
list_children_datasets(DatasetId, Opts, ListingMode) ->
    {ok, DatasetEntries, IsLast} = list_children_datasets_internal(DatasetId, Opts),
    case ListingMode of
        ?BASIC_INFO ->
            {ok, DatasetEntries, IsLast};
        ?EXTENDED_INFO ->
            {ok, extend_with_info(DatasetEntries), IsLast}
    end.

%%%===================================================================
%%% Archives API
%%%===================================================================

-spec archive(dataset:id(), archive:params(), od_user:id()) -> {ok, archive:id()} | error().
archive(DatasetId, Params, UserId) ->
    {ok, DatasetDoc} = dataset:get(DatasetId),
    {ok, State} = dataset:get_state(DatasetDoc),
    case State of
        ?ATTACHED_DATASET ->
            {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
            case archive:create(DatasetId, SpaceId, UserId, Params) of
                {ok, ArchiveDoc} ->
                    ArchiveId = archive:get_id(ArchiveDoc),
                    Timestamp = archive:get_timestamp(ArchiveDoc),
                    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
                    archives_list:add(DatasetId, SpaceId, ArchiveId, Timestamp),
                    {ok, ArchiveId};
                    % TODO VFS-7601 schedule archivisation job here
                {error, Error} ->
                    Error
            end;
        ?DETACHED_DATASET ->
            {error, ?EINVAL}
        %% TODO VFS-7208 uncomment after introducing API errors to fslogic
        % throw(?ERROR_BAD_DATA(state, <<"Detached dataset cannot be modified.">>));
    end.


-spec update_archive(archive:id(), archive:params()) -> ok | error().
update_archive(ArchiveId, Params) ->
    archive:update(ArchiveId, Params).


-spec get_archive_info(archive:id()) -> {ok, archive_info()}.
get_archive_info(ArchiveId) ->
    get_archive_info(ArchiveId, undefined).


%% @private
-spec get_archive_info(archive:id() | archive:doc(), archive_index() | undefined) -> {ok, archive_info()}.
get_archive_info(ArchiveDoc = #document{}, ArchiveIndex) ->
    ArchiveId = archive:get_id(ArchiveDoc),
    Timestamp = archive:get_timestamp(ArchiveDoc),
    {ok, #archive_info{
        id = ArchiveId,
        dataset_id = archive:get_dataset_id(ArchiveDoc),
        % DatasetId is also Uuid of file on which the dataset is established
        root_dir = archive:get_root_dir(ArchiveDoc),
        creation_timestamp = archive:get_timestamp(ArchiveDoc),
        type = archive:get_type(ArchiveDoc),
        character = archive:get_character(ArchiveDoc),
        data_structure = archive:get_data_structure(ArchiveDoc),
        metadata_structure = archive:get_metadata_structure(ArchiveDoc),
        description = archive:get_description(ArchiveDoc),
        index = case ArchiveIndex =:= undefined of
            true -> archives_list:index(ArchiveId, Timestamp);
            false -> ArchiveIndex
        end
    }};
get_archive_info(ArchiveId, ArchiveIndex) ->
    {ok, ArchiveDoc} = archive:get(ArchiveId),
    get_archive_info(ArchiveDoc, ArchiveIndex).


-spec list_archives(dataset:id(), archives_list:opts(), listing_mode()) ->
        {ok, archive_entries(), IsLast :: boolean()}.
list_archives(DatasetId, ListingOpts, ListingMode) ->
    ArchiveEntries = archives_list:list(DatasetId, ListingOpts),
    IsLast = maps:get(limit, ListingOpts) > length(ArchiveEntries),
    case ListingMode of
        ?BASIC_INFO ->
            {ok, ArchiveEntries, IsLast};
        ?EXTENDED_INFO ->
            {ok, extend_with_archive_info(ArchiveEntries), IsLast}
    end.


-spec remove_archive(archive:id()) -> ok | error().
remove_archive(ArchiveId) ->
    case archive:get(ArchiveId) of
        {ok, ArchiveDoc} ->
            case archive:delete(ArchiveId) of
                ok ->
                    DatasetId = archive:get_dataset_id(ArchiveDoc),
                    Timestamp = archive:get_timestamp(ArchiveDoc),
                    SpaceId = archive:get_space_id(ArchiveDoc),
                    archives_list:delete(DatasetId, SpaceId, ArchiveId, Timestamp);
                ?ERROR_NOT_FOUND ->
                    % there was race with other process removing the archive
                    ok
            end;
        ?ERROR_NOT_FOUND ->
            ok;
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% Util functions
%%%===================================================================

-spec get_associated_file_ctx(dataset:doc()) -> file_ctx:ctx().
get_associated_file_ctx(DatasetDoc) ->
    {ok, Uuid} = dataset:get_id(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    file_ctx:new_by_uuid(Uuid, SpaceId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec reattach(dataset:id(), data_access_control:bitmask(), data_access_control:bitmask()) -> ok | error().
reattach(DatasetId, FlagsToSet, FlagsToUnset) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    case file_meta_dataset:reattach(DatasetId, FlagsToSet, FlagsToUnset) of
        ok ->
            FileCtx = get_associated_file_ctx(Doc),
            {DatasetName, _} = file_ctx:get_aliased_name(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
            ok = dataset:mark_reattached(DatasetId),
            attached_datasets:add(SpaceId, DatasetId, DatasetName),
            detached_datasets:delete(Doc),
            dataset_eff_cache:invalidate_on_all_nodes(SpaceId),
            ok;
        {error, _} = Error ->
            Error
    end.


-spec detach_internal(dataset:id()) -> ok | error().
detach_internal(DatasetId) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, FileDoc} = file_meta:get_including_deleted(DatasetId),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    CurrProtectionFlags = file_meta:get_protection_flags(FileDoc),
    {ok, DatasetPath} = dataset_path:get(SpaceId, DatasetId),
    FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId),
    {FilePath, _FileCtx2} = file_ctx:get_logical_path(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
    FileType = file_meta:get_effective_type(FileDoc),
    DatasetName = filename:basename(FilePath),
    ok = dataset:mark_detached(DatasetId, DatasetPath, FilePath, FileType, CurrProtectionFlags),
    detached_datasets:add(SpaceId, DatasetPath, DatasetName),
    attached_datasets:delete(SpaceId, DatasetPath),
    case file_meta_dataset:detach(DatasetId) of
        ok -> dataset_eff_cache:invalidate_on_all_nodes(SpaceId);
        Error -> Error
    end.


-spec update_protection_flags(dataset:id(), data_access_control:bitmask(), data_access_control:bitmask()) -> ok.
update_protection_flags(DatasetId, FlagsToSet, FlagsToUnset) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    ok = file_meta:update_protection_flags(DatasetId, FlagsToSet, FlagsToUnset),
    dataset_eff_cache:invalidate_on_all_nodes(SpaceId).

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
    {ok, DatasetId = Uuid} = dataset:get_id(DatasetDoc),
    {ok, CreationTime} = dataset:get_creation_time(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
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
        archives_count = archives_list:length(DatasetId),
        index = FinalIndex
    }.


-spec collect_detached_info(dataset:doc(), index() | undefined) -> info().
collect_detached_info(DatasetDoc, IndexOrUndefined) ->
    {ok, DatasetId = Uuid} = dataset:get_id(DatasetDoc),
    {ok, CreationTime} = dataset:get_creation_time(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, DetachedInfo} = dataset:get_detached_info(DatasetDoc),
    RootFilePath = detached_dataset_info:get_root_file_path(DetachedInfo),
    RootFileType = detached_dataset_info:get_root_file_type(DetachedInfo),
    DetachedDatasetPath = detached_dataset_info:get_path(DetachedInfo),
    ProtectionFlags = detached_dataset_info:get_protection_flags(DetachedInfo),
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
        protection_flags = ProtectionFlags,
        eff_protection_flags = ?no_flags_mask,
        parent = detached_datasets:get_parent(SpaceId, DetachedDatasetPath),
        archives_count = archives_list:length(DatasetId),
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


-spec extend_with_archive_info(basic_archive_entries()) -> extended_archive_entries().
extend_with_archive_info(ArchiveEntries) ->
    FilterMapFun = fun({ArchiveIndex, ArchiveId}) ->
        try
            {ok, ArchiveInfo} = get_archive_info(ArchiveId, ArchiveIndex),
            {true, ArchiveInfo}
        catch _:_ ->
            % Archive can be not synchronized with other provider
            false
        end
    end,
    lists_utils:pfiltermap(FilterMapFun, ArchiveEntries, ?MAX_LIST_EXTENDED_DATASET_INFO_PROCS).


-spec entry_index(dataset:id(), file_meta:path()) -> index().
entry_index(DatasetId, RootFilePath) ->
    DatasetName = filename:basename(RootFilePath),
    datasets_structure:pack_entry_index(DatasetName, DatasetId).