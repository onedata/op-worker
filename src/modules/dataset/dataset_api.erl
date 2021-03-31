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

-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([establish/2, update/4, detach/1, remove/1, move_if_applicable/2]).
-export([get_info/1, get_effective_membership_and_protection_flags/1, get_effective_summary/1]).
-export([list_top_datasets/3, list_children_datasets/2]).
-export([get_associated_file_ctx/1]).

-type id() :: file_meta:uuid().
-type error() :: {error, term()}.

-export_type([id/0]).

% TODO VFS-7518 how should we handle race on creating dataset on the same file in 2 providers?

%%%===================================================================
%%% API functions
%%%===================================================================

-spec establish(file_ctx:ctx(), data_access_control:bitmask()) -> {ok, dataset:id()}.
establish(FileCtx, ProtectionFlags) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    Uuid = file_ctx:get_logical_uuid_const(FileCtx),
    {ok, DatasetId} = dataset:create(Uuid, SpaceId),
    {DatasetName, _FileCtx2} = file_ctx:get_aliased_name(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
    ok = file_meta:establish_dataset(Uuid, DatasetId, ProtectionFlags),
    ok = attached_datasets:add(SpaceId, DatasetId, Uuid, DatasetName),
    dataset_eff_cache:invalidate_on_all_nodes(SpaceId),
    {ok, DatasetId}.


-spec update(dataset:doc(), undefined | dataset:state(), data_access_control:bitmask(), data_access_control:bitmask()) -> ok.
update(DatasetDoc, NewState, FlagsToSet, FlagsToUnset) ->
    {ok, DatasetId} = dataset:get_id(DatasetDoc),
    {ok, CurrentState} = dataset:get_state(DatasetDoc),
    case {CurrentState, utils:ensure_defined(NewState, CurrentState)} of
        {?DETACHED_DATASET, ?ATTACHED_DATASET} ->
            reattach(DatasetId, FlagsToSet, FlagsToUnset);
        {?ATTACHED_DATASET, ?DETACHED_DATASET} ->
            % it's forbidden to change flags while detaching dataset
            case FlagsToSet == ?no_flags_mask andalso FlagsToUnset == ?no_flags_mask of
                true -> detach(DatasetId);
                false -> throw(?EINVAL)
            end;
        {?ATTACHED_DATASET, ?ATTACHED_DATASET} ->
            update_protection_flags(DatasetId, FlagsToSet, FlagsToUnset);
        {?DETACHED_DATASET, ?DETACHED_DATASET} ->
            throw(?EINVAL)
            %% TODO VFS-7208 uncomment after introducing API errors to fslogic
            % throw(?ERROR_BAD_DATA(state, <<"Detached dataset cannot be modified.">>))
    end.


-spec detach(dataset:id()) -> ok.
detach(DatasetId) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, Uuid} = dataset:get_uuid(Doc),
    {ok, FileDoc} = file_meta:get_including_deleted(Uuid),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    CurrProtectionFlags = file_meta:get_protection_flags(FileDoc),
    {ok, DatasetPath} = dataset_path:get(SpaceId, Uuid),
    FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId),
    {FileRootPath, _FileCtx2} = file_ctx:get_logical_path(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
    FileRootType = file_meta:get_type(FileDoc),
    DatasetName = filename:basename(FileRootPath),
    ok = dataset:mark_detached(DatasetId, DatasetPath, FileRootPath, FileRootType, CurrProtectionFlags),
    attached_datasets:delete(SpaceId, DatasetPath),
    detached_datasets:add(SpaceId, DatasetPath, DatasetId, DatasetName),
    file_meta:detach_dataset(Uuid),
    dataset_eff_cache:invalidate_on_all_nodes(SpaceId).


-spec remove(dataset:id() | dataset:doc()) -> ok.
remove(Doc = #document{key = DatasetId})->
    ok = remove_from_datasets_structure(Doc),
    {ok, Uuid} = dataset:get_uuid(Doc),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    ok = dataset:delete(DatasetId),
    ok = file_meta:remove_dataset(Uuid),
    dataset_eff_cache:invalidate_on_all_nodes(SpaceId);
remove(DatasetId) when is_binary(DatasetId) ->
    {ok, Doc} = dataset:get(DatasetId),
    remove(Doc).


-spec move_if_applicable(file_meta:doc(), file_meta:doc()) -> ok.
move_if_applicable(SourceDoc, TargetDoc) ->
    case file_meta:is_dataset_attached(TargetDoc) of
        false ->
            ok;
        true ->
            DatasetId = file_meta:get_dataset_id(TargetDoc),
            {ok, SpaceId} = file_meta:get_scope_id(SourceDoc),
            {ok, Uuid} = file_meta:get_uuid(SourceDoc),
            {ok, SourceParentUuid} = file_meta:get_parent_uuid(SourceDoc),
            {ok, TargetParentUuid} = file_meta:get_parent_uuid(TargetDoc),
            TargetName = file_meta:get_name(TargetDoc),
            attached_datasets:move(SpaceId, DatasetId, Uuid, SourceParentUuid, TargetParentUuid, TargetName)
    end.


-spec get_info(dataset:id()) -> {ok, dataset:info()}.
get_info(DatasetId) ->
    {ok, DatasetDoc} = dataset:get(DatasetId),
    CommonInfo = get_common_info(DatasetDoc),
    StateDependantInfo = get_state_dependant_info(DatasetDoc),
    {ok, maps:merge(CommonInfo, StateDependantInfo)}.


-spec get_effective_membership_and_protection_flags(file_ctx:ctx()) ->
    {ok, dataset:membership(), data_access_control:bitmask(), file_ctx:ctx()}.
get_effective_membership_and_protection_flags(FileCtx) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, EffCacheEntry} = dataset_eff_cache:get(FileDoc),
    {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(EffCacheEntry),
    {ok, EffProtectionFlags} = dataset_eff_cache:get_eff_protection_flags(EffCacheEntry),
    IsDirectAttached = file_meta:is_dataset_attached(FileDoc),
    EffMembership = case {IsDirectAttached, length(EffAncestorDatasets) =/= 0} of
        {true, _} -> ?DIRECT_DATASET_MEMBERSHIP;
        {false, true} -> ?ANCESTOR_DATASET_MEMBERSHIP;
        {false, false} -> ?NONE_DATASET_MEMBERSHIP
    end,
    {ok, EffMembership, EffProtectionFlags, FileCtx2}.


-spec get_effective_summary(file_ctx:ctx()) -> {ok, dataset:effective_summary()}.
get_effective_summary(FileCtx) ->
    {FileDoc, _FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, EffCacheEntry} = dataset_eff_cache:get(FileDoc),
    {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(EffCacheEntry),
    {ok, EffProtectionFlags} = dataset_eff_cache:get_eff_protection_flags(EffCacheEntry),
    {ok, #{
        <<"directDataset">> => file_meta:get_dataset_id(FileDoc),
        <<"effectiveAncestorDatasets">> => EffAncestorDatasets,
        <<"effectiveProtectionFlags">> =>  EffProtectionFlags
    }}.


-spec list_top_datasets(od_space:id(), dataset:state(), datasets_structure:opts()) ->
    {ok, datasets_structure:entries(), boolean()}.
list_top_datasets(SpaceId, ?ATTACHED_DATASET, Opts) ->
    attached_datasets:list_top_datasets(SpaceId, Opts);
list_top_datasets(SpaceId, ?DETACHED_DATASET, Opts) ->
    detached_datasets:list_top_datasets(SpaceId, Opts).


-spec list_children_datasets(dataset:id(), datasets_structure:opts()) ->
    {ok, datasets_structure:entries(), boolean()}.
list_children_datasets(DatasetId, Opts) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, State} = dataset:get_state(Doc),
    case State of
        ?ATTACHED_DATASET -> attached_datasets:list_children_datasets(Doc, Opts);
        ?DETACHED_DATASET -> detached_datasets:list(Doc, Opts)
    end.


-spec get_associated_file_ctx(dataset:doc()) -> file_ctx:ctx().
get_associated_file_ctx(DatasetDoc) ->
    {ok, Uuid} = dataset:get_uuid(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    file_ctx:new_by_uuid(Uuid, SpaceId).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec reattach(dataset:id(), data_access_control:bitmask(), data_access_control:bitmask()) -> ok | error().
reattach(DatasetId, FlagsToSet, FlagsToUnset) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, Uuid} = dataset:get_uuid(Doc),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    case file_meta:reattach_dataset(Uuid, DatasetId, FlagsToSet, FlagsToUnset) of
        ok ->
            FileCtx = get_associated_file_ctx(Doc),
            {DatasetName, _} = file_ctx:get_aliased_name(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
            ok = dataset:mark_reattached(DatasetId),
            detached_datasets:delete(Doc),
            attached_datasets:add(SpaceId, DatasetId, Uuid, DatasetName),
            dataset_eff_cache:invalidate_on_all_nodes(SpaceId),
            ok;
        {error, _} = Error ->
            Error
    end.


-spec update_protection_flags(dataset:id(), data_access_control:bitmask(), data_access_control:bitmask()) -> ok.
update_protection_flags(DatasetId, FlagsToSet, FlagsToUnset) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    {ok, Uuid} = dataset:get_uuid(Doc),
    ok = file_meta:update_protection_flags(Uuid, FlagsToSet, FlagsToUnset),
    dataset_eff_cache:invalidate_on_all_nodes(SpaceId).

-spec remove_from_datasets_structure(dataset:doc()) -> ok.
remove_from_datasets_structure(Doc) ->
    {ok, State} = dataset:get_state(Doc),
    case State of
        ?ATTACHED_DATASET -> attached_datasets:delete(Doc);
        ?DETACHED_DATASET -> detached_datasets:delete(Doc)
    end.


-spec get_state_dependant_info(dataset:doc()) -> dataset:info().
get_state_dependant_info(DatasetDoc) ->
    {ok, State} = dataset:get_state(DatasetDoc),
    case State of
        ?ATTACHED_DATASET -> get_attached_info(DatasetDoc);
        ?DETACHED_DATASET -> get_detached_info(DatasetDoc)
    end.


-spec get_common_info(dataset:doc()) -> dataset:info().
get_common_info(DatasetDoc) ->
    {ok, CreationTime} = dataset:get_creation_time(DatasetDoc),
    {ok, FileRootUuid} = dataset:get_uuid(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    #{
        <<"fileRootGuid">> => file_id:pack_guid(FileRootUuid, SpaceId),
        <<"creationTime">> => CreationTime
    }.


-spec get_attached_info(dataset:doc()) -> dataset:info().
get_attached_info(DatasetDoc) ->
    {ok, Uuid} = dataset:get_uuid(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, FileDoc} = file_meta:get(Uuid),
    FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId),
    {FileRootPath, _FileCtx2} = file_ctx:get_logical_path(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
    FileRootType = file_meta:get_type(FileDoc),
    {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(FileDoc),
    #{
        <<"state">> => ?ATTACHED_DATASET,
        <<"fileRootPath">> => FileRootPath,
        <<"fileRootType">> => FileRootType,
        <<"protectionFlags">> => file_meta:get_protection_flags(FileDoc),
        <<"parentDatasetId">> => case length(EffAncestorDatasets) == 0 of
            true -> undefined;
            false -> hd(EffAncestorDatasets)
        end
    }.


-spec get_detached_info(dataset:doc()) -> dataset:info().
get_detached_info(DatasetDoc) ->
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, DetachedInfo} = dataset:get_detached_info(DatasetDoc),
    FileRootPath = detached_dataset_info:get_file_root_path(DetachedInfo),
    FileRootType = detached_dataset_info:get_file_root_type(DetachedInfo),
    DetachedDatasetPath = detached_dataset_info:get_path(DetachedInfo),
    ProtectionFlags = detached_dataset_info:get_protection_flags(DetachedInfo),
    #{
        <<"state">> => ?DETACHED_DATASET,
        <<"fileRootPath">> => FileRootPath,
        <<"fileRootType">> => FileRootType,
        <<"protectionFlags">> => ProtectionFlags,
        <<"parentDatasetId">> => detached_datasets:get_parent(SpaceId, DetachedDatasetPath)
    }.