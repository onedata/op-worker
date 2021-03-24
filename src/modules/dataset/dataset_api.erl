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
-include_lib("ctool/include/logging.hrl").

%% API
-export([establish/1, detach/1, reattach/1, remove/1, move_if_applicable/2]).
-export([get_info/1, get_effective_membership/1, get_effective_summary/1]).
-export([list_top_datasets/3, list/2]).

-type id() :: file_meta:uuid().
-type error() :: {error, term()}.

-export_type([id/0]).

% TODO VFS-7363 how should we handle race on creating dataset on the same file in 2 providers?
 %TODO VFS-7373 handle rename between spaces and rename which changes file-guid

%%%===================================================================
%%% API functions
%%%===================================================================

-spec establish(file_ctx:ctx()) -> {ok, dataset:id()}.
establish(FileCtx) ->
    % TODO VFS-7363 ustawienie flag protected
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    Uuid = file_ctx:get_uuid_const(FileCtx),
    {ok, DatasetId} = dataset:create(Uuid, SpaceId),
    {DatasetName, _FileCtx2} = file_ctx:get_aliased_name(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
    ok = file_meta:establish_dataset(Uuid, DatasetId),
    ok = attached_datasets:add(SpaceId, DatasetId, Uuid, DatasetName),
    {ok, DatasetId}.


-spec detach(dataset:id()) -> ok.
detach(DatasetId) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, Uuid} = dataset:get_uuid(Doc),
    {ok, FileDoc} = file_meta:get_including_deleted(Uuid),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    {ok, DatasetPath} = dataset_path:get(SpaceId, Uuid),
    FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId),
    {FileRootPath, _FileCtx2} = file_ctx:get_logical_path(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
    FileRootType = file_meta:get_type(FileDoc),
    DatasetName = filename:basename(FileRootPath),
    ok = dataset:mark_detached(DatasetId, DatasetPath, FileRootPath, FileRootType),
    attached_datasets:delete(SpaceId, DatasetPath),
    detached_datasets:add(SpaceId, DatasetPath, DatasetId, DatasetName),
    file_meta:detach_dataset(Uuid),
    dataset_eff_cache:invalidate_on_all_nodes(SpaceId).


-spec reattach(dataset:id()) -> ok | error().
reattach(DatasetId) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, Uuid} = dataset:get_uuid(Doc),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    FileCtx = file_ctx:new_by_guid(file_id:pack_guid(Uuid, SpaceId)),
    case file_meta:reattach_dataset(Uuid, DatasetId) of
        ok ->
            {DatasetName, _} = file_ctx:get_aliased_name(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
            ok = dataset:mark_reattached(DatasetId),
            detached_datasets:delete(Doc),
            attached_datasets:add(SpaceId, DatasetId, Uuid, DatasetName),
            dataset_eff_cache:invalidate_on_all_nodes(SpaceId),
            ok;
        {error, _} = Error ->
            Error
    end.


-spec remove(dataset:id()) -> ok.
remove(DatasetId) ->
    {ok, Doc} = dataset:get(DatasetId),
    ok = remove_from_datasets_structure(Doc),
    {ok, Uuid} = dataset:get_uuid(Doc),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    ok = dataset:delete(DatasetId),
    ok = file_meta:remove_dataset(Uuid),
    dataset_eff_cache:invalidate_on_all_nodes(SpaceId).


-spec move_if_applicable(file_meta:doc(), file_meta:doc()) -> ok.
move_if_applicable(SourceDoc, TargetDoc) ->
    case file_meta:is_dataset_attached(TargetDoc) of
        false ->
            ok;
        true ->
            DatasetId = file_meta:get_dataset(TargetDoc),
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


-spec get_effective_membership(file_ctx:ctx()) -> dataset:membership().
get_effective_membership(FileCtx) ->
    {FileDoc, _FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(FileDoc),
    IsDirectAttached = file_meta:is_dataset_attached(FileDoc),
    case {IsDirectAttached, length(EffAncestorDatasets) =/= 0} of
        {true, _} -> ?DIRECT_DATASET_MEMBERSHIP;
        {false, true} -> ?ANCESTOR_DATASET_MEMBERSHIP;
        {false, false} -> ?NONE_DATASET_MEMBERSHIP
    end.


-spec get_effective_summary(file_ctx:ctx()) -> {ok, dataset:effective_summary()}.
get_effective_summary(FileCtx) ->
    {FileDoc, _FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(FileDoc),
    {ok, #{
        <<"directDataset">> => file_meta:get_dataset(FileDoc),
        <<"effectiveAncestorDatasets">> => EffAncestorDatasets
    }}.


-spec list_top_datasets(od_space:id(), dataset:state(), datasets_structure:opts()) ->
    {ok, datasets_structure:entries(), boolean()}.
list_top_datasets(SpaceId, ?ATTACHED_DATASET, Opts) ->
    attached_datasets:list_top_datasets(SpaceId, Opts);
list_top_datasets(SpaceId, ?DETACHED_DATASET, Opts) ->
    detached_datasets:list_top_datasets(SpaceId, Opts).


-spec list(dataset:id(), datasets_structure:opts()) ->
    {ok, datasets_structure:entries(), boolean()}.
list(DatasetId, Opts) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, State} = dataset:get_state(Doc),
    case State of
        ?ATTACHED_DATASET -> attached_datasets:list(Doc, Opts);
        ?DETACHED_DATASET -> detached_datasets:list(Doc, Opts)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

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
    #{
        <<"state">> => ?DETACHED_DATASET,
        <<"fileRootPath">> => FileRootPath,
        <<"fileRootType">> => FileRootType,
        <<"parentDatasetId">> => detached_datasets:get_parent(SpaceId, DetachedDatasetPath)
    }.