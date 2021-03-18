%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_api).
-author("Jakub Kudzia").

-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/fslogic_common.hrl").


%% API
-export([establish/1, detach/1, reattach/1, remove/1, move_if_applicable/2]).
-export([get_info/1, get_effective_membership/1, get_effective_summary/1]).
-export([list_top_datasets/3, list/2]).

-type id() :: file_meta:uuid().

-export_type([id/0]).


% todo sprawdzeine czy plik istnieje?
% TODO creation time
% TODO liczba archiwów
% TODO move, ale tylko na attached !!!
% TODO jak ogarnac konflikt polegajacy na zrobineiu datasetu rownolegle na tym samym pliku u dwoch providerow?
 %TODO handle rename between spaces and rename which changes file-guid
%%%===================================================================
%%% API functions
%%%===================================================================

establish(FileCtx) ->
    % TODO ustawienie flag protected?
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    Uuid = file_ctx:get_uuid_const(FileCtx),
    {ok, DatasetId} = dataset:create(Uuid, SpaceId),
    ok = file_meta:establish_dataset(Uuid, DatasetId),
    ok = attached_datasets:add(SpaceId, DatasetId, Uuid),
    {ok, DatasetId}.


detach(DatasetId) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, Uuid} = dataset:get_uuid(Doc),
    {ok, FileDoc} = file_meta:get(Uuid),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    {ok, DatasetPath} = dataset_path:get(SpaceId, Uuid),
    {ok, FileRootPath} = paths_cache:get_canonical(SpaceId, Uuid),
    FileRootType = file_meta:get_type(FileDoc),
    ok = dataset:mark_detached(DatasetId, DatasetPath, FileRootPath, FileRootType),
    attached_datasets:delete(SpaceId, DatasetPath),
    detached_datasets:add(SpaceId, DatasetPath, DatasetId),
    file_meta:detach_dataset(Uuid),
    dataset_eff_cache:invalidate_on_all_nodes(SpaceId).


reattach(DatasetId) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, Uuid} = dataset:get_uuid(Doc),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    ok = dataset:mark_reattached(DatasetId),
    detached_datasets:delete(Doc),
    attached_datasets:add(SpaceId, DatasetId, Uuid),
    file_meta:reattach_dataset(Uuid, DatasetId),
    dataset_eff_cache:invalidate_on_all_nodes(SpaceId).


remove(DatasetId) ->
    {ok, Doc} = dataset:get(DatasetId),
    ok = remove_from_datasets_structure(Doc),
    {ok, Uuid} = dataset:get_uuid(Doc),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    ok = dataset:delete(DatasetId),
    ok = file_meta:detach_dataset(Uuid),
    dataset_eff_cache:invalidate_on_all_nodes(SpaceId).


move_if_applicable(SourceDoc, TargetDoc) ->
    case file_meta:is_dataset_attached(TargetDoc) of
        false ->
            ok;
        true ->
            DatasetId = file_meta:get_dataset(TargetDoc),
            SpaceId = file_meta:get_scope_id(SourceDoc),
            {ok, Uuid} = file_meta:get_uuid(SourceDoc),
            {ok, SourceParentUuid} = file_meta:get_parent_uuid(SourceDoc),
            {ok, TargetParentUuid} = file_meta:get_parent_uuid(TargetDoc),
            attached_datasets:move(SpaceId, DatasetId, Uuid, SourceParentUuid, TargetParentUuid)
    end.


get_info(DatasetId) ->
    {ok, DatasetDoc} = dataset:get(DatasetId),
    CommonInfo = get_common_info(DatasetDoc),
    StateDependantInfo = get_state_dependant_info(DatasetDoc),
    {ok, maps:merge(CommonInfo, StateDependantInfo)}.


get_effective_membership(FileCtx) ->
    {FileDoc, _FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(FileDoc),
    {ok, IsDirectAttached} = file_meta:is_dataset_attached(FileDoc),
    case {IsDirectAttached, size(EffAncestorDatasets) == 0} of
        {true, _} -> ?DIRECT_DATASET_MEMBERSHIP;
        {false, true} -> ?ANCESTOR_DATASET_MEMBERSHIP;
        {false, false} -> ?NONE_DATASET_MEMBERSHIP
    end.

get_effective_summary(FileCtx) ->
    {FileDoc, _FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(FileDoc),
    {ok, #{
        <<"directDataset">> => file_meta:get_dataset(FileDoc),
        <<"effectiveAncestorDatasets">> => EffAncestorDatasets
    }}.


list_top_datasets(SpaceId, ?ATTACHED_DATASET, Opts) ->
    attached_datasets:list_top_datasets(SpaceId, Opts);
list_top_datasets(SpaceId, ?DETACHED_DATASET, Opts) ->
    detached_datasets:list_top_datasets(SpaceId, Opts).


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

remove_from_datasets_structure(Doc) ->
    {ok, State} = dataset:get_state(Doc),
    case State of
        ?ATTACHED_DATASET -> attached_datasets:delete(Doc);
        ?DETACHED_DATASET -> detached_datasets:delete(Doc)
    end.


get_state_dependant_info(DatasetDoc) ->
    {ok, State} = dataset:get_state(State),
    case State of
        ?ATTACHED_DATASET -> get_attached_info(DatasetDoc);
        ?DETACHED_DATASET -> get_detached_info(DatasetDoc)
    end.


get_common_info(DatasetDoc) ->
    {ok, CreationTime} = dataset:get_creation_time(DatasetDoc),
    {ok, FileRootUuid} = dataset:get_uuid(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    #{
        <<"fileRootGuid">> => file_id:pack_guid(FileRootUuid, SpaceId),
        <<"creationTime">> => CreationTime
    }.


get_attached_info(DatasetDoc) ->
    {ok, Uuid} = dataset:get_uuid(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, FileDoc} = file_meta:get(Uuid),
    {ok, FileRootPath} = paths_cache:get_canonical(SpaceId, FileDoc),
    {ok, FileRootType} = file_meta:get_type(FileDoc),
    {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(FileDoc),
    #{
        <<"fileRootPath">> => FileRootPath,
        <<"fileRootType">> => FileRootType,
        <<"parentDatasetId">> => case size(EffAncestorDatasets) == 0 of
            true -> undefined;
            false -> hd(EffAncestorDatasets)
        end
    }.


get_detached_info(DatasetDoc) ->
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, DetachedInfo} = dataset:get_detached_info(DatasetDoc),
    {ok, FileRootPath} = detached_dataset_info:get_file_root_path(DetachedInfo),
    {ok, FileRootType} = detached_dataset_info:get_file_root_type(DetachedInfo),
    {ok, DetachedDatasetPath} = detached_dataset_info:get_path(DetachedInfo),
    #{
        <<"fileRootPath">> => FileRootPath,
        <<"fileRootType">> => FileRootType,
        <<"parentDatasetId">> => detached_datasets:get_parent(SpaceId, DetachedDatasetPath)
    }.