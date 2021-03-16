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
-export([establish/1, detach/1, reattach/1, remove/1]).
-export([get_info/1, update_attrs/1, get_summary/1]).
-export([list_top_datasets/3, list/2]).

-type id() :: file_meta:uuid().

-export_type([id/0]).


% todo sprawdzeine czy plik istnieje?
% TODO creation time
% TODO liczba archiwów
% TODO move, ale tylko na attached !!!
% TODO jak ogarnac konflikt polegajacy na zrobineiu datasetu rownolegle na tym samym pliku u dwoch providerow?

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
    {ok, SpaceId} = dataset:get_space_id(Doc),
    {ok, DatasetPath} = dataset_path:get(SpaceId, Uuid),
    {ok, FileRootPath} = paths_cache:get_canonical(SpaceId, Uuid),
    ok = dataset:mark_detached(DatasetId, DatasetPath, FileRootPath),
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


get_info(DatasetId) ->
    {ok, DatasetDoc} = dataset:get(DatasetId),
    CommonInfo = get_common_info(DatasetDoc),
    StateDependantInfo = get_state_dependant_info(DatasetDoc),
    {ok, maps:merge(CommonInfo, StateDependantInfo)}.


update_attrs(_Arg0) ->
    erlang:error(not_implemented).


get_summary(_Arg0) ->
    % todo is dataset
    % todo is in dataset
    erlang:error(not_implemented).


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
    #{
        <<"fileRootUuid">> => FileRootUuid,
        <<"creationTime">> => CreationTime
    }.


get_attached_info(DatasetDoc) ->
    {ok, Uuid} = dataset:get_uuid(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, FileDoc} = file_meta:get(Uuid),
    {ok, FileRootPath} = paths_cache:get_canonical(SpaceId, FileDoc),
    {ok, FileRootType} = file_meta:get_type(FileDoc),
    #{
        <<"fileRootPath">> => FileRootPath,
        <<"fileRootType">> => FileRootType,
        <<"parentDatasetId">> => todo % todo
    }.


get_detached_info(DatasetDoc) ->
    {ok, DetachedInfo} = dataset:get_detached_info(DatasetDoc),
    {ok, FileRootPath} = detached_dataset_info:get_file_root_path(DetachedInfo),
    {ok, FileRootType} = detached_dataset_info:get_file_root_type(DetachedInfo),
    {ok, DetachedDatasetPath} = detached_dataset_info:get_path(DetachedInfo),


    #{
        <<"fileRootPath">> => FileRootPath,
        <<"fileRootType">> => FileRootType,
        <<"parentDatasetId">> => todo % todo
    }.