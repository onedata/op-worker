%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module which implements datasets structure for storing
%%% attached dataset entries.
%%% Under the hood it uses datasets_structure module.
%%% @end
%%%-------------------------------------------------------------------
-module(attached_datasets).
-author("Jakub Kudzia").

-include("modules/dataset/dataset.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([add/4, delete/1, delete/2, list_top_datasets/2, list_children_datasets/2, move/6]).

-define(FOREST_TYPE, <<"ATTACHED">>).

-type error() :: {error, term()}.

%%%===================================================================
%%% API functions
%%%===================================================================

-spec add(od_space:id(), dataset:id(), file_meta:uuid(), dataset:name()) -> ok | error().
add(SpaceId, DatasetId, Uuid, DatasetName) ->
    {ok, DatasetPath} = dataset_path:get(SpaceId, Uuid),
    datasets_structure:add(SpaceId, ?FOREST_TYPE, DatasetPath, DatasetId, DatasetName).


-spec delete(dataset:doc()) -> ok.
delete(DatasetDoc) ->
    {ok, Uuid} = dataset:get_uuid(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, DatasetPath} = dataset_path:get(SpaceId, Uuid),
    delete(SpaceId, DatasetPath).


-spec delete(od_space:id(), dataset:path()) -> ok.
delete(SpaceId, DatasetPath) ->
    datasets_structure:delete(SpaceId, ?FOREST_TYPE, DatasetPath).


-spec list_top_datasets(od_space:id(), datasets_structure:opts()) -> {ok, datasets_structure:entries(), boolean()}.
list_top_datasets(SpaceId, Opts) ->
    datasets_structure:list_top_datasets(SpaceId, ?FOREST_TYPE, Opts).


-spec list_children_datasets(dataset:doc(), datasets_structure:opts()) -> {ok, datasets_structure:entries(), boolean()}.
list_children_datasets(DatasetDoc, Opts) ->
    {ok, Uuid} = dataset:get_uuid(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, DatasetPath} = dataset_path:get(SpaceId, Uuid),
    datasets_structure:list_children_datasets(SpaceId, ?FOREST_TYPE, DatasetPath, Opts).


-spec move(od_space:id(), dataset:id(), file_meta:uuid(), file_meta:uuid(), file_meta:uuid(), dataset:name()) -> ok.
move(SpaceId, DatasetId, Uuid, SourceParentUuid, TargetParentUuid, TargetName) ->
    {ok, SourceParentDatasetPath} = dataset_path:get(SpaceId, SourceParentUuid),
    {ok, TargetParentDatasetPath} = dataset_path:get(SpaceId, TargetParentUuid),
    SourceDatasetPath = filename:join([SourceParentDatasetPath, Uuid]),
    TargetDatasetPath = filename:join([TargetParentDatasetPath, Uuid]),
    datasets_structure:move(SpaceId, ?FOREST_TYPE, DatasetId, SourceDatasetPath, TargetDatasetPath, TargetName).

