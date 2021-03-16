%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(detached_datasets).
-author("Jakub Kudzia").

-include("modules/dataset/dataset.hrl").

%% API
-export([add/3, get/2, delete/1, list_top_datasets/2, list/2]).

-define(FOREST_TYPE, ?DETACHED_DATASET).

%%%===================================================================
%%% API functions
%%%===================================================================

add(SpaceId, DatasetPath, DatasetId) ->
    datasets_structure:add(SpaceId, ?FOREST_TYPE, DatasetPath, DatasetId).


get(SpaceId, DatasetPath) ->
    datasets_structure:get(SpaceId, ?FOREST_TYPE, DatasetPath).


delete(DatasetDoc) ->
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, DetachedDatasetInfo} = dataset:get_detached_info(DatasetDoc),
    {ok, DetachedDatasetPath} = detached_dataset_info:get_path(DetachedDatasetInfo),
    datasets_structure:delete(SpaceId, ?FOREST_TYPE, DetachedDatasetPath).

list_top_datasets(SpaceId, Opts) ->
    datasets_structure:list_space(SpaceId, ?FOREST_TYPE, Opts).


list(DatasetDoc, Opts) ->
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, DetachedDatasetInfo} = dataset:get_detached_info(DatasetDoc),
    {ok, DetachedDatasetPath} = detached_dataset_info:get_path(DetachedDatasetInfo),
    datasets_structure:list(SpaceId, ?FOREST_TYPE, DetachedDatasetPath, Opts).

get_parent(SpaceId, DatasetPath) ->
    % todo ogarnac wyliczanie parenta z detached
    Tokens = filename:split(filename:dirname(DatasetPath)),
    lists:foldl(fun(Uuid)

%%%===================================================================
%%% Internal functions
%%%===================================================================

