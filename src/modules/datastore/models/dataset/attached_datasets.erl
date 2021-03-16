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
-module(attached_datasets).
-author("Jakub Kudzia").

-include("modules/dataset/dataset.hrl").

%% API
-export([add/3, delete/1, delete/2, list_top_datasets/2, list/2]).

-define(FOREST_TYPE, ?ATTACHED_FOREST).

%%%===================================================================
%%% API functions
%%%===================================================================

add(SpaceId, DatasetId, Uuid) ->
    {ok, DatasetPath} = dataset_path:get(SpaceId, Uuid),
    datasets_structure:add(SpaceId, ?FOREST_TYPE, DatasetPath, DatasetId).


delete(DatasetDoc) ->
    {ok, Uuid} = dataset:get_uuid(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, DatasetPath} = dataset_path:get(SpaceId, Uuid),
    delete(SpaceId, DatasetPath).


delete(SpaceId, DatasetPath) ->
    datasets_structure:delete(SpaceId, ?FOREST_TYPE, DatasetPath).


list_top_datasets(SpaceId, Opts) ->
    datasets_structure:list_space(SpaceId, ?FOREST_TYPE, Opts).


list(DatasetDoc, Opts) ->
    {ok, Uuid} = dataset:get_uuid(DatasetDoc),
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, DatasetPath} = dataset_path:get(SpaceId, Uuid),
    datasets_structure:list(SpaceId, ?FOREST_TYPE, DatasetPath, Opts).

%%%===================================================================
%%% Internal functions
%%%===================================================================

