%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module which implements datasets structure for storing
%%% detached dataset entries.
%%% Under the hood it uses datasets_structure module.
%%% @end
%%%-------------------------------------------------------------------
-module(detached_datasets).
-author("Jakub Kudzia").

-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([add/3, delete/1, list_top_datasets/2, list_children_datasets/2, get_parent/2]).

-type error() :: {error, term()}.

-define(FOREST_TYPE, ?DETACHED_DATASETS_STRUCTURE).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec add(od_space:id(), dataset:path(), dataset:name()) -> ok | error().
add(SpaceId, DatasetPath, DatasetName) ->
    datasets_structure:add(SpaceId, ?FOREST_TYPE, DatasetPath, DatasetName).


-spec delete(dataset:doc()) -> ok.
delete(DatasetDoc) ->
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, DetachedDatasetInfo} = dataset:get_detached_info(DatasetDoc),
    DetachedDatasetPath = detached_dataset_info:get_path(DetachedDatasetInfo),
    datasets_structure:delete(SpaceId, ?FOREST_TYPE, DetachedDatasetPath).


-spec list_top_datasets(od_space:id(), datasets_structure:opts()) -> {ok, datasets_structure:entries(), boolean()}.
list_top_datasets(SpaceId, Opts) ->
    datasets_structure:list_top_datasets(SpaceId, ?FOREST_TYPE, Opts).


-spec list_children_datasets(dataset:doc(), datasets_structure:opts()) -> {ok, datasets_structure:entries(), boolean()}.
list_children_datasets(DatasetDoc, Opts) ->
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    {ok, DetachedDatasetInfo} = dataset:get_detached_info(DatasetDoc),
    DetachedDatasetPath = detached_dataset_info:get_path(DetachedDatasetInfo),
    datasets_structure:list_children_datasets(SpaceId, ?FOREST_TYPE, DetachedDatasetPath, Opts).


-spec get_parent(od_space:id(), dataset:path()) -> dataset:id() | undefined.
get_parent(SpaceId, DatasetPath) ->
    Tokens = filename:split(filename:dirname(DatasetPath)),
    get_parent_helper(SpaceId, lists:reverse(Tokens)).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get(od_space:id(), dataset:path()) -> {ok, datasets_structure:entry()} | error().
get(SpaceId, DatasetPath) ->
    datasets_structure:get(SpaceId, ?FOREST_TYPE, DatasetPath).


-spec get_parent_helper(od_space:id(), [binary()]) -> dataset:id() | undefined.
get_parent_helper(_SpaceId, [<<?DIRECTORY_SEPARATOR>>]) ->
    undefined;
get_parent_helper(SpaceId, PathTokensReversed = [_Head | Tail]) ->
    case get(SpaceId, filename:join(lists:reverse(PathTokensReversed))) of
        {ok, {DatasetId, _DatasetName}} -> DatasetId;
        ?ERROR_NOT_FOUND -> get_parent_helper(SpaceId, Tail)
    end.