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
-module(dataset_path).
-author("Jakub Kudzia").

%% API
-export([get/1, get/2, get_space_path/1]).

-compile({no_auto_import,[get/1]}).



%%%===================================================================
%%% API functions
%%%===================================================================

-spec get(dataste:id()) -> {ok, dataset:path()}.
get(DatasetId) ->
    {ok, Doc} = dataset:get(DatasetId),
    Uuid = dataset:get_uuid(Doc),
    SpaceId = dataset:get_space_id(Doc),
    get(SpaceId, Uuid).


-spec get(od_space:id(), file_meta:uuid()) -> {ok, dataset:path()}.
get(SpaceId, Uuid) ->
    paths_cache:get_uuid_based(SpaceId, Uuid).


-spec get_space_path(od_space:id()) -> {ok, dataset:path()}.
get_space_path(SpaceId) ->
    paths_cache:get_uuid_based(SpaceId, fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId)).



%%%===================================================================
%%% Internal functions
%%%===================================================================

