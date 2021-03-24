%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for calculating paths that identify datasets.
%%% These paths are of type file_meta:uuid_based_path()
%%% with only one difference: SpaceUuid is used instead
%%% of SpaceId on the first element of the path
%%% (which is basically a bug in paths_cache).
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_path).
-author("Jakub Kudzia").

%% API
-export([get/2, get_space_path/1]).

-compile({no_auto_import,[get/1]}).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec get(od_space:id(), file_meta:uuid()) -> {ok, dataset:path()}.
get(SpaceId, Uuid) ->
    {ok, DatasetPath} = paths_cache:get_uuid_based(SpaceId, Uuid),
    [Sep, SpaceId | Tail] = filename:split(DatasetPath),
    {ok, filename:join([Sep, fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId) | Tail])}.


-spec get_space_path(od_space:id()) -> {ok, dataset:path()}.
get_space_path(SpaceId) ->
    get(SpaceId, fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId)).

