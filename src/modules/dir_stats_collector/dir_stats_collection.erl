%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module defining types describing elements of directory statistics
%%% collection.
%%%
%%% NOTE: This module provides information about all collection types.
%%%       It is needed for enabling statistics counting for not empty
%%%       spaces. When new type of collection is created, it has to be
%%%       added to ALL_COLLECTION_TYPES macro.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collection).
-author("Michal Wrzeszcz").


-export([consolidate/3, with/2, list_types/0]).


-type type() :: module().

-type stat_name() :: term().
-type stat_value() :: term().

-type collection() :: #{stat_name() => stat_value()}.

-type stats_selector() :: [stat_name()] | all.

-export_type([type/0, stat_name/0, stat_value/0, collection/0, stats_selector/0]).


-define(ALL_COLLECTION_TYPES, [dir_size_stats, dir_update_time_stats]).


%%%===================================================================
%%% API
%%%===================================================================

-spec consolidate(type(), collection(), collection()) -> collection().
consolidate(CollectionType, OriginalMap, DiffMap) ->
    maps:merge_with(fun CollectionType:consolidate/3, OriginalMap, DiffMap).


-spec with(stats_selector(), collection()) -> collection().
with(all, Collection) ->
    Collection;
with(StatNames, Collection) ->
    maps:with(StatNames, Collection).


-spec list_types() -> [type()].
list_types() ->
    ?ALL_COLLECTION_TYPES.