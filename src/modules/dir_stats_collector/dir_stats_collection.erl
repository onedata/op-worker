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


-include_lib("ctool/include/errors.hrl").


-export([consolidate/3, on_collection_move/2, with_all/2, list_types/0]).


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


-spec on_collection_move(type(), collection()) -> {update_source_parent, collection()} | ignore.
on_collection_move(CollectionType, Collection) ->
    maps:fold(fun
        (StatName, StatValue, ignore) ->
            case CollectionType:on_collection_move(StatName, StatValue) of
                {update_source_parent, ValueToUpdate} -> {update_source_parent, #{StatName => ValueToUpdate}};
                ignore -> ignore
            end;
        (StatName, StatValue, {update_source_parent, Acc}) ->
            case CollectionType:on_collection_move(StatName, StatValue) of
                {update_source_parent, ValueToUpdate} -> {update_source_parent, Acc#{StatName => ValueToUpdate}};
                ignore -> {update_source_parent, Acc}
            end
    end, ignore, Collection).


-spec with_all(stats_selector(), collection()) -> {ok, collection()} | ?ERROR_NOT_FOUND.
with_all(all, Collection) ->
    {ok, Collection};
with_all(StatNames, Collection) ->
    StatsCount = length(StatNames),
    FilteredMap = maps:with(StatNames, Collection),
    case maps:size(FilteredMap) of
        StatsCount -> {ok, FilteredMap};
        _ -> ?ERROR_NOT_FOUND
    end.


-spec list_types() -> [type()].
list_types() ->
    ?ALL_COLLECTION_TYPES.