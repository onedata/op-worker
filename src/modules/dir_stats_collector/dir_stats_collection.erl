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
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collection).
-author("Michal Wrzeszcz").


-export([consolidate/3, with/2]).


-type type() :: module().

-type stat_name() :: term().
-type stat_value() :: term().

-type collection() :: #{stat_name() => stat_value()}.

-type stats_selector() :: [stat_name()] | all.

-export_type([type/0, stat_name/0, stat_value/0, collection/0, stats_selector/0]).


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