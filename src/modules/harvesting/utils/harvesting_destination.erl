%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module used by modules associated with harvesting.
%%% harvesting_destination is a simple data structure that represents
%%% structure of Harvesters and Indices associated with them.
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting_destination).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([init/0, init/2, get/2, add/3, delete/3, size/1, is_empty/1,
    get_harvesters/1, fold/3, foreach/2, foreach_index/2, merge/2, merge/1]).

-type destination() :: #{od_harvester:id() => [od_harvester:index()]}.
-type harvester() :: od_harvester:id().
-type index() :: od_harvester:index().
-type indices() :: [index()].

-export_type([destination/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init() -> destination().
init() ->
    #{}.

-spec init(harvester(), index() | indices()) -> destination().
init(HarvesterId, Indices) ->
    add(HarvesterId, Indices, init()).

-spec get(harvester(), destination()) -> indices().
get(HarvesterId, Destination) ->
    maps:get(HarvesterId, Destination, []).

-spec add(harvester(), index() | indices(), destination()) -> destination().
add(_HarvesterId, [], Destination) ->
    Destination;
add(HarvesterId, NewIndices, Destination) ->
    NewIndices2 = utils:ensure_list(NewIndices),
    maps:update_with(HarvesterId, fun(Indices) ->
        Indices ++ NewIndices2
    end, NewIndices2, Destination).

-spec delete(harvester(), index() | indices(), destination()) -> destination().
delete(HarvesterId, IndicesToDelete, Destination) ->
    case maps:get(HarvesterId, Destination, []) of
        [] ->
            Destination;
        Indices ->
            IndicesToDelete2 = utils:ensure_list(IndicesToDelete),
            case Indices -- IndicesToDelete2 of
                [] ->
                    maps:remove(HarvesterId, Destination);
                UpdatedIndices ->
                    Destination#{HarvesterId => UpdatedIndices}
            end
    end.

-spec size(destination()) -> non_neg_integer().
size(Destination) ->
    map_size(Destination).

-spec is_empty(destination()) -> boolean().
is_empty(Destination) ->
    harvesting_destination:size(Destination) =:= 0.

-spec get_harvesters(destination()) -> [harvester()].
get_harvesters(Destination) ->
    maps:keys(Destination).

-spec fold(fun((od_harvester:id(), indices(), term()) -> term()),
    term(), destination()) -> term().
fold(Fun, Init, Destination) ->
    maps:fold(Fun, Init, Destination).

-spec foreach(fun((od_harvester:id(), indices()) -> term()), destination()) -> ok.
foreach(Fun, Destination) ->
    maps:map(Fun, Destination),
    ok.

-spec foreach_index(fun((od_harvester:id(), indices()) -> term()), destination()) -> ok.
foreach_index(Fun, Destination) ->
    foreach(fun(HarvesterId, Indices) ->
        lists:foreach(fun(IndexId) -> Fun(HarvesterId, IndexId) end, Indices)
    end, Destination).

-spec merge(destination(), destination()) -> destination().
merge(Destination1, Destination2) ->
    maps:fold(fun(HarvesterId, Indices, DestinationIn) ->
        add(HarvesterId, Indices, DestinationIn)
    end, Destination1, Destination2).

-spec merge([destination()]) -> destination().
merge([])->
    init();
merge([Destination])->
    Destination;
merge([Destination1, Destination2 | Destinations])->
    merge([merge(Destination1, Destination2) | Destinations]).