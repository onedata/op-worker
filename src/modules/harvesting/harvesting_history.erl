%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module that implements simple structure for storing
%%% the history of harvesting for pair
%%% {od_harvester:id(), od_harvester:index()}.
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting_history).
-author("Jakub Kudzia").

-include("modules/harvesting/harvesting.hrl").

-type history() :: #{{od_harvester:id(), od_harvester:index()} => couchbase_changes:seq()}.

-export_type([history/0]).


%% API
-export([init/0, get/3, set/4, delete/3]).

-define(KEY(HarvesterId, IndexId), {HarvesterId, IndexId}).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init() -> history().
init() ->
    #{}.

-spec get(od_harvester:id(), od_harvester:index(), history()) ->
    couchbase_changes:seq().
get(HarvesterId, IndexId, History) ->
    maps:get(?KEY(HarvesterId, IndexId), History, ?DEFAULT_HARVESTING_SEQ).

-spec set(od_harvester:id(), od_harvester:index(), couchbase_changes:seq(),
    history()) -> history().
set(HarvesterId, IndexId, Seq, History) ->
    History#{?KEY(HarvesterId, IndexId) => Seq}.

-spec delete(od_harvester:id(), od_harvester:index() | [od_harvester:index()],
    history()) -> history().
delete(HarvesterId, Indices, History) ->
    lists:foldl(fun(IndexId, HistoryIn) ->
        maps:remove(?KEY(HarvesterId, IndexId), HistoryIn)
    end, History, utils:ensure_list(Indices)).