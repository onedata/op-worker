%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module that implements simple structure for storing
%%% the progress of harvesting for pair
%%% {od_harvester:id(), od_harvester:index()}.
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting_progress).
-author("Jakub Kudzia").

-include("modules/harvesting/harvesting.hrl").

-type progress() :: #{{od_harvester:id(), od_harvester:index()} => couchbase_changes:seq()}.

-export_type([progress/0]).


%% API
-export([init/0, get/3, set/4, delete/3]).

-define(KEY(HarvesterId, IndexId), {HarvesterId, IndexId}).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init() -> progress().
init() ->
    #{}.

-spec get(od_harvester:id(), od_harvester:index(), progress()) ->
    couchbase_changes:seq().
get(HarvesterId, IndexId, Progress) ->
    maps:get(?KEY(HarvesterId, IndexId), Progress, ?DEFAULT_HARVESTING_SEQ).

-spec set(od_harvester:id(), od_harvester:index(), couchbase_changes:seq(),
    progress()) -> progress().
set(HarvesterId, IndexId, Seq, Progress) ->
    Progress#{?KEY(HarvesterId, IndexId) => Seq}.

-spec delete(od_harvester:id(), od_harvester:index() | [od_harvester:index()],
    progress()) -> progress().
delete(HarvesterId, Indices, Progress) ->
    lists:foldl(fun(IndexId, ProgressIn) ->
        maps:remove(?KEY(HarvesterId, IndexId), ProgressIn)
    end, Progress, utils:ensure_list(Indices)).