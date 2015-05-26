%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions used by node manager to coordinate
%%% clearing of not used values cached in memory.
%%% @end
%%%-------------------------------------------------------------------
-module(caches_controller).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([clear_local_cache/1, clear_global_cache/1, clear_local_cache/2, clear_global_cache/2]).
-export([clear_cache/2, clear_cache/3, should_clear_cache/1]).

-ifdef(TEST).
-export([delete_old_keys/2]).
-endif.

%%%===================================================================
%%% API
%%%===================================================================

should_clear_cache(MemUsage) ->
  {ok, TargetMemUse} = application:get_env(?APP_NAME, mem_to_clear_cache),
  MemUsage >= TargetMemUse.

clear_local_cache(Aggressive) ->
  clear_cache(Aggressive, locally_cached).

clear_local_cache(MemUsage, Aggressive) ->
  clear_cache(MemUsage, Aggressive, locally_cached).

clear_global_cache(Aggressive) ->
  clear_cache(Aggressive, globally_cached).

clear_global_cache(MemUsage, Aggressive) ->
  clear_cache(MemUsage, Aggressive, globally_cached).

clear_cache(Aggressive, StoreType) ->
  case monitoring:get_memory_stats() of
    [{<<"mem">>, MemUsage}] ->
      clear_cache(MemUsage, Aggressive, StoreType);
    _ ->
      ?warning("Not able to check memory usage"),
      cannot_check_mem_usage
  end.

clear_cache(MemUsage, true, StoreType) ->
  {ok, TargetMemUse} = application:get_env(?APP_NAME, mem_to_clear_cache),
  clear_cache(MemUsage, TargetMemUse, StoreType, [timer:minutes(10), 0]);

clear_cache(MemUsage, _, StoreType) ->
  {ok, TargetMemUse} = application:get_env(?APP_NAME, mem_to_clear_cache),
  clear_cache(MemUsage, TargetMemUse, StoreType, [timer:hours(7*24), timer:hours(24), timer:hours(1)]).

clear_cache(MemUsage, TargetMemUse, _StoreType, _TimeWindows) when MemUsage < TargetMemUse->
  ok;

clear_cache(_MemUsage, _TargetMemUse, _StoreType, []) ->
  mem_usage_too_high;

clear_cache(_MemUsage, TargetMemUse, StoreType, [TimeWindow | Windows]) ->
  caches_controller:delete_old_keys(StoreType, TimeWindow),
  case monitoring:get_memory_stats() of
    [{<<"mem">>, NewMemUsage}] ->
      clear_cache(NewMemUsage, TargetMemUse, StoreType, Windows);
    _ ->
      ?warning("Not able to check memory usage"),
      cannot_check_mem_usage
  end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

delete_old_keys(globally_cached, _TimeWindow) ->
  ok;

delete_old_keys(locally_cached, _TimeWindow) ->
  ok.
