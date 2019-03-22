%%%-------------------------------------------------------------------
%%% @author michal
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains utility functions for operating on pools.
%%% @end
%%%-------------------------------------------------------------------
-module(pool_utils).
-author("Michal Cwiertnia").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([get_documents_diff/3, get_pools_entries_and_sizes/2]).


get_pools_entries_and_sizes(Worker, PoolType) ->
  Pools = rpc:call(Worker, datastore_multiplier, get_names, [PoolType]),
  Entries = lists:map(fun(Pool) ->
    PoolName = list_to_atom("datastore_cache_active_pool_" ++ atom_to_list(Pool)),
    rpc:call(Worker, ets, foldl, [fun(Entry, Acc) -> Acc ++ [Entry] end, [], PoolName])
                      end, Pools),
  Sizes = lists:map(fun(Slot) -> rpc:call(Worker, datastore_cache_manager, get_size, [Slot]) end, Pools),
  {Entries, Sizes}.


get_documents_diff(Worker, After, Before) ->
  Ans = lists:flatten(lists:zipwith(fun(A,B) ->
    Diff = A--B,
    lists:map(fun({Key, Driver, DriverCtx}) ->
      rpc:call(Worker, Driver, get, [DriverCtx, Key])
              end, [{Key, Driver, DriverCtx} || {_,Key,_,_,Driver, DriverCtx} <- Diff])
                                    end, After, Before)),
  lists:filter(fun
                 ({ok, #document{value = #links_node{model = luma_cache}}}) -> false;
                 ({ok, #document{value = #links_forest{model = luma_cache}}}) -> false;
                 ({ok, #document{value = #links_node{model = task_pool}}}) -> false;
                 ({ok, #document{value = #links_forest{model = task_pool}}}) -> false;
                 ({ok, #document{value = #task_pool{}}}) -> false;
                 ({ok, #document{value = #permissions_cache{}}}) -> false;
                 ({ok, #document{value = #permissions_cache_helper{}}}) -> false;
                 ({ok, #document{value = #permissions_cache_helper2{}}}) -> false;
                 (_) -> true
               end, Ans).
