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
%%% TODO - sort cache documents by timestamp.
%%% @end
%%%-------------------------------------------------------------------
-module(caches_controller).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("cluster/worker/modules/datastore/datastore.hrl").
-include("cluster/worker/modules/datastore/datastore_common_internal.hrl").
-include("cluster/worker/elements/task_manager/task_manager.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([clear_local_cache/1, clear_global_cache/1, clear_local_cache/2, clear_global_cache/2]).
-export([clear_cache/2, clear_cache/3, should_clear_cache/1, get_hooks_config/1, wait_for_cache_dump/0]).
-export([delete_old_keys/2, get_cache_uuid/2, decode_uuid/1, cache_to_datastore_level/1, cache_to_task_level/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks if memory should be cleared.
%% @end
%%--------------------------------------------------------------------
-spec should_clear_cache(MemUsage :: number()) -> boolean().
should_clear_cache(MemUsage) ->
  {ok, TargetMemUse} = application:get_env(?APP_NAME, mem_to_clear_cache),
  MemUsage >= TargetMemUse.

%%--------------------------------------------------------------------
%% @doc
%% Clears local cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_local_cache(Aggressive :: boolean()) -> ok | mem_usage_too_high | cannot_check_mem_usage.
clear_local_cache(Aggressive) ->
  clear_cache(Aggressive, locally_cached).

%%--------------------------------------------------------------------
%% @doc
%% Clears local cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_local_cache(MemUsage :: number(), Aggressive :: boolean()) ->
  ok | mem_usage_too_high | cannot_check_mem_usage.
clear_local_cache(MemUsage, Aggressive) ->
  clear_cache(MemUsage, Aggressive, locally_cached).

%%--------------------------------------------------------------------
%% @doc
%% Clears global cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_global_cache(Aggressive :: boolean()) -> ok | mem_usage_too_high | cannot_check_mem_usage.
clear_global_cache(Aggressive) ->
  clear_cache(Aggressive, globally_cached).

%%--------------------------------------------------------------------
%% @doc
%% Clears global cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_global_cache(MemUsage :: number(), Aggressive :: boolean()) ->
  ok | mem_usage_too_high | cannot_check_mem_usage.
clear_global_cache(MemUsage, Aggressive) ->
  clear_cache(MemUsage, Aggressive, globally_cached).

%%--------------------------------------------------------------------
%% @doc
%% Clears cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_cache(Aggressive :: boolean(), StoreType :: globally_cached | locally_cached) ->
  ok | mem_usage_too_high | cannot_check_mem_usage.
clear_cache(Aggressive, StoreType) ->
  case monitoring:get_memory_stats() of
    [{<<"mem">>, MemUsage}] ->
      clear_cache(MemUsage, Aggressive, StoreType);
    _ ->
      ?warning("Not able to check memory usage"),
      cannot_check_mem_usage
  end.

%%--------------------------------------------------------------------
%% @doc
%% Clears cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_cache(MemUsage :: number(), Aggressive :: boolean(), StoreType :: globally_cached | locally_cached) ->
  ok | mem_usage_too_high | cannot_check_mem_usage.
clear_cache(MemUsage, true, StoreType) ->
  {ok, TargetMemUse} = application:get_env(?APP_NAME, mem_to_clear_cache),
  clear_cache(MemUsage, TargetMemUse, StoreType, [timer:minutes(10), 0]);

clear_cache(MemUsage, _, StoreType) ->
  {ok, TargetMemUse} = application:get_env(?APP_NAME, mem_to_clear_cache),
  clear_cache(MemUsage, TargetMemUse, StoreType, [timer:hours(7*24), timer:hours(24), timer:hours(1)]).

%%--------------------------------------------------------------------
%% @doc
%% Clears cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_cache(MemUsage :: number(), TargetMemUse :: number(),
    StoreType :: globally_cached | locally_cached, TimeWindows :: list()) ->
  ok | mem_usage_too_high | cannot_check_mem_usage.
clear_cache(MemUsage, TargetMemUse, _StoreType, _TimeWindows) when MemUsage < TargetMemUse ->
  ok;

clear_cache(_MemUsage, _TargetMemUse, _StoreType, []) ->
  mem_usage_too_high;

clear_cache(_MemUsage, TargetMemUse, StoreType, [TimeWindow | Windows]) ->
  caches_controller:delete_old_keys(StoreType, TimeWindow),
  timer:sleep(1000), % time for system for mem info update
  case monitoring:get_memory_stats() of
    [{<<"mem">>, NewMemUsage}] ->
      clear_cache(NewMemUsage, TargetMemUse, StoreType, Windows);
    _ ->
      ?warning("Not able to check memory usage"),
      cannot_check_mem_usage
  end.

%%--------------------------------------------------------------------
%% @doc
%% Provides hooks configuration on the basis of models list.
%% @end
%%--------------------------------------------------------------------
-spec get_hooks_config(Models :: list()) -> list().
get_hooks_config(Models) ->
  Methods = [save, get, exists, delete, update, create, fetch_link, delete_links],
  lists:foldl(fun(Model, Ans) ->
    ModelConfig = lists:map(fun(Method) ->
      {Model, Method}
    end, Methods),
    ModelConfig ++ Ans
  end, [], Models).

%%--------------------------------------------------------------------
%% @doc
%% Generates uuid on the basis of key and model name.
%% @end
%%--------------------------------------------------------------------
-spec get_cache_uuid(Key :: datastore:key(), ModelName :: model_behaviour:model_type()) -> binary().
get_cache_uuid(Key, ModelName) ->
  base64:encode(term_to_binary({ModelName, Key})).

%%--------------------------------------------------------------------
%% @doc
%% Decodes uuid to key and model name.
%% @end
%%--------------------------------------------------------------------
-spec decode_uuid(binary()) -> {Key :: datastore:key(), ModelName :: model_behaviour:model_type()}.
decode_uuid(Uuid) ->
  binary_to_term(base64:decode(Uuid)).

%%--------------------------------------------------------------------
%% @doc
%% Clears old documents from memory.
%% @end
%%--------------------------------------------------------------------
-spec delete_old_keys(StoreType :: globally_cached | locally_cached, TimeWindow :: integer()) -> ok.
delete_old_keys(globally_cached, TimeWindow) ->
  delete_old_keys(global_only, datastore_config:global_caches(), TimeWindow);

delete_old_keys(locally_cached, TimeWindow) ->
  delete_old_keys(local_only, datastore_config:local_caches(), TimeWindow).

%%--------------------------------------------------------------------
%% @doc
%% Waits for dumping cache to disk
%% @end
%%--------------------------------------------------------------------
-spec wait_for_cache_dump() ->
  ok | dump_error.
wait_for_cache_dump() ->
  wait_for_cache_dump(300).

%%--------------------------------------------------------------------
%% @doc
%% Waits for dumping cache to disk
%% @end
%%--------------------------------------------------------------------
-spec wait_for_cache_dump(N :: integer()) ->
  ok | dump_error.
wait_for_cache_dump(0) ->
  dump_error;
wait_for_cache_dump(N) ->
  case {cache_controller:list_docs_to_be_dumped(?GLOBAL_ONLY_LEVEL),
    cache_controller:list_docs_to_be_dumped(?LOCAL_ONLY_LEVEL)} of
    {{ok, []}, {ok, []}} ->
      ok;
    _ ->
      timer:sleep(timer:seconds(1)),
      wait_for_cache_dump(N-1)
  end.

%%--------------------------------------------------------------------
%% @doc
%% Translates cache name to store level.
%% @end
%%--------------------------------------------------------------------
-spec cache_to_datastore_level(ModelName :: atom()) -> datastore:store_level().
cache_to_datastore_level(ModelName) ->
  case lists:member(ModelName, datastore_config:global_caches()) of
    true -> ?GLOBAL_ONLY_LEVEL;
    _ -> ?LOCAL_ONLY_LEVEL
  end.

%%--------------------------------------------------------------------
%% @doc
%% Translates cache name to task level.
%% @end
%%--------------------------------------------------------------------
-spec cache_to_task_level(ModelName :: atom()) -> task_manager:level().
cache_to_task_level(ModelName) ->
  case lists:member(ModelName, datastore_config:global_caches()) of
    true -> ?CLUSTER_LEVEL;
    _ -> ?NODE_LEVEL
  end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Clears old documents from memory.
%% @end
%%--------------------------------------------------------------------
-spec delete_old_keys(Level :: global_only | local_only, Caches :: list(), TimeWindow :: integer()) -> ok.
% TODO Add dumping cache to disk in case of recent faliures
delete_old_keys(Level, Caches, TimeWindow) ->
  {ok, Uuids} = cache_controller:list(Level, TimeWindow),
  lists:foreach(fun(Uuid) ->
    {ModelName, Key} = decode_uuid(Uuid),
    % TODO - what happens when link is deleted in parallel to memory clearing
    safe_delete(Level, ModelName, Key),
    FullArgs = [cache_controller:model_init(), Uuid, ?PRED_ALWAYS],
    erlang:apply(datastore:level_to_driver(Level), delete, FullArgs)
  end, Uuids),
  case TimeWindow of
    0 ->
      lists:foreach(fun(Cache) ->
        {ok, Docs} = datastore:list(Level, Cache, ?GET_ALL, []),
        lists:foreach(fun(Doc) ->
          safe_delete(Level, Cache, Doc#document.key)
        end, Docs)
      end, Caches);
    _ ->
      ok
  end,
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes info from memory when it is dumped to disk.
%% @end
%%--------------------------------------------------------------------
-spec safe_delete(Level :: datastore:store_level(), ModelName :: model_behaviour:model_type(), Key :: datastore:key()) ->
  ok | datastore:generic_error().
safe_delete(Level, ModelName, Key) ->
  try
    ModelConfig = ModelName:model_init(),
    FullArgs = [ModelConfig, Key],
    {ok, Doc} = worker_proxy:call(datastore_worker,
      {driver_call, datastore:driver_to_module(datastore:level_to_driver(Level)), get, FullArgs}),

    Value = Doc#document.value,
    Pred = fun() ->
      case erlang:apply(datastore:level_to_driver(Level), get, FullArgs) of
        {ok, Doc2} ->
          Doc2#document.value =:= Value;
        _ ->
          false
      end
    end,
    FullArgs2 = [ModelConfig, Key, Pred],
    erlang:apply(datastore:level_to_driver(Level), delete, FullArgs2)
  catch
    E1:E2 ->
      ?error_stacktrace("Error in cache controller safe_delete. "
        ++"Args: ~p. Error: ~p:~p.", [{Level, ModelName, Key}, E1, E2]),
      {error, safe_delete_failed}
  end.