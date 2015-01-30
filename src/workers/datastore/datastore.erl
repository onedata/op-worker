%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-module(datastore).
-author("Rafal Slota").

-include("workers/datastore/datastore.hrl").
-include("workers/datastore/datastore_internal.hrl").

-type key() :: term().
-type document() :: #document{}.
-type value() :: term().

%% API
-export_type([key/0, value/0, document/0]).
-export([save/2, update/4, create/2, get/3, delete/3, exists/3]).


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
save(Level, #document{} = Document) ->
    ModelName = model_name(Document),
    exec_driver(ModelName, level_to_driver(Level, write), save, [Document], Document).


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
update(Level, ModelName, Key, Diff) ->
    exec_driver(ModelName, level_to_driver(Level, write), update, [Key, Diff], {Key, Diff}).


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
create(Level, #document{} = Document) ->
    ModelName = model_name(Document),
    exec_driver(ModelName, level_to_driver(Level, write), create, [Document], Document).


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
get(Level, ModelName, Key) ->
    exec_driver(ModelName, level_to_driver(Level, read), get, [Key], Key).


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
delete(Level, ModelName, Key) ->
    exec_driver(ModelName, level_to_driver(Level, write), delete, [Key], Key).


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
exists(Level, ModelName, Key) ->
    exec_driver(ModelName, level_to_driver(Level, read), exists, [Key], Key).


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
list() -> ok.


model_name(#document{value = Record}) ->
    model_name(Record);
model_name(Record) when is_tuple(Record) ->
    element(1, Record).


run_prehooks(#model_config{name = ModelName}, Method, Level, Context) ->
    ensure_state_loaded(),
    Hooked = ets:lookup(datastore_local_state, {ModelName, Method}),
    HooksRes =
        lists:map(
            fun({_, HookedModule}) ->
                HookedModule:before(ModelName, Method, Level, Context)
            end, Hooked),
    case HooksRes -- [ok] of
        [] -> ok;
        [Interrupt | _] ->
            Interrupt
    end.


run_posthooks(#model_config{name = ModelName}, Method, Level, Context, Return) ->
    ensure_state_loaded(),
    Hooked = ets:lookup(datastore_local_state, {ModelName, Method}),
    lists:foreach(
            fun({_, HookedModule}) ->
                spawn(fun() -> HookedModule:'after'(ModelName, Method, Level, Context, Return) end)
            end, Hooked),
    Return.


load_local_state(Models) ->
    ets:new(?LOCAL_STATE, [named_table, public, bag]),
    lists:foreach(
        fun(ModelName) ->
            #model_config{hooks = Hooks} = ModelName:model_init(),
            lists:foreach(
                fun(Hook) ->

                    ets:insert(?LOCAL_STATE, {Hook, ModelName})
                end, Hooks)
        end, Models),
    ok.

ensure_state_loaded() ->
    case ets:info(?LOCAL_STATE) of
        undefined ->
            load_local_state(?MODELS);
        _ -> ok
    end.


level_to_driver(persistence, _) ->
    ?PERSISTENCE_DRIVER;
level_to_driver(l_cache, _) ->
    ?LOCAL_CACHE_DRIVER;
level_to_driver(d_cache, _) ->
    ?DISTRIBUTED_CACHE_DRIVER;
level_to_driver(cache, _) ->
    [?LOCAL_CACHE_DRIVER, ?DISTRIBUTED_CACHE_DRIVER];
level_to_driver(all, _) ->
    [?LOCAL_CACHE_DRIVER, ?DISTRIBUTED_CACHE_DRIVER, ?PERSISTENCE_DRIVER].


driver_to_level(?PERSISTENCE_DRIVER) ->
    persistence;
driver_to_level(?LOCAL_CACHE_DRIVER) ->
    l_cache;
driver_to_level(?DISTRIBUTED_CACHE_DRIVER) ->
    d_cache.


exec_driver(ModelName, [Driver], Method, Args, Context) when is_atom(Driver) ->
    exec_driver(ModelName, Driver, Method, Args, Context);
exec_driver(ModelName, [Driver | Rest], Method, Args, Context) when is_atom(Driver) ->
    case exec_driver(ModelName, Driver, Method, Args, Context) of
        {error, Reason} ->
            {error, Reason};
        _ ->
            exec_driver(ModelName, Rest, Method, Args, Context)
    end;
exec_driver(ModelName, Driver, Method, Args, Context) when is_atom(Driver) ->
    ModelConfig = ModelName:model_init(),
    Return =
        case run_prehooks(ModelConfig, Method, driver_to_level(Driver), Context) of
            ok ->
                erlang:apply(Driver, Method, [ModelConfig | Args]);
            {error, Reason} ->
                {error, Reason}
        end,
    run_posthooks(ModelConfig, Method, driver_to_level(Driver), Context, Return).

