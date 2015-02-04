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
-type document_diff() :: #{term() => term()}.
-type bucket() :: atom().


-type generic_error() :: {error, Reason :: any()}.
-type not_found_error(Reason) :: {error, {not_found, Reason}}.
-type update_error() :: not_found_error(any()) | generic_error().
-type create_error() :: generic_error() | {error, already_exists}.
-type get_error() :: not_found_error(any()) | generic_error().

%% API
-export_type([key/0, value/0, document/0, document_diff/0, bucket/0]).
-export_type([generic_error/0, not_found_error/1, update_error/0, create_error/0, get_error/0]).
-export([save/2, update/4, create/2, get/3, delete/3, exists/3]).


%%%===================================================================
%%% API
%%%===================================================================


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


%%%===================================================================
%%% Internal functions
%%%===================================================================


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


level_to_driver(disk_only, _) ->
    ?PERSISTENCE_DRIVER;
level_to_driver(local_only, _) ->
    ?LOCAL_CACHE_DRIVER;
level_to_driver(global_only, _) ->
    ?DISTRIBUTED_CACHE_DRIVER;
level_to_driver(locally_cached, _) ->
    [?LOCAL_CACHE_DRIVER, ?PERSISTENCE_DRIVER];
level_to_driver(globally_cached, _) ->
    [?DISTRIBUTED_CACHE_DRIVER, ?PERSISTENCE_DRIVER].



driver_to_level(?PERSISTENCE_DRIVER) ->
    disk_only;
driver_to_level(?LOCAL_CACHE_DRIVER) ->
    local_only;
driver_to_level(?DISTRIBUTED_CACHE_DRIVER) ->
    global_only.


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
                FullArgs = [ModelConfig | Args],
                case Driver of
                    ?PERSISTENCE_DRIVER ->
                        worker_proxy:call(datastore_worker, {driver_call, Driver, Method, FullArgs});
                    _ ->
                        erlang:apply(Driver, Method, FullArgs)
                end;
            {error, Reason} ->
                {error, Reason}
        end,
    run_posthooks(ModelConfig, Method, driver_to_level(Driver), Context, Return).

