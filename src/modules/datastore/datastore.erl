%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Main datastore API implementation.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore).
-author("Rafal Slota").

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% ETS name for local (node scope) state.
-define(LOCAL_STATE, datastore_local_state).

%% #document types
-type key() :: undefined | term().
-type document() :: #document{}.
-type value() :: term().
-type document_diff() :: #{term() => term()} | fun((OldValue :: value()) -> NewValue :: value()).
-type bucket() :: atom() | binary().

-export_type([key/0, value/0, document/0, document_diff/0, bucket/0]).

%% Error types
-type generic_error() :: {error, Reason :: term()}.
-type not_found_error(Reason) :: {error, {not_found, Reason}}.
-type update_error() :: not_found_error(term()) | generic_error().
-type create_error() :: generic_error() | {error, already_exists}.
-type get_error() :: not_found_error(term()) | generic_error().

-export_type([generic_error/0, not_found_error/1, update_error/0, create_error/0, get_error/0]).

%% API utility types
-type store_level() :: disk_only | local_only | global_only | locally_cached | globally_cached.

-export_type([store_level/0]).

%% API
-export([save/2, update/4, create/2, get/3, delete/3, exists/3]).
-export([configs_per_bucket/1, ensure_state_loaded/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves given #document.
%% @end
%%--------------------------------------------------------------------
-spec save(Level :: store_level(), Document :: datastore:document()) ->
    {ok, datastore:key()} | datastore:generic_error().
save(Level, #document{} = Document) ->
    ModelName = model_name(Document),
    exec_driver(ModelName, level_to_driver(Level), save, [maybe_gen_uuid(Document)]).

%%--------------------------------------------------------------------
%% @doc
%% Updates given by key document by replacing given fields with new values.
%% @end
%%--------------------------------------------------------------------
-spec update(Level :: store_level(), ModelName :: model_behaviour:model_type(),
    Key :: datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Level, ModelName, Key, Diff) ->
    exec_driver(ModelName, level_to_driver(Level), update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% Creates new #document.
%% @end
%%--------------------------------------------------------------------
-spec create(Level :: store_level(), Document :: datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(Level, #document{} = Document) ->
    ModelName = model_name(Document),
    exec_driver(ModelName, level_to_driver(Level), create, [maybe_gen_uuid(Document)]).

%%--------------------------------------------------------------------
%% @doc
%% Gets #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec get(Level :: store_level(), ModelName :: model_behaviour:model_type(),
    Key :: datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Level, ModelName, Key) ->
    exec_driver(ModelName, level_to_driver(Level), get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec delete(Level :: store_level(), ModelName :: model_behaviour:model_type(),
    Key :: datastore:key()) -> ok | datastore:generic_error().
delete(Level, ModelName, Key) ->
    exec_driver(ModelName, level_to_driver(Level), delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Checks if #document with given key exists. This method shall not be used with
%% multiple drivers at once - use *_only levels.
%%--------------------------------------------------------------------
-spec exists(Level :: store_level(), ModelName :: model_behaviour:model_type(),
    Key :: datastore:key()) -> true | false | datastore:generic_error().
exists(Level, ModelName, Key) ->
    exec_driver(ModelName, level_to_driver(Level), exists, [Key]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% For given #document generates key if undefined and returns updated #document structure.
%% @end
%%--------------------------------------------------------------------
-spec maybe_gen_uuid(document()) -> document().
maybe_gen_uuid(#document{key = undefined} = Doc) ->
    Doc#document{key = datastore_utils:gen_uuid()};
maybe_gen_uuid(#document{} = Doc) ->
    Doc.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets model name for given document/record.
%% @end
%%--------------------------------------------------------------------
-spec model_name(tuple() | document()) -> atom().
model_name(#document{value = Record}) ->
    model_name(Record);
model_name(Record) when is_tuple(Record) ->
    element(1, Record).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs all pre-hooks for given model, method and context.
%% @end
%%--------------------------------------------------------------------
-spec run_prehooks(Config :: model_behaviour:model_config(),
    Method :: model_behaviour:model_action(), Level :: store_level(),
    Context :: term()) -> ok | {error, Reason :: term()}.
run_prehooks(#model_config{name = ModelName}, Method, Level, Context) ->
    Hooked = ets:lookup(?LOCAL_STATE, {ModelName, Method}),
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs asynchronously all post-hooks for given model, method, context and
%% return value. Returns given return value.
%% @end
%%--------------------------------------------------------------------
-spec run_posthooks(Config :: model_behaviour:model_config(),
    Model :: model_behaviour:model_action(), Level :: store_level(),
    Context :: term(), ReturnValue) -> ReturnValue when ReturnValue :: term().
run_posthooks(#model_config{name = ModelName}, Method, Level, Context, Return) ->
    Hooked = ets:lookup(?LOCAL_STATE, {ModelName, Method}),
    lists:foreach(
        fun({_, HookedModule}) ->
            spawn(fun() ->
                HookedModule:'after'(ModelName, Method, Level, Context, Return) end)
        end, Hooked),
    Return.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes local (node scope) datastore state with given models.
%% Returns initialized configuration.
%% @end
%%--------------------------------------------------------------------
-spec load_local_state(Models :: [model_behaviour:model_type()]) ->
    [model_behaviour:model_config()].
load_local_state(Models) ->
    ets:new(?LOCAL_STATE, [named_table, public, bag]),
    lists:map(
        fun(ModelName) ->
            Config = #model_config{hooks = Hooks} = ModelName:model_init(),
            lists:foreach(
                fun(Hook) ->
                    ets:insert(?LOCAL_STATE, {Hook, ModelName})
                end, Hooks),
            Config
        end, Models).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Organizes given models into #{bucket -> [model]} map.
%% @end
%%--------------------------------------------------------------------
-spec configs_per_bucket(Configs :: [model_behaviour:model_config()]) ->
    #{bucket() => [model_behaviour:model_config()]}.
configs_per_bucket(Configs) ->
    lists:foldl(
        fun(ModelConfig, Acc) ->
            #model_config{bucket = Bucket} = ModelConfig,
            maps:put(Bucket, [ModelConfig | maps:get(Bucket, Acc, [])], Acc)
        end, #{}, Configs).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs init_bucket/1 for each datastore driver using given models'.
%% @end
%%--------------------------------------------------------------------
-spec init_drivers(Configs :: [model_behaviour:model_config()]) ->
    ok | no_return().
init_drivers(Configs) ->
    lists:foreach(
        fun({Bucket, Models}) ->
            ok = ?PERSISTENCE_DRIVER:init_bucket(Bucket, Models),
            ok = ?LOCAL_CACHE_DRIVER:init_bucket(Bucket, Models),
            ok = ?DISTRIBUTED_CACHE_DRIVER:init_bucket(Bucket, Models)
        end, maps:to_list(configs_per_bucket(Configs))).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Loads local state and initializes datastore drivers if needed.
%% @end
%%--------------------------------------------------------------------
-spec ensure_state_loaded() -> ok | {error, Reason :: term()}.
ensure_state_loaded() ->
    try
        case ets:info(?LOCAL_STATE) of
            undefined ->
                Configs = load_local_state(?MODELS),
                init_drivers(Configs);
            _ -> ok
        end
    catch
        Type:Reason ->
            ?error_stacktrace("Cannot initialize datastore local state due to"
            " ~p: ~p", [Type, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Translates store level into list of drivers.
%% @end
%%--------------------------------------------------------------------
-spec level_to_driver(Level :: store_level()) -> [Driver :: atom()].
level_to_driver(disk_only) ->
    ?PERSISTENCE_DRIVER;
level_to_driver(local_only) ->
    ?LOCAL_CACHE_DRIVER;
level_to_driver(global_only) ->
    ?DISTRIBUTED_CACHE_DRIVER;
level_to_driver(locally_cached) ->
    [?LOCAL_CACHE_DRIVER, ?PERSISTENCE_DRIVER];
level_to_driver(globally_cached) ->
    [?DISTRIBUTED_CACHE_DRIVER, ?PERSISTENCE_DRIVER].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Reverses level_to_driver/1
%% @end
%%--------------------------------------------------------------------
-spec driver_to_level(atom()) -> store_level().
driver_to_level(?PERSISTENCE_DRIVER) ->
    disk_only;
driver_to_level(?LOCAL_CACHE_DRIVER) ->
    local_only;
driver_to_level(?DISTRIBUTED_CACHE_DRIVER) ->
    global_only.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes given model action on given driver(s).
%% @end
%%--------------------------------------------------------------------
-spec exec_driver(model_behaviour:model_type(), [Driver] | Driver,
    Method :: model_behaviour:model_action(), [term()]) ->
    ok | {ok, term()} | {error, term()} when Driver :: atom().
exec_driver(ModelName, [Driver], Method, Args) when is_atom(Driver) ->
    exec_driver(ModelName, Driver, Method, Args);
exec_driver(ModelName, [Driver | Rest], Method, Args) when is_atom(Driver) ->
    case exec_driver(ModelName, Driver, Method, Args) of
        {error, Reason} ->
            {error, Reason};
        _ ->
            exec_driver(ModelName, Rest, Method, Args)
    end;
exec_driver(ModelName, Driver, Method, Args) when is_atom(Driver) ->
    ModelConfig = ModelName:model_init(),
    Return =
        case run_prehooks(ModelConfig, Method, driver_to_level(Driver), Args) of
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
    run_posthooks(ModelConfig, Method, driver_to_level(Driver), Args, Return).

