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
-type link_error() :: generic_error() | {error, link_not_found}.

-export_type([generic_error/0, not_found_error/1, update_error/0, create_error/0, get_error/0, link_error/0]).

%% API utility types
-type store_level() :: disk_only | local_only | global_only | locally_cached | globally_cached.
-type delete_predicate() :: fun(() -> boolean()).
-type list_fun() :: fun((Obj :: term(), AccIn :: term()) -> {next, Acc :: term()} | {abort, Acc :: term()}).
-type exists_return() :: boolean() | no_return().

-export_type([store_level/0, delete_predicate/0, list_fun/0, exists_return/0]).

%% Links' types
-type normalized_link_target() :: {key(), model_behaviour:model_type()}.
-type link_target() :: #document{} | normalized_link_target().
-type link_name() :: atom() | binary().
-type link_spec() :: {link_name(), link_target()}.
-type normalized_link_spec() :: {link_name(), normalized_link_target()}.


-export_type([link_target/0, link_name/0, link_spec/0, normalized_link_spec/0, normalized_link_target/0]).

%% API
-export([save/2, update/4, create/2, get/3, list/4, delete/4, delete/3, exists/3]).
-export([fetch_link/2, fetch_link/3, add_links/2, add_links/3, delete_links/2, delete_links/3,
         foreach_link/3, foreach_link/4, fetch_link_target/2, fetch_link_target/3,
         link_walk/3, link_walk/4]).
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
%% Executes given funcion for each model's record. After each record function may interrupt operation.
%% @end
%%--------------------------------------------------------------------
-spec list(Level :: store_level(), ModelName :: model_behaviour:model_type(), Fun :: list_fun(), AccIn :: term()) ->
    {ok, Handle :: term()} | datastore:generic_error() | no_return().
list(Level, ModelName, Fun, AccIn) ->
    exec_driver(ModelName, level_to_driver(Level), list, [Fun, AccIn]).


%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec delete(Level :: store_level(), ModelName :: model_behaviour:model_type(),
    Key :: datastore:key(), Pred :: delete_predicate()) -> ok | datastore:generic_error().
delete(Level, ModelName, Key, Pred) ->
    case exec_driver(ModelName, level_to_driver(Level), delete, [Key, Pred]) of
        ok ->
            spawn(fun() -> delete_links(Key, ModelName, all) end),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec delete(Level :: store_level(), ModelName :: model_behaviour:model_type(),
    Key :: datastore:key()) -> ok | datastore:generic_error().
delete(Level, ModelName, Key) ->
    delete(Level, ModelName, Key, ?PRED_ALWAYS).



%%--------------------------------------------------------------------
%% @doc
%% Checks if #document with given key exists. This method shall not be used with
%% multiple drivers at once - use *_only levels.
%% @end
%%--------------------------------------------------------------------
-spec exists(Level :: store_level(), ModelName :: model_behaviour:model_type(),
    Key :: datastore:key()) -> {ok, boolean()} | datastore:generic_error().
exists(Level, ModelName, Key) ->
    exec_driver(ModelName, level_to_driver(Level), exists, [Key]).


%%--------------------------------------------------------------------
%% @doc
%% Adds links to given document.
%% @end
%%--------------------------------------------------------------------
-spec add_links(document(), link_spec() | [link_spec()]) -> ok | generic_error().
add_links(#document{key = Key} = Doc, Links) ->
    add_links(Key, model_name(Doc), Links).


%%--------------------------------------------------------------------
%% @doc
%% Adds given links to the document with given key.
%% @end
%%--------------------------------------------------------------------
-spec add_links(key(), model_behaviour:model_type(), link_spec() | [link_spec()]) ->
    ok | generic_error().
add_links(Key, ModelName, {_LinkName, _LinkTarget} = LinkSpec) ->
    add_links(Key, ModelName, [LinkSpec]);
add_links(Key, ModelName, Links) when is_list(Links) ->
    _ModelConfig = ModelName:model_init(),
    exec_driver(ModelName, ?PERSISTENCE_DRIVER, add_links, [Key, normalize_link_target(Links)]).


%%--------------------------------------------------------------------
%% @doc
%% Removes links from given document. There is special link name 'all' which removes all links.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(document(), link_name() | [link_name()] | all) -> ok | generic_error().
delete_links(#document{key = Key} = Doc, LinkNames) ->
    delete_links(Key, model_name(Doc), LinkNames).


%%--------------------------------------------------------------------
%% @doc
%% Removes links from the document with given key. There is special link name 'all' which removes all links.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(key(), model_behaviour:model_type(), link_name() | [link_name()] | all) -> ok | generic_error().
delete_links(Key, ModelName, LinkNames) when is_list(LinkNames); LinkNames =:= all ->
    _ModelConfig = ModelName:model_init(),
    exec_driver(ModelName, ?PERSISTENCE_DRIVER, delete_links, [Key, LinkNames]);
delete_links(Key, ModelName, LinkName) ->
    delete_links(Key, ModelName, [LinkName]).


%%--------------------------------------------------------------------
%% @doc
%% Gets specified link from given document.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(document(), link_name()) -> {ok, normalized_link_target()} | link_error().
fetch_link(#document{key = Key} = Doc, LinkName) ->
    fetch_link(Key, model_name(Doc), LinkName).


%%--------------------------------------------------------------------
%% @doc
%% Gets specified link from the document given by key.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(key(), model_behaviour:model_type(), link_name()) ->
    {ok, normalized_link_target()} | generic_error().
fetch_link(Key, ModelName, LinkName) ->
    _ModelConfig = ModelName:model_init(),
    exec_driver(ModelName, ?PERSISTENCE_DRIVER, fetch_link, [Key, LinkName]).


%%--------------------------------------------------------------------
%% @doc
%% Gets document pointed by given link of given document.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link_target(document(), link_name()) -> {ok, document()} | generic_error().
fetch_link_target(#document{key = Key} = Doc, LinkName) ->
    fetch_link_target(Key, model_name(Doc), LinkName).


%%--------------------------------------------------------------------
%% @doc
%% Gets document pointed by given link of document given by key.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link_target(key(), model_behaviour:model_type(), link_name()) ->
    {ok, document()} | generic_error().
fetch_link_target(Key, ModelName, LinkName) ->
    case fetch_link(Key, ModelName, LinkName) of
        {ok, _Target = {TargetKey, TargetModel}} ->
            TargetModel:get(TargetKey);
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Executes given function for each link of given document - similar to 'foldl'.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(document(), fun((link_name(), link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | link_error().
foreach_link(#document{key = Key} = Doc, Fun, AccIn) ->
    foreach_link(Key, model_name(Doc), Fun, AccIn).


%%--------------------------------------------------------------------
%% @doc
%% Executes given function for each link of the document given by key - similar to 'foldl'.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(Key :: key(), ModelName :: model_behaviour:model_type(),
    fun((link_name(), link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | link_error().
foreach_link(Key, ModelName, Fun, AccIn) ->
    exec_driver(ModelName, ?PERSISTENCE_DRIVER, foreach_link, [Key, Fun, AccIn]).


%%--------------------------------------------------------------------
%% @doc
%% "Walks" from link to link and fetches either all encountered documents (for Mode == get_all - not yet implemted),
%% or just last document (for Mode == get_leaf). Starts on given document.
%% @end
%%--------------------------------------------------------------------
-spec link_walk(document(), [link_name()], get_leaf | get_all) ->
    {ok, document() | [document()]} | link_error() | get_error().
link_walk(#document{key = StartKey} = StartDoc, LinkNames, Mode) when is_atom(Mode), is_list(LinkNames) ->
    link_walk(StartKey, model_name(StartDoc), LinkNames, Mode).


%%--------------------------------------------------------------------
%% @doc
%% "Walks" from link to link and fetches either all encountered documents (for Mode == get_all - not yet implemted),
%% or just last document (for Mode == get_leaf). Starts on the document given by key.
%% @end
%%--------------------------------------------------------------------
-spec link_walk(Key :: key(), ModelName :: model_behaviour:model_type(), [link_name()], get_leaf | get_all) ->
    {ok, document() | [document()]} | link_error() | get_error().
link_walk(_Key, _ModelName, _Links, get_all) ->
    erlang:error(not_inplemented);
link_walk(Key, ModelName, [LastLink], get_leaf) ->
    case fetch_link_target(Key, ModelName, LastLink) of
        {ok, #document{} = Leaf} ->
            {ok, Leaf};
        {error, Reason} ->
            {error, Reason}
    end;
link_walk(Key, ModelName, [NextLink | R], get_leaf) ->
    case fetch_link(Key, ModelName, NextLink) of
        {ok, {TargetKey, TargetMod}} ->
            link_walk(TargetKey, TargetMod, R, get_leaf);
        {error, Reason} ->
            {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

normalize_link_target([]) ->
    [];
normalize_link_target([Link | R]) ->
    [normalize_link_target(Link) | normalize_link_target(R)];
normalize_link_target({LinkName, #document{key = TargetKey} = Doc}) ->
    normalize_link_target({LinkName, {TargetKey, model_name(Doc)}});
normalize_link_target({_LinkName, {_TargetKey, ModelName}} = ValidLink) when is_atom(ModelName) ->
    ValidLink.



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
    Method :: store_driver_behaviour:driver_action(), [term()]) ->
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

