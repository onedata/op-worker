%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model that is used to controle memory utilization by global cache.
%%% @end
%%%-------------------------------------------------------------------
-module(global_cache_controller).
-author("Michal Wrzeszcz").
-behaviour(model_behaviour).

-include("global_definitions.hrl").
-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_model.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include_lib("ctool/include/logging.hrl").

%% model_behaviour callbacks and API
-export([save/1, get/1, list/0, list/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4]).

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1. 
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    datastore:save(global_only, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2. 
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(global_only, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1. 
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    datastore:create(global_only, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(global_only, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    datastore:list(global_only, ?MODEL_NAME, ?GET_ALL, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns list records older then DocAge (in ms).
%% @end
%%--------------------------------------------------------------------
-spec list(DocAge :: integer()) -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list(DocAge) ->
    Now = os:timestamp(),
    Filter = fun
        ('$end_of_table', Acc) ->
            {abort, Acc};
        (#document{key = Uuid, value = V}, Acc) ->
            T = V#global_cache_controller.timestamp,
            case timer:now_diff(Now, T) >= 1000*DocAge of
                true ->
                    {next, [Uuid | Acc]};
                false ->
                    {next, Acc}
            end
    end,
    datastore:list(global_only, ?MODEL_NAME, Filter, []).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(global_only, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1. 
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(global_only, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0. 
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(global_cc_bucket, get_hooks_config(),
        ?DEFAULT_STORE_LEVEL, ?DEFAULT_STORE_LEVEL, false).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5. 
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(ModelName, save, Level, [Doc], {ok, _}) ->
    update_usage_info(Doc#document.key, ModelName, Level, Doc);
'after'(ModelName, update, Level, [Key, Diff], {ok, _}) ->
    update_usage_info(Key, ModelName, Level, {Key, Diff});
'after'(ModelName, create, Level, [Doc], {ok, _}) ->
    update_usage_info(Doc#document.key, ModelName, Level, Doc);
'after'(ModelName, get, _Level, [Key], _ReturnValue) ->
    update_usage_info(Key, ModelName);
'after'(ModelName, delete, Level, [Key, Pred], ok) ->
    del_usage_info(Key, ModelName, Level, {Key, Pred});
'after'(ModelName, exists, _Level, [Key], {ok, true}) ->
    update_usage_info(Key, ModelName);
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4. 
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) ->
    ok | datastore:generic_error().
before(ModelName, save, disk_only, [Doc]) ->
    check_disk_op(Doc#document.key, ModelName, Doc, save);
before(ModelName, update, disk_only, [Key, Diff]) ->
    check_disk_op(Key, ModelName, {Key, Diff}, save);
before(ModelName, create, disk_only, [Doc]) ->
    check_disk_op(Doc#document.key, ModelName, Doc, save);
before(ModelName, delete, disk_only, [Key, Pred]) ->
    check_disk_op(Key, ModelName, {Key, Pred}, del);
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Provides hooks configuration.
%% @end
%%--------------------------------------------------------------------
-spec get_hooks_config() -> list().
get_hooks_config() ->
    caches_controller:get_hooks_config(?GLOBAL_CACHES).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates information about usage of a document.
%% @end
%%--------------------------------------------------------------------
-spec update_usage_info(Key :: datastore:key(), ModelName :: model_behaviour:model_type()) ->
    {ok, datastore:key()} | datastore:generic_error().
update_usage_info(Key, ModelName) ->
    Uuid = caches_controller:get_cache_uuid(Key, ModelName),
    V = #global_cache_controller{timestamp = os:timestamp()},
    Doc = #document{key = Uuid, value = V},
    save(Doc).

update_usage_info(Key, ModelName, Level, Value) ->
    try
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),
        {OldStatus, OldV} = case ?MODULE:get(Uuid) of % get is also BIF
            {ok, OldDoc} ->
                ValueDoc = OldDoc#document.value,
                {ValueDoc#global_cache_controller.status, ValueDoc#global_cache_controller.to_be_saved};
            {error, {not_found, _}} ->
                {[], []}
        end,
        case {OldStatus, Level, OldV, Value} of
            {_, global_only, _, V} ->
                SaveV = #global_cache_controller{timestamp = os:timestamp(), status = to_disk, to_be_saved = V},
                Doc = #document{key = Uuid, value = SaveV},
                save(Doc);
            {to_disk, disk_only, V, V} ->
                SaveV = #global_cache_controller{timestamp = os:timestamp(), status = ok, to_be_saved = []},
                Doc = #document{key = Uuid, value = SaveV},
                save(Doc);
            ToLog ->
                ?debug("Not standard global cache update_usage_info: ~p, Args: ~p", [ToLog, {Key, ModelName, Level, Value}]),
                ok
        end
    catch
        E1:E2 ->
            ?error_stacktrace("Error in global cache controller update_usage_info. "
                +"Args: ~p. Error: ~p:~p.", [{Key, ModelName, Level, Value}, E1, E2])
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes information about usage of a document.
%% @end
%%--------------------------------------------------------------------
-spec del_usage_info(Key :: datastore:key(), ModelName :: model_behaviour:model_type()) ->
    ok | datastore:generic_error().
del_usage_info(Key, ModelName, Level, Value) ->
    try
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),
        {OldStatus, OldV} = case ?MODULE:get(Uuid) of % get is also BIF
                                {ok, OldDoc} ->
                                    ValueDoc = OldDoc#document.value,
                                    {ValueDoc#global_cache_controller.status, ValueDoc#global_cache_controller.to_be_saved};
                                {error, {not_found, _}} ->
                                    {[], []}
                            end,
        case {OldStatus, Level, OldV, Value} of
            {_, global_only, _, V} ->
                SaveV = #global_cache_controller{timestamp = os:timestamp(), status = to_del, to_be_saved = V},
                Doc = #document{key = Uuid, value = SaveV},
                save(Doc);
            {to_del, disk_only, V, V} ->
                delete(Uuid);
            ToLog ->
                ?debug("Not standard global cache del_usage_info: ~p, Args: ~p", [ToLog, {Key, ModelName, Level, Value}]),
                ok
        end
    catch
        E1:E2 ->
            ?error_stacktrace("Error in global cache controller del_usage_info. "
                +"Args: ~p. Error: ~p:~p.", [{Key, ModelName, Level, Value}, E1, E2])
    end.

check_disk_op(Key, ModelName, Value, Op) ->
    try
        {ok, SleepTime} = application:get_env(?APP_NAME, cache_to_disk_delay_ms),
        timer:sleep(SleepTime),
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),
        {Status, V} = case ?MODULE:get(Uuid) of % get is also BIF
            {ok, OldDoc} ->
                ValueDoc = OldDoc#document.value,
                {ValueDoc#global_cache_controller.status, ValueDoc#global_cache_controller.to_be_saved};
            {error, {not_found, _}} ->
                {[], []}
        end,
        case {Op, Status, V, Value} of
            {save, to_disk, ToDisk, ToDisk} ->
                ok;
            {del, to_del, ToDel, ToDel} ->
                ok;
            ToLog ->
                ?debug("Not standard global cache check_disk_op: ~p, Args: ~p", [ToLog, {Key, ModelName, Value, Op}]),
                {error, cache_value_changed}
        end
    catch
        E1:E2 ->
            ?error_stacktrace("Error in global cache controller check_disk_op. "
                +"Args: ~p. Error: ~p:~p.", [{Key, ModelName, Value, Op}, E1, E2])
    end.

przeniesc memory post_hook do disk pre_hook
dodac monitorowanie linkow
zamieniamy get/save na update, przy czyszczeniu sprawdzamy czy jest zgodnosc cache/zawartosc pamieci (po skasowaniu info o cache)
czyszczac cache uzywamy delete bez hookow (sami kasujemy info o cache)
co jakis czas robimy czyszczenie starych wpisow mimo wszystko, zeby zapewnic spojnosc cache/dysk