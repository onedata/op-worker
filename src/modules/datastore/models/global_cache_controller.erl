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
-export([save/1, get/1, list/0, list/1, exists/1, delete/1, delete/2, update/2, create/1, model_init/0,
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
            U = V#global_cache_controller.last_user,
            case (timer:now_diff(Now, T) >= 1000*DocAge) and (U =:= non) of
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
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key(), datastore:delete_predicate()) -> ok | datastore:generic_error().
delete(Key, Pred) ->
    datastore:delete(global_only, ?MODULE, Key, Pred).

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
'after'(ModelName, save, disk_only, [Doc], {ok, _}) ->
    end_disk_op(Doc#document.key, ModelName, save);
'after'(ModelName, update, disk_only, [Key, _Diff], {ok, _}) ->
    end_disk_op(Key, ModelName, update);
'after'(ModelName, create, disk_only, [Doc], {ok, _}) ->
    end_disk_op(Doc#document.key, ModelName, create);
'after'(ModelName, get, _Level, [Key], {ok, _}) ->
    update_usage_info(Key, ModelName);
'after'(ModelName, delete, local_only, [Key, _Pred], ok) ->
    end_disk_op(Key, ModelName, delete);
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
    start_disk_op(Doc#document.key, ModelName);
before(ModelName, update, disk_only, [Key, _Diff]) ->
    start_disk_op(Key, ModelName);
before(ModelName, create, disk_only, [Doc]) ->
    start_disk_op(Doc#document.key, ModelName);
before(ModelName, delete, disk_only, [Key, _Pred]) ->
    start_disk_op(Key, ModelName);
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
    UpdateFun = fun(Record) ->
        Record#global_cache_controller{timestamp = os:timestamp()}
    end,
    {ok, _} = update(Uuid, UpdateFun).

end_disk_op(Key, ModelName, Op) ->
    try
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),
        Pid = self(),
        case Op of
            delete ->
                Pred = fun() ->
                    LastUser = case ?MODULE:get(Uuid) of % get is also BIF
                                   {ok, Doc} ->
                                       Value = Doc#document.value,
                                       Value#global_cache_controller.last_user;
                                   {error, {not_found, _}} ->
                                       non
                               end,
                    case LastUser of
                        Pid ->
                            true;
                        _ ->
                            false
                    end
                end,
                delete(Uuid, Pred);
            _ ->
                UpdateFun = fun
                    (#global_cache_controller{last_user = Pid} = Record) ->
                        Record#global_cache_controller{last_user = non}
                end,
                update(Uuid, UpdateFun)
        end,
        ok
    catch
        E1:E2 ->
            ?error_stacktrace("Error in global cache controller end_disk_op. "
                +"Args: ~p. Error: ~p:~p.", [{Key, ModelName}, E1, E2]),
            {error, ending_disk_op_failed}
    end.

start_disk_op(Key, ModelName) ->
    try
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),
        Pid = self(),

        V = #global_cache_controller{last_user = Pid, timestamp = os:timestamp()},
        Doc = #document{key = Uuid, value = V},
        save(Doc),

        {ok, SleepTime} = application:get_env(?APP_NAME, cache_to_disk_delay_ms),
        timer:sleep(SleepTime),
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),

        LastUser = case ?MODULE:get(Uuid) of % get is also BIF
            {ok, Doc} ->
                Value = Doc#document.value,
                Value#global_cache_controller.last_user;
            {error, {not_found, _}} ->
                Pid
        end,
        case LastUser of
            Pid ->
                {ok, SavedValue} = ModelName:get(Key),
                {ok, save, SavedValue};
            _ ->
                {error, not_last_user}
        end
    catch
        E1:E2 ->
            ?error_stacktrace("Error in global cache controller start_disk_op. "
                +"Args: ~p. Error: ~p:~p.", [{Key, ModelName}, E1, E2]),
            {error, preparing_disk_op_failed}
    end.

%% dodac monitorowanie linkow