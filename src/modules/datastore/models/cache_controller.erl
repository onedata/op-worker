%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model that is used to controle memory utilization by caches.
%%% @end
%%%-------------------------------------------------------------------
-module(cache_controller).
-author("Michal Wrzeszcz").
-behaviour(model_behaviour).

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_model.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% model_behaviour callbacks and API
-export([save/1, get/1, list/0, list/1, exists/1, delete/1, delete/2, update/2, create/1,
    save/2, get/2, list/2, exists/2, delete/3, update/3, create/2,
    model_init/0, 'after'/5, before/4, list_docs_to_be_dumped/1]).

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
    datastore:save(?STORE_LEVEL, Document).

save(Level, Document) ->
    datastore:save(Level, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2. 
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

update(Level, Key, Diff) ->
    datastore:update(Level, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1. 
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

create(Level, Document) ->
    datastore:create(Level, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

get(Level, Key) ->
    datastore:get(Level, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    datastore:list(?STORE_LEVEL, ?MODEL_NAME, ?GET_ALL, []).

list(Level) ->
    datastore:list(Level, ?MODEL_NAME, ?GET_ALL, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of records older then DocAge (in ms).
%% @end
%%--------------------------------------------------------------------
-spec list(Level :: datastore:store_level(), DocAge :: integer()) ->
    {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list(Level, MinDocAge) ->
    Now = os:timestamp(),
    Filter = fun
        ('$end_of_table', Acc) ->
            {abort, Acc};
        (#document{key = Uuid, value = V}, Acc) ->
            T = V#cache_controller.timestamp,
            U = V#cache_controller.last_user,
            Age = timer:now_diff(Now, T),
            case U of
                non when Age >= 1000*MinDocAge ->
                    {next, [Uuid | Acc]};
                _ ->
                    {next, Acc}
            end
    end,
    datastore:list(Level, ?MODEL_NAME, Filter, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of records not persisted.
%% @end
%%--------------------------------------------------------------------
-spec list_docs_to_be_dumped(Level :: datastore:store_level()) ->
    {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list_docs_to_be_dumped(Level) ->
    Filter = fun
        ('$end_of_table', Acc) ->
            {abort, Acc};
        (#document{value = #cache_controller{last_user = non}}, Acc) ->
            {next, Acc};
        (#document{key = Uuid}, Acc) ->
            {next, [Uuid | Acc]}
    end,
    datastore:list(Level, ?MODEL_NAME, Filter, []).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

delete(Level, Key) ->
    datastore:delete(Level, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:store_level(), datastore:key(), datastore:delete_predicate()) -> ok | datastore:generic_error().
delete(Level, Key, Pred) ->
    datastore:delete(Level, ?MODULE, Key, Pred).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1. 
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

exists(Level, Key) ->
    ?RESPONSE(datastore:exists(Level, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0. 
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(cc_bucket, get_hooks_config(),
        ?DEFAULT_STORE_LEVEL, ?DEFAULT_STORE_LEVEL, false).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5. 
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok | datastore:generic_error().
'after'(ModelName, get, disk_only, [Key], {ok, Doc}) ->
    Level2 = caches_controller:cache_to_datastore_level(ModelName),
    update_usage_info(Key, ModelName, Doc, Level2);
'after'(ModelName, get, Level, [Key], {ok, _}) ->
    update_usage_info(Key, ModelName, Level);
'after'(ModelName, exists, Level, [Key], {ok, true}) ->
    update_usage_info(Key, ModelName, Level);
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
    ok | {ok, save, [datastore:document()]} | datastore:generic_error().
before(ModelName, Method, Level, Context) ->
    Level2 = caches_controller:cache_to_datastore_level(ModelName),
    before(ModelName, Method, Level, Context, Level2).
before(ModelName, save, disk_only, [Doc] = Args, Level2) ->
    start_disk_op(Doc#document.key, ModelName, save, Args, Level2);
before(ModelName, update, disk_only, [Key, _Diff] = Args, Level2) ->
    start_disk_op(Key, ModelName, update, Args, Level2);
before(ModelName, create, disk_only, [Doc] = Args, Level2) ->
    start_disk_op(Doc#document.key, ModelName, create, Args, Level2);
before(ModelName, delete, disk_only, [Key, _Pred] = Args, Level2) ->
    start_disk_op(Key, ModelName, delete, Args, Level2);
before(ModelName, get, disk_only, [Key], Level2) ->
    check_get(Key, ModelName, Level2);
before(ModelName, fetch_link, disk_only, [Key, LinkName], Level2) ->
    check_get(Key, ModelName, LinkName, Level2);
before(ModelName, delete_links, disk_only, [Key, LinkNames] = Args, Level2) ->
    log_link_del(Key, ModelName, LinkNames, start, Args, Level2);
before(_ModelName, _Method, _Level, _Context, _Level2) ->
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
    caches_controller:get_hooks_config(?GLOBAL_CACHES ++ ?LOCAL_CACHES).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates information about usage of a document.
%% @end
%%--------------------------------------------------------------------
-spec update_usage_info(Key :: datastore:key(), ModelName :: model_behaviour:model_type(),
    Level :: datastore:store_level()) -> {ok, datastore:key()} | datastore:generic_error().
update_usage_info(Key, ModelName, Level) ->
    Uuid = caches_controller:get_cache_uuid(Key, ModelName),
    UpdateFun = fun(Record) ->
        Record#cache_controller{timestamp = os:timestamp()}
    end,
    case update(Level, Uuid, UpdateFun) of
        {ok, Ok} ->
            {ok, Ok};
        {error, {not_found, ?MODEL_NAME}} ->
            TS = os:timestamp(),
            V = #cache_controller{timestamp = TS, last_action_time = TS},
            Doc = #document{key = Uuid, value = V},
            create(Level, Doc)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates information about usage of a document and saves doc to memory.
%% @end
%%--------------------------------------------------------------------
-spec update_usage_info(Key :: datastore:key(), ModelName :: model_behaviour:model_type(),
    Doc :: datastore:document(), Level :: datastore:store_level()) -> {ok, datastore:key()} | datastore:generic_error().
update_usage_info(Key, ModelName, Doc, Level) ->
    update_usage_info(Key, ModelName, Level),
    datastore:create(Level, Doc),
    datastore:foreach_link(disk_only, Key, ModelName,
        fun(LinkName, LinkTarget, _) ->
            datastore:add_links(Level, Key, ModelName, {LinkName, LinkTarget})
        end,
        []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if get operation should be performed.
%% @end
%%--------------------------------------------------------------------
-spec check_get(Key :: datastore:key(), ModelName :: model_behaviour:model_type(), Level :: datastore:store_level()) ->
    ok | {error, {not_found, model_behaviour:model_type()}}.
check_get(Key, ModelName, Level) ->
    Uuid = caches_controller:get_cache_uuid(Key, ModelName),
    case get(Level, Uuid) of
        {ok, Doc} ->
            Value = Doc#document.value,
            case Value#cache_controller.action of
                delete -> {error, {not_found, ModelName}};
                _ -> ok
            end;
        {error, {not_found, _}} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if get link operation should be performed.
%% @end
%%--------------------------------------------------------------------
-spec check_get(Key :: datastore:key(), ModelName :: model_behaviour:model_type(),
    LinkName :: datastore:link_name(), Level :: datastore:store_level()) -> ok | {error, link_not_found}.
check_get(Key, ModelName, LinkName, Level) ->
    Uuid = caches_controller:get_cache_uuid(Key, ModelName),
    case get(Level, Uuid) of
        {ok, Doc} ->
            Value = Doc#document.value,
            Links = Value#cache_controller.deleted_links,
            case lists:member(LinkName, Links) orelse lists:member(all, Links) of
                true -> {error, link_not_found};
                _ -> ok
            end;
        {error, {not_found, _}} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Delates info about dumping of cache to disk.
%% @end
%%--------------------------------------------------------------------
-spec delete_dump_info(Uuid :: binary(), Owner :: list(), Level :: datastore:store_level()) ->
    ok | datastore:generic_error().
delete_dump_info(Uuid, Owner, Level) ->
    Pred = fun() ->
        LastUser = case get(Level, Uuid) of
                       {ok, Doc} ->
                           Value = Doc#document.value,
                           Value#cache_controller.last_user;
                       {error, {not_found, _}} ->
                           non
                   end,
        case LastUser of
            Owner ->
                true;
            _ ->
                false
        end
    end,
    delete(Level, Uuid, Pred).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves dump information after disk operation.
%% @end
%%--------------------------------------------------------------------
-spec end_disk_op(Uuid :: binary(), Owner :: list(), ModelName :: model_behaviour:model_type(),
    Op :: atom(), Level :: datastore:store_level()) -> ok | {error, ending_disk_op_failed}.
end_disk_op(Uuid, Owner, ModelName, Op, Level) ->
    try
        case Op of
            delete ->
                delete_dump_info(Uuid, Owner, Level);
            _ ->
                UpdateFun = fun
                    (#cache_controller{last_user = LastUser} = Record) ->
                        case LastUser of
                            Owner ->
                                Record#cache_controller{last_user = non, action = non,
                                    last_action_time = os:timestamp()};
                            _ ->
                                throw(user_changed)
                        end
                end,
                update(Level, Uuid, UpdateFun)
        end,
        ok
    catch
        E1:E2 ->
            ?error_stacktrace("Error in cache_controller end_disk_op. Args: ~p. Error: ~p:~p.",
                [{Uuid, Owner, ModelName, Op}, E1, E2]),
            {error, ending_disk_op_failed}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves dump information about disk operation and decides if it should be done.
%% @end
%%--------------------------------------------------------------------
-spec start_disk_op(Key :: datastore:key(), ModelName :: model_behaviour:model_type(),
    Op :: atom(), Args :: list(), Level :: datastore:store_level()) -> ok | {task, task_manager:task()} | {error, Error} when
    Error :: not_last_user | preparing_disk_op_failed.
start_disk_op(Key, ModelName, Op, Args, Level) ->
    try
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),
        Pid = pid_to_list(self()),

        UpdateFun = fun(Record) ->
            Record#cache_controller{last_user = Pid, timestamp = os:timestamp(), action = Op}
        end,
        % TODO - not transactional updates in local store - add transactional create and update on ets
        % TODO - create/delete possible race
        case update(Level, Uuid, UpdateFun) of
            {ok, _Ok} ->
                ok;
            {error, {not_found, ?MODEL_NAME}} ->
                TS = os:timestamp(),
                V = #cache_controller{last_user = Pid, timestamp = TS, action = Op, last_action_time = TS},
                Doc = #document{key = Uuid, value = V},
                create(Doc)
        end,

        {ok, SleepTime} = application:get_env(?APP_NAME, cache_to_disk_delay_ms),
        timer:sleep(SleepTime),

        Task = fun() ->
            {LastUser, LAT} = case get(Level, Uuid) of
                                  {ok, Doc2} ->
                                      Value = Doc2#document.value,
                                      {Value#cache_controller.last_user, Value#cache_controller.last_action_time};
                                  {error, {not_found, _}} ->
                                      {Pid, 0}
                              end,
            ToDo = case LastUser of
                Pid ->
                    case Op of
                        delete ->
                            ok;
                        _ ->
                            {ok, SavedValue} = datastore:get(Level, ModelName, Key),
                            {ok, save, [SavedValue]}
                    end;
                _ ->
                    {ok, ForceTime} = application:get_env(?APP_NAME, cache_to_disk_force_delay_ms),
                    case timer:now_diff(os:timestamp(), LAT) >= 1000*ForceTime of
                        true ->
                            UpdateFun2 = fun(Record) ->
                                Record#cache_controller{last_action_time = os:timestamp()}
                            end,
                            update(Level, Uuid, UpdateFun2),
                            {ok, SavedValue} = datastore:get(Level, ModelName, Key),
                            {ok, save, [SavedValue]};
                        _ ->
                            {error, not_last_user}
                    end
            end,

            ModelConfig = ModelName:model_init(),
            Ans = case ToDo of
                {ok, NewMethod, NewArgs} ->
                    FullArgs = [ModelConfig | NewArgs],
                    worker_proxy:call(datastore_worker, {driver_call, ?PERSISTENCE_DRIVER, NewMethod, FullArgs}, timer:minutes(5));
                ok ->
                    FullArgs = [ModelConfig | Args],
                    worker_proxy:call(datastore_worker, {driver_call, ?PERSISTENCE_DRIVER, Op, FullArgs}, timer:minutes(5));
                Other ->
                    Other
            end,

            ok = case Ans of
                     ok ->
                         end_disk_op(Uuid, Pid, ModelName, Op, Level);
                     {ok, _} ->
                         end_disk_op(Uuid, Pid, ModelName, Op, Level);
                     {error, not_last_user} -> ok;
                     WrongAns -> WrongAns
                 end
        end,
        {task, Task}
    catch
        E1:E2 ->
            ?error_stacktrace("Error in cache_controller start_disk_op. Args: ~p. Error: ~p:~p.",
                [{Key, ModelName, Op}, E1, E2]),
            {error, preparing_disk_op_failed}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves info about links deleting.
%% @end
%%--------------------------------------------------------------------
-spec log_link_del(Key :: datastore:key(), ModelName :: model_behaviour:model_type(),
    LinkNames :: list() | all, Phase :: start | stop, Args :: list(), Level :: datastore:store_level()) ->
    ok | {error, preparing_disk_op_failed} | {error, ending_disk_op_failed}.
log_link_del(Key, ModelName, LinkNames, start, Args, Level) ->
    try
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),
        UpdateFun = fun(#cache_controller{deleted_links = DL} = Record) ->
            case LinkNames of
                LNs when is_list(LNs) ->
                    Record#cache_controller{deleted_links = DL ++ LinkNames};
                _ ->
                    Record#cache_controller{deleted_links = DL ++ [LinkNames]}
            end
        end,
        case update(Level, Uuid, UpdateFun) of
            {ok, _} ->
                ok;
            {error, {not_found,cache_controller}} ->
                TS = os:timestamp(),
                V = case LinkNames of
                        LNs when is_list(LNs) ->
                            #cache_controller{timestamp = TS, deleted_links = LinkNames};
                        _ ->
                            #cache_controller{timestamp = TS, deleted_links = [LinkNames]}
                    end,
                Doc = #document{key = Uuid, value = V},
                % TODO what happen if create fails?
                create(Level, Doc),
                Task = fun() ->
                    ModelConfig = ModelName:model_init(),
                    FullArgs = [ModelConfig | Args],
                    ok = worker_proxy:call(datastore_worker, {driver_call, ?PERSISTENCE_DRIVER, delete_links, FullArgs}, timer:minutes(5)),
                    {ok, _} = log_link_del(Key, ModelName, LinkNames, stop, Args, Level)
                end,
                {task, Task}
        end
    catch
        E1:E2 ->
            ?error_stacktrace("Error in cache_controller log_link_del. Args: ~p. Error: ~p:~p.",
                [{Key, ModelName, LinkNames, start}, E1, E2]),
            {error, preparing_disk_op_failed}
    end;
log_link_del(Key, ModelName, LinkNames, stop, _Args, Level) ->
    try
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),
        UpdateFun = fun(#cache_controller{deleted_links = DL} = Record) ->
            case LinkNames of
                LNs when is_list(LNs) ->
                    Record#cache_controller{deleted_links = DL -- LinkNames};
                _ ->
                    Record#cache_controller{deleted_links = DL -- [LinkNames]}
            end
        end,
        update(Level, Uuid, UpdateFun)
    catch
        E1:E2 ->
            ?error_stacktrace("Error in cache_controller log_link_del. Args: ~p. Error: ~p:~p.",
                [{Key, ModelName, LinkNames, stop}, E1, E2]),
            {error, preparing_disk_op_failed}
    end.
