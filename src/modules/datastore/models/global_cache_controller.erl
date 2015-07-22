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
    'after'/5, before/4, list_docs_be_dumped/0]).

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
%% Returns list of records older then DocAge (in ms).
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
%% Returns list of records not persisted.
%% @end
%%--------------------------------------------------------------------
-spec list_docs_be_dumped() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list_docs_be_dumped() ->
    Filter = fun
        ('$end_of_table', Acc) ->
            {abort, Acc};
        (#document{key = Uuid, value = V}, Acc) ->
            U = V#global_cache_controller.last_user,
            case U =/= non of
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
'after'(ModelName, get, disk_only, [Key], {ok, Doc}) ->
    update_usage_info(Key, ModelName, Doc);
'after'(ModelName, get, global_only, [Key], {ok, _}) ->
    update_usage_info(Key, ModelName);
'after'(ModelName, delete, disk_only, [Key, _Pred], ok) ->
    end_disk_op(Key, ModelName, delete);
'after'(ModelName, delete, global_only, [Key, _Pred], ok) ->
    delete_dump_info(Key, ModelName);
'after'(ModelName, exists, global_only, [Key], {ok, true}) ->
    update_usage_info(Key, ModelName);
'after'(ModelName, delete_links, disk_only, [Key, LinkNames], ok) ->
    log_link_del(Key, ModelName, LinkNames, stop);
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
    start_disk_op(Doc#document.key, ModelName, save);
before(ModelName, update, disk_only, [Key, _Diff]) ->
    start_disk_op(Key, ModelName, update);
before(ModelName, create, disk_only, [Doc]) ->
    start_disk_op(Doc#document.key, ModelName, create);
before(ModelName, delete, disk_only, [Key, _Pred]) ->
    start_disk_op(Key, ModelName, delete);
before(ModelName, get, disk_only, [Key]) ->
    check_get(Key, ModelName);
before(ModelName, fetch_link, disk_only, [Key, LinkName]) ->
    check_get(Key, ModelName, LinkName);
before(ModelName, delete_links, disk_only, [Key, LinkNames]) ->
    log_link_del(Key, ModelName, LinkNames, start);
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
    case update(Uuid, UpdateFun) of
        {ok, Ok} ->
            {ok, Ok};
        {error,{not_found,global_cache_controller}} ->
            TS = os:timestamp(),
            V = #global_cache_controller{timestamp = TS, last_action_time = TS},
            Doc = #document{key = Uuid, value = V},
            create(Doc)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates information about usage of a document and saves doc to memory.
%% @end
%%--------------------------------------------------------------------
-spec update_usage_info(Key :: datastore:key(), ModelName :: model_behaviour:model_type(),
    Doc :: datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().
update_usage_info(Key, ModelName, Doc) ->
    update_usage_info(Key, ModelName),
    datastore:create(global_only, Doc),
    datastore:foreach_link(disk_only, Key, ModelName,
        fun(LinkName, LinkTarget, _) ->
            datastore:add_links(global_only, Key, ModelName, {LinkName, LinkTarget})
        end,
    []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if get operation should be performed.
%% @end
%%--------------------------------------------------------------------
-spec check_get(Key :: datastore:key(), ModelName :: model_behaviour:model_type()) ->
    ok | {error, {not_found, model_behaviour:model_type()}}.
check_get(Key, ModelName) ->
    Uuid = caches_controller:get_cache_uuid(Key, ModelName),
    case get(Uuid) of
        {ok, Doc} ->
            Value = Doc#document.value,
            case Value#global_cache_controller.action of
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
    LinkName :: datastore:link_name()) -> ok | {error, link_not_found}.
check_get(Key, ModelName, LinkName) ->
    Uuid = caches_controller:get_cache_uuid(Key, ModelName),
    case get(Uuid) of
        {ok, Doc} ->
            Value = Doc#document.value,
            Links = Value#global_cache_controller.deleted_links,
            case lists:member(LinkName, Links) or lists:member(all, Links) of
                true -> {error, link_not_found};
                _ -> ok
            end;
        {error, {not_found, _}} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Delates info about dumping of cache to disk
%% @end
%%--------------------------------------------------------------------
-spec delete_dump_info(Key :: datastore:key(), ModelName :: model_behaviour:model_type()) ->
    ok | datastore:generic_error().
delete_dump_info(Key, ModelName) ->
    Uuid = caches_controller:get_cache_uuid(Key, ModelName),
    delete_dump_info(Uuid).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Delates info about dumping of cache to disk
%% @end
%%--------------------------------------------------------------------
-spec delete_dump_info(Uuid :: binary()) ->
    ok | datastore:generic_error().
delete_dump_info(Uuid) ->
    Pid = self(),
    Pred = fun() ->
        LastUser = case get(Uuid) of
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
    delete(Uuid, Pred).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves dump information after disk operation.
%% @end
%%--------------------------------------------------------------------
-spec end_disk_op(Key :: datastore:key(), ModelName :: model_behaviour:model_type(),
    Op :: atom()) -> ok | {error, ending_disk_op_failed}.
end_disk_op(Key, ModelName, Op) ->
    try
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),
        case Op of
            delete ->
                delete_dump_info(Uuid);
            _ ->
                UpdateFun = fun
                    (#global_cache_controller{last_user = LastUser} = Record) ->
                        case LastUser of
                            Pid ->
                                Record#global_cache_controller{last_user = non, action = non, last_action_time = os:timestamp()};
                            _ ->
                                throw(user_changed)
                        end
                end,
                update(Uuid, UpdateFun)
        end,
        ok
    catch
        E1:E2 ->
            ?error_stacktrace("Error in global cache controller end_disk_op. "
                ++ "Args: ~p. Error: ~p:~p.", [{Key, ModelName, Op}, E1, E2]),
            {error, ending_disk_op_failed}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves dump information about disk operation and decides if it should be done.
%% @end
%%--------------------------------------------------------------------
-spec start_disk_op(Key :: datastore:key(), ModelName :: model_behaviour:model_type(),
    Op :: atom()) -> ok | {ok, save, [SavedValue]} | {error, Error} when
    SavedValue :: datastore:document(),
    Error :: not_last_user | preparing_disk_op_failed.
start_disk_op(Key, ModelName, Op) ->
    try
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),
        Pid = self(),

        UpdateFun = fun(Record) ->
            Record#global_cache_controller{last_user = Pid, timestamp = os:timestamp(), action = Op}
        end,
        case update(Uuid, UpdateFun) of
            {ok, Ok} ->
                {ok, Ok};
            {error,{not_found,global_cache_controller}} ->
                TS = os:timestamp(),
                V = #global_cache_controller{last_user = Pid, timestamp = TS, action = Op, last_action_time = TS},
                Doc = #document{key = Uuid, value = V},
                create(Doc)
        end,

        {ok, SleepTime} = application:get_env(?APP_NAME, cache_to_disk_delay_ms),
        timer:sleep(SleepTime),
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),

        {LastUser, LAT} = case get(Uuid) of
                              {ok, Doc2} ->
                                  Value = Doc2#document.value,
                                  {Value#global_cache_controller.last_user, Value#global_cache_controller.last_action_time};
                              {error, {not_found, _}} ->
                                  {Pid, 0}
                          end,
        case LastUser of
            Pid ->
                case Op of
                    delete ->
                        ok;
                    _ ->
                        {ok, SavedValue} = datastore:get(global_only, ModelName, Key),
                        {ok, save, [SavedValue]}
                end;
            _ ->
                {ok, ForceTime} = application:get_env(?APP_NAME, cache_to_disk_force_delay_ms),
                case timer:now_diff(os:timestamp(), LAT) >= 1000*ForceTime of
                    true ->
                        UpdateFun2 = fun(Record) ->
                            Record#global_cache_controller{last_action_time = os:timestamp()}
                        end,
                        update(Uuid, UpdateFun2),
                        {ok, SavedValue} = datastore:get(global_only, ModelName, Key),
                        {ok, save, [SavedValue]};
                    _ ->
                        {error, not_last_user}
                end
        end
    catch
        E1:E2 ->
            ?error_stacktrace("Error in global cache controller start_disk_op. "
            ++ "Args: ~p. Error: ~p:~p.", [{Key, ModelName, Op}, E1, E2]),
            {error, preparing_disk_op_failed}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves info about links deleting.
%% @end
%%--------------------------------------------------------------------
-spec log_link_del(Key :: datastore:key(), ModelName :: model_behaviour:model_type(),
    LinkNames :: list() | all, Phase :: start | stop) ->
    {ok, datastore:key()} | datastore:generic_error().
log_link_del(Key, ModelName, LinkNames, start) ->
    try
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),

        UpdateFun = fun(#global_cache_controller{deleted_links = DL} = Record) ->
            case LinkNames of
                LNs when is_list(LNs) ->
                    Record#global_cache_controller{deleted_links = DL ++ LinkNames};
                _ ->
                    Record#global_cache_controller{deleted_links = DL ++ [LinkNames]}
            end
        end,
        case update(Uuid, UpdateFun) of
            {ok, Ok} ->
                {ok, Ok};
            {error,{not_found,global_cache_controller}} ->
                TS = os:timestamp(),
                V = case LinkNames of
                        LNs when is_list(LNs) ->
                            #global_cache_controller{timestamp = TS, deleted_links = LinkNames};
                        _ ->
                            #global_cache_controller{timestamp = TS, deleted_links = [LinkNames]}
                    end,
                Doc = #document{key = Uuid, value = V},
                create(Doc)
        end
    catch
        E1:E2 ->
            ?error_stacktrace("Error in global cache controller log_link_del. "
            ++ "Args: ~p. Error: ~p:~p.", [{Key, ModelName, LinkNames, start}, E1, E2]),
            {error, preparing_disk_op_failed}
    end;
log_link_del(Key, ModelName, LinkNames, stop) ->
    try
        Uuid = caches_controller:get_cache_uuid(Key, ModelName),
        UpdateFun = fun(#global_cache_controller{deleted_links = DL} = Record) ->
            case LinkNames of
                LNs when is_list(LNs) ->
                    Record#global_cache_controller{deleted_links = DL -- LinkNames};
                _ ->
                    Record#global_cache_controller{deleted_links = DL -- [LinkNames]}
            end
        end,
        update(Uuid, UpdateFun)
    catch
        E1:E2 ->
            ?error_stacktrace("Error in global cache controller log_link_del. "
            ++ "Args: ~p. Error: ~p:~p.", [{Key, ModelName, LinkNames, stop}, E1, E2]),
            {error, ending_disk_op_failed}
    end.
