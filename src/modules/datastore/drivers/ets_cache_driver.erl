%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc ETS based cache implementation.
%%% @end
%%%-------------------------------------------------------------------
-module(ets_cache_driver).
-author("Rafal Slota").
-behaviour(store_driver_behaviour).

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_internal_def.hrl").
-include_lib("ctool/include/logging.hrl").

%% store_driver_behaviour callbacks
-export([init_bucket/3, healthcheck/1]).
-export([save/2, update/3, create/2, exists/2, get/2, list/3, delete/3]).
-export([add_links/3, delete_links/3, fetch_link/3, foreach_link/4]).

%%%===================================================================
%%% store_driver_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_bucket/2.
%% @end
%%--------------------------------------------------------------------
-spec init_bucket(Bucket :: datastore:bucket(), Models :: [model_behaviour:model_config()],
    NodeToSync :: node()) -> ok.
init_bucket(_Bucket, Models, _NodeToSync) ->
    lists:foreach(
        fun(#model_config{} = ModelConfig) ->
            Ans = (catch ets:new(table_name(ModelConfig), [named_table, public, set])),
            ?info("Creating ets table: ~p, result: ~p", [table_name(ModelConfig), Ans])
        end, Models).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback save/2.
%% @end
%%--------------------------------------------------------------------
-spec save(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:key()} | datastore:generic_error().
save(#model_config{} = ModelConfig, #document{key = Key, value = Value}) ->
    true = ets:insert(table_name(ModelConfig), {Key, Value}),
    {ok, Key}.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback update/3.
%% @end
%%--------------------------------------------------------------------
-spec update(model_behaviour:model_config(), datastore:key(),
    Diff :: datastore:document_diff()) -> {ok, datastore:key()} | datastore:update_error().
update(#model_config{bucket = _Bucket} = _ModelConfig, _Key, Diff) when is_function(Diff) ->
    erlang:error(not_implemented);
update(#model_config{name = ModelName} = ModelConfig, Key, Diff) when is_map(Diff) ->
    case ets:lookup(table_name(ModelConfig), Key) of
        [] ->
            {error, {not_found, ModelName}};
        [{_, Value}] ->
            NewValue = maps:merge(datastore_utils:shallow_to_map(Value), Diff),
            true = ets:insert(table_name(ModelConfig), {Key, datastore_utils:shallow_to_record(NewValue)}),
            {ok, Key}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create/2.
%% @end
%%--------------------------------------------------------------------
-spec create(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(#model_config{} = ModelConfig, #document{key = Key, value = Value}) ->
    case ets:insert_new(table_name(ModelConfig), {Key, Value}) of
        false -> {error, already_exists};
        true -> {ok, Key}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:document()} | datastore:get_error().
get(#model_config{name = ModelName} = ModelConfig, Key) ->
    case ets:lookup(table_name(ModelConfig), Key) of
        [{_, Value}] ->
            {ok, #document{key = Key, value = Value}};
        [] ->
            {error, {not_found, ModelName}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback list/3.
%% @end
%%--------------------------------------------------------------------
-spec list(model_behaviour:model_config(),
    Fun :: datastore:list_fun(), AccIn :: term()) -> no_return().
list(#model_config{} = _ModelConfig, _Fun, _AccIn) ->
    error(not_supported).



%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete/2.
%% @end
%%--------------------------------------------------------------------
-spec delete(model_behaviour:model_config(), datastore:key(), datastore:delete_predicate()) ->
    ok | datastore:generic_error().
delete(#model_config{} = ModelConfig, Key, Pred) ->
    case Pred() of
        true ->
            true = ets:delete(table_name(ModelConfig), Key),
            ok;
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(model_behaviour:model_config(), datastore:key()) ->
    {ok, boolean()} | datastore:generic_error().
exists(#model_config{} = ModelConfig, Key) ->
    {ok, ets:member(table_name(ModelConfig), Key)}.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback healthcheck/1.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck(WorkerState :: term()) -> ok | {error, Reason :: term()}.
healthcheck(State) ->
    maps:fold(
        fun
            (_, #model_config{name = ModelName}, ok) ->
                case ets:info(table_name(ModelName)) of
                    undefined ->
                        {error, {no_ets, table_name(ModelName)}};
                    _ -> ok
                end;
            (_, _, Acc) -> Acc
        end, ok, State).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback add_links/3.
%% @end
%%--------------------------------------------------------------------
-spec add_links(model_behaviour:model_config(), datastore:key(), [datastore:normalized_link_spec()]) ->
    no_return().
add_links(_, _, _) ->
    erlang:error(not_implemented).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete_links/3.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(model_behaviour:model_config(), datastore:key(), [datastore:normalized_link_spec()] | all) ->
    no_return().
delete_links(_, _, _) ->
    erlang:error(not_implemented).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback fetch_link/3.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(model_behaviour:model_config(), datastore:key(), datastore:link_name()) ->
    no_return().
fetch_link(_, _, _) ->
    erlang:error(not_implemented).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback foreach_link/4.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(model_behaviour:model_config(), Key :: datastore:key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    no_return().
foreach_link(_, _Key, _, _AccIn) ->
    erlang:error(not_implemented).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets ETS table name for given model.
%% @end
%%--------------------------------------------------------------------
-spec table_name(model_behaviour:model_config() | atom()) -> atom().
table_name(#model_config{name = ModelName}) ->
    table_name(ModelName);
table_name(TabName) when is_atom(TabName) ->
    binary_to_atom(<<"lc_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).