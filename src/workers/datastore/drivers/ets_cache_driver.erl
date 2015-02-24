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

-include("workers/datastore/datastore.hrl").

%% store_driver_behaviour callbacks
-export([init_bucket/2, healthcheck/1]).
-export([save/2, update/3, create/2, exists/2, get/2, list_init/2, list_next/2, delete/3]).

%%%===================================================================
%%% store_driver_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_bucket/2.
%% @end
%%--------------------------------------------------------------------
-spec init_bucket(Bucket :: datastore:bucket(), Models :: [model_behaviour:model_config()]) -> ok.
init_bucket(_Bucket, Models) ->
    lists:foreach(
        fun(#model_config{} = ModelConfig) ->
            catch ets:new(table_name(ModelConfig), [named_table, public, set])
        end, Models),
    ok.

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
update(#model_config{} = ModelConfig, Key, Diff) when is_map(Diff) ->
    case ets:lookup(table_name(ModelConfig), Key) of
        [] ->
            {error, {not_found, missing_or_deleted}};
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
get(#model_config{} = ModelConfig, Key) ->
    case ets:lookup(table_name(ModelConfig), Key) of
        [{_, Value}] ->
            {ok, #document{key = Key, value = Value}};
        [] ->
            {error, {not_found, missing_or_deleted}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Initializes list operation. In order to get records, use list_next/2 afterwards.
%% @end
%%--------------------------------------------------------------------
-spec list_init(model_behaviour:model_config(), BatchSize :: non_neg_integer()) ->
    {ok, Handle :: term()} | datastore:generic_error().
list_init(#model_config{} = _ModelConfig, _BatchSize) ->
    error(not_supported).


%%--------------------------------------------------------------------
%% @doc
%% Returns list of next records for given table cursor.
%% @end
%%--------------------------------------------------------------------
-spec list_next(model_behaviour:model_config(), Handle :: term()) ->
    {ok, {[datastore:document()], Handle :: term()}} | datastore:generic_error().
list_next(#model_config{} = _ModelConfig, _Handle) ->
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
    true | false | datastore:generic_error().
exists(#model_config{} = ModelConfig, Key) ->
    ets:member(table_name(ModelConfig), Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback healthcheck/1.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck(WorkerState :: term()) -> ok | {error, Reason :: term()}.
healthcheck(_) ->
    ok.

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