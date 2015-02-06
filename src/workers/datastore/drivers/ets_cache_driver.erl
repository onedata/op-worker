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

%% API
-export([init_bucket/2]).
-export([save/2, update/3, create/2, exists/2, get/2, delete/2]).

%%%===================================================================
%%% API
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
-spec save(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().
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
-spec create(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:key()} | datastore:create_error().
create(#model_config{} = ModelConfig, #document{key = Key, value = Value}) ->
    case ets:insert_new(table_name(ModelConfig), {Key, Value}) of
        false -> {error, already_exists};
        true  -> {ok, Key}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:document()} | datastore:get_error().
get(#model_config{} = ModelConfig, Key) ->
    case ets:lookup(table_name(ModelConfig), Key) of
        [{_, Value}] ->
            {ok, #document{key = Key, value = Value}};
        [] ->
            {error, {not_found, missing_or_deleted}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete/2.
%% @end
%%--------------------------------------------------------------------
-spec delete(model_behaviour:model_config(), datastore:key()) -> ok | datastore:generic_error().
delete(#model_config{} = ModelConfig, Key) ->
    true = ets:delete(table_name(ModelConfig), Key),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(model_behaviour:model_config(), datastore:key()) -> true | false | datastore:generic_error().
exists(#model_config{} = ModelConfig, Key) ->
    ets:member(table_name(ModelConfig), Key).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Gets ETS table name for given model.
%% @end
%%--------------------------------------------------------------------
-spec table_name(model_behaviour:model_config() | atom()) -> atom().
table_name(#model_config{name = ModelName}) ->
    table_name(ModelName);
table_name(TabName) when is_atom(TabName) ->
    binary_to_atom(<<"lc_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).