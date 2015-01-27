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
-module(local_cache_driver).
-author("Rafal Slota").

-include("workers/datastore/datastore.hrl").

%% API
-export([init_bucket/1]).
-export([save/2, update/3, create/2, exists/2, get/2, delete/2]).

%%%===================================================================
%%% API
%%%===================================================================

init_bucket({_Bucket, Models}) ->
    lists:foreach(
        fun(#model_config{} = ModelConfig) ->
            ets:new(table_name(ModelConfig), [named_table, public, set])
        end, Models),
    ok.


save(#model_config{} = ModelConfig, #document{key = Key, value = Value}) ->
    true = ets:insert(table_name(ModelConfig), {Key, Value}),
    {ok, Key}.


update(#model_config{} = ModelConfig, Key, Diff) when is_map(Diff) ->
    case ets:lookup(table_name(ModelConfig), Key) of
        [] ->
            {error, {not_found, missing_or_deleted}};
        [{_, Value}] ->
            NewValue = maps:merge(datastore_utils:shallow_to_map(Value), Diff),
            true = ets:insert(table_name(ModelConfig), {Key, datastore_utils:shallow_to_record(NewValue)}),
            {ok, Key}
    end.


create(#model_config{} = ModelConfig, #document{key = Key, value = Value}) ->
    case ets:insert_new(table_name(ModelConfig), {Key, Value}) of
        false -> {error, already_exists};
        true  -> {ok, Key}
    end.


exists(#model_config{} = ModelConfig, Key) ->
    ets:member(table_name(ModelConfig), Key).


get(#model_config{} = ModelConfig, Key) ->
    case ets:lookup(table_name(ModelConfig), Key) of
        [{_, Value}] ->
            {ok, #document{key = Key, value = Value}};
        [] ->
            {error, {not_found, missing_or_deleted}}
    end.


delete(#model_config{} = ModelConfig, Key) ->
    true = ets:delete(table_name(ModelConfig), Key),
    ok.


list(#model_config{} = _ModelConfig) ->
    erlang:error(not_implemented).

%%%===================================================================
%%% Internal functions
%%%===================================================================

table_name(#model_config{name = ModelName}) ->
    table_name(ModelName);
table_name(TabName) when is_atom(TabName) ->
    binary_to_atom(<<"lc_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).