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
-module(mnesia_cache_driver).
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
init_bucket(_BucketName, Models) ->
    lists:foreach(
        fun(#model_config{name = ModelName, fields = Fields}) ->
            case mnesia:create_table(table_name(ModelName), [{record_name, ModelName}, {attributes, [key | Fields]},
                {ram_copies, [node()] ++ nodes()}, {type, set}]) of
                {atomic, ok} -> ok;
                {aborted, {already_exists, _}} ->
                    mnesia:add_table_copy(table_name(ModelName), node(), ram_copies);
                {aborted, Reason} -> throw(Reason)
            end
        end, Models),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback save/2.
%% @end
%%--------------------------------------------------------------------
-spec save(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().
save(#model_config{} = ModelConfig, #document{key = Key, value = Value} = _Document) ->
    transaction(fun() ->
        case mnesia:write(table_name(ModelConfig), inject_key(Key, Value), write) of
            ok -> {ok, Key};
            Reason ->
                {error, Reason}
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(model_behaviour:model_config(), datastore:key(),
    Diff :: datastore:document_diff()) -> {ok, datastore:key()} | datastore:update_error().
update(#model_config{} = ModelConfig, Key, Diff) when is_map(Diff) ->
    transaction(fun() ->
        case mnesia:read(table_name(ModelConfig), Key, write) of
            [] ->
                {error, {not_found, missing_or_deleted}};
            [Value] ->
                NewValue = maps:merge(datastore_utils:shallow_to_map(strip_key(Value)), Diff),
                ok = mnesia:write(table_name(ModelConfig), inject_key(Key, datastore_utils:shallow_to_record(NewValue)), write),
                {ok, Key};
            Reason ->
                {error, Reason}
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create/2.
%% @end
%%--------------------------------------------------------------------
-spec create(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:key()} | datastore:create_error().
create(#model_config{} = ModelConfig, #document{key = Key, value = Value}) ->
    transaction(fun() ->
        case mnesia:read(table_name(ModelConfig), Key) of
            [] ->
                ok = mnesia:write(table_name(ModelConfig), inject_key(Key, Value), write),
                {ok, Key};
            [_Record] ->
                {error, already_exists};
            Reason ->
                {error, Reason}
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:document()} | datastore:get_error().
get(#model_config{} = ModelConfig, Key) ->
    transaction(fun() ->
        case mnesia:read(table_name(ModelConfig), Key) of
            [] ->
                {error, {not_found, missing_or_deleted}};
            [Value] ->
                {ok, strip_key(Value)};
            Reason ->
                {error, Reason}
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete/2.
%% @end
%%--------------------------------------------------------------------
-spec delete(model_behaviour:model_config(), datastore:key()) -> ok | datastore:generic_error().
delete(#model_config{} = ModelConfig, Key) ->
    transaction(fun() ->
        case mnesia:delete(table_name(ModelConfig), Key, write) of
            ok ->
                ok;
            Reason ->
                {error, Reason}
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(model_behaviour:model_config(), datastore:key()) -> true | false | datastore:generic_error().
exists(#model_config{} = ModelConfig, Key) ->
    transaction(fun() ->
        case mnesia:read(table_name(ModelConfig), Key) of
            []          -> false;
            [_Record]   -> true
        end
    end).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Gets Mnesia table name for given model.
%% @end
%%--------------------------------------------------------------------
-spec table_name(model_behaviour:model_config() | atom()) -> atom().
table_name(#model_config{name = ModelName}) ->
    table_name(ModelName);
table_name(TabName) when is_atom(TabName) ->
    binary_to_atom(<<"dc_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).


%%--------------------------------------------------------------------
%% @doc
%% Gets Mnesia table name for given model.
%% @end
%%--------------------------------------------------------------------
-spec table_name(model_behaviour:model_config() | atom()) -> atom().
inject_key(Key, Tuple) when is_tuple(Tuple) ->
    [RecordName | Fields] = tuple_to_list(Tuple),
    list_to_tuple([RecordName, Key] ++ Fields).


%%--------------------------------------------------------------------
%% @doc
%% Gets Mnesia table name for given model.
%% @end
%%--------------------------------------------------------------------
-spec table_name(model_behaviour:model_config() | atom()) -> atom().
strip_key(Tuple) when is_tuple(Tuple) ->
    [RecordName, _Key | Fields] = tuple_to_list(Tuple),
    list_to_tuple([RecordName | Fields]).


%%--------------------------------------------------------------------
%% @doc
%% Convinience function for executing transaction within Mnesia
%% @end
%%--------------------------------------------------------------------
-spec transaction(Fun :: fun() -> term()) -> atom().
transaction(Fun) ->
    case mnesia:transaction(Fun) of
        {atomic, Res} -> Res;
        {aborted, Reason} ->
            {error, Reason}
    end.