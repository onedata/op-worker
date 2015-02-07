%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Riak database driver.
%%% @end
%%%-------------------------------------------------------------------
-module(mnesia_cache_driver).
-author("Rafal Slota").
-behaviour(store_driver_behaviour).

-include("workers/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% store_driver_behaviour callbacks
-export([init_bucket/2, healthcheck/1]).
-export([save/2, update/3, create/2, exists/2, get/2, delete/2]).

%%%===================================================================
%%% store_driver_behaviour callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_bucket/2.
%% @end
%%--------------------------------------------------------------------
-spec init_bucket(Bucket :: datastore:bucket(), Models :: [model_behaviour:model_config()]) -> ok.
init_bucket(_BucketName, Models) ->
    lists:foreach( %% model
        fun(#model_config{name = ModelName, fields = Fields}) ->
            Table = table_name(ModelName),
            case get_active_nodes(Table) of
                [] -> %% No mnesia nodes -> create new table
                    case mnesia:create_table(Table, [{record_name, ModelName}, {attributes, [key | Fields]},
                        {ram_copies, [node()]}, {type, set}]) of
                        {atomic, ok} -> ok;
                        {aborted, {already_exists, _}} ->
                            ok;
                        {aborted, Reason} ->
                            ?error("Cannot init mnesia cluster (table ~p) on node ~p due to ~p", [Table, node(), Reason]),
                            throw(Reason)
                    end;
                [MnesiaNode | _] -> %% there is at least one mnesia node -> join cluster


                    case rpc:call(MnesiaNode, mnesia, change_config, [extra_db_nodes, [node()]]) of
                        {atomic, ok} ->
                            case rpc:call(MnesiaNode, mnesia, add_table_copy, [Table, node(), ram_copies]) of
                                {atomic, ok} ->
                                    ?info("Expanding mnesia cluster (table ~p) from ~p to ~p", [Table, MnesiaNode, node()]);
                                {aborted, Reason} ->
                                    ?error("Cannot replicate mnesia table ~p to node ~p due to: ~p", [Table, node(), Reason])
                            end,
                            ok;
                        {aborted, Reason} ->
                            ?error("Cannot expand mnesia cluster (table ~p) on node ~p due to ~p", [Table, node(), Reason]),
                            throw(Reason)
                    end
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
update(#model_config{} = ModelConfig, Key, Diff) ->
    transaction(fun() ->
        case mnesia:read(table_name(ModelConfig), Key, write) of
            [] ->
                {error, {not_found, missing_or_deleted}};
            [Value] when is_map(Diff) ->
                NewValue = maps:merge(datastore_utils:shallow_to_map(strip_key(Value)), Diff),
                ok = mnesia:write(table_name(ModelConfig), inject_key(Key, datastore_utils:shallow_to_record(NewValue)), write),
                {ok, Key};
            [Value] when is_function(Diff) ->
                NewValue = Diff(strip_key(Value)),
                ok = mnesia:write(table_name(ModelConfig), inject_key(Key, NewValue), write),
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
                {ok, #document{key = Key, value = strip_key(Value)}};
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


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback healthcheck/1.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck(WorkerState :: term()) -> ok | {error, Reason :: any()}.
healthcheck(_) ->
    ok.


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
%% Inserts given key as second element of given tuple.
%% @end
%%--------------------------------------------------------------------
-spec inject_key(Key :: datastore:key(), Tuple :: tuple()) -> NewTuple :: tuple().
inject_key(Key, Tuple) when is_tuple(Tuple) ->
    [RecordName | Fields] = tuple_to_list(Tuple),
    list_to_tuple([RecordName, Key] ++ Fields).


%%--------------------------------------------------------------------
%% @doc
%% Strips second element of given tuple (reverses inject_key/2).
%% @end
%%--------------------------------------------------------------------
-spec strip_key(Tuple :: tuple()) -> NewTuple :: tuple().
strip_key(Tuple) when is_tuple(Tuple) ->
    [RecordName, _Key | Fields] = tuple_to_list(Tuple),
    list_to_tuple([RecordName | Fields]).


%%--------------------------------------------------------------------
%% @doc
%% Convinience function for executing transaction within Mnesia
%% @end
%%--------------------------------------------------------------------
-spec transaction(Fun :: fun(() -> term())) -> atom().
transaction(Fun) ->
    case mnesia:transaction(Fun) of
        {atomic, Res} -> Res;
        {aborted, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Gets all active Mnesia nodes which have given Table.
%% @end
%%--------------------------------------------------------------------
-spec get_active_nodes(Table :: atom()) -> [Node :: atom()].
get_active_nodes(Table) ->
    {Replies0, _} = rpc:multicall(nodes(), mnesia, table_info, [Table, where_to_commit]),
    Replies1 = lists:flatten(Replies0),
    Replies2 = [Node || {Node, ram_copies} <- Replies1],
    lists:usort(Replies2).