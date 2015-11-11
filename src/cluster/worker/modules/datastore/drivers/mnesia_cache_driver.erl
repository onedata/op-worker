%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Mnesia database driver.
%%% @end
%%%-------------------------------------------------------------------
-module(mnesia_cache_driver).
-author("Rafal Slota").
-behaviour(store_driver_behaviour).

-include("global_definitions.hrl").
-include("cluster/worker/modules/datastore/datastore.hrl").
-include("cluster/worker/modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% store_driver_behaviour callbacks
-export([init_bucket/3, healthcheck/1]).
%% TODO Add non_transactional updates (each update creates tmp ets!)
-export([save/2, update/3, create/2, create_or_update/3, exists/2, get/2, list/3, delete/3]).
-export([add_links/3, delete_links/3, fetch_link/3, foreach_link/4]).
-export([run_synchronized/3]).

-record(links, {key, link_map = #{}}).

%% Batch size for list operation
-define(LIST_BATCH_SIZE, 100).

-define(MNESIA_WAIT_TIMEOUT, timer:seconds(20)).

%%%===================================================================
%%% store_driver_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_bucket/2.
%% @end
%%--------------------------------------------------------------------
-spec init_bucket(Bucket :: datastore:bucket(), Models :: [model_behaviour:model_config()], NodeToSync :: node()) -> ok.
init_bucket(_BucketName, Models, NodeToSync) ->
    lists:foreach( %% model
        fun(#model_config{name = ModelName, fields = Fields}) ->
            Node = node(),
            Table = table_name(ModelName),
            LinkTable = links_table_name(ModelName),
            TransactionTable = transaction_table_name(ModelName),
            case NodeToSync == Node of
                true -> %% No mnesia nodes -> create new table
                    MakeTable = fun(TabName, RecordName, RecordFields) ->
                        Ans = case mnesia:create_table(TabName, [{record_name, RecordName}, {attributes, RecordFields},
                            {ram_copies, [Node]}, {type, set}]) of
                            {atomic, ok} -> ok;
                            {aborted, {already_exists, TabName}} ->
                                ok;
                            {aborted, Reason} ->
                                ?error("Cannot init mnesia cluster (table ~p) on node ~p due to ~p", [TabName, node(), Reason]),
                                throw(Reason)
                        end,
                        ?info("Creating mnesia table: ~p, result: ~p", [TabName, Ans])
                    end,
                    {
                        MakeTable(Table, ModelName, [key | Fields]),
                        MakeTable(LinkTable, links, record_info(fields, links)),
                        MakeTable(TransactionTable, ModelName, [key | Fields])
                    };
                _ -> %% there is at least one mnesia node -> join cluster
                    Tables = [table_name(MName) || MName <- datastore_config:models()] ++
                             [links_table_name(MName) || MName <- datastore_config:models()] ++
                             [transaction_table_name(MName) || MName <- datastore_config:models()],
                    ok = rpc:call(NodeToSync, mnesia, wait_for_tables, [Tables, ?MNESIA_WAIT_TIMEOUT]),
                    ExpandTable = fun(TabName) ->
                        case rpc:call(NodeToSync, mnesia, change_config, [extra_db_nodes, [Node]]) of
                            {ok, [Node]} ->
                                case rpc:call(NodeToSync, mnesia, add_table_copy, [TabName, Node, ram_copies]) of
                                    {atomic, ok} ->
                                        ?info("Expanding mnesia cluster (table ~p) from ~p to ~p", [TabName, NodeToSync, node()]);
                                    {aborted, Reason} ->
                                        ?error("Cannot replicate mnesia table ~p to node ~p due to: ~p", [TabName, node(), Reason])
                                end,
                                ok;
                            {error, Reason} ->
                                ?error("Cannot expand mnesia cluster (table ~p) on node ~p due to ~p", [TabName, node(), Reason]),
                                throw(Reason)
                        end
                    end,
                    {
                        ExpandTable(Table),
                        ExpandTable(LinkTable),
                        ExpandTable(TransactionTable)
                    }
            end
        end, Models),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback save/2.
%% @end
%%--------------------------------------------------------------------
-spec save(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(#model_config{} = ModelConfig, #document{key = Key, value = Value} = _Document) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun() ->
        ok = mnesia:write(table_name(ModelConfig), inject_key(Key, Value), write),
        {ok, Key}
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(model_behaviour:model_config(), datastore:ext_key(),
    Diff :: datastore:document_diff()) -> {ok, datastore:ext_key()} | datastore:update_error().
update(#model_config{name = ModelName} = ModelConfig, Key, Diff) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun() ->
        case mnesia:read(table_name(ModelConfig), Key, write) of
            [] ->
                {error, {not_found, ModelName}};
            [Value] when is_map(Diff) ->
                NewValue = maps:merge(datastore_utils:shallow_to_map(strip_key(Value)), Diff),
                ok = mnesia:write(table_name(ModelConfig),
                    inject_key(Key, datastore_utils:shallow_to_record(NewValue)), write),
                {ok, Key};
            [Value] when is_function(Diff) ->
                NewValue = Diff(strip_key(Value)),
                ok = mnesia:write(table_name(ModelConfig), inject_key(Key, NewValue), write),
                {ok, Key}
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create/2.
%% @end
%%--------------------------------------------------------------------
-spec create(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(#model_config{} = ModelConfig, #document{key = Key, value = Value}) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun() ->
        case mnesia:read(table_name(ModelConfig), Key) of
            [] ->
                ok = mnesia:write(table_name(ModelConfig), inject_key(Key, Value), write),
                {ok, Key};
            [_Record] ->
                {error, already_exists}
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create_or_update/2.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(model_behaviour:model_config(), datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create_or_update(#model_config{} = ModelConfig, #document{key = Key, value = Value}, Diff) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun() ->
        case mnesia:read(table_name(ModelConfig), Key, write) of
            [] ->
                ok = mnesia:write(table_name(ModelConfig), inject_key(Key, Value), write),
                {ok, Key};
            [OldValue] when is_map(Diff) ->
                NewValue = maps:merge(datastore_utils:shallow_to_map(strip_key(OldValue)), Diff),
                ok = mnesia:write(table_name(ModelConfig),
                    inject_key(Key, datastore_utils:shallow_to_record(NewValue)), write),
                {ok, Key};
            [OldValue] when is_function(Diff) ->
                NewValue = Diff(strip_key(OldValue)),
                ok = mnesia:write(table_name(ModelConfig), inject_key(Key, NewValue), write),
                {ok, Key}
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get(#model_config{name = ModelName} = ModelConfig, Key) ->
    case mnesia:dirty_read(table_name(ModelConfig), Key) of
        [] -> {error, {not_found, ModelName}};
        [Value] -> {ok, #document{key = Key, value = strip_key(Value)}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback list/3.
%% @end
%%--------------------------------------------------------------------
-spec list(model_behaviour:model_config(),
    Fun :: datastore:list_fun(), AccIn :: term()) ->
    {ok, Handle :: term()} | datastore:generic_error() | no_return().
list(#model_config{} = ModelConfig, Fun, AccIn) ->
    SelectAll = [{'_', [], ['$_']}],
    mnesia_run(async_dirty, fun() ->
        case mnesia:select(table_name(ModelConfig), SelectAll, ?LIST_BATCH_SIZE, none) of
            {Obj, Handle} ->
                list_next(Obj, Handle, Fun, AccIn);
            '$end_of_table' ->
                list_next('$end_of_table', undefined, Fun, AccIn)
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback add_links/3.
%% @end
%%--------------------------------------------------------------------
-spec add_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()]) ->
    ok | datastore:generic_error().
add_links(#model_config{} = ModelConfig, Key, LinkSpec) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun() ->
        Links = #links{} =
            case mnesia:read(links_table_name(ModelConfig), Key, write) of
                [] ->
                    #links{key = Key};
                [Value] ->
                    Value
            end,
        Links1 = Links#links{link_map = maps:merge(Links#links.link_map, maps:from_list(LinkSpec))},
        ok = mnesia:write(links_table_name(ModelConfig), Links1, write)
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete_links/3.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:link_name()] | all) ->
    ok | datastore:generic_error().
delete_links(#model_config{} = ModelConfig, Key, all) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun() ->
        ok = mnesia:delete(links_table_name(ModelConfig), Key, write)
    end);
delete_links(#model_config{} = ModelConfig, Key, LinkNames) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun() ->
        Links = #links{} =
            case mnesia:read(links_table_name(ModelConfig), Key, write) of
                [] ->
                    #links{key = Key};
                [Value] ->
                    Value
            end,
        LinksMap =
            lists:foldl(fun(LinkName, Map) ->
                maps:remove(LinkName, Map)
            end, Links#links.link_map, LinkNames),
        Links1 = Links#links{link_map = LinksMap},
        ok = mnesia:write(links_table_name(ModelConfig), Links1, write)
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback fetch_link/3.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(model_behaviour:model_config(), datastore:ext_key(), datastore:link_name()) ->
    {ok, datastore:link_target()} | datastore:link_error().
fetch_link(#model_config{} = ModelConfig, Key, LinkName) ->
    case mnesia:dirty_read(links_table_name(ModelConfig), Key) of
        [] -> {error, link_not_found};
        [Value] ->
            Map = Value#links.link_map,
            case maps:get(LinkName, Map, undefined) of
                undefined ->
                    {error, link_not_found};
                {_TargetKey, _TargetModel} = Target ->
                    {ok, Target}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback foreach_link/4.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(model_behaviour:model_config(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:link_error().
foreach_link(#model_config{} = ModelConfig, Key, Fun, AccIn) ->
    case mnesia:dirty_read(links_table_name(ModelConfig), Key) of
        [] -> {ok, AccIn};
        [Value] ->
            Map = Value#links.link_map,
            try maps:fold(Fun, AccIn, Map) of
                AccOut -> {ok, AccOut}
            catch
                _:Reason ->
                    {error, Reason}
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Internat helper - accumulator for list/3.
%% @end
%%--------------------------------------------------------------------
-spec list_next([term()] | '$end_of_table', term(), datastore:list_fun(), term()) ->
    {ok, Acc :: term()} | datastore:generic_error().
list_next([Obj | R], Handle, Fun, AccIn) ->
    Doc =  #document{key = get_key(Obj), value = strip_key(Obj)},
    case Fun(Doc, AccIn) of
        {next, NewAcc} ->
            list_next(R, Handle, Fun, NewAcc);
        {abort, NewAcc} ->
            {ok, NewAcc}
    end;
list_next('$end_of_table' = EoT, Handle, Fun, AccIn) ->
    case Fun(EoT, AccIn) of
        {next, NewAcc} ->
            list_next(EoT, Handle, Fun, NewAcc);
        {abort, NewAcc} ->
            {ok, NewAcc}
    end;
list_next([], Handle, Fun, AccIn) ->
    case mnesia:select(Handle) of
        {Objects, NewHandle} ->
            list_next(Objects, NewHandle, Fun, AccIn);
        '$end_of_table' ->
            list_next('$end_of_table', undefined, Fun, AccIn)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete/2.
%% @end
%%--------------------------------------------------------------------
-spec delete(model_behaviour:model_config(), datastore:ext_key(), datastore:delete_predicate()) ->
    ok | datastore:generic_error().
delete(#model_config{} = ModelConfig, Key, Pred) ->
    mnesia_run(maybe_transaction(ModelConfig, sync_transaction), fun() ->
        case Pred() of
            true ->
                ok = mnesia:delete(table_name(ModelConfig), Key, write);
            false ->
                ok
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, boolean()} | datastore:generic_error().
exists(#model_config{} = ModelConfig, Key) ->
    case mnesia:dirty_read(table_name(ModelConfig), Key) of
        [] -> {ok, false};
        [_Record] -> {ok, true}
    end.


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
                case mnesia:table_info(table_name(ModelName), where_to_write) of
                    Nodes when is_list(Nodes) ->
                        case lists:member(node(), Nodes) of
                            true -> ok;
                            false ->
                                {error, {no_active_mnesia_table, table_name(ModelName)}}
                        end;
                    {error, Error} -> {error, Error};
                    Error -> {error, Error}
                end;
            (_, _, Acc) -> Acc
        end, ok, State).


%%--------------------------------------------------------------------
%% @doc
%% Runs given function within locked ResourceId. This function makes sure that 2 funs with same ResourceId won't
%% run at the same time.
%% @end
%%--------------------------------------------------------------------
-spec run_synchronized(model_behaviour:model_config(), ResourceId :: binary(), fun(() -> Result)) -> Result
    when Result :: term().
run_synchronized(#model_config{name = ModelName}, ResourceID, Fun) ->
    mnesia_run(sync_transaction,
        fun() ->
            Nodes = lists:usort(mnesia:table_info(table_name(ModelName), where_to_write)),
            case mnesia:lock({global, ResourceID, Nodes}, write) of
                ok ->
                    Fun();
                Nodes0 ->
                    case lists:usort(Nodes0) of
                        Nodes ->
                            Fun();
                        LessNodes ->
                            {error, {lock_error, Nodes -- LessNodes}}
                    end
            end
        end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
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
%% @private
%% @doc
%% Gets Mnesia links table name for given model.
%% @end
%%--------------------------------------------------------------------
-spec links_table_name(model_behaviour:model_config() | atom()) -> atom().
links_table_name(#model_config{name = ModelName}) ->
    links_table_name(ModelName);
links_table_name(TabName) when is_atom(TabName) ->
    binary_to_atom(<<"dc_links_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets Mnesia transaction table name for given model.
%% @end
%%--------------------------------------------------------------------
-spec transaction_table_name(atom()) -> atom().
transaction_table_name(TabName) when is_atom(TabName) ->
    binary_to_atom(<<"dc_transaction_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Inserts given key as second element of given tuple.
%% @end
%%--------------------------------------------------------------------
-spec inject_key(Key :: datastore:ext_key(), Tuple :: tuple()) -> NewTuple :: tuple().
inject_key(Key, Tuple) when is_tuple(Tuple) ->
    [RecordName | Fields] = tuple_to_list(Tuple),
    list_to_tuple([RecordName | [Key | Fields]]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Strips second element of given tuple (reverses inject_key/2).
%% @end
%%--------------------------------------------------------------------
-spec strip_key(Tuple :: tuple()) -> NewTuple :: tuple().
strip_key(Tuple) when is_tuple(Tuple) ->
    [RecordName, _Key | Fields] = tuple_to_list(Tuple),
    list_to_tuple([RecordName | Fields]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns key of a tuple.
%% @end
%%--------------------------------------------------------------------
-spec get_key(Tuple :: tuple()) -> Key :: term().
get_key(Tuple) when is_tuple(Tuple) ->
    [_RecordName, Key | _Fields] = tuple_to_list(Tuple),
    Key.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convinience function for executing given Mnesia's transaction-like function and normalizing Result.
%% Available methods: sync_dirty, async_dirty, sync_transaction, transaction.
%% @end
%%--------------------------------------------------------------------
-spec mnesia_run(Method :: atom(), Fun :: fun(() -> term())) -> term().
mnesia_run(Method, Fun) when Method =:= sync_dirty; Method =:= async_dirty ->
    try mnesia:Method(Fun) of
        Result ->
            Result
    catch
        _:Reason ->
            {error, Reason}
    end;
mnesia_run(Method, Fun) when Method =:= sync_transaction; Method =:= transaction ->
    case mnesia:Method(Fun) of
        {atomic, Result} ->
            Result;
        {aborted, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% If transactions are enabled in #model_config{} returns given TransactionType.
%% If transactions are disabled in #model_config{} returns corresponding dirty mode.
%% @end
%%--------------------------------------------------------------------
-spec maybe_transaction(model_behaviour:model_config(), atom()) -> atom().
maybe_transaction(#model_config{transactional_global_cache = false}, TransactionType) ->
    case TransactionType of
        sync_transaction -> sync_dirty
    end;
maybe_transaction(#model_config{transactional_global_cache = true}, TransactionType) ->
    TransactionType.