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
-module(distributed_cache_driver).
-author("Rafal Slota").

-include("workers/datastore/datastore.hrl").

%% API
-export([init_bucket/1]).
-export([save/2, update/3, create/2, exists/2, get/2, delete/2]).

%%%===================================================================
%%% API
%%%===================================================================

init_bucket({_BucketName, Models}) ->
    lists:foreach(
        fun(#model_config{name = ModelName, fields = Fields}) ->
            {atomic, ok} = mnesia:create_table(table_name(ModelName), [{record_name, ModelName}, {attributes, [key | Fields]},
                {ram_copies, [node()] ++ nodes()}, {type, set}])
        end, Models),
    ok.


save(#model_config{} = ModelConfig, #document{key = Key, value = Value} = Document) ->
    transaction(fun() ->
        case mnesia:write(table_name(ModelConfig), inject_key(Key, Value), write) of
            ok -> {ok, Key};
            Reason ->
                {error, Reason}
        end
    end).


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


exists(#model_config{} = ModelConfig, Key) ->
    transaction(fun() ->
        case mnesia:read(table_name(ModelConfig), Key) of
            []          -> false;
            [_Record]   -> true
        end
    end).


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


delete(#model_config{} = ModelConfig, Key) ->
    transaction(fun() ->
        case mnesia:delete(table_name(ModelConfig), Key, write) of
            ok ->
                ok;
            Reason ->
                {error, Reason}
        end
    end).


list(#model_config{} = ModelConfig) ->
    erlang:error(not_implemented).


%%%===================================================================
%%% Internal functions
%%%===================================================================


table_name(#model_config{name = ModelName}) ->
    table_name(ModelName);
table_name(TabName) when is_atom(TabName) ->
    binary_to_atom(<<"dc_", (erlang:atom_to_binary(TabName, utf8))/binary>>, utf8).


inject_key(Key, Tuple) when is_tuple(Tuple) ->
    [RecordName | Fields] = tuple_to_list(Tuple),
    list_to_tuple([RecordName, Key] ++ Fields).


strip_key(Tuple) when is_tuple(Tuple) ->
    [RecordName, _Key | Fields] = tuple_to_list(Tuple),
    list_to_tuple([RecordName | Fields]).


transaction(Fun) ->
    case mnesia:transaction(Fun) of
        {atomic, Res} -> Res;
        {aborted, Reason} ->
            {error, Reason}
    end.