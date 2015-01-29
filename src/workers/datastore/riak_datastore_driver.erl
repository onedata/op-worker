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
-module(riak_datastore_driver).
-author("Rafal Slota").

-include("workers/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_bucket/1, connect/1, call/3]).
-export([save/2, create/2, update/3, exists/2, get/2, delete/2]).

%%%===================================================================
%%% API
%%%===================================================================

init_bucket(Bucket) ->
    ?info("Riak init with nodes: ~p", [datastore_worker:state_get(riak_nodes)]),

    ok.

save(#model_config{bucket = Bucket} = _ModelConfig, #document{key = Key, value = Value}) ->
    RiakOP = riakc_map:to_op(to_riak_obj(Value)),
    Key1 = maybe_generate_key(Key),
    case call(riakc_pb_socket, update_type, [{<<"maps">>, to_binary(Bucket)}, to_binary(Key1), RiakOP]) of
        ok -> {ok, Key1};
        {error, Reason} ->
            {error, Reason}
    end.


update(#model_config{} = ModelConfig, Key, Diff) when is_map(Diff) ->
    case ets:lookup(table_name(ModelConfig), Key) of
        [] ->
            {error, {not_found, missing_or_deleted}};
        [{_, Value}] ->
            NewValue = maps:merge(datastore_utils:shallow_to_map(Value), Diff),
            true = ets:insert(table_name(ModelConfig), {Key, datastore_utils:shallow_to_record(NewValue)}),
            {ok, Key}
    end.


create(#model_config{bucket = Bucket} = _ModelConfig, #document{key = Key, value = Value}) ->
    RiakOP = riakc_map:to_op(to_riak_obj(Value)),
    Key1 = maybe_generate_key(Key),
    case call(riakc_pb_socket, update_type, [{<<"maps">>, to_binary(Bucket)}, to_binary(Key1), RiakOP]) of
        ok -> {ok, Key1};
        {error, Reason} ->
            {error, Reason}
    end.


exists(#model_config{bucket = Bucket} = _ModelConfig, Key) ->
    case call(riakc_pb_socket, fetch_type, [{<<"maps">>, to_binary(Bucket)}, to_binary(Key)]) of
        {ok, {notfound, _}} ->
            false;
        {error, _Reason} ->
            %% @todo: log
            false;
        {ok, _} ->
            true
    end.


get(#model_config{bucket = Bucket} = _ModelConfig, Key) ->
    case call(riakc_pb_socket, fetch_type, [{<<"maps">>, to_binary(Bucket)}, to_binary(Key)]) of
        {ok, Result} ->
            {ok, #document{key = Key, value = datastore_utils:shallow_to_record(form_riak_obj(map, Result))}};
        {error, Reason} ->
            {error, Reason}
    end.


delete(#model_config{bucket = Bucket} = _ModelConfig, Key) ->
    case call(riakc_pb_socket, delete, [{<<"maps">>, to_binary(Bucket)}, to_binary(Key)]) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.


list(#model_config{} = _ModelConfig) ->
    erlang:error(not_implemented).


%%%===================================================================
%%% Internal functions
%%%===================================================================

form_riak_obj(map, Obj) ->
    riakc_map:fold(
        fun({K, Type}, V, Acc) ->
            maps:put(from_binary(K), form_riak_obj(Type, V), Acc)
        end, #{}, Obj);
form_riak_obj(counter, Obj) when is_integer(Obj) ->
    Obj;
form_riak_obj(counter, Obj) ->
    riakc_counter:value(Obj);
form_riak_obj(register, Obj) when is_binary(Obj) ->
    from_binary(Obj);
form_riak_obj(register, Obj) ->
    from_binary(riakc_register:value(Obj)).

to_riak_obj(Term) when is_tuple(Term) ->
    Map = datastore_utils:shallow_to_map(Term),
    RMap0 = riakc_map:new(),
    RMap1 = maps:fold(
                fun(K, V, Acc) ->
                    RiakObj = to_riak_obj(V),
                    Module = riakc_datatype:module_for_term(RiakObj),
                    Type = Module:type(),
                    riakc_map:update({to_binary(K), Type}, fun(_) -> RiakObj end, Acc)
                end, RMap0, Map);
to_riak_obj(Int) when is_integer(Int) ->
    Counter = riakc_counter:new(),
    riakc_counter:increment(Int, Counter);
to_riak_obj(Bin) when is_binary(Bin) ->
    Register = riakc_register:new(),
    riakc_register:set(Bin, Register);
to_riak_obj(Atom) when is_atom(Atom) ->
    Register = riakc_register:new(),
    Bin = atom_to_binary(Atom, utf8),
    riakc_register:set(<<"ATOM::", Bin/binary>>, Register).


table_name(#model_config{name = ModelName}) ->
    table_name(ModelName);
table_name(TabName) when is_atom(TabName) ->
    erlang:atom_to_binary(TabName, utf8).


call(Module, Method, Args) ->
    call(Module, Method, Args, 3).

call(Module, Method, Args, Retry) when Retry >= 0 ->
    {Node, Pid} = select_connection(),
    try apply(Module, Method, [Pid | Args]) of
        Result -> Result
    catch
        _:Reason ->
            ?error_stacktrace("Failed to call Riak node ~p due to ~p", [Node, Reason]),
            NewConn = [Conn || {N, _} = Conn <- datastore_worker:state_get(riak_connections), N =/= Node],
            datastore_worker:state_put(riak_connections, NewConn),
            call(Module, Method, Args, Retry - 1)
    end;
call(_Module, _Method, _Args, _Retry) ->
    {error, no_riak_nodes}.


select_connection() ->
    Connections = get_connections(),
    lists:nth(crypto:rand_uniform(1, length(Connections) + 1), Connections).


get_connections() ->
    case datastore_worker:state_get(riak_connections) of
        [_ | _] = Connections ->
            Connections;
        _ ->
            Connections = connect(datastore_worker:state_get(riak_nodes)),
            datastore_worker:state_put(riak_connections, Connections),
            Connections
    end.


connect([{HostName, Port} = Node | R]) ->
    case riakc_pb_socket:start_link(binary_to_list(HostName), Port) of
        {ok, Pid} ->
            [{Node, Pid} | connect(R)];
        {error, Reason} ->
            ?error("Cannot connect to riak node ~p due to ~p", [Node, Reason]),
            connect(R)
    end;
connect([]) ->
    [].

to_binary(Term) when is_binary(Term) ->
    Term;
to_binary(Term) when is_atom(Term) ->
    <<"ATOM::", (atom_to_binary(Term, utf8))/binary>>;
to_binary(Term) when is_list(Term) ->
    list_to_binary(Term);
to_binary(Term) ->
    term_to_base64(Term).

to_key(Term) ->
    {to_binary(Term), register}.


term_to_base64(Term) ->
    Base = base64:encode(term_to_binary(Term)),
    <<"OBJ::", Base/binary>>.

base64_to_term(<<"OBJ::", Base/binary>>) ->
    binary_to_term(base64:decode(Base)).

from_binary(<<"OBJ::", _>> = Bin) ->
    base64_to_term(Bin);
from_binary(<<"ATOM::", Atom/binary>>) ->
    binary_to_atom(Atom, utf8);
from_binary(Bin) ->
    Bin.


maybe_generate_key(undefined) ->
    base64:encode(crypto:rand_bytes(32));
maybe_generate_key(Key) ->
    Key.