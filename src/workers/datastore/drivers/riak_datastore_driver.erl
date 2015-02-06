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
-behaviour(store_driver_behaviour).

-include("workers/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% Bukcet type that is defined in database and configured to store "map" data type
-define(RIAK_BUCKET_TYPE, <<"maps">>).

%% Encoded object prefix
-define(OBJ_PREFIX, "OBJ::").

%% Encoded atom prefix
-define(ATOM_PREFIX, "ATOM::").

-type riak_node() :: {HostName :: binary(), Port :: non_neg_integer()}.
-type riak_connection() :: {riak_node(), ConnectionHandle :: term()}.


%% API
-export([init_bucket/2]).
-export([save/2, create/2, update/3, exists/2, get/2, delete/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_bucket/2.
%% @end
%%--------------------------------------------------------------------
-spec init_bucket(Bucket :: datastore:bucket(), Models :: [model_behaviour:model_config()]) -> ok.
init_bucket(_Bucket, _Models) ->
    ?debug("Riak init with nodes: ~p", [datastore_worker:state_get(riak_nodes)]),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_bucket/2.
%% @end
%%--------------------------------------------------------------------
-spec save(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().
save(#model_config{bucket = Bucket} = _ModelConfig, #document{key = Key, rev = Rev, value = Value}) ->
    RiakObj = to_riak_obj(Value, Rev),
    RiakOP = riakc_map:to_op(RiakObj),
    case call(riakc_pb_socket, update_type, [{?RIAK_BUCKET_TYPE, bucket_encode(Bucket)}, to_binary(Key), RiakOP]) of
        ok -> {ok, Key};
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback update/3.
%% @end
%%--------------------------------------------------------------------
-spec update(model_behaviour:model_config(), datastore:key(),
    Diff :: datastore:document_diff()) -> {ok, datastore:key()} | datastore:update_error().
update(#model_config{bucket = _Bucket} = _ModelConfig, _Key, Diff) when is_function(Diff) ->
    erlang:error(not_implemented);
update(#model_config{bucket = Bucket} = _ModelConfig, Key, Diff) when is_map(Diff) ->
    case call(riakc_pb_socket, fetch_type, [{?RIAK_BUCKET_TYPE, bucket_encode(Bucket)}, to_binary(Key)]) of
        {ok, Result} ->
            NewRMap =
                maps:fold(
                    fun(K, V, Acc) ->
                        RiakObj = to_riak_obj(V),
                        Module = riakc_datatype:module_for_term(RiakObj),
                        Type = Module:type(),
                        riakc_map:update({to_binary(K), Type}, fun(_) -> RiakObj end, Acc)
                    end, Result, Diff),
            case call(riakc_pb_socket, update_type, [{?RIAK_BUCKET_TYPE, bucket_encode(Bucket)}, to_binary(Key), riakc_map:to_op(NewRMap)]) of
                ok -> {ok, Key};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create/2.
%% @end
%%--------------------------------------------------------------------
-spec create(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:key()} | datastore:create_error().
create(#model_config{bucket = Bucket} = _ModelConfig, #document{key = Key, value = Value}) ->
    RiakOP = riakc_map:to_op(to_riak_obj(Value)),
    case call(riakc_pb_socket, update_type, [{?RIAK_BUCKET_TYPE, bucket_encode(Bucket)}, to_binary(Key), RiakOP]) of
        ok -> {ok, Key};
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:document()} | datastore:get_error().
get(#model_config{bucket = Bucket} = _ModelConfig, Key) ->
    case call(riakc_pb_socket, fetch_type, [{?RIAK_BUCKET_TYPE, bucket_encode(Bucket)}, to_binary(Key)]) of
        {ok, Result} ->
            {ok, #document{key = Key, rev = Result,
                value = datastore_utils:shallow_to_record(form_riak_obj(map, Result))}};
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete/2.
%% @end
%%--------------------------------------------------------------------
-spec delete(model_behaviour:model_config(), datastore:key()) -> ok | datastore:generic_error().
delete(#model_config{bucket = Bucket} = _ModelConfig, Key) ->
    case call(riakc_pb_socket, delete, [{?RIAK_BUCKET_TYPE, bucket_encode(Bucket)}, to_binary(Key)]) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(model_behaviour:model_config(), datastore:key()) -> true | false | datastore:generic_error().
exists(#model_config{bucket = Bucket} = _ModelConfig, Key) ->
    case call(riakc_pb_socket, fetch_type, [{?RIAK_BUCKET_TYPE, bucket_encode(Bucket)}, to_binary(Key)]) of
        {ok, {notfound, _}} ->
            false;
        {error, Reason} ->
            {error, Reason};
        {ok, _} ->
            true
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Translates given riak object to erlang term.
%% @end
%%--------------------------------------------------------------------
-spec form_riak_obj(map | counter | register, Obj :: term()) -> term().
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


%%--------------------------------------------------------------------
%% @doc
%% Translates given erlang map into riak object that maybe already initialized.
%% @end
%%--------------------------------------------------------------------
-spec to_riak_obj(term(), undefined | term()) -> term().
to_riak_obj(Term, undefined) when is_tuple(Term) ->
    to_riak_obj(Term);
to_riak_obj(Term, Rev) when is_tuple(Term) ->
    Map = datastore_utils:shallow_to_map(Term),
    RMap0 = Rev,
    maps:fold(
        fun(K, V, Acc) ->
            RiakObj = to_riak_obj(V),
            Module = riakc_datatype:module_for_term(RiakObj),
            Type = Module:type(),
            riakc_map:update({to_binary(K), Type}, fun(_) -> RiakObj end, Acc)
        end, RMap0, Map).


%%--------------------------------------------------------------------
%% @doc
%% Translates given erlang term into new riak object.
%% @end
%%--------------------------------------------------------------------
-spec to_riak_obj(term()) -> term().
to_riak_obj(Term) when is_tuple(Term) ->
    to_riak_obj(Term, riakc_map:new());
to_riak_obj(Int) when is_integer(Int) ->
    Counter = riakc_counter:new(),
    riakc_counter:increment(Int, Counter);
to_riak_obj(Bin) when is_binary(Bin) ->
    Register = riakc_register:new(),
    riakc_register:set(Bin, Register);
to_riak_obj(Atom) when is_atom(Atom) ->
    Register = riakc_register:new(),
    Bin = atom_to_binary(Atom, utf8),
    riakc_register:set(<<"ATOM::", Bin/binary>>, Register);
to_riak_obj(Term) ->
    Register = riakc_register:new(),
    Bin = to_binary(Term),
    riakc_register:set(Bin, Register).


%%--------------------------------------------------------------------
%% @doc
%% Calls given MFA with riak connection handle added as first argument.
%% @end
%%--------------------------------------------------------------------
-spec call(Module :: atom(), Method :: atom(), Args :: [term()]) -> term() | {error, no_riak_nodes}.
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


%%--------------------------------------------------------------------
%% @doc
%% Selects riak connection from connection pool retuned by get_connections/0.
%% @end
%%--------------------------------------------------------------------
-spec select_connection() -> riak_connection().
select_connection() ->
    Connections = get_connections(),
    lists:nth(crypto:rand_uniform(1, length(Connections) + 1), Connections).


%%--------------------------------------------------------------------
%% @doc
%% Gets riak active connections. When no connection is available, tries to
%% estabilish new connections.
%% @end
%%--------------------------------------------------------------------
-spec get_connections() -> [riak_connection()].
get_connections() ->
    case datastore_worker:state_get(riak_connections) of
        [_ | _] = Connections ->
            Connections;
        _ ->
            Connections = connect(datastore_worker:state_get(riak_nodes)),
            datastore_worker:state_put(riak_connections, Connections),
            Connections
    end.


%%--------------------------------------------------------------------
%% @doc
%% Connects to given Riak database nodes.
%% @end
%%--------------------------------------------------------------------
-spec connect([riak_node()]) -> [riak_connection()].
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


%%--------------------------------------------------------------------
%% @doc
%% Encodes given term to base64 binary.
%% @end
%%--------------------------------------------------------------------
-spec term_to_base64(term()) -> binary().
term_to_base64(Term) ->
    Base = base64:encode(term_to_binary(Term)),
    <<?OBJ_PREFIX, Base/binary>>.


%%--------------------------------------------------------------------
%% @doc
%% Decodes given base64 binary to erlang term (reverses term_to_base64/1).
%% @end
%%--------------------------------------------------------------------
-spec base64_to_term(binary()) -> term().
base64_to_term(<<?OBJ_PREFIX, Base/binary>>) ->
    binary_to_term(base64:decode(Base)).


%%--------------------------------------------------------------------
%% @doc
%% Encodes given given term as binary which maybe human readable if possible.
%% @end
%%--------------------------------------------------------------------
-spec to_binary(term()) -> binary().
to_binary(Term) when is_binary(Term) ->
    Term;
to_binary(Term) when is_atom(Term) ->
    <<?ATOM_PREFIX, (atom_to_binary(Term, utf8))/binary>>;
to_binary(Term) ->
    term_to_base64(Term).


%%--------------------------------------------------------------------
%% @doc
%% Translates given database "register" object to erlang term (reverses to_binary/1).
%% @end
%%--------------------------------------------------------------------
-spec from_binary(binary()) -> term().
from_binary(<<?OBJ_PREFIX, _>> = Bin) ->
    base64_to_term(Bin);
from_binary(<<?ATOM_PREFIX, Atom/binary>>) ->
    binary_to_atom(Atom, utf8);
from_binary(Bin) ->
    Bin.


%%--------------------------------------------------------------------
%% @doc
%% Encodes geven bucket name to format supported by database.
%% @end
%%--------------------------------------------------------------------
-spec bucket_encode(datastore:bucket() | binary()) -> binary().
bucket_encode(Bucket) when is_atom(Bucket) ->
    atom_to_binary(Bucket, utf8);
bucket_encode(Bucket) when is_binary(Bucket) ->
    Bucket.