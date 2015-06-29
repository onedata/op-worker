%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc CouchBase database driver.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_datastore_driver).
-author("Rafal Slota").
-behaviour(store_driver_behaviour).

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% Bukcet type that is defined in database and configured to store "map" data type
-define(RIAK_BUCKET_TYPE, <<"maps">>).

%% Encoded object prefix
-define(OBJ_PREFIX, "OBJ::").

%% Encoded atom prefix
-define(ATOM_PREFIX, "ATOM::").

-define(LINKS_KEY_SUFFIX, "$$").

-define(POOLS, lists:map(fun(E) -> erlang:list_to_atom("cberl_" ++ erlang:integer_to_list(E)) end, lists:seq(1, 1200))).

-define(POOL_ID, lists:nth(crypto:rand_uniform(1, length(?POOLS) + 1), ?POOLS)).

-type riak_node() :: {HostName :: binary(), Port :: non_neg_integer()}.
-type riak_connection() :: {riak_node(), ConnectionHandle :: term()}.


-define('CBE_ADD',      1).
-define('CBE_REPLACE',  2).
-define('CBE_SET',      3).
-define('CBE_APPEND',   4).
-define('CBE_PREPEND',  5).

-define('CMD_CONNECT',    0).
-define('CMD_STORE',      1).
-define('CMD_MGET',       2).
-define('CMD_UNLOCK',     3).
-define('CMD_MTOUCH',     4).
-define('CMD_ARITHMETIC', 5).
-define('CMD_REMOVE',     6).
-define('CMD_HTTP',       7).

-type handle() :: binary().

-record(instance, {handle :: handle(),
    bucketname :: string(),
    transcoder :: module(),
    connected :: true | false,
    opts :: list()}).

-type key() :: string().
-type value() :: string() | list() | integer() | binary().
-type operation_type() :: add | replace | set | append | prepend.
-type instance() :: #instance{}.
-type http_type() :: view | management | raw.
-type http_method() :: get | post | put | delete.

%% store_driver_behaviour callbacks
-export([init_bucket/3, healthcheck/1]).
-export([save/2, create/2, update/3, exists/2, get/2, list/3, delete/3]).
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
init_bucket(_Bucket, _Models, _NodeToSync) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_bucket/2.
%% @end
%%--------------------------------------------------------------------
-spec save(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(#model_config{bucket = Bucket} = _ModelConfig, #document{key = Key, rev = Rev, value = Value}) ->
    case byte_size(term_to_binary(Value)) > 5 * 1024 * 1024 of
        true -> error(term_to_big);
        false -> ok
    end,
    ?info("WTF ~p ~p", [Value, to_binary(Value)]),
    case call(set, [to_binary(Key), 0, to_binary(Value)]) of
        ok ->
            {ok, Key};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback update/3.
%% @end
%%--------------------------------------------------------------------
-spec update(model_behaviour:model_config(), datastore:ext_key(),
    Diff :: datastore:document_diff()) -> {ok, datastore:ext_key()} | datastore:update_error().
update(#model_config{bucket = _Bucket} = _ModelConfig, _Key, Diff) when is_function(Diff) ->
    erlang:error(not_implemented);
update(#model_config{bucket = Bucket, name = ModelName} = ModelConfig, Key, Diff) when is_map(Diff) ->
    case get(ModelConfig, Key) of
        {error, Reason} ->
            {error, Reason};
        {ok, #document{value = Value} = Doc} ->
            NewValue = maps:merge(datastore_utils:shallow_to_map(Value), Diff),
            save(ModelConfig, Doc#document{value = datastore_utils:shallow_to_record(NewValue)})
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create/2.
%% @end
%%--------------------------------------------------------------------
-spec create(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(#model_config{bucket = Bucket} = _ModelConfig, #document{key = Key, value = Value}) ->
    case byte_size(term_to_binary(Value)) > 5 * 1024 * 1024 of
        true -> error(term_to_big);
        false -> ok
    end,
    ?info("WTF ~p ~p", [Value, to_binary(Value)]),
    case call(add, [to_binary(Key), 0, to_binary(Value)]) of
        ok ->
            {ok, Key};
        {error, key_eexists} ->
            {error, already_exists};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get(#model_config{bucket = Bucket, name = ModelName} = _ModelConfig, Key) ->
    case call(get, [to_binary(Key)]) of
        {error, key_enoent} ->
            {error, {not_found, ModelName}};
        {error, Reason} ->
            {error, Reason};
        {ok, {CAS, Value}} ->
            {ok, #document{key = Key, rev = CAS,
                value = from_binary(Value)}}
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
-spec delete(model_behaviour:model_config(), datastore:ext_key(), datastore:delete_predicate()) ->
    ok | datastore:generic_error().
delete(#model_config{bucket = Bucket} = _ModelConfig, Key, Pred) ->
    case Pred() of
        true ->
            case call(remove, [to_binary(Key)]) of
                ok ->
                    ok;
                {error, key_enoent} ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, boolean()} | datastore:generic_error().
exists(#model_config{bucket = Bucket} = ModelConfig, Key) ->
    case get(ModelConfig, Key) of
        {error, {not_found, _}} ->
            {ok, false};
        {error, Reason} ->
            {error, Reason};
        {ok, _} ->
            {ok, true}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback add_links/3.
%% @end
%%--------------------------------------------------------------------
-spec add_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()]) ->
    ok | datastore:generic_error().
add_links(#model_config{bucket = Bucket} = ModelConfig, Key, Links) when is_list(Links) ->
    case get(ModelConfig, links_doc_key(Key)) of
        {ok, #document{value = LinkMap}} ->
            add_links4(ModelConfig, Key, Links, LinkMap);
        {error, {not_found, _}} ->
            add_links4(ModelConfig, Key, Links, #{});
        {error, Reason} ->
            {error, Reason}
    end.

-spec add_links4(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()], InternalCtx :: term()) ->
    ok | datastore:generic_error().
add_links4(#model_config{bucket = Bucket} = ModelConfig, Key, [], Ctx) ->
    case save(ModelConfig, #document{key = links_doc_key(Key), value = Ctx}) of
        {ok, _} -> ok;
        {error, Reason} ->
            {error, Reason}
    end;
add_links4(#model_config{bucket = _Bucket} = ModelConfig, Key, [{LinkName, LinkTarget} | R], Ctx) ->
    add_links4(ModelConfig, Key, R, maps:put(LinkName, LinkTarget, Ctx)).


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback delete_links/3.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:link_name()] | all) ->
    ok | datastore:generic_error().
delete_links(#model_config{bucket = Bucket} = ModelConfig, Key, all) ->
    delete(ModelConfig, links_doc_key(Key), ?PRED_ALWAYS);
delete_links(#model_config{bucket = Bucket} = ModelConfig, Key, Links) ->
    case get(ModelConfig, links_doc_key(Key)) of
        {ok, #document{value = LinkMap}} ->
            delete_links4(ModelConfig, Key, Links, LinkMap);
        {error, {not_found, _}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

-spec delete_links4(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()] | all, InternalCtx :: term()) ->
    ok | datastore:generic_error().
delete_links4(#model_config{bucket = Bucket} = ModelConfig, Key, [], Ctx) ->
    case save(ModelConfig, #document{key = links_doc_key(Key), value = Ctx}) of
        {ok, _} -> ok;
        {error, Reason} ->
            {error, Reason}
    end;
delete_links4(#model_config{} = ModelConfig, Key, [Link | R], Ctx) ->
    delete_links4(ModelConfig, Key, R, maps:remove(Link, Ctx)).



%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback fetch_links/3.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(model_behaviour:model_config(), datastore:ext_key(), datastore:link_name()) ->
    {ok, datastore:link_target()} | datastore:link_error().
fetch_link(#model_config{bucket = Bucket} = ModelConfig, Key, LinkName) ->
    case get(ModelConfig, links_doc_key(Key)) of
        {ok, #document{value = LinkMap}} ->
            case maps:get(LinkName, LinkMap, undefined) of
                undefined ->
                    {error, link_not_found};
                LinkTarget ->
                    {ok, LinkTarget}
            end;
        {error, {not_found, _}} ->
            {error, link_not_found};
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback foreach_link/4.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(model_behaviour:model_config(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:link_error().
foreach_link(#model_config{bucket = Bucket} = ModelConfig, Key, Fun, AccIn) ->
    case get(ModelConfig, links_doc_key(Key)) of
        {ok, #document{value = LinkMap}} ->
            {ok, maps:fold(Fun, AccIn, LinkMap)};
        {error, {not_found, _}} ->
            AccIn;
        {error, Reason} ->
            {error, Reason}
    end.


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
%% Connects to given CouchBase database nodes.
%% @end
%%--------------------------------------------------------------------
-spec connect() -> ok.
connect() ->
    case whereis(?POOL_ID) of
        PID when is_pid(PID) ->
            ok;
        _ ->
            URLs =
                lists:map(fun({Hostname, Port}) ->
                    binary_to_list(Hostname) ++ ":" ++ integer_to_list(Port)
                end, datastore_worker:state_get(db_nodes)),
            Hosts = string:join(URLs, ";"),
            lists:foreach(fun(Pool) ->
                cberl:start_link(Pool, length(URLs) * 5, Hosts, "", "", "default")
            end, ?POOLS),
            ?debug("CouchBase init with nodes: ~p", [Hosts])
    end,
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Encodes given term to base64 binary.
%% @end
%%--------------------------------------------------------------------
-spec term_to_base64(term()) -> binary().
term_to_base64(Term) ->
    Base = base64:encode(term_to_binary(Term)),
    <<?OBJ_PREFIX, Base/binary>>.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Decodes given base64 binary to erlang term (reverses term_to_base64/1).
%% @end
%%--------------------------------------------------------------------
-spec base64_to_term(binary()) -> term().
base64_to_term(<<?OBJ_PREFIX, Base/binary>>) ->
    binary_to_term(base64:decode(Base)).

%%--------------------------------------------------------------------
%% @private
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
%% @private
%% @doc
%% Translates given database "register" object to erlang term (reverses to_binary/1).
%% @end
%%--------------------------------------------------------------------
-spec from_binary(binary()) -> term().
from_binary(<<?OBJ_PREFIX, _/binary>> = Bin) ->
    base64_to_term(Bin);
from_binary(<<?ATOM_PREFIX, Atom/binary>>) ->
    binary_to_atom(Atom, utf8);
from_binary(Bin) ->
    Bin.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Encodes geven bucket name to format supported by database.
%% @end
%%--------------------------------------------------------------------
-spec bucket_encode(datastore:bucket()) -> binary().
bucket_encode(Bucket) when is_atom(Bucket) ->
    atom_to_binary(Bucket, utf8);
bucket_encode(Bucket) when is_binary(Bucket) ->
    Bucket.

links_doc_key(Key) ->
    BinKey = to_binary(Key),
    <<BinKey/binary, ?LINKS_KEY_SUFFIX>>.


call(Method, Args) ->
    call(Method, Args, 5, undefined).
call(Method, Args, Retry, LastError) when Retry > 0 ->
%%     {Node, Pid} = select_connection(),
    {Node, Pid} = select_connection_nif(),
    try apply(cberl_internal, Method, [Pid] ++ Args) of
        {error, Reason} ->
            {error, Reason};
        {_, {error, Reason}} ->
            {error, Reason};
        ok -> ok;
        {ok, Res} ->
            {ok, Res};
        {Key, CAS, Value} when is_binary(Key), is_binary(Value) ->
            {ok, {CAS, Value}};
        {'EXIT', _, _} = E ->
            call(Method, Args, Retry - 1, E);
        {shutdown, _} = E ->
            call(Method, Args, Retry - 1, E);
        {normal, _} = E ->
            call(Method, Args, Retry - 1, E);
        Other ->
            {error, Other}
    catch
        Class:Reason0 ->
            NewConn = [Conn || {N, _} = Conn <- datastore_worker:state_get(couchbase_connections), N =/= Node],
            datastore_worker:state_put(couchbase_connections, NewConn),
            ?error_stacktrace("CouchBase connection error (type ~p): ~p ~p", [Class, Reason0, NewConn]),
            call(Method, Args, Retry - 1, Reason0)
    end;
call(Method, Args, _, LastError) ->
    ?error_stacktrace("CouchBase communication retry failed. Last error: ~p", [LastError]),
    {error, {communication_failure, LastError}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Selects riak connection from connection pool retuned by get_connections/0.
%% @end
%%--------------------------------------------------------------------
-spec select_connection() -> riak_connection().
select_connection() ->
    Connections = get_connections(),
    lists:nth(crypto:rand_uniform(1, length(Connections) + 1), Connections).

select_connection_nif() ->
    Connections = get_connections_nif(),
    lists:nth(crypto:rand_uniform(1, length(Connections) + 1), Connections).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets riak active connections. When no connection is available, tries to
%% estabilish new connections.
%% @end
%%--------------------------------------------------------------------
-spec get_connections() -> [riak_connection()].
get_connections() ->
    case datastore_worker:state_get(couchbase_connections) of
        [_ | _] = Connections ->
            Connections;
        _ ->
            L = datastore_worker:state_get(db_nodes),
            Connections = connect(L),
            datastore_worker:state_put(couchbase_connections, Connections),
            Connections
    end.

get_connections_nif() ->
    case datastore_worker:state_get(couchbase_connections) of
        [_ | _] = Connections ->
            Connections;
        _ ->
            L = datastore_worker:state_get(db_nodes),
            Connections = connect_nif(L),
            datastore_worker:state_put(couchbase_connections, Connections),
            Connections
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Connects to given Riak database nodes.
%% @end
%%--------------------------------------------------------------------
-spec connect([riak_node()]) -> [riak_connection()].
connect([{Hostname, Port} = Node | R]) ->
    case cberl_worker:start_link([{host, binary_to_list(Hostname) ++ ":" ++ integer_to_list(Port)}, {username, ""},
                                    {password, ""}, {bucketname, "default"}, {transcoder, cberl_transcoder}]) of
        {ok, Pid} ->
            [{Node, Pid} | connect(R)];
        {error, Reason} ->
            ?error("Cannot connect to couchbase node ~p due to ~p", [Node, Reason]),
            connect(R)
    end;
connect([]) ->
    [].

connect_nif([{Hostname, Port} = Node | R]) ->
    {ok, Handle} = cberl_nif:new(),
    State = #instance{handle = Handle,
        transcoder = cberl_transcoder,
        bucketname ="default",
        opts = [binary_to_list(Hostname) ++ ":" ++ integer_to_list(Port), "", "", "default"],
        connected = false},
    State2 = case cberl_async:connect(State) of
                 ok -> State#instance{connected = true};
                 {error, _} -> State#instance{connected = false}
             end,
    ?info("CONN NIF ~p", [State2]),
    [{Node, State2} | connect(R)];
connect_nif([]) ->
    [].