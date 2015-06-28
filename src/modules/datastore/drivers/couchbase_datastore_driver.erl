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

-define(POOL_ID, cberl_default).

-type riak_node() :: {HostName :: binary(), Port :: non_neg_integer()}.
-type riak_connection() :: {riak_node(), ConnectionHandle :: term()}.

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
    connect(),
    case call(set, [?POOL_ID, to_binary(Key), 0, to_binary(Value)]) of
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
    connect(),
    case call(add, [?POOL_ID, to_binary(Key), 0, to_binary(Value)]) of
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
    connect(),
    case call(get, [?POOL_ID, to_binary(Key)]) of
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
    connect(),
    case Pred() of
        true ->
            case call(remove, [?POOL_ID, to_binary(Key)]) of
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
    connect(),
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
            cberl:start_link(?POOL_ID, length(URLs) * 5, Hosts, "", "", "default"),
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
    call(Method, Args, 5).
call(Method, Args, Retry) when Retry > 0 ->
    catch connect(),
    case apply(cberl, Method, Args) of
        {error, Reason} ->
            {error, Reason};
        {_, {error, Reason}} ->
            {error, Reason};
        ok -> ok;
        {ok, Res} ->
            {ok, Res};
        {Key, CAS, Value} when is_binary(Key), is_binary(Value) ->
            {ok, {CAS, Value}};
        {'EXIT', _, _} ->
            call(Method, Args, Retry - 1);
        {shutdown, _} ->
            call(Method, Args, Retry - 1);
        {normal, _} ->
            call(Method, Args, Retry - 1);
        Other ->
            {error, Other}
    end.