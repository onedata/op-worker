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
-module(couchdb_datastore_driver).
-author("Rafal Slota").
-behaviour(store_driver_behaviour).

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% Encoded object prefix
-define(OBJ_PREFIX, "OBJ::").

%% Encoded atom prefix
-define(ATOM_PREFIX, "ATOM::").

-define(LINKS_KEY_SUFFIX, "$$").

%% store_driver_behaviour callbacks
-export([init_bucket/3, healthcheck/1, init_driver/1]).
-export([save/2, create/2, update/3, create_or_update/3, exists/2, get/2, list/3, delete/3]).
-export([add_links/3, delete_links/3, fetch_link/3, foreach_link/4]).

-export([start_gateway/4]).

%%%===================================================================
%%% store_driver_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_driver/1.
%% @end
%%--------------------------------------------------------------------
-spec init_driver(worker_host:plugin_state()) -> {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init_driver(#{db_nodes := DBNodes} = State) ->
    Gateways = lists:map(
        fun({N, {Hostname, Port}}) ->
            GWState = proc_lib:start_link(?MODULE, start_gateway, [self(), N, Hostname, 8091], timer:seconds(5)),
            {N, GWState}
        end, lists:zip(lists:seq(1, length(DBNodes)), DBNodes)),
    {ok, State#{db_gateways => maps:from_list(Gateways)}}.


start_gateway(Parent, N, Hostname, Port) ->
    GWPort = 5084 + N,
    GWAdminPort = GWPort + 1000,
    ?info("Statring couchbase gateway #~p: localhost:~p => ~p:~p", [N, GWPort, Hostname, Port]),

    BinPath = "/opt/couchbase-sync-gateway/bin/sync_gateway",
    PortFD = erlang:open_port({spawn_executable, BinPath}, [stderr_to_stdout, {line, 4 * 1024}, {args, [
        "-bucket", "default",
        "-url", "http://" ++ binary_to_list(Hostname) ++ ":" ++ integer_to_list(Port),
        "-adminInterface", "127.0.0.1:" ++ integer_to_list(GWAdminPort),
        "-interface", ":" ++ integer_to_list(GWPort)
    ]}]),
    erlang:link(PortFD),

    State = #{
        server => self(), port_fd => PortFD, status => running, id => {node(), N},
        gw_port => GWPort, gw_admin_port => GWAdminPort, db_hostname => Hostname, db_port => Port
    },
    proc_lib:init_ack(Parent, State),
    gateway_loop(State).


gateway_loop(#{port_fd := PortFD, id := {_, N} = ID, db_hostname := Hostname, db_port := Port} = State) ->
    NewState =
        receive
            {PortFD, {data, {_, Data}}} ->
%%                ?info("[CouchBase Gateway ~p] ~p", [ID, Data]),
                State;
            {PortFD, closed} ->
                State#{status => closed};
            {'EXIT', PortFD, Reason} ->
                ?error("CouchBase gateway's port ~p exited with reason: ~p", [State, Reason]),
                State#{status => failed};
            Other ->
                ?warning("[CouchBase Gateway ~p] ~p", [ID, Other]),
                State
        after timer:seconds(5) ->
            gateway_loop(State)
        end,
    case NewState of
        #{status := running} ->
            gateway_loop(NewState);
        #{status := closed} ->
            ok;
        #{status := failed} ->
            start_gateway(self(), N, Hostname, Port)
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback init_bucket/3.
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
save(#model_config{name = ModelName} = ModelConfig, #document{rev = undefined, key = Key} = Doc) ->
    datastore:run_synchronized(ModelName, to_binary({?MODULE, Key}),
        fun() ->
            case get(ModelConfig, Key) of
                {error, {not_found, _}} ->
                    create(ModelConfig, Doc);
                {error, Reason} ->
                    {error, Reason};
                {ok, #document{rev = undefined}} ->
                    create(ModelConfig, Doc);
                {ok, #document{rev = Rev}} ->
                    ?info("Resave with rev: ~p", [Rev]),
                    save(ModelConfig, Doc#document{rev = Rev})
            end
        end);
save(#model_config{bucket = Bucket} = _ModelConfig, #document{key = Key, rev = Rev, value = Value}) ->
    case byte_size(term_to_binary(Value)) > 512 * 1024 of
        true -> error(term_to_big);
        false -> ok
    end,

    {ok, DB} = get_db(),
    
    {Props} = to_json_term(Value),
    Doc = {[{<<"_rev">>, Rev}, {<<"_id">>, to_driver_key(Bucket, Key)} | Props]},
    ?info("Save ~p with rev: ~p", [Key, Rev]),
    case couchbeam:save_doc(DB, Doc) of
        {ok, {_}} ->
            {ok, Key};
        {error, conflict} ->
            {error, already_exists};
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
update(#model_config{bucket = _Bucket, name = ModelName} = ModelConfig, Key, Diff) when is_map(Diff) ->
    datastore:run_synchronized(ModelName, to_binary({?MODULE, Key}),
        fun() ->
            case get(ModelConfig, Key) of
                {error, Reason} ->
                    {error, Reason};
                {ok, #document{value = Value} = Doc} ->
                    NewValue = maps:merge(datastore_utils:shallow_to_map(Value), Diff),
                    save(ModelConfig, Doc#document{value = datastore_utils:shallow_to_record(NewValue)})
            end
        end).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create/2.
%% @end
%%--------------------------------------------------------------------
-spec create(model_behaviour:model_config(), datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(#model_config{bucket = Bucket} = _ModelConfig, #document{key = Key, value = Value}) ->
    case byte_size(term_to_binary(Value)) > 512 * 1024 of
        true -> error(term_to_big);
        false -> ok
    end,

    {ok, DB} = get_db(),
    
    {Props} = to_json_term(Value),
    Doc = {[{<<"_id">>, to_driver_key(Bucket, Key)} | Props]},
    case couchbeam:save_doc(DB, Doc) of
        {ok, {_}} ->
            {ok, Key};
        {error, conflict} ->
            {error, already_exists};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create_or_update/2.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(model_behaviour:model_config(), datastore:document(), Diff :: datastore:document_diff()) ->
    %%     {ok, datastore:ext_key()} | datastore:create_error().
    no_return().
create_or_update(#model_config{} = _ModelConfig, #document{key = _Key, value = _Value}, _Diff) ->
    erlang:error(not_implemented).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get(#model_config{bucket = Bucket, name = ModelName} = _ModelConfig, Key) ->
    {ok, DB} = get_db(),
    
    case couchbeam:open_doc(DB, to_driver_key(Bucket, Key)) of
        {ok, {Proplist} = Doc} ->
            {_, Rev} = lists:keyfind(<<"_rev">>, 1, Proplist),
            Proplist1 = lists:keydelete(<<"_id">>, 1, Proplist),
            Proplist2 = lists:keydelete(<<"_rev">>, 1, Proplist1),
            {ok, #document{key = Key, value = from_json_term({Proplist2}), rev = Rev}};
        {error, {not_found, _}} ->
            {error, {not_found, ModelName}};
        {error, not_found} ->
            {error, {not_found, ModelName}};
        {error, Reason} ->
            {error, Reason}
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
delete(#model_config{bucket = Bucket, name = ModelName} = ModelConfig, Key, Pred) ->
    datastore:run_synchronized(ModelName, to_binary({?MODULE, Key}),
        fun() ->
            {ok, DB} = get_db(),
            
            case Pred() of
                true ->
                    case get(ModelConfig, Key) of
                        {error, {not_found, _}} ->
                            ok;
                        {error, not_found} ->
                            ok;
                        {error, Reason} ->
                            {error, Reason};
                        {ok, #document{value = Value, rev = Rev}} ->
                            {Props} = to_json_term(Value),
                            Doc = {[{<<"_id">>, to_driver_key(Bucket, Key)}, {<<"_rev">>, Rev} | Props]},
                            case couchbeam:delete_doc(DB, Doc) of
                                ok ->
                                    ok;
                                {ok, _} ->
                                    ok;
                                {error, key_enoent} ->
                                    ok;
                                {error, Reason} ->
                                    {error, Reason}
                            end
                    end;
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
exists(#model_config{bucket = _Bucket} = ModelConfig, Key) ->
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
add_links(#model_config{bucket = _Bucket} = ModelConfig, Key, Links) when is_list(Links) ->
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
add_links4(#model_config{bucket = _Bucket} = ModelConfig, Key, [], Ctx) ->
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
delete_links(#model_config{bucket = _Bucket} = ModelConfig, Key, all) ->
    delete(ModelConfig, links_doc_key(Key), ?PRED_ALWAYS);
delete_links(#model_config{bucket = _Bucket} = ModelConfig, Key, Links) ->
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
delete_links4(#model_config{bucket = _Bucket} = ModelConfig, Key, [], Ctx) ->
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
fetch_link(#model_config{bucket = _Bucket} = ModelConfig, Key, LinkName) ->
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
foreach_link(#model_config{bucket = _Bucket} = ModelConfig, Key, Fun, AccIn) ->
    case get(ModelConfig, links_doc_key(Key)) of
        {ok, #document{value = LinkMap}} ->
            {ok, maps:fold(Fun, AccIn, LinkMap)};
        {error, {not_found, _}} ->
            {ok, AccIn};
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
    try
        case get_db() of
            {ok, _} -> ok;
            {error, Reason} ->
                {error, Reason}
        end
    catch
        _:R -> {error, R}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================


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


rev(Rev) when is_list(Rev) ->
    list_to_binary(Rev);
rev(Rev) when is_binary(Rev) ->
    Rev.

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


to_json_term(Term) when is_integer(Term) ->
    Term;
to_json_term(Term) when is_binary(Term) ->
    Term;
to_json_term(Term) when is_boolean(Term) ->
    Term;
to_json_term(Term) when is_float(Term) ->
    Term;
to_json_term(Term) when is_list(Term) ->
    [to_json_term(Elem) || Elem <- Term];
to_json_term(Term) when is_atom(Term) ->
    to_binary(Term);
to_json_term(Term) when is_tuple(Term) ->
    Elems = tuple_to_list(Term),
    Proplist0 = [{<<"RECORD::">>, <<"unknown">>} | lists:zip(lists:seq(1, length(Elems)), Elems)],
    Proplist1 = [{to_binary(Key), to_json_term(Value)} || {Key, Value} <- Proplist0],
    {Proplist1};
to_json_term(Term) when is_map(Term) ->
    Proplist0 = maps:to_list(Term),
    Proplist1 = [{to_binary(Key), to_json_term(Value)} || {Key, Value} <- Proplist0],
    {Proplist1};
to_json_term(Term) ->
    to_binary(Term).


from_json_term(Term) when is_integer(Term) ->
    Term;
from_json_term(Term) when is_boolean(Term) ->
    Term;
from_json_term(Term) when is_float(Term) ->
    Term;
from_json_term(Term) when is_list(Term) ->
    [from_json_term(Elem) || Elem <- Term];
from_json_term({Term}) when is_list(Term) ->
    case lists:keyfind(<<"RECORD::">>, 1, Term) of
        false ->
            Proplist2 = [{from_binary(Key), from_json_term(Value)} || {Key, Value} <- Term],
            maps:from_list(Proplist2);
        {_, RecordType} ->
            Proplist0 = [{from_binary(Key), from_json_term(Value)} || {Key, Value} <- Term, Key =/= <<"RECORD::">>],
            Proplist1 = lists:sort(Proplist0),
            {_, Values} = lists:unzip(Proplist1),
            list_to_tuple(Values)
    end;
from_json_term(Term) when is_binary(Term) ->
    from_binary(Term).


-spec links_doc_key(Key :: datastore:key()) -> BinKey :: binary().
links_doc_key(Key) ->
    BinKey = to_binary(Key),
    <<BinKey/binary, ?LINKS_KEY_SUFFIX>>.

-spec to_driver_key(Bucket :: datastore:bucket(), Key :: datastore:key()) -> BinKey :: binary().
to_driver_key(Bucket, Key) ->
    base64:encode(term_to_binary({Bucket, Key})).


-spec get_db() -> {ok, term()} | {error, term()}.
get_db() ->
    Gateways = maps:values(datastore_worker:state_get(db_gateways)),
    ActiveGateways = [GW || #{status := running} = GW <- Gateways],

    case ActiveGateways of
        [] ->
            ?error("Unable to select CouchBase Gateway: no active gateway among: ~p", [Gateways]),
            {error, no_active_gateway};
        _ ->
            #{gw_port := Port} = lists:nth(crypto:rand_uniform(1, length(ActiveGateways) + 1), ActiveGateways),
            Server = couchbeam:server_connection("localhost", Port),
            couchbeam:open_db(Server, <<"default">>)
    end.

