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

%% Protocol driver module
-define(DRIVER, couchbeam).


%% store_driver_behaviour callbacks
-export([init_bucket/3, healthcheck/1]).
-export([save/2, create/2, update/3, create_or_update/3, exists/2, get/2, list/3, delete/3]).
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
save(#model_config{} = ModelConfig, #document{rev = undefined} = Doc) ->
    create(ModelConfig, Doc);
save(#model_config{bucket = Bucket} = _ModelConfig, #document{key = Key, rev = Rev, value = Value}) ->
    case byte_size(term_to_binary(Value)) > 512 * 1024 of
        true -> error(term_to_big);
        false -> ok
    end,

    Server = get_server(),
    {ok, DB} = couchbeam:open_db(Server, <<"default">>),
    {Props} =  to_json_term(Value),
    Doc = {[{<<"_rev">>, to_binary(Rev)}, {<<"_id">>, to_binary(Key)} | Props]},
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
update(#model_config{bucket = _Bucket} = ModelConfig, Key, Diff) when is_map(Diff) ->
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
    case byte_size(term_to_binary(Value)) > 512 * 1024 of
        true -> error(term_to_big);
        false -> ok
    end,
    Server = get_server(),
    {ok, DB} = couchbeam:open_db(Server, <<"default">>),
    {Props} = to_json_term(Value),
    Doc = {[{<<"_id">>, to_binary(Key)} | Props]},
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
    Server = get_server(),
    {ok, DB} = couchbeam:open_db(Server, <<"default">>),
    case couchbeam:open_doc(DB, to_binary(Key)) of
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
delete(#model_config{bucket = Bucket} = ModelConfig, Key, Pred) ->
    Server = get_server(),
    {ok, DB} = couchbeam:open_db(Server, <<"default">>),
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
                    Doc = {[{<<"_id">>, to_binary(Key)}, {<<"_rev">>, to_binary(Rev)} | Props]},
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
    end.

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
        case ensure_couchbeam_connected() of
            ok -> ok;
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


-spec exec(mcd, add | set, Key :: binary(), Value :: binary(), Expiration :: non_neg_integer()) ->
                  ok | {ok, term()} | {error, term()}.
exec(mcd, add, Key, Value, Expiration) ->
    callmc_text(do, [{add, 0, Expiration}, Key, Value]);
exec(mcd, set, Key, Value, Expiration) ->
    callmc_text(set, [Key, Value, Expiration]).


-spec exec(mcd, get | remove, Key :: binary()) ->
                  ok | {ok, term()} | {error, term()}.
exec(mcd, get, Key) ->
    callmc_text(get, [Key]);
exec(mcd, remove, Key) ->
    callmc_text(delete, [Key]).


-spec callmc_text(Method :: atom(), Args :: [term()]) -> ok | {ok, term()} | {error, term()}.
callmc_text(Method, Args) ->
    callmc_text(Method, Args, 5, undefined).
callmc_text(Method, Args, Retry, LastError) when Retry > 0 ->
    ensure_couchbeam_connected(),
    try apply(mcd, Method, ['MCDCluster'] ++ Args) of
        {error, notfound} ->
            {error, key_enoent};
        {error, all_nodes_down} ->
            datastore_worker:state_put(mc_text_connected, {error, all_nodes_down}),
    catch mcd_cluster:stop('MCDCluster'),
            callmc_text(Method, Args, Retry - 1, all_nodes_down);
        {error, noproc} ->
            datastore_worker:state_put(mc_text_connected, {error, noproc}),
    catch mcd_cluster:stop('MCDCluster'),
            callmc_text(Method, Args, Retry - 1, noproc);
        {error, {normal, _}} ->
            datastore_worker:state_put(mc_text_connected, {error, no_genserver}),
    catch mcd_cluster:stop('MCDCluster'),
            callmc_text(Method, Args, Retry - 1, no_genserver);
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
            callmc_text(Method, Args, Retry - 1, E);
        {shutdown, _} = E ->
            callmc_text(Method, Args, Retry - 1, E);
        {normal, _} = E ->
            callmc_text(Method, Args, Retry - 1, E);

        Other ->
            {error, Other}
    catch
        Class:Reason0 ->
            datastore_worker:state_put(mc_text_connected, {error, Reason0}),
            ?error_stacktrace("CouchBase connection error (type ~p): ~p", [Class, Reason0]),
            callmc_text(Method, Args, Retry - 1, Reason0)
    end;
callmc_text(_Method, _Args, _, LastError) ->
    ?error_stacktrace("CouchBase communication retry failed. Last error: ~p", [LastError]),
    {error, {communication_failure, LastError}}.

-spec ensure_couchbeam_connected() -> ok | {error, term()}.
ensure_couchbeam_connected() ->
    case datastore_worker:state_get(couchbeam_connected) of
        {ok, _} -> ok;
        ok -> ok;
        _ ->
            try
                L = datastore_worker:state_get(db_nodes),
                [{SH, SP} | _] = Servers =
                    lists:map(
                      fun({Hostname, Port}) ->
                              {binary_to_list(Hostname), Port}
                      end, L),
                spawn(
                    fun() ->
                        CMD = "./sync_gateway -bucket default -url http://" ++ SH ++ ":8091 -interface :5985",
                        PWD = os:cmd("pwd"),
                        ResCMD = os:cmd(CMD),
                        ?info("================> sync_gateway ~p ~p ~p", [CMD, PWD, ResCMD])
                    end),
                Res = couchbeam:server_connection("localhost", 5985),
                timer:sleep(1000),
                ?info("Starting couchbeam ~p", [Res]),
                datastore_worker:state_put(couchbeam_connected, {ok, Res})
            catch
                _:Reason ->
                    ?error_stacktrace("Could start couchbeam (couchbase connection) due to: ~p", [Reason]),
                    {error, Reason}
            end
    end.

get_server() ->
    ensure_couchbeam_connected(),
    {ok, S} = datastore_worker:state_get(couchbeam_connected),
    S.

