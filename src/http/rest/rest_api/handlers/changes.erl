%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for changes streaming
%%% @end
%%%--------------------------------------------------------------------
-module(changes).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include("http/rest/http_status.hrl").
-include("modules/dbsync/common.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2, content_types_accepted/2]).

%% resource functions
-export([get_space_changes/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
rest_init(Req, _Opts) ->
    {ok, Req, #{}}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), #{}) -> ok.
terminate(_, _, #{changes_stream := Stream}) ->
    gen_changes:stop(Stream).
%%    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), #{} | {error, term()}) -> {[binary()], req(), #{}}.
allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), #{}) -> {true | {false, binary()} | halt, req(), #{}}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), #{}) -> {[{binary(), atom()}], req(), #{}}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_space_changes}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), #{}) ->
    {[{binary(), atom()}], req(), #{}}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/json">>, set_file_attribute}
    ], Req, State}.


%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/attributes/{path}'
%% @doc This method returns selected file attributes.
%%
%% HTTP method: GET
%%
%% @param path File path (e.g. &#39;/My Private Space/testfiles/file1.txt&#39;)
%% @param attribute Type of attribute to query for.
%%--------------------------------------------------------------------
-spec get_space_changes(req(), #{}) -> {term(), req(), #{}}.
get_space_changes(Req, State) ->
    {State2, Req2} = validator:parse_space_id(Req, State),
    {State3, Req3} = validator:parse_timeout(Req2, State2),
    {State4, Req4} = validator:parse_last_seq(Req3, State3),

    State5 = init_stream(State4),

    StreamFun = fun(SendChunk) ->
        stream_loop(SendChunk, State5)
    end,
    Req5 = cowboy_req:set_resp_body_fun(chunked, StreamFun, Req4),
    {ok, Req6} = cowboy_req:reply(?HTTP_OK, [], same(Req5)),
    {halt, Req6, State5}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Init couchbeam changes stream
%% @end
%%--------------------------------------------------------------------
-spec init_stream(State :: #{}) -> #{}.
init_stream(State = #{last_seq := Since}) ->
    ?info("[ changes ]: Starting stream ~p", [Since]),
    Ref = make_ref(),
    Pid = self(),

    NewSince =
        case Since of
            <<"now">> ->
                0; %todo get current seq
            _ ->
                Since
        end,
    {ok, Stream} = couchdb_datastore_driver:changes_start_link(
        fun
            (_, stream_ended, _) ->
                Pid ! {Ref, stream_ended};
            (Seq, Doc, Model) ->
                Pid ! {Ref, #change{seq = Seq, doc = Doc, model = Model}}
        end, NewSince, infinity),
    State#{changes_stream => Stream, ref => Ref}.

%%        {ok, {_, Db}} = couchdb_datastore_driver:get_db(),
%%        {ok, Ref} = couchbeam_changes:follow(Db, [continuous, include_docs, {since, 0}]),
%%        State#{ref => Ref}.




%%--------------------------------------------------------------------
%% @doc
%% Listens for events and pushes them to the socket
%% @end
%%--------------------------------------------------------------------
-spec stream_loop(function(), #{}) -> ok | no_return().
stream_loop(SendChunk, State = #{timeout := Timeout, ref := Ref}) ->
    receive
        {Ref, stream_ended} ->
            ok;
        {Ref, Change = #change{seq = Seq, doc = Doc, model = Model}} ->
            ?info("CHANGE: ~p", [Change]),
            stream_loop(SendChunk, State)
%%        {Ref, {done, LastSeq}} ->
%%            ?info("CHANGE DONE ~p", [LastSeq]),
%%            ok;
%%        {Ref, {change, Row}} ->
%%            ?info("CHANGE ~p", [Row]),
%%            stream_loop(SendChunk, State);
%%        {Ref, {error, Msg}} ->
%%            ?info("CHANGE ERROR ~p", [Msg]),
%%            ok
    after
        Timeout ->
            ok
    end.

%%--------------------------------------------------------------------
%% @todo fix types in cowboy_req
%% @doc
%% Returns the same object in a way that dialyzer will not know its type.
%% @end
%%--------------------------------------------------------------------
-spec same(term()) -> term().
same(X) ->
    put(x, X),
    get(x).