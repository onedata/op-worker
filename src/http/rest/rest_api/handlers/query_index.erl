%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for querying indexes.
%%% @end
%%%--------------------------------------------------------------------
-module(query_index).
-author("Tomasz Lichon").

-include("http/http_common.hrl").
-include("http/rest/rest_api/rest_errors.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2]).

%% resource functions
-export([query_index/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
rest_init(Req, State) ->
    {ok, Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), #{}) -> ok.
terminate(_, _, _) ->
    ok.

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
        {<<"application/json">>, query_index}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc This method returns the list of files which match the query on a predefined index.
%%--------------------------------------------------------------------
-spec query_index(req(), #{}) -> {term(), req(), #{}}.
query_index(Req, State) ->
    {StateWithId, ReqWithId} = validator:parse_id(Req, State),
    {StateWithBbox, ReqWithBbox} = validator:parse_bbox(ReqWithId, StateWithId),
    {StateWithDescending, ReqWithDescending} = validator:parse_descending(ReqWithBbox, StateWithBbox),
    {StateWithEndkey, ReqWithEndkey} = validator:parse_endkey(ReqWithDescending, StateWithDescending),
    {StateWithInclusiveEnd, ReqWithInclusiveEnd} = validator:parse_inclusive_end(ReqWithEndkey, StateWithEndkey),
    {StateWithKey, ReqWithKey} = validator:parse_key(ReqWithInclusiveEnd, StateWithInclusiveEnd),
%%    {StateWithKeys, ReqWithKeys} = validator:parse_keys(ReqWithKey, StateWithKey), %todo VFS-2369 support complex keys
    {StateWithLimit, ReqWithLimit} = validator:parse_limit(ReqWithKey, StateWithKey),
    {StateWithSkip, ReqWithSkip} = validator:parse_skip(ReqWithLimit, StateWithLimit),
    {StateWithStale, ReqWithStale} = validator:parse_stale(ReqWithSkip, StateWithSkip),
    {StateWithStartkey, ReqWithStartkey} = validator:parse_startkey(ReqWithStale, StateWithStale),

    #{auth := _Auth, id := Id} = StateWithStartkey,

    Options = prepare_options(StateWithStartkey),
    {ok, Results} = indexes:query_view(Id, Options),

    {json_utils:encode(Results), ReqWithStartkey, StateWithStartkey}.

%%--------------------------------------------------------------------
%% @doc
%% Convert Request parameters to couchdb view query options
%% @end
%%--------------------------------------------------------------------
-spec prepare_options(#{} | list()) -> list().
prepare_options(Map) when is_map(Map) ->
    prepare_options(maps:to_list(Map));
prepare_options([]) ->
    [];
prepare_options([{id, _} | Rest]) ->
    prepare_options(Rest);
prepare_options([{auth, _} | Rest]) ->
    prepare_options(Rest);
prepare_options([{bbox, _} | Rest]) ->
    prepare_options(Rest);

prepare_options([{descending, true} | Rest]) ->
    [descending | prepare_options(Rest)];
prepare_options([{descending, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{descending, _} | _Rest]) ->
    throw(?ERROR_INVALID_DESCENDING);

prepare_options([{endkey, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{endkey, Endkey} | Rest]) ->
    [{endkey, Endkey} | prepare_options(Rest)];

prepare_options([{startkey, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{startkey, StartKey} | Rest]) ->
    [{startkey, StartKey} | prepare_options(Rest)];

prepare_options([{inclusive_end, true} | Rest]) ->
    [inclusive_end | prepare_options(Rest)];
prepare_options([{inclusive_end, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{inclusive_end, _} | _Rest]) ->
    throw(?ERROR_INVALID_INCLUSIVE_END);

prepare_options([{key, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{key, Key} | Rest]) ->
    [{key, Key} | prepare_options(Rest)];

prepare_options([{keys, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{keys, Keys} | Rest]) when is_list(Keys) ->
    [{keys, Keys} | prepare_options(Rest)];
prepare_options([{keys, Key} | Rest]) ->
    [{keys, [Key]} | prepare_options(Rest)];

prepare_options([{limit, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{limit, Limit} | Rest]) ->
    case catch binary_to_integer(Limit) of
        N when is_integer(N) ->
            [{limit, N} | prepare_options(Rest)];
        _Error ->
            throw(?ERROR_INVALID_LIMIT)
    end;

prepare_options([{skip, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{skip, Skip} | Rest]) ->
    case catch binary_to_integer(Skip) of
        N when is_integer(N) ->
            [{skip, N} | prepare_options(Rest)];
        _Error ->
            throw(?ERROR_INVALID_SKIP)
    end;

prepare_options([{stale, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{stale, <<"ok">>} | Rest]) ->
    [{stale, ok} | prepare_options(Rest)];
prepare_options([{stale, <<"update_after">>} | Rest]) ->
    [{stale, update_after} | prepare_options(Rest)];
prepare_options([{stale, _} | _]) ->
    throw(?ERROR_INVALID_STALE).