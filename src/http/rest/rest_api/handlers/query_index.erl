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
-spec terminate(Reason :: term(), req(), maps:map()) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), maps:map() | {error, term()}) -> {[binary()], req(), maps:map()}.
allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | halt, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) -> {[{binary(), atom()}], req(), maps:map()}.
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
-spec query_index(req(), maps:map()) -> {term(), req(), maps:map()}.
query_index(Req, State) ->
    {StateWithId, ReqWithId} = validator:parse_id(Req, State),
    {StateWithBbox, ReqWithBbox} = validator:parse_bbox(ReqWithId, StateWithId),
    {StateWithDescending, ReqWithDescending} = validator:parse_descending(ReqWithBbox, StateWithBbox),
    {StateWithEndkey, ReqWithEndkey} = validator:parse_endkey(ReqWithDescending, StateWithDescending),
    {StateWithInclusiveEnd, ReqWithInclusiveEnd} = validator:parse_inclusive_end(ReqWithEndkey, StateWithEndkey),
    {StateWithKey, ReqWithKey} = validator:parse_key(ReqWithInclusiveEnd, StateWithInclusiveEnd),
    {StateWithKeys, ReqWithKeys} = validator:parse_keys(ReqWithKey, StateWithKey),
    {StateWithLimit, ReqWithLimit} = validator:parse_limit(ReqWithKeys, StateWithKeys),
    {StateWithSkip, ReqWithSkip} = validator:parse_skip(ReqWithLimit, StateWithLimit),
    {StateWithStale, ReqWithStale} = validator:parse_stale(ReqWithSkip, StateWithSkip),
    {StateWithStartkey, ReqWithStartkey} = validator:parse_startkey(ReqWithStale, StateWithStale),
    {StateWithStartRange, ReqWithStartRange} = validator:parse_start_range(ReqWithStartkey, StateWithStartkey),
    {StateWithEndRange, ReqWithEndRange} = validator:parse_end_range(ReqWithStartRange, StateWithStartRange),
    {StateWithSpatial, ReqWithSpatial} = validator:parse_spatial(ReqWithEndRange, StateWithEndRange),

    #{auth := _Auth, id := Id} = StateWithSpatial,

    Options = prepare_options(StateWithSpatial),
    {ok, Guids} = indexes:query_view(Id, Options),
    ObjectIds = lists:map(fun(Guid) ->
        {ok, ObjectId} = cdmi_id:guid_to_objectid(Guid),
        ObjectId
    end, Guids),

    {json_utils:encode_map(ObjectIds), ReqWithSpatial, StateWithSpatial}.

%%--------------------------------------------------------------------
%% @doc
%% Convert Request parameters to couchdb view query options
%% @end
%%--------------------------------------------------------------------
-spec prepare_options(maps:map() | list()) -> list().
prepare_options(Map) when is_map(Map) ->
    prepare_options(maps:to_list(Map));
prepare_options([]) ->
    [];
prepare_options([{id, _} | Rest]) ->
    prepare_options(Rest);
prepare_options([{auth, _} | Rest]) ->
    prepare_options(Rest);

prepare_options([{bbox, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{bbox, Bbox} | Rest]) ->
    [{bbox, Bbox} | prepare_options(Rest)];

prepare_options([{descending, true} | Rest]) ->
    [descending | prepare_options(Rest)];
prepare_options([{descending, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{descending, _} | _Rest]) ->
    throw(?ERROR_INVALID_DESCENDING);

prepare_options([{endkey, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{endkey, Endkey} | Rest]) ->
    try
        [{endkey, couchbeam_ejson:decode(Endkey)} | prepare_options(Rest)]
    catch
        _:_ ->
            throw(?ERROR_INVALID_ENDKEY)
    end;

prepare_options([{startkey, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{startkey, StartKey} | Rest]) ->
    try
        [{startkey, couchbeam_ejson:decode(StartKey)} | prepare_options(Rest)]
    catch
        _:_ ->
            throw(?ERROR_INVALID_STARTKEY)
    end;

prepare_options([{inclusive_end, true} | Rest]) ->
    [inclusive_end | prepare_options(Rest)];
prepare_options([{inclusive_end, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{inclusive_end, _} | _Rest]) ->
    throw(?ERROR_INVALID_INCLUSIVE_END);

prepare_options([{key, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{key, Key} | Rest]) ->
    try
        [{key, couchbeam_ejson:decode(Key)} | prepare_options(Rest)]
    catch
        _:_ ->
            throw(?ERROR_INVALID_KEY)
    end;

prepare_options([{keys, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{keys, Keys} | Rest]) ->
    try
        DecodedKeys = couchbeam_ejson:decode(Keys),
        true = is_list(DecodedKeys),
        [{keys, DecodedKeys} | prepare_options(Rest)]
    catch
        _:_ ->
            throw(?ERROR_INVALID_KEYS)
    end;

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
prepare_options([{stale, <<"false">>} | Rest]) ->
    [{stale, false} | prepare_options(Rest)];
prepare_options([{stale, _} | _]) ->
    throw(?ERROR_INVALID_STALE);
prepare_options([{spatial, true} | Rest]) ->
    [{spatial, true} | prepare_options(Rest)];
prepare_options([{spatial, false} | Rest]) ->
    prepare_options(Rest);

prepare_options([{start_range, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{start_range, Endkey} | Rest]) ->
    try
        [{start_range, couchbeam_ejson:decode(Endkey)} | prepare_options(Rest)]
    catch
        _:_ ->
            throw(?ERROR_INVALID_ENDKEY)
    end;

prepare_options([{end_range, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{end_range, Endkey} | Rest]) ->
    try
        [{end_range, couchbeam_ejson:decode(Endkey)} | prepare_options(Rest)]
    catch
        _:_ ->
            throw(?ERROR_INVALID_ENDKEY)
    end.