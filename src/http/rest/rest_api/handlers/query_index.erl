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
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2]).

%% resource functions
-export([query_index/2]).

%%%===================================================================
%%% API
%%%===================================================================

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
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | stop, req(), maps:map()}.
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
%% '/api/v3/oneprovider/spaces/{sid}/indexes/{index_name}/query'
%% @doc This method returns the list of files which match the query on a
%% predefined index.
%%
%% HTTP method: GET
%%
%% @param sid Id of the space within which index exist.
%% @param name Name of the index.
%%--------------------------------------------------------------------
-spec query_index(req(), maps:map()) -> {term(), req(), maps:map()}.
query_index(Req, State) ->
    {StateWithSpaceId, ReqWithSpaceId} = validator:parse_space_id(Req, State),
    {StateWithIndexName, ReqWithIndexName} = validator:parse_index_name(ReqWithSpaceId, StateWithSpaceId),

    % get options
    {StateWithBbox, ReqWithBbox} = validator:parse_bbox(ReqWithIndexName, StateWithIndexName),
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

    #{space_id := SpaceId, index_name := IndexName} = StateWithSpatial,
    Options = index_utils:sanitize_query_options(StateWithSpatial),

    case index:query(SpaceId, IndexName, Options) of
        {ok, {Rows}} ->
            QueryResult = lists:map(fun(Row) -> maps:from_list(Row) end, Rows),
            {json_utils:encode(QueryResult), ReqWithSpatial, StateWithSpatial};
        {error, ?EINVAL} ->
            throw(?ERROR_AMBIGUOUS_INDEX_NAME);
        {error, not_found} ->
            throw(?ERROR_INDEX_NOT_FOUND)
    end.
