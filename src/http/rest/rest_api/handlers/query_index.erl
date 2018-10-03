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
        {ok, QueryResult} ->
            case process_query_result(QueryResult) of
                {error, index_function} ->
                    throw(?ERROR_INDEX_FUNCTION);
                ProcessedResult ->
                    {json_utils:encode(ProcessedResult), ReqWithSpatial, StateWithSpatial}
            end;
        {error, not_found} ->
            throw(?ERROR_INDEX_NOT_FOUND);
        {error, not_supported} ->
            ok % TODO index not supported on this provider
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Processes query result before returning it to user.
%% @end
%%-------------------------------------------------------------------
-spec process_query_result(datastore_json:ejson()) -> datastore_json:ejson().
process_query_result(QueryResult) ->
    {Rows} = QueryResult,
    Rows2 = lists:map(fun(Row) ->
        {<<"value">>, Value} = lists:keyfind(<<"value">>, 1, Row),
        Value2 = process_value(Value),
        lists:keyreplace(<<"value">>, 1, Value2, Row)
    end, Rows),
    Rows2.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Processes query values from one row.
%% FileUuid is replaced with cdmi objectid.
%% @end
%%-------------------------------------------------------------------
-spec process_value(file_meta:uuid() | [term()]) ->
    cdmi_id:objectid() | [term()].
process_value([Uuid | RestValues]) ->
    ObjectId = process_value(Uuid),
    [ObjectId | RestValues];
process_value(Uuid) ->
    try
        Guid = fslogic_uuid:uuid_to_guid(Uuid),
        cdmi_id:guid_to_objectid(Guid)
    catch
        Error:Reason ->
            ?error_stacktrace("Processing result of index query failed due to ~p:~p", [Error, Reason]),
            {error, index_function}
    end.
