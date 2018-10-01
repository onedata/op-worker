%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for listing indexes.
%%% @end
%%%--------------------------------------------------------------------
-module(index_collection).
-author("Tomasz Lichon").

-include("http/http_common.hrl").
-include("http/rest/rest_api/rest_errors.hrl").

-define(DEFAULT_LIMIT, 100).

%% API
-export([
    terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2
]).

%% resource functions
-export([list_indexes/2]).

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
        {<<"application/json">>, list_indexes}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/spaces/{sid}/indexes'
%% @doc This method returns the list of indexes defined within space.
%%
%% HTTP method: GET
%%
%% @param sid Id of the space to query.
%%--------------------------------------------------------------------
-spec list_indexes(req(), maps:map()) -> {term(), req(), maps:map()}.
list_indexes(Req, State) ->
    {State1, Req1} = validator:parse_space_id(Req, State),
    {State2, Req2} = validator:parse_dir_limit(Req1, State1),
    {State3, Req3} = validator:parse_page_token(Req2, State2),

    #{space_id := SpaceId, limit := LimitOrUndef, page_token := PageToken} = State1,

    Limit = utils:ensure_defined(LimitOrUndef, undefined, ?DEFAULT_LIMIT),

    {StartId, Offset} = case PageToken of
        <<"null">> ->
            {undefined, 0};
        _ ->
            % Start after the page token (link key from last listing) if it is given
            {PageToken, 1}
    end,

    {ok, Indexes} = index:list(SpaceId, StartId, Offset, Limit),

    NextPageToken = case length(Indexes) of
        Limit ->
            #{<<"nextPageToken">> => lists:last(Indexes)};
        _ ->
            #{}
    end,

    Result = maps:merge(#{<<"indexes">> => Indexes}, NextPageToken),

    Response = json_utils:encode(Result),
    {Response, Req3, State3}.
