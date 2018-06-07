%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for listing and managing transfers.
%%% @end
%%%--------------------------------------------------------------------
-module(transfers).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").

-define(DEFAULT_LIMIT, 100).


%% API
-export([terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2]).

%% resource functions
-export([list_transfers/2]).

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
        {<<"application/json">>, list_transfers}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/space/:sid/transfers'
%% @doc Returns the list of all transfer IDs.
%%
%% HTTP method: GET
%%
%% @param state Specifies the state of transfers to list. The default is "ongoing".\n
%% @param limit Allows to limit the number of returned transfers.\n
%%--------------------------------------------------------------------
-spec list_transfers(req(), maps:map()) -> {term(), req(), maps:map()}.
list_transfers(Req, State) ->
    {State2, Req2} = validator:parse_space_id(Req, State),
    {State3, Req3} = validator:parse_transfer_state(Req2, State2),
    {State4, Req4} = validator:parse_dir_limit(Req3, State3),
    {State5, Req5} = validator:parse_page_token(Req4, State4),

    #{
        space_id := SpaceId, transfer_state := TransferState,
        limit := LimitOrUndef, page_token := PageToken
    } = State5,

    Limit = utils:ensure_defined(LimitOrUndef, undefined, ?DEFAULT_LIMIT),

    {StartId, Offset} = case PageToken of
        <<"null">> ->
            {undefined, 0};
        _ ->
            % Start after the page token (link key from last listing) if it is given
            {PageToken, 1}
    end,

    {ok, Transfers} = case TransferState of
        <<"waiting">> ->
            transfer:list_waiting_transfers(SpaceId, StartId, Offset, Limit);
        <<"ongoing">> ->
            transfer:list_ongoing_transfers(SpaceId, StartId, Offset, Limit);
        <<"ended">> ->
            transfer:list_ended_transfers(SpaceId, StartId, Offset, Limit)
    end,

    NextPageToken = case length(Transfers) of
        Limit ->
            {ok, LinkKey} = transfer:get_link_key(lists:last(Transfers)),
            #{<<"nextPageToken">> => LinkKey};
        _ ->
            #{}
    end,

    Result = maps:merge(#{<<"transfers">> => Transfers}, NextPageToken),

    Response = json_utils:encode(Result),
    {Response, Req5, State5}.
