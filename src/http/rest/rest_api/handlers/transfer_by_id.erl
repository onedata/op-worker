%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for getting details and managing transfers.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_by_id).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").


%% API
-export([terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2, delete_resource/2, content_types_accepted/2]).

%% resource functions
-export([get_transfer/2, restart_transfer/2]).

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
    {[<<"GET">>, <<"DELETE">>, <<"PATCH">>], Req, State}.

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
        {<<"application/json">>, get_transfer}
    ], Req, State}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/transfers/{tid}'
%% @doc Cancels a scheduled or active transfer. Returns 400 in case the transfer
%% is already completed, canceled or failed.\n
%%
%% HTTP method: DELETE
%%
%% @param tid Transfer ID.
%%--------------------------------------------------------------------
-spec delete_resource(req(), maps:map()) -> {term(), req(), maps:map()}.
delete_resource(Req, State) ->
    {State2, Req2} = validator:parse_id(Req, State),

    #{id := Id} = State2,

    ok = transfer:cancel(Id),
    {true, Req2, State2}.


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) ->
    {[{atom() | binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {'*', restart_transfer}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/transfers/{tid}'
%% @doc Returns status of specific transfer. In case the transfer has been
%% scheduled for entire folder, the result is a list of transfer statuses for
%% each item in the folder.
%%
%% HTTP method: GET
%%
%% @param tid Transfer ID.
%% @end
%%--------------------------------------------------------------------
-spec get_transfer(req(), maps:map()) -> {term(), req(), maps:map()}.
get_transfer(Req, State) ->
    {State2, Req2} = validator:parse_id(Req, State),

    #{id := Id} = State2,

    Transfer = transfer:get_info(Id),
    Response = json_utils:encode(Transfer),
    {Response, Req2, State2}.

%%-------------------------------------------------------------------
%% '/api/v3/oneprovider/transfers/{tid}'
%% @doc Restarts transfer with given tid.
%%
%% HTTP method: PATCH
%%
%% @param tid Transfer ID.
%%-------------------------------------------------------------------
-spec restart_transfer(req(), maps:map()) -> {term(), req(), maps:map()}.
restart_transfer(Req, State) ->
    {State2, Req2} = validator:parse_id(Req, State),
    #{id := Id} = State2,
    case transfer:restart(Id) of
        {ok, _} ->
            ok;
        {error, not_target_provider} ->
            throw(?ERROR_NOT_TARGET_PROVIDER);
        {error, not_source_provider} ->
            throw(?ERROR_NOT_SOURCE_PROVIDER);
        {error, {not_found, transfer}} ->
            throw(?ERROR_TRANSFER_NOT_FOUND)
    end,
    {true, Req2, State2}.
