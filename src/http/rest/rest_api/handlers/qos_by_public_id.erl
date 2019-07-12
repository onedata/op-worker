%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for getting details of qos and for deleting qos
%%% @end
%%%--------------------------------------------------------------------
-module(qos_by_public_id).
-author("Michal Cwiertnia").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").

%% API
-export([
    terminate/3, allowed_methods/2, is_authorized/2, content_types_provided/2,
    delete_resource/2
]).

%% resource functions
-export([get_qos/2, qos_record_to_map/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), maps:map()) -> ok.
terminate(_, _, #{}) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), maps:map() | {error, term()}) -> {[binary()], req(), maps:map()}.
allowed_methods(Req, State) ->
    {[<<"GET">>, <<"DELETE">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | halt, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/qos-public-id/{id}'
%% @doc Removes qos specified by qos id.
%%
%% HTTP method: DELETE
%% @end
%%--------------------------------------------------------------------
-spec delete_resource(req(), maps:map()) -> {term(), req(), maps:map()}.
delete_resource(Req, State) ->
    {StateWithId, ReqWithId} = validator:parse_qos_id(Req, State),
    #{auth := Auth, qos_id := QosId} = StateWithId,

    ok = logical_file_manager:remove_qos(Auth, QosId),
    {stop, ReqWithId, StateWithId}.


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) -> {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_qos}
    ], Req, State}.


%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/qos-public-id/{id}'
%% @doc Gets details of qos specified by id.
%%
%% HTTP method: GET
%% @end
%%--------------------------------------------------------------------
-spec get_qos(req(), maps:map()) -> {term(), req(), maps:map()}.
get_qos(Req, State) ->
    {StateWithId, ReqWithId} = validator:parse_qos_id(Req, State),
    #{auth := Auth, qos_id := QosId} = StateWithId,

    {ok, Qos} = logical_file_manager:get_qos_details(Auth, QosId),
    QosAsMap = qos_record_to_map(Qos),

    Response = json_utils:encode(
        QosAsMap#{
            <<"qosId">> => QosId,
            <<"fulfilled">> => lfm_qos:check_qos_fulfilled(Auth, QosId)
        }),
    FinalReq = cowboy_req:reply(?HTTP_200_OK, #{}, Response, ReqWithId),
    {stop, FinalReq, StateWithId}.

-spec qos_record_to_map(#qos_item{}) -> maps:map().
qos_record_to_map(Qos) ->
    #{
        <<"qosExpression">> => Qos#qos_item.expression,
        <<"replicasNum">> => Qos#qos_item.replicas_num
    }.
