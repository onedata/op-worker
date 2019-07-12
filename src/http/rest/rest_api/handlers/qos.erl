%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for adding new qos for file and for listing file's qos.
%%% @end
%%%--------------------------------------------------------------------
-module(qos).
-author("Michal Cwiertnia").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").

%% API
-export([
    terminate/3, allowed_methods/2, is_authorized/2,
    content_types_accepted/2, content_types_provided/2
]).

%% resource functions
-export([add_qos/2, list_qos/2]).

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
    {[<<"POST">>,  <<"GET">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | halt, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) -> {[{binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/json">>, add_qos}
    ], Req, State}.


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) -> {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, list_qos}
    ], Req, State}.


%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/qos/{path}'
%% @doc Returns following information about file qos:
%%      - list containing Id of all qos item documents attached to this file
%%      - information indicating if qos is fulfilled
%%      - mapping storage id to list containing Id of all qos item documents that
%%        requires file to be on this storage
%%
%% HTTP method: GET
%%
%% @param path Path to the file.
%%--------------------------------------------------------------------
-spec list_qos(req(), maps:map()) -> {term(), req(), maps:map()}.
list_qos(Req, State = #{resource_type := id}) ->
    {StateWithId, ReqWithId} = validator:parse_objectid(Req, State),
    list_qos_internal(ReqWithId, StateWithId);
list_qos(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    list_qos_internal(ReqWithPath, StateWithPath).

%%--------------------------------------------------------------------
%% @doc Internal version of list_qos/2
%%--------------------------------------------------------------------
-spec list_qos_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
list_qos_internal(Req, State) ->
    #{auth := Auth} = State,
    FileKey = get_file(State),

    {ok, {QosList, TargetStorages}} = logical_file_manager:get_file_qos(Auth, FileKey),

    Response = json_utils:encode(#{
        <<"fulfilled">> => lfm_qos:check_qos_fulfilled(Auth, QosList),
        <<"targetStorages">> => TargetStorages,
        <<"qosList">> => QosList
    }),
    FinalReq = cowboy_req:reply(?HTTP_200_OK, #{}, Response, Req),
    {stop, FinalReq, State}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/qos/{path_or_id}'
%% @doc Adds new qos for file. Returns Id of the qos item document.
%%
%% HTTP method: POST
%%
%% @param path Path to the file.
%% @param expression Qos expression.
%%--------------------------------------------------------------------
-spec add_qos(req(), maps:map()) -> {term(), req(), maps:map()}.
add_qos(Req, State = #{resource_type := id}) ->
    {StateWithId, ReqWithId} = validator:parse_objectid(Req, State),
    add_qos_internal(ReqWithId, StateWithId);
add_qos(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    add_qos_internal(ReqWithPath, StateWithPath).

%%--------------------------------------------------------------------
%% @doc Internal version of add_qos/2
%%--------------------------------------------------------------------
-spec add_qos_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
add_qos_internal(Req, State) ->
    {StateWithQos, ReqWithQos} = validator:parse_add_qos_body(Req, State),

    #{
        auth := Auth,
        qos_expression := QosExpression,
        replicas_num := ReplicasNum
    } = StateWithQos,

    FileKey = get_file(StateWithQos),
    {ok, QosId} = logical_file_manager:add_qos(Auth, FileKey, QosExpression, ReplicasNum),

    Response = json_utils:encode(#{<<"qosId">> => QosId}),
    FinalReq = cowboy_req:reply(?HTTP_200_OK, #{}, Response, ReqWithQos),
    {stop, FinalReq, StateWithQos}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Get file entry from state
%% @end
%%--------------------------------------------------------------------
-spec get_file(maps:map()) -> {guid, binary()} | {path, binary()}.
get_file(#{id := Id}) ->
    {guid, Id};
get_file(#{path := Path}) ->
    {path, Path}.
