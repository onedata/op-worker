%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler serving metrics.
%%% @end
%%%--------------------------------------------------------------------
-module(metrics_handler).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("http/rest/http_status.hrl").

-define(AVAILABLE_METRICS, [storage_quota, storage_used, data_access_kbs,
    block_access_iops, block_access_latency, remote_transfer_kbs,
    connected_users, remote_access_kbs, metada_access_ops]).

-define(DEFAULT_STEP, '5m').
-define(AVAILABLE_STEPS, ['5m', '1h', '1d', '1m', '1y']).

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, malformed_request/2,
    is_authorized/2, content_types_provided/2]).

%% resource functions
-export([get_metric/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
rest_init(Req, Map) ->
    {ok, Req, Map}.

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
%% @doc @equiv pre_handler:malformed_request/2
%%--------------------------------------------------------------------
-spec malformed_request(req(), #{}) -> {boolean(), req(), #{}}.
malformed_request(Req, State) ->
    rest_arg_parser:malformed_metrics_request(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), #{}) -> {true | {false, binary()} | halt, req(), #{}}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), #{}) ->
    {[{atom() | binary(), atom()}], req(), #{}}.
content_types_provided(Req, State) ->
    {[
        {<<"application/x-gzip">>, get_metric}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Handles GET
%%--------------------------------------------------------------------
-spec get_metric(req(), #{}) -> {term(), req(), #{}}.
get_metric(Req, #{auth := Auth, subject_type := Type, id:= Id} = State) ->
    {Metric, Req2} = cowboy_req:qs_val(<<"metric">>, Req),
    {Step, Req2} = cowboy_req:qs_val(<<"step">>, Req),
    {ok, Data} = onedata_metrics_api:get_metric(Auth, Type, Id,
        transform_metric(Metric), transform_step(Step)),
    {Data, Req2, State}.

%%--------------------------------------------------------------------
%% @doc
%% Transform metric type to atom and validate it.
%% @end
%%--------------------------------------------------------------------
-spec transform_metric(binary() | undefined) -> onedata_metrics_api:metric_type().
transform_metric(undefined) ->
    throw(?BAD_REQUEST); %todo error
transform_metric(MetricType) ->
    MetricTypeAtom = binary_to_atom(MetricType, utf8),
    case lists:member(MetricTypeAtom, ?AVAILABLE_METRICS) of
        true ->
            MetricTypeAtom;
        false ->
            throw(?BAD_REQUEST) %todo error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Transform step to atom and validate it.
%% @end
%%--------------------------------------------------------------------
-spec transform_step(binary() | undefined) -> onedata_metrics_api:step().
transform_step(undefined) ->
    ?DEFAULT_STEP;
transform_step(Step) ->
    StepAtom = binary_to_atom(Step, utf8),
    case lists:member(StepAtom, ?AVAILABLE_STEPS) of
        true ->
            StepAtom;
        false ->
            throw(?BAD_REQUEST) %todo error
    end.