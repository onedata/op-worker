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
-module(metrics).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").

-define(AVAILABLE_SPACE_METRICS, [storage_quota, storage_used, data_access,
    block_access, connected_users, remote_transfer
]).
-define(AVAILABLE_USER_METRICS, [storage_used, data_access,
    block_access, remote_transfer
]).

-define(DEFAULT_STEP, '5m').
-define(AVAILABLE_STEPS, ['5m', '1h', '1d', '1m']).

%% API
-export([rest_init/2, terminate/3, allowed_methods/2,
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
-spec content_types_provided(req(), #{}) ->
    {[{atom() | binary(), atom()}], req(), #{}}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_metric}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Handles GET
%%--------------------------------------------------------------------
-spec get_metric(req(), #{}) -> {term(), req(), #{}}.
get_metric(Req, State) ->
    {State2, Req2} = validator:parse_space_id(Req, State),
    {State3, Req3} = validator:parse_user_id(Req2, State2),
    {Metric, Req4} = cowboy_req:qs_val(<<"metric">>, Req3),
    {Step, Req5} = cowboy_req:qs_val(<<"step">>, Req4),

    #{auth := Auth, subject_type := SubjectType, secondary_subject_type := SecondarySubjectType, space_id := SpaceId, user_id := UId} = State3,

    space_membership:check_with_auth(Auth, SpaceId),
    case space_info:get_or_fetch(Auth, SpaceId) of
        {ok, #document{value = #space_info{providers = Providers}}} ->
            Json =
                lists:map(fun(ProviderId) ->
                    case onedata_metrics_api:get_metric(Auth, SubjectType, SpaceId, SecondarySubjectType, UId,
                        transform_metric(Metric, SubjectType, SecondarySubjectType), transform_step(Step), ProviderId, json)
                    of
                        {ok, Data} ->
                            DecodedJson = json_utils:decode(Data),
                            [
                                {<<"providerId">>, ProviderId},
                                {<<"rrd">>, DecodedJson}
                            ];
                        {error, ?ENOENT} ->
                            [
                                {<<"providerId">>, ProviderId},
                                {<<"rrd">>, <<>>}
                            ]
                    end
                end, Providers),
            Response = json_utils:encode(Json),
            {Response, Req5, State3};
        {error, {not_found, _}} ->
            throw(?ERROR_NOT_FOUND)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Transform metric type to atom and validate it.
%% @end
%%--------------------------------------------------------------------
-spec transform_metric(binary() | undefined, onedata_metrics_api:subject_type(), onedata_metrics_api:subject_type()) -> onedata_metrics_api:metric_type().
transform_metric(undefined, _, _) ->
    throw(?ERROR_INVALID_METRIC);
transform_metric(MetricType, space, undefined) ->
    MetricTypeAtom = binary_to_atom(MetricType, utf8),
    case lists:member(MetricTypeAtom, ?AVAILABLE_SPACE_METRICS) of
        true ->
            MetricTypeAtom;
        false ->
            throw(?ERROR_INVALID_METRIC)
    end;
transform_metric(MetricType, space, user) ->
    MetricTypeAtom = binary_to_atom(MetricType, utf8),
    case lists:member(MetricTypeAtom, ?AVAILABLE_USER_METRICS) of
        true ->
            MetricTypeAtom;
        false ->
            throw(?ERROR_INVALID_METRIC)
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
            throw(?ERROR_INVALID_STEP)
    end.