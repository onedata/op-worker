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
-module(onedata_metrics).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("http/rest/rest.hrl").
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
-export([terminate/3, allowed_methods/2,
    is_authorized/2, content_types_provided/2]).

%% resource functions
-export([get_metric/2]).

-type subject_type() :: provider | space | user | undefined.
-type subject_id() :: binary() | undefined.
-type metric_type() ::
    storage_quota | storage_used | data_access_kbs |
    block_access_iops | block_access_latency | remote_transfer_kbs |
    connected_users | remote_access_kbs | metada_access_ops.
-type step() :: '5m' | '1h' | '1d' | '1m'.
-type format() :: 'json' | 'xml'.

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
    rest_auth:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) ->
    {[{atom() | binary(), atom()}], req(), maps:map()}.
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
-spec get_metric(req(), maps:map()) -> {term(), req(), maps:map()}.
get_metric(Req, State) ->
    {State2, Req2} = validator:parse_space_id(Req, State),
    {State3, Req3} = validator:parse_user_id(Req2, State2),
    ParsedQs = cowboy_req:parse_qs(Req3),
    Metric = proplists:get_value(<<"metric">>, ParsedQs),
    Step = proplists:get_value(<<"step">>, ParsedQs),

    #{
        auth := SessionId,
        subject_type := SubjectType,
        secondary_subject_type := SecondarySubjectType,
        space_id := SpaceId,
        user_id := UId
    } = State3,

    space_membership:check_with_auth(SessionId, SpaceId),
    case space_logic:get_provider_ids(SessionId, SpaceId) of
        {ok, Providers} ->
            Json =
                lists:map(fun(ProviderId) ->
                    case get_metric_internal(SessionId, SubjectType, SpaceId, SecondarySubjectType, UId,
                        transform_metric(Metric, SubjectType, SecondarySubjectType), transform_step(Step), ProviderId, json)
                    of
                        {ok, Data} ->
                            DecodedJson = json_utils:decode(Data),
                            #{
                                <<"providerId">> => ProviderId,
                                <<"rrd">> => DecodedJson
                            };
                        {error, ?ENOENT} ->
                            #{
                                <<"providerId">> => ProviderId,
                                <<"rrd">> => <<>>
                            }
                    end
                end, Providers),
            Response = json_utils:encode(Json),
            {Response, Req3, State3};
        {error, not_found} ->
            throw(?ERROR_NOT_FOUND_REST)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Transform metric type to atom and validate it.
%% @end
%%--------------------------------------------------------------------
-spec transform_metric(binary() | undefined, subject_type(), subject_type()) -> metric_type().
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
-spec transform_step(binary() | undefined) -> step().
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

%%--------------------------------------------------------------------
%% @doc
%% Get RRD database for given metric.
%% @end
%%--------------------------------------------------------------------
-spec get_metric_internal(rest_auth:auth(), subject_type(), subject_id(),
    subject_type(), subject_id(), metric_type(), step(), oneprovider:id(),
    format()) -> {ok, binary()} | {error, term()}.
get_metric_internal(_Auth, SubjectType, SubjectId, undefined, _,
    MetricType, Step, ProviderId, Format) ->
    MonitoringId = #monitoring_id{
        main_subject_type = SubjectType,
        main_subject_id = SubjectId,
        metric_type = MetricType,
        provider_id = ProviderId
    },
    worker_proxy:call(monitoring_worker, {export, MonitoringId, Step, Format});
get_metric_internal(_Auth, SubjectType, SubjectId, SecondarySubjectType, SecondarySubjectId,
    MetricType, Step, ProviderId, Format) ->
    MonitoringId = #monitoring_id{
        main_subject_type = SubjectType,
        main_subject_id = SubjectId,
        metric_type = MetricType,
        secondary_subject_id = SecondarySubjectId,
        secondary_subject_type = SecondarySubjectType,
        provider_id = ProviderId
    },
    worker_proxy:call(monitoring_worker, {export, MonitoringId, Step, Format}).
