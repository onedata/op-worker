%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations corresponding to op_metrics model.
%%% @end
%%%-------------------------------------------------------------------
-module(op_metrics).
-author("Bartosz Walkowicz").

-behaviour(op_logic_behaviour).

-include("op_logic.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

-export([op_logic_plugin/0]).
-export([operation_supported/3]).
-export([create/1, get/2, update/1, delete/1]).
-export([authorize/2, data_signature/1]).

-type subject_type() :: space | user.
-type subject_id() :: undefined | binary().
-type metric_type() ::
    storage_quota | storage_used |
    data_access | block_access |
    remote_transfer |connected_users.
-type step() :: '5m' | '1h' | '1d' | '1m'.
-type format() :: 'json'.

-define(DEFAULT_STEP, <<"5m">>).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns the op logic plugin module that handles model logic.
%% @end
%%--------------------------------------------------------------------
op_logic_plugin() ->
    op_metrics.


%%--------------------------------------------------------------------
%% @doc
%% Determines if given operation is supported based on operation, aspect and
%% scope (entity type is known based on the plugin itself).
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), op_logic:aspect(),
    op_logic:scope()) -> boolean().
operation_supported(get, space, private) -> true;
operation_supported(get, {user, _}, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% Creates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a resource (aspect of entity) based on op logic request and
%% prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{client = Cl, data = Data, gri = #gri{id = SpaceId, aspect = space}}, _) ->
    Metric = binary_to_atom(maps:get(<<"metric">>, Data), utf8),
    Step = binary_to_atom(maps:get(<<"step">>, Data, ?DEFAULT_STEP), utf8),
    get_metric(
        Cl#client.id, SpaceId,
        space, SpaceId,
        undefined, undefined,
        Metric, Step
    );

get(#op_req{client = Cl, data = Data, gri = #gri{id = SpaceId, aspect = {user, UserId}}}, _) ->
    Metric = binary_to_atom(maps:get(<<"metric">>, Data), utf8),
    Step = binary_to_atom(maps:get(<<"step">>, Data, ?DEFAULT_STEP), utf8),
    get_metric(
        Cl#client.id, SpaceId,
        space, SpaceId,
        user, UserId,
        Metric, Step
    );

get(_, _) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% Updates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% Deletes a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% Determines if requesting client is authorized to perform given operation,
%% based on op logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), entity_logic:entity()) -> boolean().
authorize(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = space}} = Req, _) ->
    op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId);

authorize(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = {user, _}}} = Req, _) ->
    op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId);

authorize(_, _) ->
    false.


%%--------------------------------------------------------------------
%% @doc
%% Returns data signature for given request.
%% Returns a map with 'required', 'optional' and 'at_least_one' keys.
%% Under each of them, there is a map:
%%      Key => {type_constraint, value_constraint}
%% Which means how value of given Key should be validated.
%% @end
%%--------------------------------------------------------------------
-spec data_signature(op_logic:req()) -> op_validator:data_signature().
data_signature(#op_req{operation = get, gri = #gri{aspect = space}}) -> #{
    required => #{
        <<"metric">> => {binary, [
            <<"storage_quota">>,
            <<"storage_used">>,
            <<"data_access">>,
            <<"block_access">>,
            <<"connected_users">>,
            <<"remote_transfer">>
        ]}
    },
    optional => #{
        <<"step">> => {binary, [<<"5m">>, <<"1h">>, <<"1d">>, <<"1m">>]}
    }
};

data_signature(#op_req{operation = get, gri = #gri{aspect = {user, _}}}) -> #{
    required => #{
        <<"metric">> => {binary, [
            <<"storage_used">>,
            <<"data_access">>,
            <<"block_access">>,
            <<"remote_transfer">>
        ]}
    },
    optional => #{
        <<"step">> => {binary, [<<"5m">>, <<"1h">>, <<"1d">>, <<"1m">>]}
    }
};

data_signature(_) -> #{}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_metric(session:id(), od_space:id(), subject_type(), subject_id(),
    subject_type(), subject_id(), metric_type(), step()) ->
    {ok, [maps:map()]} | {error, term()}.
get_metric(
    SessionId, SpaceId,
    SubjectType, SubjectId, SecondarySubjectType, SecondarySubjectId,
    Metric, Step
) ->
    case space_logic:get_provider_ids(SessionId, SpaceId) of
        {ok, Providers} ->
            Json = lists:map(fun(ProviderId) ->
                case get_metric_internal(
                    SessionId,
                    SubjectType, SubjectId,
                    SecondarySubjectType, SecondarySubjectId,
                    Metric, Step,
                    ProviderId,
                    json
                ) of
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
            {ok, Json};
        {error, not_found} ->
            throw(?ERROR_NOT_FOUND)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get RRD database for given metric.
%% @end
%%--------------------------------------------------------------------
-spec get_metric_internal(rest_auth:auth(), subject_type(), subject_id(),
    subject_type(), subject_id(), metric_type(), step(), oneprovider:id(),
    format()) -> {ok, binary()} | {error, term()}.
get_metric_internal(
    _Auth, SubjectType, SubjectId, undefined, _,
    MetricType, Step, ProviderId, Format
) ->
    MonitoringId = #monitoring_id{
        main_subject_type = SubjectType,
        main_subject_id = SubjectId,
        metric_type = MetricType,
        provider_id = ProviderId
    },
    worker_proxy:call(monitoring_worker, {export, MonitoringId, Step, Format});
get_metric_internal(
    _Auth, SubjectType, SubjectId, SecondarySubjectType, SecondarySubjectId,
    MetricType, Step, ProviderId, Format
) ->
    MonitoringId = #monitoring_id{
        main_subject_type = SubjectType,
        main_subject_id = SubjectId,
        metric_type = MetricType,
        secondary_subject_id = SecondarySubjectId,
        secondary_subject_type = SecondarySubjectType,
        provider_id = ProviderId
    },
    worker_proxy:call(monitoring_worker, {export, MonitoringId, Step, Format}).
