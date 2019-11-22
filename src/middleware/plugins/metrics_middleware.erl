%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations (create, get, update, delete)
%%% corresponding to metrics/statistic gathering.
%%% @end
%%%-------------------------------------------------------------------
-module(metrics_middleware).
-author("Bartosz Walkowicz").

-behaviour(middleware_plugin).

-include("middleware/middleware.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).

-type metric_type() ::
    storage_quota | storage_used |
    data_access | block_access |
    remote_transfer | connected_users.
-type step() :: '5m' | '1h' | '1d' | '1m'.

-define(DEFAULT_STEP, <<"5m">>).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(middleware:operation(), gri:aspect(),
    middleware:scope()) -> boolean().
operation_supported(get, space, private) -> true;
operation_supported(get, {user, _}, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = get, gri = #gri{aspect = space}}) -> #{
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

data_spec(#op_req{operation = get, gri = #gri{aspect = {user, _}}}) -> #{
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
}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(_) ->
    {ok, {undefined, 1}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?NOBODY}, _) ->
    false;

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = space
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_STATISTICS);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {user, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_STATISTICS).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = space}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = {user, _}}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = Auth, data = Data, gri = #gri{id = SpaceId, aspect = space}}, _) ->
    Metric = binary_to_atom(maps:get(<<"metric">>, Data), utf8),
    Step = binary_to_atom(maps:get(<<"step">>, Data, ?DEFAULT_STEP), utf8),
    get_metric(Auth#auth.session_id, SpaceId, undefined, Metric, Step);

get(#op_req{auth = Auth, data = Data, gri = #gri{id = SpaceId, aspect = {user, UserId}}}, _) ->
    Metric = binary_to_atom(maps:get(<<"metric">>, Data), utf8),
    Step = binary_to_atom(maps:get(<<"step">>, Data, ?DEFAULT_STEP), utf8),
    get_metric(Auth#auth.session_id, SpaceId, UserId, Metric, Step).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_metric(session:id(), od_space:id(), undefined | od_user:id(),
    metric_type(), step()) -> {ok, [map()]} | {error, term()}.
get_metric(SessionId, SpaceId, UserId, Metric, Step) ->
    case space_logic:get_provider_ids(SessionId, SpaceId) of
        {ok, Providers} ->
            Json = lists:map(fun(ProviderId) ->
                case get_metric_internal(SpaceId, UserId, Metric, Step, ProviderId) of
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
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get RRD database for given metric.
%% @end
%%--------------------------------------------------------------------
-spec get_metric_internal(od_space:id(), undefined | od_user:id(),
    metric_type(), step(), oneprovider:id()) -> {ok, binary()} | {error, term()}.
get_metric_internal(SpaceId, undefined, MetricType, Step, ProviderId) ->
    MonitoringId = #monitoring_id{
        main_subject_type = space,
        main_subject_id = SpaceId,
        metric_type = MetricType,
        provider_id = ProviderId
    },
    worker_proxy:call(monitoring_worker, {export, MonitoringId, Step, json});
get_metric_internal(SpaceId, UserId, MetricType, Step, ProviderId) ->
    MonitoringId = #monitoring_id{
        main_subject_type = space,
        main_subject_id = SpaceId,
        metric_type = MetricType,
        secondary_subject_type = user,
        secondary_subject_id = UserId,
        provider_id = ProviderId
    },
    worker_proxy:call(monitoring_worker, {export, MonitoringId, Step, json}).
