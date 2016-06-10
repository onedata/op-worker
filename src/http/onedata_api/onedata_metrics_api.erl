%%%-------------------------------------------------------------------
%%% @author Tomasz Lichoń
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Public api for metrics, available in protocol plugins.
%%% @end
%%%-------------------------------------------------------------------
-module(onedata_metrics_api).

-type gzip() :: binary().
-type subject_type() :: provider | space | user.
-type subject_id() :: binary().
-type metric_type() :: storage_quota | storage_used | data_access_kbs |
block_access_iops | block_access_latency | remote_transfer_kbs |
connected_users | remote_access_kbs | metada_access_ops.
-type step() :: '5m' | '1h' | '1d' | '1m' | '1y'.
-type format() :: 'json' | 'xml'.

-export_type([gzip/0, subject_type/0, subject_id/0, metric_type/0, step/0]).

-export([get_metric/7]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get RRD database for given metric.
%% @end
%%--------------------------------------------------------------------
-spec get_metric(onedata_auth_api:auth(), subject_type(), subject_id(),
    metric_type(), step(), oneprovider:id(), format()) -> {ok, binary()}.
get_metric(_Auth, _SubjectType, _SubjectId, _MetricType, _Step, _Format, _ProviderId) ->
    {ok, <<"json_data">>}. %todo