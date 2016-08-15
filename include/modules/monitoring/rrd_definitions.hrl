%%%--------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% RRD monitoring databases definitions.
%%% @end
%%%--------------------------------------------------------------------
-ifndef(RRD_DEFINITIONS_HRL).
-define(RRD_DEFINITIONS_HRL, 1).

-define(RRDTOOL_POOL_NAME, rrdtool_pool_name).
-define(RRDTOOL_POOL_TRANSACTION_TIMEOUT, timer:seconds(60)).

-define(MONITORING_SESSION_ID, <<"monitoring_session">>).
-define(MONITORING_SUB_ID, binary:decode_unsigned(
    crypto:hash(md5, <<"monitoring">>)) rem 16#FFFFFFFFFFFF).

-define(_5_MIN_COUNT_IN_5_MIN, 1).
-define(_5_MIN_COUNT_IN_1_H, 12).
-define(_5_MIN_COUNT_IN_1_DAY, 288).
-define(_5_MIN_COUNT_IN_31_DAYS, 8928).

-define(_1_H_COUNT_IN_1_WEEK, 168).
-define(_1_DAY_COUNT_IN_31_DAYS, 31).
-define(_31_DAYS_COUNT_IN_1_YEAR, 12).

-define(STEP_IN_SECONDS, 300). %% five minutes
-define(HEARTBEAT_IN_SECONDS, 600). %% ten minutes
-define(DATASOURCE_PARAMS, [?HEARTBEAT_IN_SECONDS, undefined, undefined]).

-define(MAKESPAN_FOR_STEP, #{
    '5m' => "1d",
    '1h' => "7d",
    '1d' => "31d",
    '1m' => "1y"
}).

-define(LAST_RRAS, #{
    '5m' => {'LAST', 0.5, ?_5_MIN_COUNT_IN_5_MIN,   ?_5_MIN_COUNT_IN_1_DAY},
    '1h' => {'LAST', 0.5, ?_5_MIN_COUNT_IN_1_H,     ?_1_H_COUNT_IN_1_WEEK},
    '1d' => {'LAST', 0.5, ?_5_MIN_COUNT_IN_1_DAY,   ?_1_DAY_COUNT_IN_31_DAYS},
    '1m' => {'LAST', 0.5, ?_5_MIN_COUNT_IN_31_DAYS, ?_31_DAYS_COUNT_IN_1_YEAR}
}).

-define(AVERAGE_RRAS, #{
    '5m' => {'AVERAGE', 0.5, ?_5_MIN_COUNT_IN_5_MIN,   ?_5_MIN_COUNT_IN_1_DAY},
    '1h' => {'AVERAGE', 0.5, ?_5_MIN_COUNT_IN_1_H,     ?_1_H_COUNT_IN_1_WEEK},
    '1d' => {'AVERAGE', 0.5, ?_5_MIN_COUNT_IN_1_DAY,   ?_1_DAY_COUNT_IN_31_DAYS},
    '1m' => {'AVERAGE', 0.5, ?_5_MIN_COUNT_IN_31_DAYS, ?_31_DAYS_COUNT_IN_1_YEAR}
}).

-record(rrd_definition, {
    datastores = [] :: [rrd_utils:datastore()],
    rras_map = #{} :: #{atom() => rrd_utils:rra()},
    options = [{step, ?STEP_IN_SECONDS}] :: rrd_utils:options(),
    unit = "" :: string()
}).

-define(STORAGE_USED_RRD, #rrd_definition{
    datastores = [{"storage_used", 'GAUGE', ?DATASOURCE_PARAMS}],
    rras_map = ?LAST_RRAS,
    unit = "bytes"
}).

-define(STORAGE_QUOTA_RRD, #rrd_definition{
    datastores = [{"storage_quota", 'GAUGE', ?DATASOURCE_PARAMS}],
    rras_map = ?LAST_RRAS,
    unit = "bytes"
}).

-define(CONNECTED_USERS_RRD, #rrd_definition{
    datastores = [{"connected_users", 'GAUGE', ?DATASOURCE_PARAMS}],
    rras_map = ?LAST_RRAS,
    unit = "users_count"
}).

-define(DATA_ACCESS_RRD, #rrd_definition{
    datastores = [{"data_access_read", 'ABSOLUTE', ?DATASOURCE_PARAMS},
        {"data_access_write", 'ABSOLUTE', ?DATASOURCE_PARAMS}],
    rras_map = ?AVERAGE_RRAS,
    unit = "bytes/s"
}).

-define(BLOCK_ACCESS_IOPS_RRD, #rrd_definition{
    datastores = [{"block_access_read", 'ABSOLUTE', ?DATASOURCE_PARAMS},
        {"block_access_write", 'ABSOLUTE', ?DATASOURCE_PARAMS}],
    rras_map = ?AVERAGE_RRAS,
    unit = "iops/s"
}).

-define(REMOTE_TRANSFER_RRD, #rrd_definition{
    datastores = [{"remote_transfer_in", 'ABSOLUTE', ?DATASOURCE_PARAMS}],
    rras_map = ?AVERAGE_RRAS,
    unit = "bits/s"
}).

-endif.
