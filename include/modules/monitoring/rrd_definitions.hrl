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

-define(_5_MIN_COUNT_IN_5_MIN, 1).
-define(_5_MIN_COUNT_IN_1_H, 12).
-define(_5_MIN_COUNT_IN_1_DAY, 288).
-define(_5_MIN_COUNT_IN_31_DAYS, 8928).

-define(_1_H_COUNT_IN_1_WEEK, 168).
-define(_1_DAY_COUNT_IN_31_DAYS, 31).
-define(_31_DAYS_COUNT_IN_1_YEAR, 12).

-define(DEFAULT_STEP_IN_SECONDS, 300). %% five minutes
-define(DEFAULT_HEARTBEAT_IN_SECONDS, 600). %% ten minutes

-define(MAKESPAN_FOR_STEP, #{
    '5m' => "1d",
    '1h' => "7d",
    '1d' => "31d",
    '1m' => "1y"
}).

-record(rrd_definition, {
    datastore = [] :: rrd_utils:datastore(),
    rras_map = #{
        '5m' => {'LAST', 0.5, ?_5_MIN_COUNT_IN_5_MIN,   ?_5_MIN_COUNT_IN_1_DAY},
        '1h' => {'LAST', 0.5, ?_5_MIN_COUNT_IN_1_H,     ?_1_H_COUNT_IN_1_WEEK},
        '1d' => {'LAST', 0.5, ?_5_MIN_COUNT_IN_1_DAY,   ?_1_DAY_COUNT_IN_31_DAYS},
        '1m' => {'LAST', 0.5, ?_5_MIN_COUNT_IN_31_DAYS, ?_31_DAYS_COUNT_IN_1_YEAR}
    } :: #{atom() => rrd_utils:rra()},
    options = [{step, ?DEFAULT_STEP_IN_SECONDS}] :: rrd_utils:options()
}).

-define(STORAGE_USED_PER_SPACE_RRD, #rrd_definition{
    datastore = {"storage_used", 'GAUGE',
        [?DEFAULT_HEARTBEAT_IN_SECONDS, undefined, undefined]}
}).

-define(STORAGE_QUOTA_PER_SPACE_RRD, #rrd_definition{
    datastore = {"storage_quota", 'GAUGE',
        [?DEFAULT_HEARTBEAT_IN_SECONDS, undefined, undefined]}
}).

-endif.
