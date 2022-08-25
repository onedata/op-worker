%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of automation store schema related record
%%% used in CT tests.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_TEST_STORE_HRL).
-define(ATM_TEST_STORE_HRL, 1).


-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/time_series/common.hrl").


-define(EXP_WINDOW(__METRIC_CONFIG, __TIMESTAMP, __VALUE), #{
    <<"value">> => __VALUE,
    <<"timestamp">> => __TIMESTAMP - __TIMESTAMP rem __METRIC_CONFIG#metric_config.resolution
}).

-define(MAX_FILE_SIZE_TS_NAME, <<"max_file_size">>).
-define(MAX_FILE_SIZE_METRIC_NAME, ?MAX_FILE_SIZE_TS_NAME).
-define(MAX_FILE_SIZE_METRIC_CONFIG, #metric_config{
    resolution = ?MONTH_RESOLUTION,
    retention = 1,
    aggregator = max
}).
-define(MAX_FILE_SIZE_METRIC_WINDOW(__TIMESTAMP, __VALUE), ?EXP_WINDOW(
    ?MAX_FILE_SIZE_METRIC_CONFIG, __TIMESTAMP, __VALUE
)).

-define(MAX_FILE_SIZE_TS_SCHEMA, #time_series_schema{
    name_generator_type = exact,
    name_generator = ?MAX_FILE_SIZE_TS_NAME,
    unit = bytes,
    metrics = #{?MAX_FILE_SIZE_TS_NAME => ?MAX_FILE_SIZE_METRIC_CONFIG}
}).

-define(MINUTE_METRIC_NAME, <<"minute">>).
-define(MINUTE_METRIC_CONFIG, #metric_config{
    resolution = ?MINUTE_RESOLUTION,
    retention = 120,
    aggregator = sum
}).
-define(EXP_MINUTE_METRIC_WINDOW(__TIMESTAMP, __VALUE), ?EXP_WINDOW(
    ?MINUTE_METRIC_CONFIG, __TIMESTAMP, __VALUE
)).

-define(HOUR_METRIC_NAME, <<"hour">>).
-define(HOUR_METRIC_CONFIG, #metric_config{
    resolution = ?HOUR_RESOLUTION,
    retention = 48,
    aggregator = sum
}).
-define(EXP_HOUR_METRIC_WINDOW(__TIMESTAMP, __VALUE), ?EXP_WINDOW(
    ?HOUR_METRIC_CONFIG, __TIMESTAMP, __VALUE
)).

-define(DAY_METRIC_NAME, <<"day">>).
-define(DAY_METRIC_CONFIG, #metric_config{
    resolution = ?DAY_RESOLUTION,
    retention = 60,
    aggregator = sum
}).
-define(EXP_DAY_METRIC_WINDOW(__TIMESTAMP, __VALUE), ?EXP_WINDOW(
    ?DAY_METRIC_CONFIG, __TIMESTAMP, __VALUE
)).

-define(COUNT_TS_NAME_GENERATOR, <<"count_">>).
-define(COUNT_TS_SCHEMA, #time_series_schema{
    name_generator_type = add_prefix,
    name_generator = ?COUNT_TS_NAME_GENERATOR,
    unit = counts_per_sec,
    metrics = #{
        ?MINUTE_METRIC_NAME => ?MINUTE_METRIC_CONFIG,
        ?HOUR_METRIC_NAME => ?HOUR_METRIC_CONFIG,
        ?DAY_METRIC_NAME => ?DAY_METRIC_CONFIG
    }
}).


-endif.
