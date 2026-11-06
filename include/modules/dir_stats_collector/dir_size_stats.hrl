%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros used by dir_size_stats collection.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DIR_SIZE_STATS_HRL).
-define(DIR_SIZE_STATS_HRL, 1).


-include_lib("ctool/include/time_series/common.hrl").


%%%===================================================================
%%% Names of statistics.
%%%===================================================================

-define(PHYSICAL_SIZE_TS_PREFIX_STR, "physical_size_").

-define(REG_FILE_AND_LINK_COUNT, <<"reg_file_and_link_count">>).
-define(DIR_COUNT, <<"dir_count">>).
-define(FILE_ERROR_COUNT, <<"file_error_count">>). % Count of unexpected errors during initialization of reg files stats
-define(DIR_ERROR_COUNT, <<"dir_error_count">>). % Count of unexpected errors during initialization of dir stats

% See dir_size_stats.erl doc for different sizes explanation
-define(LOGICAL_SIZE, <<"logical_size">>).
-define(VIRTUAL_SIZE, <<"virtual_size">>).
-define(PHYSICAL_SIZE(StorageId), <<?PHYSICAL_SIZE_TS_PREFIX_STR, StorageId/binary>>).


%%%===================================================================
%%% Metric ids for each statistic.
%%%===================================================================

-define(MINUTE_METRIC, <<"minute">>).
-define(HOUR_METRIC, <<"hour">>).
-define(DAY_METRIC, <<"day">>).
-define(MONTH_METRIC, <<"month">>).


%%%===================================================================
%%% Misc
%%%===================================================================


-define(DIR_SIZE_STATS_METRICS, #{
    ?MINUTE_METRIC => #metric_config{
        resolution = ?MINUTE_RESOLUTION,
        retention = 720,
        aggregator = last
    },
    ?HOUR_METRIC => #metric_config{
        resolution = ?HOUR_RESOLUTION,
        retention = 1440,
        aggregator = last
    },
    ?DAY_METRIC => #metric_config{
        resolution = ?DAY_RESOLUTION,
        retention = 550,
        aggregator = last
    },
    ?MONTH_METRIC => #metric_config{
        resolution = ?MONTH_RESOLUTION,
        retention = 360,
        aggregator = last
    }
}).


-define(DIR_SIZE_STATS_COLLECTION_SCHEMA, #time_series_collection_schema{time_series_schemas = [
    #time_series_schema{
        name_generator_type = exact,
        name_generator = ?REG_FILE_AND_LINK_COUNT,
        unit = none,
        metrics = ?DIR_SIZE_STATS_METRICS
    },
    #time_series_schema{
        name_generator_type = exact,
        name_generator = ?DIR_COUNT,
        unit = none,
        metrics = ?DIR_SIZE_STATS_METRICS
    },
    #time_series_schema{
        name_generator_type = exact,
        name_generator = ?FILE_ERROR_COUNT,
        unit = none,
        metrics = ?DIR_SIZE_STATS_METRICS
    },
    #time_series_schema{
        name_generator_type = exact,
        name_generator = ?DIR_ERROR_COUNT,
        unit = none,
        metrics = ?DIR_SIZE_STATS_METRICS
    },
    #time_series_schema{
        name_generator_type = exact,
        name_generator = ?VIRTUAL_SIZE,
        unit = bytes,
        metrics = ?DIR_SIZE_STATS_METRICS
    },
    #time_series_schema{
        name_generator_type = exact,
        name_generator = ?LOGICAL_SIZE,
        unit = bytes,
        metrics = ?DIR_SIZE_STATS_METRICS
    },
    #time_series_schema{
        name_generator_type = add_prefix,
        name_generator = <<?PHYSICAL_SIZE_TS_PREFIX_STR>>,
        unit = bytes,
        metrics = ?DIR_SIZE_STATS_METRICS
    }
]}).


-endif.