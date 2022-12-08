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

-define(SIZE_ON_STORAGE_TS_PREFIX_STR, "storage_use_").

-define(REG_FILE_AND_LINK_COUNT, <<"reg_file_and_link_count">>).
-define(DIR_COUNT, <<"dir_count">>).
-define(ERRORS_COUNT, <<"errors_count">>).
-define(TOTAL_SIZE, <<"total_size">>).
-define(SIZE_ON_STORAGE(StorageId), <<?SIZE_ON_STORAGE_TS_PREFIX_STR, StorageId/binary>>).


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
        name_generator = ?ERRORS_COUNT,
        unit = none,
        metrics = ?DIR_SIZE_STATS_METRICS
    },
    #time_series_schema{
        name_generator_type = exact,
        name_generator = ?TOTAL_SIZE,
        unit = bytes,
        metrics = ?DIR_SIZE_STATS_METRICS
    },
    #time_series_schema{
        name_generator_type = add_prefix,
        name_generator = <<?SIZE_ON_STORAGE_TS_PREFIX_STR>>,
        unit = bytes,
        metrics = ?DIR_SIZE_STATS_METRICS
    }
]}).


-endif.