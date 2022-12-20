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


-include("atm/atm_test_schema_drafts.hrl").
-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/time_series/common.hrl").


-define(ATM_INTEGER_DATA_SPEC, #atm_data_spec{type = atm_integer_type}).
-define(ATM_STRING_DATA_SPEC, #atm_data_spec{type = atm_string_type}).
-define(ATM_OBJECT_DATA_SPEC, #atm_data_spec{type = atm_object_type}).

-define(ANY_MEASUREMENT_DATA_SPEC, #atm_data_spec{
    type = atm_time_series_measurement_type,
    value_constraints = #{specs => [#atm_time_series_measurement_spec{
        name_matcher_type = has_prefix,
        name_matcher = <<>>,
        unit = none
    }]}
}).

-define(EXP_WINDOW(__METRIC_CONFIG, __TIMESTAMP, __VALUE), #{
    <<"value">> => __VALUE,
    <<"timestamp">> => __TIMESTAMP - __TIMESTAMP rem __METRIC_CONFIG#metric_config.resolution,
    <<"firstMeasurementTimestamp">> => null,
    <<"lastMeasurementTimestamp">> => null
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

-define(ATM_TS_STORE_CONFIG, #atm_time_series_store_config{
    time_series_collection_schema = #time_series_collection_schema{
        time_series_schemas = [
            ?MAX_FILE_SIZE_TS_SCHEMA,
            ?COUNT_TS_SCHEMA
        ]
    }
}).

-define(ATM_TS_STORE_SCHEMA_DRAFT(__ID), #atm_store_schema_draft{
    id = __ID,
    type = time_series,
    config = ?ATM_TS_STORE_CONFIG,
    requires_initial_content = false
}).

-define(CORRECT_ATM_TS_DISPATCH_RULES, [
    #atm_time_series_dispatch_rule{
        measurement_ts_name_matcher_type = has_prefix,
        measurement_ts_name_matcher = <<"count_">>,
        target_ts_name_generator = ?COUNT_TS_NAME_GENERATOR,
        prefix_combiner = converge
    },
    #atm_time_series_dispatch_rule{
        measurement_ts_name_matcher_type = exact,
        measurement_ts_name_matcher = <<"size">>,
        target_ts_name_generator = ?MAX_FILE_SIZE_TS_NAME,
        prefix_combiner = overwrite
    }
]).

-define(ATM_TREE_FOREST_STORE_SCHEMA_DRAFT(__ID, __ITEM_DATA_SPEC, __INITIAL_CONTENT), #atm_store_schema_draft{
    id = __ID,
    type = tree_forest,
    config = #atm_tree_forest_store_config{item_data_spec = __ITEM_DATA_SPEC},
    requires_initial_content = false,
    default_initial_content = __INITIAL_CONTENT
}).

-define(ATM_LIST_STORE_SCHEMA_DRAFT(__ID, __ITEM_DATA_SPEC, __INITIAL_CONTENT), #atm_store_schema_draft{
    id = __ID,
    type = list,
    config = #atm_list_store_config{item_data_spec = __ITEM_DATA_SPEC},
    requires_initial_content = false,
    default_initial_content = __INITIAL_CONTENT
}).

-define(INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(__ID, __DEFAULT_INITIAL_CONTENT),
    ?ATM_LIST_STORE_SCHEMA_DRAFT(__ID, ?ATM_INTEGER_DATA_SPEC, __DEFAULT_INITIAL_CONTENT)
).
-define(INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(__ID), ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(__ID, undefined)).

-define(ATM_SV_STORE_SCHEMA_DRAFT(__ID, __ITEM_DATA_SPEC, __INITIAL_CONTENT), #atm_store_schema_draft{
    id = __ID,
    type = single_value,
    config = #atm_single_value_store_config{item_data_spec = __ITEM_DATA_SPEC},
    requires_initial_content = false,
    default_initial_content = __INITIAL_CONTENT
}).

-define(ATM_RANGE_STORE_SCHEMA_DRAFT(__ID, __INITIAL_CONTENT), #atm_store_schema_draft{
    id = __ID,
    type = range,
    config = #atm_range_store_config{},
    requires_initial_content = false,
    default_initial_content = __INITIAL_CONTENT
}).


-endif.
