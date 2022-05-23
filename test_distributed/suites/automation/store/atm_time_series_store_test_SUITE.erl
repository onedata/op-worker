%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation time series store.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_time_series_store_test_SUITE).
-author("Bartosz Walkowicz").

-include("atm/atm_test_store.hrl").
-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% exported for CT
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    create_test/1,
    manage_content_test/1,
    not_supported_iteration_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_test,
        manage_content_test,
        not_supported_iteration_test
    ]}
].

all() -> [
    {group, all_tests}
].


-define(COUNTER_OF_ALL_COUNTS_TS_NAME, <<"counter_of_all_counts">>).
-define(COUNTER_OF_ALL_COUNTS_TS_SCHEMA, #atm_time_series_schema{
    name_generator_type = exact,
    name_generator = ?COUNTER_OF_ALL_COUNTS_TS_NAME,
    unit = none,
    metrics = #{
        ?MINUTE_METRIC_NAME => ?MINUTE_METRIC_CONFIG,
        ?HOUR_METRIC_NAME => ?HOUR_METRIC_CONFIG,
        ?DAY_METRIC_NAME => ?DAY_METRIC_CONFIG
    }
}).

-define(ATM_STORE_CONFIG, #atm_time_series_store_config{schemas = [
    ?MAX_FILE_SIZE_TS_SCHEMA,
    ?COUNT_TS_SCHEMA,
    ?COUNTER_OF_ALL_COUNTS_TS_SCHEMA
]}).

-define(DISPATCH_RULES, [
    #atm_time_series_dispatch_rule{
        measurement_ts_name_matcher_type = exact,
        measurement_ts_name_matcher = <<"mp3">>,
        target_ts_name_generator = <<"count_">>,
        prefix_combiner = overwrite
    },
    #atm_time_series_dispatch_rule{
        measurement_ts_name_matcher_type = has_prefix,
        measurement_ts_name_matcher = <<"size">>,
        target_ts_name_generator = ?MAX_FILE_SIZE_TS_NAME,
        prefix_combiner = converge
    },
    #atm_time_series_dispatch_rule{
        measurement_ts_name_matcher_type = has_prefix,
        measurement_ts_name_matcher = <<"count_ct_">>,
        target_ts_name_generator = <<"count_">>,
        prefix_combiner = concatenate
    },
    #atm_time_series_dispatch_rule{
        measurement_ts_name_matcher_type = has_prefix,
        measurement_ts_name_matcher = <<"count_cn_">>,
        target_ts_name_generator = <<"count_">>,
        prefix_combiner = converge
    },
    % dispatch rules can be duplicated or be each other's generalization / specialization
    % in such case the measurement will be inserted multiple times (duplicated)
    #atm_time_series_dispatch_rule{
        measurement_ts_name_matcher_type = has_prefix,
        measurement_ts_name_matcher = <<"count_over_">>,
        target_ts_name_generator = <<"count_">>,
        prefix_combiner = overwrite
    },
    #atm_time_series_dispatch_rule{
        measurement_ts_name_matcher_type = has_prefix,
        measurement_ts_name_matcher = <<"count_over_">>,
        target_ts_name_generator = <<"count_">>,
        prefix_combiner = overwrite
    },
    #atm_time_series_dispatch_rule{
        measurement_ts_name_matcher_type = has_prefix,
        measurement_ts_name_matcher = <<"count">>,
        target_ts_name_generator = ?COUNTER_OF_ALL_COUNTS_TS_NAME,
        prefix_combiner = overwrite
    }
]).

-define(NOW(), global_clock:timestamp_seconds()).

-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).
-define(erpc(Expr), ?erpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% Test functions
%%%===================================================================


create_test(_Config) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),
    AtmStoreSchema = build_store_schema(?ATM_STORE_CONFIG),

    ?assertThrow(
        ?ERROR_BAD_DATA(<<"initialContent">>, <<"Time series store does not accept initial content">>),
        ?erpc(atm_store_api:create(AtmWorkflowExecutionAuth, [], AtmStoreSchema))
    ),

    {ok, #document{key = AtmStoreId}} = ?assertMatch(
        {ok, #document{value = #atm_store{initial_content = undefined, frozen = false}}},
        ?rpc(atm_store_api:create(AtmWorkflowExecutionAuth, undefined, AtmStoreSchema))
    ),

    % Assert only ts for exact generators are initiated
    ExpLayout = #{
        ?MAX_FILE_SIZE_TS_NAME => [?MAX_FILE_SIZE_METRIC_NAME],
        ?COUNTER_OF_ALL_COUNTS_TS_NAME => lists:sort([?MINUTE_METRIC_NAME, ?HOUR_METRIC_NAME, ?DAY_METRIC_NAME])
    },
    ?assertEqual(ExpLayout, get_layout(AtmWorkflowExecutionAuth, AtmStoreId)),
    ?assertEqual(
        #{
            ?MAX_FILE_SIZE_TS_NAME => #{
                ?MAX_FILE_SIZE_METRIC_NAME => []
            },
            ?COUNTER_OF_ALL_COUNTS_TS_NAME => #{
                ?MINUTE_METRIC_NAME => [],
                ?HOUR_METRIC_NAME => [],
                ?DAY_METRIC_NAME => []
            }
        },
        get_slice(AtmWorkflowExecutionAuth, AtmStoreId, ExpLayout)
    ).


manage_content_test(_Config) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),
    AtmStoreSchema = build_store_schema(?ATM_STORE_CONFIG),
    SortedCountTSMetricNames = lists:sort([?MINUTE_METRIC_NAME, ?HOUR_METRIC_NAME, ?DAY_METRIC_NAME]),

    AtmStoreId = create_store(AtmWorkflowExecutionAuth, AtmStoreSchema),
    ExpLayout0 = #{
        ?MAX_FILE_SIZE_TS_NAME => [?MAX_FILE_SIZE_METRIC_NAME],
        ?COUNTER_OF_ALL_COUNTS_TS_NAME => SortedCountTSMetricNames
    },
    ?assertEqual(ExpLayout0, get_layout(AtmWorkflowExecutionAuth, AtmStoreId)),

    ContentUpdateOpts = #atm_time_series_store_content_update_options{
        dispatch_rules = ?DISPATCH_RULES
    },

    % Assert that only valid measurements are accepted
    lists:foreach(fun({InvalidData, ExpError}) ->
        ?assertThrow(ExpError, ?erpc(atm_store_api:update_content(
            AtmWorkflowExecutionAuth, InvalidData, ContentUpdateOpts, AtmStoreId
        )))
    end, [
        {5, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(5, atm_time_series_measurement_type)},
        {<<"BIN">>, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"BIN">>, atm_time_series_measurement_type)},
        {
            [#{<<"ts">> => <<"name">>}],
            ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                [#{<<"ts">> => <<"name">>}],
                atm_array_type,
                #{<<"$[0]">> => errors:to_json(?ERROR_ATM_DATA_TYPE_UNVERIFIED(
                    #{<<"ts">> => <<"name">>}, atm_time_series_measurement_type
                ))}
            )
        },
        {
            [#{<<"tsName">> => <<"mp3">>, <<"timestamp">> => 1, <<"value">> => 10}, 10],
            ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                [#{<<"tsName">> => <<"mp3">>, <<"timestamp">> => 1, <<"value">> => 10}, 10],
                atm_array_type,
                #{<<"$[1]">> => errors:to_json(
                    ?ERROR_ATM_DATA_TYPE_UNVERIFIED(10, atm_time_series_measurement_type)
                )}
            )
        }
    ]),

    % Timestamps of other measurements will be calculated based on Timestamp1 so to
    % ensure correctness during execution it is set to the 3rd hour from beginning of day window
    Timestamp1 = infer_window_timestamp(?NOW(), ?DAY_METRIC_CONFIG) + 3 * ?HOUR_RESOLUTION + 1,
    Measurements1 = [
        #{<<"tsName">> => <<"mp3">>, <<"timestamp">> => Timestamp1, <<"value">> => 10}
    ],
    ExpLayout1 = ExpLayout0#{<<"count_mp3">> => SortedCountTSMetricNames},
    ExpWindows1 = #{
        ?MAX_FILE_SIZE_TS_NAME => #{?MAX_FILE_SIZE_METRIC_NAME => []},
        <<"count_mp3">> => #{
            ?MINUTE_METRIC_NAME => [?EXP_MINUTE_METRIC_WINDOW(Timestamp1, 10)],
            ?HOUR_METRIC_NAME => [?EXP_HOUR_METRIC_WINDOW(Timestamp1, 10)],
            ?DAY_METRIC_NAME => [?EXP_DAY_METRIC_WINDOW(Timestamp1, 10)]
        },
        ?COUNTER_OF_ALL_COUNTS_TS_NAME => #{
            ?MINUTE_METRIC_NAME => [],
            ?HOUR_METRIC_NAME => [],
            ?DAY_METRIC_NAME => []
        }
    },
    ?assertEqual(ok, ?rpc(atm_store_api:update_content(
        AtmWorkflowExecutionAuth, Measurements1, ContentUpdateOpts, AtmStoreId
    ))),
    ?assertEqual(ExpLayout1, get_layout(AtmWorkflowExecutionAuth, AtmStoreId)),
    ?assertEqual(ExpWindows1, get_slice(AtmWorkflowExecutionAuth, AtmStoreId, ExpLayout1)),

    Timestamp2 = Timestamp1 - 3601,
    Timestamp3 = Timestamp1 + 120,
    Measurements2 = [
        #{<<"tsName">> => <<"mp3">>, <<"timestamp">> => Timestamp3, <<"value">> => 100},
        #{<<"tsName">> => <<"count_ct_supplies">>, <<"timestamp">> => Timestamp1, <<"value">> => 0},
        #{<<"tsName">> => <<"count_over_unicorns">>, <<"timestamp">> => Timestamp1, <<"value">> => 7},
        #{<<"tsName">> => <<"mp3">>, <<"timestamp">> => Timestamp2, <<"value">> => 111},
        % measurement not matched with any dispatch rule should be ignored
        #{<<"tsName">> => <<"mp4">>, <<"timestamp">> => Timestamp1, <<"value">> => 1000000000},
        % measurements with the same tsName and timestamps should be handled properly
        #{<<"tsName">> => <<"size_mp3">>, <<"timestamp">> => Timestamp1, <<"value">> => 1024},
        #{<<"tsName">> => <<"count_cn_resources">>, <<"timestamp">> => Timestamp1, <<"value">> => 1},
        #{<<"tsName">> => <<"count_cn_resources">>, <<"timestamp">> => Timestamp1, <<"value">> => 2},
        #{<<"tsName">> => <<"size_mp3">>, <<"timestamp">> => Timestamp1, <<"value">> => 2048}
    ],
    ExpLayout2 = ExpLayout1#{
        <<"count_cn_resources">> => SortedCountTSMetricNames,
        <<"count_count_ct_supplies">> => SortedCountTSMetricNames,
        <<"count_unicorns">> => SortedCountTSMetricNames
    },
    ExpWindows2 = #{
        ?MAX_FILE_SIZE_TS_NAME => #{
            ?MAX_FILE_SIZE_METRIC_NAME => [?MAX_FILE_SIZE_METRIC_WINDOW(Timestamp1, 2048)]
        },
        <<"count_cn_resources">> => #{
            ?MINUTE_METRIC_NAME => [?EXP_MINUTE_METRIC_WINDOW(Timestamp1, 3)],
            ?HOUR_METRIC_NAME => [?EXP_HOUR_METRIC_WINDOW(Timestamp1, 3)],
            ?DAY_METRIC_NAME => [?EXP_DAY_METRIC_WINDOW(Timestamp1, 3)]
        },
        <<"count_count_ct_supplies">> => #{
            ?MINUTE_METRIC_NAME => [?EXP_MINUTE_METRIC_WINDOW(Timestamp1, 0)],
            ?HOUR_METRIC_NAME => [?EXP_HOUR_METRIC_WINDOW(Timestamp1, 0)],
            ?DAY_METRIC_NAME => [?EXP_DAY_METRIC_WINDOW(Timestamp1, 0)]
        },
        <<"count_mp3">> => #{
            ?MINUTE_METRIC_NAME => [
                ?EXP_MINUTE_METRIC_WINDOW(Timestamp3, 100),
                ?EXP_MINUTE_METRIC_WINDOW(Timestamp1, 10),
                ?EXP_MINUTE_METRIC_WINDOW(Timestamp2, 111)
            ],
            ?HOUR_METRIC_NAME => [
                ?EXP_HOUR_METRIC_WINDOW(Timestamp1, 110),
                ?EXP_HOUR_METRIC_WINDOW(Timestamp2, 111)
            ],
            ?DAY_METRIC_NAME => [
                ?EXP_DAY_METRIC_WINDOW(Timestamp1, 221)
            ]
        },
        % the dispatch rule with "count_over_" prefix matcher is duplicated twice,
        % which should cause the measurement values to be doubled (each measurement is
        % inserted twice)
        <<"count_unicorns">> => #{
            ?MINUTE_METRIC_NAME => [?EXP_MINUTE_METRIC_WINDOW(Timestamp1, 14)],
            ?HOUR_METRIC_NAME => [?EXP_HOUR_METRIC_WINDOW(Timestamp1, 14)],
            ?DAY_METRIC_NAME => [?EXP_DAY_METRIC_WINDOW(Timestamp1, 14)]
        },
        ?COUNTER_OF_ALL_COUNTS_TS_NAME => #{
            ?MINUTE_METRIC_NAME => [
                ?EXP_MINUTE_METRIC_WINDOW(Timestamp1, 10)
            ],
            ?HOUR_METRIC_NAME => [
                ?EXP_HOUR_METRIC_WINDOW(Timestamp1, 10)
            ],
            ?DAY_METRIC_NAME => [
                ?EXP_DAY_METRIC_WINDOW(Timestamp1, 10)
            ]
        }
    },
    ?assertEqual(ok, ?rpc(atm_store_api:update_content(
        AtmWorkflowExecutionAuth, Measurements2, ContentUpdateOpts, AtmStoreId
    ))),
    ?assertEqual(ExpLayout2, get_layout(AtmWorkflowExecutionAuth, AtmStoreId)),
    ?assertEqual(ExpWindows2, get_slice(AtmWorkflowExecutionAuth, AtmStoreId, ExpLayout2)),

    % Assert operation is atomic (it it fails no metric should be modified)
    InvalidContentUpdateOpts = #atm_time_series_store_content_update_options{
        dispatch_rules = [
            #atm_time_series_dispatch_rule{
                measurement_ts_name_matcher_type = exact,
                measurement_ts_name_matcher = <<"mp3">>,
                target_ts_name_generator = <<"count_">>,
                prefix_combiner = overwrite
            },
            % below dispatch rule does not match any ts generator
            #atm_time_series_dispatch_rule{
                measurement_ts_name_matcher_type = exact,
                measurement_ts_name_matcher = <<"size_mp3">>,
                target_ts_name_generator = <<"shroedinger_">>,
                prefix_combiner = overwrite
            }
        ]
    },
    ExpError = ?ERROR_BAD_DATA(<<"dispatchRules">>, <<
        "Time series name generator 'shroedinger_' specified in one of the dispatch rules "
        "does not reference any defined time series schema"
    >>),
    ?assertThrow(ExpError, ?erpc(atm_store_api:update_content(
        AtmWorkflowExecutionAuth, Measurements2, InvalidContentUpdateOpts, AtmStoreId
    ))),
    ?assertEqual(ExpLayout2, get_layout(AtmWorkflowExecutionAuth, AtmStoreId)),
    ?assertEqual(ExpWindows2, get_slice(AtmWorkflowExecutionAuth, AtmStoreId, ExpLayout2)),

    % Test slices
    ExpSliceWindows1 = #{
        ?MAX_FILE_SIZE_TS_NAME => #{
            ?MAX_FILE_SIZE_METRIC_NAME => [?MAX_FILE_SIZE_METRIC_WINDOW(Timestamp1, 2048)]
        },
        <<"count_cn_resources">> => #{
            ?MINUTE_METRIC_NAME => [?EXP_MINUTE_METRIC_WINDOW(Timestamp1, 3)],
            ?DAY_METRIC_NAME => [?EXP_DAY_METRIC_WINDOW(Timestamp1, 3)]
        }
    },
    ?assertEqual(
        ExpSliceWindows1,
        get_slice(AtmWorkflowExecutionAuth, AtmStoreId, #{
            ?MAX_FILE_SIZE_TS_NAME => [?MAX_FILE_SIZE_METRIC_NAME],
            <<"count_cn_resources">> => [?MINUTE_METRIC_NAME, ?DAY_METRIC_NAME]
        })
    ),

    ExpSliceWindows2 = #{
        <<"count_mp3">> => #{
            ?MINUTE_METRIC_NAME => [?EXP_MINUTE_METRIC_WINDOW(Timestamp1, 10)],
            ?HOUR_METRIC_NAME => [?EXP_HOUR_METRIC_WINDOW(Timestamp1, 110)],
            ?DAY_METRIC_NAME => [?EXP_DAY_METRIC_WINDOW(Timestamp1, 221)]
        }
    },
    ?assertEqual(
        ExpSliceWindows2,
        get_slice(
            AtmWorkflowExecutionAuth, AtmStoreId, #{<<"count_mp3">> => SortedCountTSMetricNames},
            Timestamp1, 1
        )
    ).


not_supported_iteration_test(_Config) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    AtmStoreSchema = build_store_schema(?ATM_STORE_CONFIG),
    AtmStoreId = create_store(AtmWorkflowExecutionAuth, AtmStoreSchema),

    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmStoreSchema#atm_store_schema.id,
        max_batch_size = rand:uniform(8)
    },

    ?assertError(
        {exception, not_supported, _},
        ?erpc(atm_store_api:acquire_iterator(AtmStoreId, AtmStoreIteratorSpec))
    ).


%===================================================================
% Helper functions
%===================================================================


%% @private
-spec create_workflow_execution_auth() -> atm_workflow_execution_auth:record().
create_workflow_execution_auth() ->
    atm_store_test_utils:create_workflow_execution_auth(
        ?PROVIDER_SELECTOR, user1, space_krk
    ).


%% @private
-spec build_store_schema(atm_store_config:record()) -> atm_store_schema:record().
build_store_schema(AtmStoreConfig) ->
    % time_series store does not allow initial content and as such 'requires_initial_content'
    % is always set as 'false' (it is enforced by oz)
    atm_store_test_utils:build_store_schema(AtmStoreConfig, false).


%% @private
-spec create_store(atm_workflow_execution_auth:record(), atm_store_schema:record()) ->
    atm_store:id().
create_store(AtmWorkflowExecutionAuth, AtmStoreSchema) ->
    {ok, #document{key = AtmStoreId}} = ?assertMatch(
        {ok, #document{value = #atm_store{initial_content = undefined, frozen = false}}},
        ?rpc(atm_store_api:create(AtmWorkflowExecutionAuth, undefined, AtmStoreSchema))
    ),
    AtmStoreId.


%% @private
-spec infer_window_timestamp(ts_windows:timestamp_seconds(), metric_config:record()) ->
    ts_windows:timestamp_seconds().
infer_window_timestamp(Time, #metric_config{resolution = Resolution}) ->
    Time - Time rem Resolution.


%% @private
-spec get_layout(atm_workflow_execution_auth:record(), atm_store:id()) ->
    time_series_collection:layout().
get_layout(AtmWorkflowExecutionAuth, AtmStoreId) ->
    BrowseOpts = #atm_time_series_store_content_browse_options{
        request = #atm_time_series_store_content_get_layout_req{}
    },

    #atm_time_series_store_content_browse_result{
        result = #atm_time_series_store_content_layout{
            layout = Layout
        }
    } = ?rpc(atm_store_api:browse_content(
        AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId
    )),

    maps:map(fun(_, MetricNames) -> lists:sort(MetricNames) end, Layout).


%% @private
-spec get_slice(
    atm_workflow_execution_auth:record(),
    atm_store:id(),
    time_series_collection:layout()
) ->
    #{time_series_collection:time_series_id() => #{ts_metric:id() => [ts_windows:value()]}}.
get_slice(AtmWorkflowExecutionAuth, AtmStoreId, Layout) ->
    get_slice(AtmWorkflowExecutionAuth, AtmStoreId, Layout, undefined).


%% @private
-spec get_slice(
    atm_workflow_execution_auth:record(),
    atm_store:id(),
    time_series_collection:metrics_by_time_series(),
    undefined | atm_time_series_store_content_browse_options:timestamp()
) ->
    #{time_series_collection:time_series_id() => #{ts_metric:id() => [ts_windows:value()]}}.
get_slice(AtmWorkflowExecutionAuth, AtmStoreId, Layout, StartTimestamp) ->
    get_slice(AtmWorkflowExecutionAuth, AtmStoreId, Layout, StartTimestamp, 10000000000).


%% @private
-spec get_slice(
    atm_workflow_execution_auth:record(),
    atm_store:id(),
    time_series_collection:metrics_by_time_series(),
    undefined | atm_time_series_store_content_browse_options:timestamp(),
    atm_time_series_store_content_browse_options:window_limit()
) ->
    #{time_series_collection:time_series_id() => #{ts_metric:id() => [ts_windows:value()]}}.
get_slice(AtmWorkflowExecutionAuth, AtmStoreId, Layout, StartTimestamp, WindowLimit) ->
    BrowseOpts = #atm_time_series_store_content_browse_options{
        request = #atm_time_series_store_content_get_slice_req{
            layout = Layout,
            start_timestamp = StartTimestamp,
            window_limit = WindowLimit
        }
    },

    #atm_time_series_store_content_browse_result{
        result = #atm_time_series_store_content_slice{
            slice = Slice
        }
    } = ?rpc(atm_store_api:browse_content(
        AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId
    )),

    Slice.


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, atm_store_test_utils],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_, Config) ->
    Config.


end_per_group(_, _Config) ->
    ok.


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
