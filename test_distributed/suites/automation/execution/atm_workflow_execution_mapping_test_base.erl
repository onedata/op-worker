%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bases for tests of mapping task arguments and results.
%%% NOTE: stores have dedicated test suites and as such only basic mapping
%%% cases are tested here (test cases check overall integration between
%%% various components rather than concrete one).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_mapping_test_base).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("atm/atm_test_store.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    map_arguments/0,

    map_results_to_audit_log_store/0,
    map_results_to_list_store/0,
    map_results_to_range_store/0,
    map_results_to_single_value_store/0,
    map_results_to_time_series_store/0,
    map_results_to_tree_forest_store/0,

    map_results_to_workflow_audit_log_store/0,
    map_results_to_task_audit_log_store/0,
    map_results_to_task_time_series_store/0,

    map_results_to_multiple_stores/0
]).


-define(ATM_INTEGER_DATA_SPEC, #atm_data_spec{type = atm_integer_type}).
-define(ATM_OBJECT_DATA_SPEC, #atm_data_spec{type = atm_object_type}).


-define(ITERATED_STORE_SCHEMA_ID, <<"iterated_st">>).
-define(ITERATED_LIST_STORE_SCHEMA_DRAFT(__ITEM_DATA_SPEC, __INITIAL_CONTENT),
    ?LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, __ITEM_DATA_SPEC, __INITIAL_CONTENT)
).
-define(LIST_STORE_SCHEMA_DRAFT(__ID, __ITEM_DATA_SPEC, __INITIAL_CONTENT), #atm_store_schema_draft{
    id = __ID,
    type = list,
    config = #atm_list_store_config{item_data_spec = __ITEM_DATA_SPEC},
    requires_initial_content = false,
    default_initial_content = __INITIAL_CONTENT
}).

-define(ATM_TIME_SERIES_STORE_CONFIG, #atm_time_series_store_config{
    time_series_collection_schema = #time_series_collection_schema{
        time_series_schemas = [
            ?MAX_FILE_SIZE_TS_SCHEMA,
            ?COUNT_TS_SCHEMA
        ]
    }
}).
-define(ATM_TIME_SERIES_DISPATCH_RULES, [
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

-define(ECHO_TASK_SCHEMA_DRAFT(__RESULT_MAPPERS), #atm_task_schema_draft{
    lambda_id = ?ECHO_LAMBDA_ID,
    lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
    argument_mappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
    result_mappings = __RESULT_MAPPERS,
    time_series_store_config = ?ATM_TIME_SERIES_STORE_CONFIG
}).

-define(RESULT_MAPPING_WORKFLOW_SCHEMA_DRAFT(
    __TESTCASE,
    __STORE_SCHEMA_DRAFTS,
    __ITERATED_ITEM_DATA_SPEC,
    __TARGET_STORE_SCHEMA_ID,
    __TARGET_STORE_CONTENT_UPDATE_OPTIONS
),
    #atm_workflow_schema_dump_draft{
        name = str_utils:to_binary(__TESTCASE),
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = __STORE_SCHEMA_DRAFTS,
            lanes = [#atm_lane_schema_draft{
                parallel_boxes = [#atm_parallel_box_schema_draft{tasks = [
                    ?ECHO_TASK_SCHEMA_DRAFT([#atm_task_schema_result_mapper{
                        result_name = ?ECHO_ARG_NAME,
                        store_schema_id = __TARGET_STORE_SCHEMA_ID,
                        store_content_update_options = __TARGET_STORE_CONTENT_UPDATE_OPTIONS
                    }])
                ]}],
                store_iterator_spec = #atm_store_iterator_spec_draft{
                    store_schema_id = ?ITERATED_STORE_SCHEMA_ID,
                    max_batch_size = ?RAND_INT(5, 8)
                }
            }]
        },
        supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
            ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(
                __ITERATED_ITEM_DATA_SPEC,
                ?RAND_ELEMENT([return_value, file_pipe])
            )
        }}
    }
).

-record(map_results_to_global_store_test_spec, {
    testcase :: atom(),
    iterated_item_spec :: atm_data_spec:record(),
    iterated_items :: [automation:item()],
    target_store_type :: automation:store_type(),
    target_store_config :: atm_store_config:record(),
    target_store_update_options :: atm_store_content_update_options:record()
}).
-type map_results_to_global_store_test_spec() :: #map_results_to_global_store_test_spec{}.

-record(map_results_to_store_test_spec, {
    testcase :: atom(),
    global_store_schema_drafts :: [atm_test_schema_factory:atm_store_schema_draft()],
    iterated_item_spec :: atm_data_spec:record(),
    iterated_items :: [automation:item()],
    target_store_schema_id :: automation:id(),
    target_store_type :: automation:store_type(),
    target_store_update_options :: atm_store_content_update_options:record()
}).
-type map_results_to_store_test_spec() :: #map_results_to_store_test_spec{}.


-define(NOW(), global_clock:timestamp_seconds()).


%%%===================================================================
%%% Tests
%%%===================================================================


map_arguments() ->
    ConstValue = <<"makaron">>,

    IteratedItems = lists:map(fun(Num) -> #{<<"num">> => Num} end, lists:seq(1, 40)),

    SingleValueStoreSchemaId = <<"sv">>,
    SingleValueStoreContent = <<"ursus">>,
    SingleValueStoreSchemaDraft = #atm_store_schema_draft{
        id = SingleValueStoreSchemaId,
        type = single_value,
        config = #atm_single_value_store_config{item_data_spec = #atm_data_spec{type = atm_string_type}},
        requires_initial_content = false,
        default_initial_content = SingleValueStoreContent
    },

    TargetStoreSchemaId = <<"target_st">>,
    ExpTargetStoreFinalContent = lists:sort(lists:map(fun(#{<<"num">> := Num}) ->
        #{
            <<"const">> => ConstValue,
            <<"iterated">> => Num,
            <<"sv_content">> => SingleValueStoreContent
        }
    end, IteratedItems)),

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = #atm_workflow_schema_dump_draft{
            name = str_utils:to_binary(?FUNCTION_NAME),
            revision_num = 1,
            revision = #atm_workflow_schema_revision_draft{
                stores = [
                    ?LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, ?ATM_OBJECT_DATA_SPEC, IteratedItems),
                    SingleValueStoreSchemaDraft,
                    ?LIST_STORE_SCHEMA_DRAFT(TargetStoreSchemaId, ?ATM_OBJECT_DATA_SPEC, [])
                ],
                lanes = [#atm_lane_schema_draft{
                    parallel_boxes = [#atm_parallel_box_schema_draft{tasks = [
                        #atm_task_schema_draft{
                            lambda_id = ?ECHO_LAMBDA_ID,
                            lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
                            argument_mappings = [#atm_task_schema_argument_mapper{
                                argument_name = ?ECHO_ARG_NAME,
                                value_builder = #atm_task_argument_value_builder{
                                    type = object,
                                    recipe = #{
                                        <<"const">> => #atm_task_argument_value_builder{
                                            type = const,
                                            recipe = ConstValue
                                        },
                                        <<"iterated">> => #atm_task_argument_value_builder{
                                            type = iterated_item,
                                            recipe = [<<"num">>]
                                        },
                                        <<"sv_content">> => #atm_task_argument_value_builder{
                                            type = single_value_store_content,
                                            recipe = SingleValueStoreSchemaId
                                        }
                                    }
                                }
                            }],
                            result_mappings = [#atm_task_schema_result_mapper{
                                result_name = ?ECHO_ARG_NAME,
                                store_schema_id = TargetStoreSchemaId,
                                store_content_update_options = #atm_list_store_content_update_options{function = append}
                            }]
                        }
                    ]}],
                    store_iterator_spec = #atm_store_iterator_spec_draft{store_schema_id = ?ITERATED_STORE_SCHEMA_ID}
                }]
            },
            supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
                ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(?ATM_OBJECT_DATA_SPEC)
            }}
        },
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{selector = {1, 1}}],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(AtmMockCallCtx = #atm_mock_call_ctx{
                    workflow_execution_exp_state = ExpState0
                }) ->
                    assert_exp_target_store_content(
                        list,
                        ExpTargetStoreFinalContent,
                        atm_workflow_execution_test_runner:browse_store(TargetStoreSchemaId, undefined, AtmMockCallCtx)
                    ),
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable({1, 1}, ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_finished(ExpState1)}
                end
            }
        }]
    }).


map_results_to_audit_log_store() ->
    IteratedItemDataSpec = #atm_data_spec{type = atm_integer_type},

    map_results_to_global_store_test_base(#map_results_to_global_store_test_spec{
        testcase = ?FUNCTION_NAME,
        iterated_item_spec = IteratedItemDataSpec,
        iterated_items = lists:seq(20, 200, 4),
        target_store_type = audit_log,
        target_store_config = #atm_audit_log_store_config{log_content_data_spec = IteratedItemDataSpec},
        target_store_update_options = #atm_audit_log_store_content_update_options{function = append}
    }).


map_results_to_list_store() ->
    IteratedItemDataSpec = #atm_data_spec{type = atm_string_type},

    map_results_to_global_store_test_base(#map_results_to_global_store_test_spec{
        testcase = ?FUNCTION_NAME,
        iterated_item_spec = IteratedItemDataSpec,
        iterated_items = lists_utils:generate(fun() -> ?RAND_STR() end, ?RAND_INT(30, 50)),
        target_store_type = list,
        target_store_config = #atm_list_store_config{item_data_spec = IteratedItemDataSpec},
        target_store_update_options = #atm_list_store_content_update_options{function = append}
    }).


map_results_to_range_store() ->
    IteratedItems = lists_utils:generate(fun() ->
        #{
            <<"start">> => ?RAND_INT(0, 100),
            <<"end">> => ?RAND_INT(500, 1000),
            <<"step">> => ?RAND_INT(5, 25)
        }
    end, ?RAND_INT(30, 50)),

    map_results_to_global_store_test_base(#map_results_to_global_store_test_spec{
        testcase = ?FUNCTION_NAME,
        iterated_item_spec = #atm_data_spec{type = atm_range_type},
        iterated_items = IteratedItems,
        target_store_type = range,
        target_store_config = #atm_range_store_config{},
        target_store_update_options = #atm_range_store_content_update_options{}
    }).


map_results_to_single_value_store() ->
    IteratedItemDataSpec = #atm_data_spec{type = atm_object_type},

    map_results_to_global_store_test_base(#map_results_to_global_store_test_spec{
        testcase = ?FUNCTION_NAME,
        iterated_item_spec = IteratedItemDataSpec,
        iterated_items = gen_random_object_list(),
        target_store_type = single_value,
        target_store_config = #atm_single_value_store_config{item_data_spec = IteratedItemDataSpec},
        target_store_update_options = #atm_single_value_store_content_update_options{}
    }).


map_results_to_time_series_store() ->
    map_results_to_global_store_test_base(#map_results_to_global_store_test_spec{
        testcase = ?FUNCTION_NAME,
        iterated_item_spec = ?ANY_MEASUREMENT_DATA_SPEC,
        iterated_items = gen_random_time_series_measurements(),
        target_store_type = time_series,
        target_store_config = ?ATM_TIME_SERIES_STORE_CONFIG,
        target_store_update_options = #atm_time_series_store_content_update_options{
            dispatch_rules = ?ATM_TIME_SERIES_DISPATCH_RULES
        }
    }).


map_results_to_tree_forest_store() ->
    IteratedItemDataSpec = #atm_data_spec{type = atm_file_type},
    FileObjects = onenv_file_test_utils:create_and_sync_file_tree(
        user1, ?SPACE_SELECTOR, lists_utils:generate(fun() -> #file_spec{} end, 30)
    ),
    IteratedItems = lists:map(fun(#object{guid = Guid}) ->
        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        #{<<"file_id">> => ObjectId}
    end, FileObjects),

    map_results_to_global_store_test_base(#map_results_to_global_store_test_spec{
        testcase = ?FUNCTION_NAME,
        iterated_item_spec = IteratedItemDataSpec,
        iterated_items = IteratedItems,
        target_store_type = tree_forest,
        target_store_config = #atm_tree_forest_store_config{item_data_spec = IteratedItemDataSpec},
        target_store_update_options = #atm_tree_forest_store_content_update_options{function = append}
    }).


%% @private
-spec map_results_to_global_store_test_base(map_results_to_global_store_test_spec()) ->
    ok.
map_results_to_global_store_test_base(#map_results_to_global_store_test_spec{
    testcase = Testcase,
    iterated_item_spec = IteratedItemDataSpec,
    iterated_items = IteratedItems,
    target_store_type = TargetStoreType,
    target_store_config = TargetStoreConfig,
    target_store_update_options = TargetStoreContentUpdateOptions
}) ->
    IteratedStoreSchemaDraft = ?ITERATED_LIST_STORE_SCHEMA_DRAFT(IteratedItemDataSpec, IteratedItems),

    TargetStoreSchemaId = <<"target_st">>,
    TargetStoreSchemaDraft = #atm_store_schema_draft{
        id = TargetStoreSchemaId,
        type = TargetStoreType,
        config = TargetStoreConfig
    },

    map_results_to_store_test_base(#map_results_to_store_test_spec{
        testcase = Testcase,
        global_store_schema_drafts = [IteratedStoreSchemaDraft, TargetStoreSchemaDraft],
        iterated_item_spec = IteratedItemDataSpec,
        iterated_items = IteratedItems,
        target_store_schema_id = TargetStoreSchemaId,
        target_store_type = TargetStoreType,
        target_store_update_options = TargetStoreContentUpdateOptions
    }).


map_results_to_workflow_audit_log_store() ->
    IteratedItemDataSpec = #atm_data_spec{type = atm_object_type},
    IteratedItems = gen_random_object_list(),
    IteratedStoreSchemaDraft = ?ITERATED_LIST_STORE_SCHEMA_DRAFT(IteratedItemDataSpec, IteratedItems),

    map_results_to_store_test_base(#map_results_to_store_test_spec{
        testcase = ?FUNCTION_NAME,
        global_store_schema_drafts = [IteratedStoreSchemaDraft],
        iterated_item_spec = IteratedItemDataSpec,
        iterated_items = IteratedItems,
        target_store_schema_id = ?WORKFLOW_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID,
        target_store_type = audit_log,
        target_store_update_options = #atm_audit_log_store_content_update_options{function = append}
    }).


map_results_to_task_audit_log_store() ->
    IteratedItemDataSpec = #atm_data_spec{type = atm_object_type},
    IteratedItems = gen_random_object_list(),
    IteratedStoreSchemaDraft = ?ITERATED_LIST_STORE_SCHEMA_DRAFT(IteratedItemDataSpec, IteratedItems),

    map_results_to_store_test_base(#map_results_to_store_test_spec{
        testcase = ?FUNCTION_NAME,
        global_store_schema_drafts = [IteratedStoreSchemaDraft],
        iterated_item_spec = IteratedItemDataSpec,
        iterated_items = IteratedItems,
        target_store_schema_id = ?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID,
        target_store_type = audit_log,
        target_store_update_options = #atm_audit_log_store_content_update_options{function = append}
    }).


map_results_to_task_time_series_store() ->
    IteratedItemDataSpec = ?ANY_MEASUREMENT_DATA_SPEC,
    IteratedItems = gen_random_time_series_measurements(),
    IteratedStoreSchemaDraft = ?ITERATED_LIST_STORE_SCHEMA_DRAFT(IteratedItemDataSpec, IteratedItems),

    map_results_to_store_test_base(#map_results_to_store_test_spec{
        testcase = ?FUNCTION_NAME,
        global_store_schema_drafts = [IteratedStoreSchemaDraft],
        iterated_item_spec = IteratedItemDataSpec,
        iterated_items = IteratedItems,
        target_store_schema_id = ?CURRENT_TASK_TIME_SERIES_STORE_SCHEMA_ID,
        target_store_type = time_series,
        target_store_update_options = #atm_time_series_store_content_update_options{
            dispatch_rules = ?ATM_TIME_SERIES_DISPATCH_RULES
        }
    }).


%% @private
-spec map_results_to_store_test_base(map_results_to_store_test_spec()) -> ok.
map_results_to_store_test_base(#map_results_to_store_test_spec{
    testcase = Testcase,
    global_store_schema_drafts = StoreSchemaDrafts,
    iterated_item_spec = IteratedItemDataSpec,
    iterated_items = IteratedItems,
    target_store_schema_id = TargetStoreSchemaId,
    target_store_type = TargetStoreType,
    target_store_update_options = TargetStoreContentUpdateOptions
}) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?RESULT_MAPPING_WORKFLOW_SCHEMA_DRAFT(
            Testcase,
            StoreSchemaDrafts,
            IteratedItemDataSpec,
            TargetStoreSchemaId,
            TargetStoreContentUpdateOptions
        ),
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                handle_task_execution_stopped = #atm_step_mock_spec{
                    after_step_hook = fun(AtmMockCallCtx = #atm_mock_call_ctx{
                        call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                    }) ->
                        assert_exp_target_store_content(
                            TargetStoreType,
                            IteratedItems,
                            atm_workflow_execution_test_runner:browse_store(
                                TargetStoreSchemaId, AtmTaskExecutionId, AtmMockCallCtx
                            )
                        )
                    end
                }
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable({1, 1}, ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_finished(ExpState1)}
                end
            }
        }]
    }).


map_results_to_multiple_stores() ->
    IteratedItems = gen_random_time_series_measurements(),

    TargetListStoreSchemaId = <<"target_list_store">>,
    TargetSingleValueStoreSchemaId = <<"target_single_value_store">>,

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = #atm_workflow_schema_dump_draft{
            name = str_utils:to_binary(?FUNCTION_NAME),
            revision_num = 1,
            revision = #atm_workflow_schema_revision_draft{
                stores = [
                    ?LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, ?ANY_MEASUREMENT_DATA_SPEC, IteratedItems),

                    ?LIST_STORE_SCHEMA_DRAFT(TargetListStoreSchemaId, ?ANY_MEASUREMENT_DATA_SPEC, []),
                    #atm_store_schema_draft{
                        id = TargetSingleValueStoreSchemaId,
                        type = single_value,
                        config = #atm_single_value_store_config{item_data_spec = ?ANY_MEASUREMENT_DATA_SPEC}
                    }
                ],
                lanes = [#atm_lane_schema_draft{
                    parallel_boxes = [#atm_parallel_box_schema_draft{tasks = [
                        #atm_task_schema_draft{
                            lambda_id = ?ECHO_LAMBDA_ID,
                            lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
                            time_series_store_config = ?ATM_TIME_SERIES_STORE_CONFIG,
                            argument_mappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
                            result_mappings = [
                                #atm_task_schema_result_mapper{
                                    result_name = ?ECHO_ARG_NAME,
                                    store_schema_id = TargetListStoreSchemaId,
                                    store_content_update_options = #atm_list_store_content_update_options{
                                        function = append
                                    }
                                },
                                #atm_task_schema_result_mapper{
                                    result_name = ?ECHO_ARG_NAME,
                                    store_schema_id = TargetSingleValueStoreSchemaId,
                                    store_content_update_options = #atm_single_value_store_content_update_options{}
                                },
                                #atm_task_schema_result_mapper{
                                    result_name = ?ECHO_ARG_NAME,
                                    store_schema_id = ?CURRENT_TASK_TIME_SERIES_STORE_SCHEMA_ID,
                                    store_content_update_options = #atm_time_series_store_content_update_options{
                                        dispatch_rules = ?ATM_TIME_SERIES_DISPATCH_RULES
                                    }
                                }
                            ]
                        }
                    ]}],
                    store_iterator_spec = #atm_store_iterator_spec_draft{store_schema_id = ?ITERATED_STORE_SCHEMA_ID}
                }]
            },
            supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
                ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(?ATM_OBJECT_DATA_SPEC)
            }}
        },
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                handle_task_execution_stopped = #atm_step_mock_spec{
                    after_step_hook = fun(AtmMockCallCtx = #atm_mock_call_ctx{
                        call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                    }) ->
                        lists:foreach(fun({TargetStoreType, TargetStoreSchemaId}) ->
                            TargetStoreContent = atm_workflow_execution_test_runner:browse_store(
                                TargetStoreSchemaId, AtmTaskExecutionId, AtmMockCallCtx
                            ),
                            assert_exp_target_store_content(TargetStoreType, IteratedItems, TargetStoreContent)
                        end, [
                            {list, TargetListStoreSchemaId},
                            {single_value, TargetSingleValueStoreSchemaId},
                            {time_series, ?CURRENT_TASK_TIME_SERIES_STORE_SCHEMA_ID}
                        ])
                    end
                }
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable({1, 1}, ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_finished(ExpState1)}
                end
            }
        }]
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec gen_random_object_list() -> [json_utils:json_map()].
gen_random_object_list() ->
    lists_utils:generate(
        fun() -> #{?RAND_STR() => ?RAND_STR()} end,
        ?RAND_INT(30, 50)
    ).


%% @private
-spec gen_random_time_series_measurements() -> [json_utils:json_map()].
gen_random_time_series_measurements() ->
    lists_utils:generate(fun(_) ->
        #{
            <<"tsName">> => ?RAND_ELEMENT([<<"count_erl">>, <<"size">>, ?RAND_STR()]),
            <<"timestamp">> => ?RAND_ELEMENT([?NOW() - 100, ?NOW(), ?NOW() + 3700]),
            <<"value">> => ?RAND_INT(10000000)
        }
    end, 100).


%% @private
-spec assert_exp_target_store_content(
    automation:store_type(),
    [automation:item()],
    json_utils:json_term()
) ->
    ok.
assert_exp_target_store_content(audit_log, SrcListStoreContent, #{
    <<"logEntries">> := Logs,
    <<"isLast">> := true
}) ->
    LogContents = lists:sort(lists:map(
        fun(#{<<"content">> := LogContent}) -> LogContent end,
        Logs
    )),
    ?assertEqual(lists:sort(SrcListStoreContent), LogContents);

assert_exp_target_store_content(list, SrcListStoreContent, #{
    <<"items">> := Items,
    <<"isLast">> := true
}) ->
    ?assertEqual(
        lists:sort(SrcListStoreContent),
        lists:sort(extract_value_from_infinite_log_entry(Items))
    );

assert_exp_target_store_content(range, SrcListStoreContent, Range) ->
    ?assert(lists:member(Range, SrcListStoreContent));

assert_exp_target_store_content(single_value, SrcListStoreContent, #{
    <<"success">> := true,
    <<"value">> := Value
}) ->
    ?assert(lists:member(Value, SrcListStoreContent));

assert_exp_target_store_content(time_series, SrcListStoreContent, #{<<"slice">> := Slice}) ->
    ExpSlice = lists:foldl(fun
        (#{
            <<"tsName">> := <<"count_erl">>,
            <<"timestamp">> := Timestamp,
            <<"value">> := Value
        }, Acc) ->
            WindowsAcc = maps:get(<<"count_erl">>, Acc, #{}),

            Acc#{<<"count_erl">> => #{
                <<"minute">> => insert_window_to_metric_with_sum_aggregator(
                    ?EXP_MINUTE_METRIC_WINDOW(Timestamp, Value),
                    maps:get(<<"minute">>, WindowsAcc, [])
                ),
                <<"hour">> => insert_window_to_metric_with_sum_aggregator(
                    ?EXP_HOUR_METRIC_WINDOW(Timestamp, Value),
                    maps:get(<<"hour">>, WindowsAcc, [])
                ),
                <<"day">> => insert_window_to_metric_with_sum_aggregator(
                    ?EXP_DAY_METRIC_WINDOW(Timestamp, Value),
                    maps:get(<<"day">>, WindowsAcc, [])
                )
            }};

        (#{
            <<"tsName">> := <<"size">>,
            <<"timestamp">> := Timestamp,
            <<"value">> := Value
        }, Acc) ->
            WindowsAcc = maps:get(?MAX_FILE_SIZE_TS_NAME, Acc, #{}),

            Acc#{?MAX_FILE_SIZE_TS_NAME => #{
                ?MAX_FILE_SIZE_TS_NAME => insert_window_to_metric_with_max_aggregator(
                    ?MAX_FILE_SIZE_METRIC_WINDOW(Timestamp, Value),
                    maps:get(?MAX_FILE_SIZE_TS_NAME, WindowsAcc, [])
                )
            }};

        (_, Acc) ->
            % other measurements should be ignored as they are not mapped to any ts
            Acc
    end, #{}, SrcListStoreContent),

    ?assertEqual(ExpSlice, Slice);

assert_exp_target_store_content(tree_forest, SrcListStoreContent, #{
    <<"treeRoots">> := TreeRoots,
    <<"isLast">> := true
}) ->
    TreeRootFileIds = lists:sort(lists:map(
        fun(TreeRootValue) -> maps:with([<<"file_id">>], TreeRootValue) end,
        extract_value_from_infinite_log_entry(TreeRoots))
    ),
    ?assertEqual(lists:sort(SrcListStoreContent), TreeRootFileIds).


%% @private
-spec extract_value_from_infinite_log_entry(json_utils:json_map()) ->
    json_utils:json_map().
extract_value_from_infinite_log_entry(Entries) ->
    lists:map(fun(#{<<"value">> := Value}) -> Value end, Entries).


%% @private
-spec insert_window_to_metric_with_sum_aggregator(json_utils:json_map(), [json_utils:json_map()]) ->
    [json_utils:json_map()].
insert_window_to_metric_with_sum_aggregator(Window, []) ->
    [Window];

insert_window_to_metric_with_sum_aggregator(
    #{<<"value">> := Value1, <<"timestamp">> := Timestamp},
    [#{<<"value">> := Value2, <<"timestamp">> := Timestamp} | Tail]
) ->
    [#{<<"value">> => Value1 + Value2, <<"timestamp">> => Timestamp} | Tail];

insert_window_to_metric_with_sum_aggregator(
    Window = #{<<"timestamp">> := Timestamp1},
    [Head = #{<<"timestamp">> := Timestamp2} | Tail]
) when Timestamp2 > Timestamp1 ->
    [Head | insert_window_to_metric_with_sum_aggregator(Window, Tail)];

insert_window_to_metric_with_sum_aggregator(
    Window = #{<<"timestamp">> := Timestamp1},
    Windows = [#{<<"timestamp">> := Timestamp2} | _]
) when Timestamp2 < Timestamp1 ->
    [Window | Windows].


%% @private
-spec insert_window_to_metric_with_max_aggregator(json_utils:json_map(), [json_utils:json_map()]) ->
    [json_utils:json_map()].
insert_window_to_metric_with_max_aggregator(
    #{<<"value">> := Value1, <<"timestamp">> := Timestamp},
    [CurrentMaxWindow = #{<<"value">> := Value2, <<"timestamp">> := Timestamp}]
) when Value1 =< Value2 ->
    [CurrentMaxWindow];

insert_window_to_metric_with_max_aggregator(NewMaxWindow, _) ->
    [NewMaxWindow].
