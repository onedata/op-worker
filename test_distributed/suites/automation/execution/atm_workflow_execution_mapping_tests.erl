%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of mapping task arguments and results.
%%% NOTE: stores have dedicated test suites and as such only basic mapping
%%% cases are tested here (test cases check overall integration between
%%% various components rather than concrete one).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_mapping_tests).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").

-export([
    acquire_lambda_config/0,

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


-define(ITERATED_STORE_SCHEMA_ID, <<"iterated_st">>).
-define(ITERATED_LIST_STORE_SCHEMA_DRAFT(__ITEM_DATA_SPEC, __INITIAL_CONTENT),
    ?ATM_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, __ITEM_DATA_SPEC, __INITIAL_CONTENT)
).

-define(ATM_LAMBDA_CONFIG_PARAMETER_INT, <<"int">>).
-define(ATM_LAMBDA_CONFIG_PARAMETER_STR, <<"str">>).
-define(ATM_LAMBDA_CONFIG_PARAMETER_BOOL, <<"bool">>).

-define(ATM_WORKFLOW_SCHEMA_DRAFT(
    __TESTCASE,
    __DOCKER_IMAGES_ID,
    __STORE_SCHEMA_DRAFTS,
    __ITERATED_ITEM_DATA_SPEC,
    __LAMBDA_CONFIG_VALUES,
    __ARGUMENT_MAPPING,
    __RESULT_MAPPINGS
),
    #atm_workflow_schema_dump_draft{
        name = str_utils:to_binary(__TESTCASE),
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = __STORE_SCHEMA_DRAFTS,
            lanes = [#atm_lane_schema_draft{
                parallel_boxes = [#atm_parallel_box_schema_draft{tasks = [#atm_task_schema_draft{
                    lambda_id = ?ECHO_LAMBDA_ID,
                    lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
                    lambda_config = __LAMBDA_CONFIG_VALUES,
                    argument_mappings = __ARGUMENT_MAPPING,
                    result_mappings = __RESULT_MAPPINGS,
                    time_series_store_config = ?ATM_TS_STORE_CONFIG
                }]}],
                store_iterator_spec = #atm_store_iterator_spec_draft{
                    store_schema_id = ?ITERATED_STORE_SCHEMA_ID,
                    max_batch_size = ?RAND_INT(5, 8)
                }
            }]
        },
        supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
            ?ECHO_LAMBDA_REVISION_NUM => #atm_lambda_revision_draft{
                operation_spec = #atm_openfaas_operation_spec_draft{
                    docker_image = __DOCKER_IMAGES_ID
                },
                config_parameter_specs = [
                    #atm_parameter_spec{
                        name = ?ATM_LAMBDA_CONFIG_PARAMETER_INT,
                        data_spec = #atm_data_spec{
                            type = atm_number_type,
                            value_constraints = #{integers_only => true}
                        },
                        is_optional = true,
                        default_value = 0
                    },
                    #atm_parameter_spec{
                        name = ?ATM_LAMBDA_CONFIG_PARAMETER_STR,
                        data_spec = #atm_data_spec{type = atm_string_type},
                        is_optional = true,
                        default_value = <<>>
                    },
                    #atm_parameter_spec{
                        name = ?ATM_LAMBDA_CONFIG_PARAMETER_BOOL,
                        data_spec = #atm_data_spec{type = atm_boolean_type},
                        is_optional = true,
                        default_value = false
                    }
                ],
                argument_specs = [#atm_parameter_spec{
                    name = ?ECHO_ARG_NAME,
                    data_spec = __ITERATED_ITEM_DATA_SPEC,
                    is_optional = false
                }],
                result_specs = [#atm_lambda_result_spec{
                    name = ?ECHO_ARG_NAME,
                    data_spec = __ITERATED_ITEM_DATA_SPEC,
                    relay_method = ?RAND_ELEMENT([return_value, file_pipe])
                }]
            }
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


-define(NOW_SEC(), global_clock:timestamp_seconds()).


%%%===================================================================
%%% Tests
%%%===================================================================


acquire_lambda_config() ->
    IteratedItems = lists:map(fun(Num) -> #{<<"num">> => Num} end, lists:seq(1, 40)),
    TargetStoreSchemaId = <<"target_st">>,

    StoreSchemaDrafts = [
        ?ATM_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, ?ATM_OBJECT_DATA_SPEC, IteratedItems),
        ?ATM_LIST_STORE_SCHEMA_DRAFT(TargetStoreSchemaId, ?ATM_OBJECT_DATA_SPEC, [])
    ],
    ArgumentMappings = [#atm_task_schema_argument_mapper{
        argument_name = ?ECHO_ARG_NAME,
        value_builder = #atm_task_argument_value_builder{type = iterated_item}
    }],
    ResultMappings = [#atm_task_schema_result_mapper{
        result_name = ?ECHO_ARG_NAME,
        store_schema_id = TargetStoreSchemaId,
        store_content_update_options = #atm_list_store_content_update_options{function = append}
    }],

    AtmLambdaExecutionConfigValues = #{
        ?ATM_LAMBDA_CONFIG_PARAMETER_INT => ?RAND_INT(1000000),
        ?ATM_LAMBDA_CONFIG_PARAMETER_STR => ?RAND_STR()
    },
    % Atm lambda execution config is returned as lambda result
    % by ?ECHO_CONFIG_DOCKER_ID (see atm_openfaas_docker_mock.erl)
    % and then mapped to target store (by checking target store content
    % it is possible to check what config lambda received)
    ExpTargetStoreFinalContent = lists:map(fun(_) ->
        % Ensure default values are also supplemented
        AtmLambdaExecutionConfigValues#{?ATM_LAMBDA_CONFIG_PARAMETER_BOOL => false}
    end, IteratedItems),

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            ?FUNCTION_NAME,
            ?ECHO_CONFIG_DOCKER_ID,
            StoreSchemaDrafts,
            ?ATM_OBJECT_DATA_SPEC,
            AtmLambdaExecutionConfigValues,
            ArgumentMappings,
            ResultMappings
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{selector = {1, 1}}
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_hook = fun(AtmMockCallCtx) ->
                    TargetStoreContent = atm_workflow_execution_test_utils:browse_store(
                        TargetStoreSchemaId, undefined, AtmMockCallCtx
                    ),
                    assert_exp_target_store_content(list, ExpTargetStoreFinalContent, TargetStoreContent)
                end,
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_finished
                ]
            }
        }]
    }).


map_arguments() ->
    ConstValue = <<"makaron">>,

    SVStoreSchemaId = <<"sv">>,
    SVStoreContent = <<"ursus">>,

    IteratedItems = lists:map(fun(Num) -> #{<<"num">> => Num} end, lists:seq(1, 40)),

    TargetStoreSchemaId = <<"target_st">>,

    StoreSchemaDrafts = [
        ?ATM_SV_STORE_SCHEMA_DRAFT(SVStoreSchemaId, ?ATM_STRING_DATA_SPEC, SVStoreContent),
        ?ATM_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, ?ATM_OBJECT_DATA_SPEC, IteratedItems),
        ?ATM_LIST_STORE_SCHEMA_DRAFT(TargetStoreSchemaId, ?ATM_OBJECT_DATA_SPEC, [])
    ],
    ArgumentMappings = [#atm_task_schema_argument_mapper{
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
                    recipe = SVStoreSchemaId
                }
            }
        }
    }],
    ResultMappings = [#atm_task_schema_result_mapper{
        result_name = ?ECHO_ARG_NAME,
        store_schema_id = TargetStoreSchemaId,
        store_content_update_options = #atm_list_store_content_update_options{function = append}
    }],

    ExpTargetStoreFinalContent = lists:sort(lists:map(fun(#{<<"num">> := Num}) ->
        #{
            <<"const">> => ConstValue,
            <<"iterated">> => Num,
            <<"sv_content">> => SVStoreContent
        }
    end, IteratedItems)),

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            ?FUNCTION_NAME,
            ?ECHO_DOCKER_IMAGE_ID,
            StoreSchemaDrafts,
            ?ATM_OBJECT_DATA_SPEC,
            #{},
            ArgumentMappings,
            ResultMappings
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{selector = {1, 1}}
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_hook = fun(AtmMockCallCtx) ->
                    TargetStoreContent = atm_workflow_execution_test_utils:browse_store(
                        TargetStoreSchemaId, undefined, AtmMockCallCtx
                    ),
                    assert_exp_target_store_content(list, ExpTargetStoreFinalContent, TargetStoreContent)
                end,
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_finished
                ]
            }
        }]
    }).


map_results_to_audit_log_store() ->
    IteratedItemDataSpec = #atm_data_spec{type = atm_number_type},

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
        target_store_config = ?ATM_TS_STORE_CONFIG,
        target_store_update_options = #atm_time_series_store_content_update_options{
            dispatch_rules = ?CORRECT_ATM_TS_DISPATCH_RULES
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
            dispatch_rules = ?CORRECT_ATM_TS_DISPATCH_RULES
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
    ArgumentMappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
    ResultMappings = [#atm_task_schema_result_mapper{
        result_name = ?ECHO_ARG_NAME,
        store_schema_id = TargetStoreSchemaId,
        store_content_update_options = TargetStoreContentUpdateOptions
    }],

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            Testcase,
            ?ECHO_DOCKER_IMAGE_ID,
            StoreSchemaDrafts,
            IteratedItemDataSpec,
            #{},
            ArgumentMappings,
            ResultMappings
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                handle_task_execution_stopped = #atm_step_mock_spec{
                    after_step_hook = fun(AtmMockCallCtx = #atm_mock_call_ctx{
                        call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                    }) ->
                        TargetStoreContent = atm_workflow_execution_test_utils:browse_store(
                            TargetStoreSchemaId, AtmTaskExecutionId, AtmMockCallCtx
                        ),
                        assert_exp_target_store_content(TargetStoreType, IteratedItems, TargetStoreContent)
                    end
                }
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_finished
                ]
            }
        }]
    }).


map_results_to_multiple_stores() ->
    IteratedItems = gen_random_time_series_measurements(),

    TargetListStoreSchemaId = <<"target_list_store">>,
    TargetSVStoreSchemaId = <<"target_single_value_store">>,

    StoreSchemaDrafts = [
        ?ATM_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, ?ANY_MEASUREMENT_DATA_SPEC, IteratedItems),
        ?ATM_LIST_STORE_SCHEMA_DRAFT(TargetListStoreSchemaId, ?ANY_MEASUREMENT_DATA_SPEC, undefined),
        ?ATM_SV_STORE_SCHEMA_DRAFT(TargetSVStoreSchemaId, ?ANY_MEASUREMENT_DATA_SPEC, undefined)
    ],
    ArgumentMappings = [
        ?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)
    ],
    ResultMappings = [
        #atm_task_schema_result_mapper{
            result_name = ?ECHO_ARG_NAME,
            store_schema_id = TargetListStoreSchemaId,
            store_content_update_options = #atm_list_store_content_update_options{
                function = append
            }
        },
        #atm_task_schema_result_mapper{
            result_name = ?ECHO_ARG_NAME,
            store_schema_id = TargetSVStoreSchemaId,
            store_content_update_options = #atm_single_value_store_content_update_options{}
        },
        #atm_task_schema_result_mapper{
            result_name = ?ECHO_ARG_NAME,
            store_schema_id = ?CURRENT_TASK_TIME_SERIES_STORE_SCHEMA_ID,
            store_content_update_options = #atm_time_series_store_content_update_options{
                dispatch_rules = ?CORRECT_ATM_TS_DISPATCH_RULES
            }
        }
    ],

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            ?FUNCTION_NAME,
            ?ECHO_DOCKER_IMAGE_ID,
            StoreSchemaDrafts,
            ?ANY_MEASUREMENT_DATA_SPEC,
            #{},
            ArgumentMappings,
            ResultMappings
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                handle_task_execution_stopped = #atm_step_mock_spec{
                    after_step_hook = fun(AtmMockCallCtx = #atm_mock_call_ctx{
                        call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                    }) ->
                        lists:foreach(fun({TargetStoreType, TargetStoreSchemaId}) ->
                            TargetStoreContent = atm_workflow_execution_test_utils:browse_store(
                                TargetStoreSchemaId, AtmTaskExecutionId, AtmMockCallCtx
                            ),
                            assert_exp_target_store_content(TargetStoreType, IteratedItems, TargetStoreContent)
                        end, [
                            {list, TargetListStoreSchemaId},
                            {single_value, TargetSVStoreSchemaId},
                            {time_series, ?CURRENT_TASK_TIME_SERIES_STORE_SCHEMA_ID}
                        ])
                    end
                }
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_finished
                ]
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
            <<"timestamp">> => ?RAND_ELEMENT([?NOW_SEC() - 100, ?NOW_SEC(), ?NOW_SEC() + 3700]),
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
    Window = #{<<"value">> := Value1, <<"timestamp">> := Timestamp},
    [#{<<"value">> := Value2, <<"timestamp">> := Timestamp} | Tail]
) ->
    [Window#{<<"value">> => Value1 + Value2, <<"timestamp">> => Timestamp} | Tail];

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