%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of iterating over list, range, single_value and tree_forest stores
%%% (iteration over audit_log and time_series is not supported in schema).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_iteration_tests).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").

-export([
    iterate_over_list_store/0,
    %% TODO VFS-10854 fix after introducing exception store not validating input
    iterate_over_list_store_with_some_inaccessible_items/0,
    %% TODO VFS-10854 fix after introducing exception store not validating input
    iterate_over_list_store_with_all_items_inaccessible/0,
    iterate_over_empty_list_store/0,

    iterate_over_range_store/0,
    iterate_over_empty_range_store/0,

    iterate_over_single_value_store/0,
    %% TODO VFS-10854 fix after introducing exception store not validating input
    iterate_over_single_value_store_with_all_items_inaccessible/0,
    iterate_over_empty_single_value_store/0,

    iterate_over_tree_forest_store/0,
    iterate_over_tree_forest_store_with_some_inaccessible_items/0,
    iterate_over_tree_forest_store_with_all_items_inaccessible/0,
    iterate_over_empty_tree_forest_store/0
]).

-record(iterate_over_file_store_test_spec, {
    testcase :: atom(),
    store_type :: automation:store_type(),
    initial_files :: [onenv_file_test_utils:object()],
    % atm_file_type values are references to file entity in op. When those files
    % are removed the references for them should be omitted during iteration
    files_to_remove_before_iteration_starts :: [onenv_file_test_utils:object()],
    exp_iterated_files :: [onenv_file_test_utils:object()]
}).
-type iterate_over_file_store_test_spec() :: #iterate_over_file_store_test_spec{}.

-define(ITERATED_STORE_SCHEMA_ID, <<"iterated_st">>).

-define(FOREACH_TASK_DRAFT(__ID), #atm_task_schema_draft{
    id = __ID,
    lambda_id = ?ECHO_LAMBDA_ID,
    lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
    argument_mappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
    result_mappings = []
}).

-define(FOREACH_WORKFLOW_SCHEMA_DRAFT(
    __TESTCASE, __ITERATED_STORE_SCHEMA_DRAFT, __ITERATED_ITEM_DATA_SPEC
),
    #atm_workflow_schema_dump_draft{
        name = str_utils:to_binary(__TESTCASE),
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [__ITERATED_STORE_SCHEMA_DRAFT],
            lanes = [#atm_lane_schema_draft{
                parallel_boxes = [
                    #atm_parallel_box_schema_draft{
                        id = <<"pb1">>,
                        tasks = [
                            ?FOREACH_TASK_DRAFT(<<"task1">>),
                            ?FOREACH_TASK_DRAFT(<<"task2">>)
                        ]
                    },
                    #atm_parallel_box_schema_draft{
                        id = <<"pb2">>,
                        tasks = [?FOREACH_TASK_DRAFT(<<"task3">>)]
                    }
                ],
                store_iterator_spec = #atm_store_iterator_spec_draft{
                    store_schema_id = ?ITERATED_STORE_SCHEMA_ID,
                    max_batch_size = ?RAND_INT(5, 8)
                },
                max_retries = 0
            }]
        },
        supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
            ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(__ITERATED_ITEM_DATA_SPEC)
        }}
    }
).


-define(CACHE_KEY(__TESTCASE, __ATM_TASK_SCHEMA_ID), {__TESTCASE, __ATM_TASK_SCHEMA_ID}).


%%%===================================================================
%%% Tests
%%%===================================================================


iterate_over_list_store() ->
    InitialFiles = create_initial_files(),

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(#iterate_over_file_store_test_spec{
        testcase = ?FUNCTION_NAME,
        store_type = list,
        initial_files = InitialFiles,
        files_to_remove_before_iteration_starts = [],
        exp_iterated_files = InitialFiles
    }).


iterate_over_list_store_with_some_inaccessible_items() ->
    InitialFiles = [DirObject | FileObjects] = create_initial_files(),
    FilesToRemove = lists_utils:random_sublist(FileObjects),

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(#iterate_over_file_store_test_spec{
        testcase = ?FUNCTION_NAME,
        store_type = list,
        initial_files = InitialFiles,
        files_to_remove_before_iteration_starts = FilesToRemove,
        exp_iterated_files = [DirObject | FileObjects -- FilesToRemove]
    }).


iterate_over_list_store_with_all_items_inaccessible() ->
    InitialFiles = create_initial_files(),

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(#iterate_over_file_store_test_spec{
        testcase = ?FUNCTION_NAME,
        store_type = list,
        initial_files = InitialFiles,
        files_to_remove_before_iteration_starts = InitialFiles,
        exp_iterated_files = []
    }).


iterate_over_empty_list_store() ->
    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(#iterate_over_file_store_test_spec{
        testcase = ?FUNCTION_NAME,
        store_type = list,
        initial_files = [],
        files_to_remove_before_iteration_starts = [],
        exp_iterated_files = []
    }).


iterate_over_range_store() ->
    Range = #{<<"start">> => -100, <<"end">> => 100, <<"step">> => 2},

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?FOREACH_WORKFLOW_SCHEMA_DRAFT(
            ?FUNCTION_NAME,
            ?ATM_RANGE_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, Range),
            ?ATM_NUMBER_DATA_SPEC
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                run_task_for_item = #atm_step_mock_spec{
                    before_step_hook = build_record_iterated_items_hook(?FUNCTION_NAME)
                }
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_finished
                ]
            }
        }]
    }),
    assert_all_items_were_iterated(?FUNCTION_NAME, lists:seq(-100, 99, 2)).


iterate_over_empty_range_store() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?FOREACH_WORKFLOW_SCHEMA_DRAFT(
            ?FUNCTION_NAME,
            ?ATM_RANGE_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, undefined),
            ?ATM_NUMBER_DATA_SPEC
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                handle_task_execution_stopped = build_handle_task_execution_stopped_mock_spec_for_skipped_tasks()
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_finished
                ]
            }
        }]
    }).


iterate_over_single_value_store() ->
    [DirObject | _] = create_initial_files(),

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(#iterate_over_file_store_test_spec{
        testcase = ?FUNCTION_NAME,
        store_type = single_value,
        initial_files = DirObject,
        files_to_remove_before_iteration_starts = [],
        exp_iterated_files = [DirObject]
    }).


iterate_over_single_value_store_with_all_items_inaccessible() ->
    [DirObject | _] = create_initial_files(),

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(#iterate_over_file_store_test_spec{
        testcase = ?FUNCTION_NAME,
        store_type = single_value,
        initial_files = DirObject,
        files_to_remove_before_iteration_starts = [DirObject],
        exp_iterated_files = []
    }).


iterate_over_empty_single_value_store() ->
    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(#iterate_over_file_store_test_spec{
        testcase = ?FUNCTION_NAME,
        store_type = single_value,
        initial_files = undefined,
        files_to_remove_before_iteration_starts = [],
        exp_iterated_files = []
    }).


iterate_over_tree_forest_store() ->
    InitialFiles = [DirObject, A | FileObjects] = create_initial_files(),

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(#iterate_over_file_store_test_spec{
        testcase = ?FUNCTION_NAME,
        store_type = tree_forest,
        initial_files = InitialFiles,
        files_to_remove_before_iteration_starts = [],
        exp_iterated_files = lists:flatten([A, DirObject#object.children, DirObject, FileObjects])
    }).


iterate_over_tree_forest_store_with_some_inaccessible_items() ->
    InitialFiles = [DirObject | FileObjects] = create_initial_files(),
    FilesToRemove = [DirObject | lists_utils:random_sublist(FileObjects)],

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(#iterate_over_file_store_test_spec{
        testcase = ?FUNCTION_NAME,
        store_type = tree_forest,
        initial_files = InitialFiles,
        files_to_remove_before_iteration_starts = FilesToRemove,
        exp_iterated_files = InitialFiles -- FilesToRemove
    }).


iterate_over_tree_forest_store_with_all_items_inaccessible() ->
    InitialFiles = create_initial_files(),

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(#iterate_over_file_store_test_spec{
        testcase = ?FUNCTION_NAME,
        store_type = tree_forest,
        initial_files = InitialFiles,
        files_to_remove_before_iteration_starts = InitialFiles,
        exp_iterated_files = []
    }).


iterate_over_empty_tree_forest_store() ->
    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(#iterate_over_file_store_test_spec{
        testcase = ?FUNCTION_NAME,
        store_type = tree_forest,
        initial_files = [],
        files_to_remove_before_iteration_starts = [],
        exp_iterated_files = []
    }).


%% @private
-spec iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(iterate_over_file_store_test_spec()) ->
    ok.
iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(TestSpec = #iterate_over_file_store_test_spec{
    testcase = Testcase,
    store_type = AtmStoreType,
    files_to_remove_before_iteration_starts = [_ | _] = FilesToRemove,
    exp_iterated_files = ExpIteratedFiles
}) when
    AtmStoreType =:= list;
    AtmStoreType =:= single_value
->
    AtmFileDataSpec = build_file_data_spec(),
    AtmStoreInitialContent = build_store_input(TestSpec),
    AtmStoreSchemaDraft = build_store_schema(AtmStoreType, AtmFileDataSpec, AtmStoreInitialContent),

    ExpIteratedEntries = lists:map(fun file_object_to_atm_file_value/1, ExpIteratedFiles),

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?FOREACH_WORKFLOW_SCHEMA_DRAFT(
            Testcase, AtmStoreSchemaDraft, AtmFileDataSpec
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                prepare_lane = #atm_step_mock_spec{
                    before_step_hook = fun(_MockCallCtx) ->
                        lists_utils:pforeach(fun(#object{guid = Guid}) ->
                            onenv_file_test_utils:rm_and_sync_file(user1, Guid)
                        end, FilesToRemove)
                    end
                },
                run_task_for_item = #atm_step_mock_spec{
                    before_step_hook = build_record_iterated_items_hook(
                        Testcase,
                        fun filter_out_everything_but_file_id_from_atm_file_value/1
                    ),
                    after_step_exp_state_diff = atm_workflow_execution_test_utils:build_task_step_exp_state_diff(#{
                        [<<"task1">>, <<"task2">>] => fun(#atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState,
                            call_args = [_, _, AtmTaskExecutionId, _, ItemBatch]
                        }) ->
                            {true, atm_workflow_execution_exp_state_builder:expect(ExpState, [
                                {task, AtmTaskExecutionId, items_scheduled, length(ItemBatch)},
                                {task, AtmTaskExecutionId, items_failed, length(ItemBatch)}
                            ])}
                        end
                    })
                },
                handle_task_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = atm_workflow_execution_test_utils:build_task_step_exp_state_diff(#{
                        [<<"task1">>, <<"task2">>] => [{task, ?TASK_ID_PLACEHOLDER, failed}],
                        <<"task3">> => [{task, ?TASK_ID_PLACEHOLDER, skipped}]
                    })
                },

                handle_lane_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        {lane_run, {1, 1}, failed},
                        workflow_stopping
                    ]
                }
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    workflow_failed
                ]
            }
        }]
    }),
    assert_all_items_were_iterated(Testcase, ExpIteratedEntries);

iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(TestSpec = #iterate_over_file_store_test_spec{
    testcase = Testcase,
    store_type = AtmStoreType,
    files_to_remove_before_iteration_starts = FilesToRemove,
    exp_iterated_files = ExpIteratedFiles
}) ->
    AtmFileDataSpec = build_file_data_spec(),
    AtmStoreInitialContent = build_store_input(TestSpec),
    AtmStoreSchemaDraft = build_store_schema(AtmStoreType, AtmFileDataSpec, AtmStoreInitialContent),
    ExpIteratedEntries = lists:map(fun file_object_to_atm_file_value/1, ExpIteratedFiles),

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?FOREACH_WORKFLOW_SCHEMA_DRAFT(
            Testcase, AtmStoreSchemaDraft, AtmFileDataSpec
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                prepare_lane = #atm_step_mock_spec{
                    before_step_hook = fun(_MockCallCtx) ->
                        lists_utils:pforeach(fun(#object{guid = Guid}) ->
                            onenv_file_test_utils:rm_and_sync_file(user1, Guid)
                        end, FilesToRemove)
                    end
                },
                run_task_for_item = #atm_step_mock_spec{
                    before_step_hook = build_record_iterated_items_hook(
                        Testcase,
                        fun filter_out_everything_but_file_id_from_atm_file_value/1
                    )
                },
                handle_task_execution_stopped = case ExpIteratedFiles of
                    [] -> build_handle_task_execution_stopped_mock_spec_for_skipped_tasks();
                    _ -> #atm_step_mock_spec{}
                end
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_finished
                ]
            }
        }]
    }),
    assert_all_items_were_iterated(Testcase, ExpIteratedEntries).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_initial_files() -> [onenv_file_test_utils:object()].
create_initial_files() ->
    onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE_SELECTOR, lists:flatten([
        #dir_spec{children = [
            #file_spec{name = <<"file1">>},
            #file_spec{name = <<"file2">>},
            #file_spec{name = <<"file3">>}
        ]},
        lists_utils:generate(fun() -> #file_spec{} end, 30)
    ])).


%% @private
build_file_data_spec() ->
    #atm_file_data_spec{
        file_type = 'ANY',
        attributes = lists:usort([file_id | ?RAND_SUBLIST(?ATM_FILE_ATTRIBUTES)])
    }.


%% @private
build_store_input(#iterate_over_file_store_test_spec{initial_files = undefined}) ->
    undefined;
build_store_input(#iterate_over_file_store_test_spec{initial_files = InitialFile = #object{}}) ->
    file_object_to_atm_file_value(InitialFile);
build_store_input(#iterate_over_file_store_test_spec{initial_files = InitialFiles}) ->
    lists:map(fun file_object_to_atm_file_value/1, InitialFiles).


%% @private
build_store_schema(list, AtmFileDataSpec, AtmStoreInput) ->
    ?ATM_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, AtmFileDataSpec, AtmStoreInput);
build_store_schema(single_value, AtmFileDataSpec, AtmStoreInput) ->
    ?ATM_SV_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, AtmFileDataSpec, AtmStoreInput);
build_store_schema(tree_forest, AtmFileDataSpec, AtmStoreInput) ->
    ?ATM_TREE_FOREST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, AtmFileDataSpec, AtmStoreInput).


%% @private
-spec build_handle_task_execution_stopped_mock_spec_for_skipped_tasks() ->
    atm_execution_test_runner:step_mock_spec().
build_handle_task_execution_stopped_mock_spec_for_skipped_tasks() ->
    #atm_step_mock_spec{
        after_step_exp_state_diff = atm_workflow_execution_test_utils:build_task_step_exp_state_diff(#{
            [<<"task1">>, <<"task2">>, <<"task3">>] => [
                % No job was ever executed so lane run transitions to active status just now
                {lane_run, {1, 1}, active},
                {task, ?TASK_ID_PLACEHOLDER, skipped}
            ]
        })
    }.


%% @private
-spec file_object_to_atm_file_value(onenv_file_test_utils:object()) -> automation:item().
file_object_to_atm_file_value(#object{guid = Guid}) ->
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),
    #{<<"file_id">> => ObjectId}.


%% @private
-spec filter_out_everything_but_file_id_from_atm_file_value(automation:item()) -> automation:item().
filter_out_everything_but_file_id_from_atm_file_value(AtmFileValue) ->
    maps:with([<<"file_id">>], AtmFileValue).


%% @private
-spec build_record_iterated_items_hook(term()) ->
    atm_workflow_execution_test_runner:hook().
build_record_iterated_items_hook(Testcase) ->
    build_record_iterated_items_hook(Testcase, fun(Item) -> Item end).


%% @private
-spec build_record_iterated_items_hook(
    term(),
    fun((automation:item()) -> automation:item())
) ->
    atm_workflow_execution_test_runner:hook().
build_record_iterated_items_hook(Testcase, Mapper) ->
    fun(#atm_mock_call_ctx{
        workflow_execution_exp_state = ExpState,
        call_args = [_, _, AtmTaskExecutionId, _, ItemBatch]
    }) ->
        record_iterated_items(
            Testcase,
            atm_workflow_execution_exp_state_builder:get_task_schema_id(AtmTaskExecutionId, ExpState),
            lists:map(Mapper, ItemBatch)
        )
    end.


%% @private
-spec assert_all_items_were_iterated(term(), [automation:item()]) -> ok.
assert_all_items_were_iterated(Testcase, ExpIteratedItems) ->
    lists:foreach(fun(AtmTaskSchemaId) ->
        ?assertEqual(
            lists:sort(ExpIteratedItems),
            get_recorded_iterated_items(Testcase, AtmTaskSchemaId)
        )
    end, [<<"task1">>, <<"task2">>, <<"task3">>]).


%% @private
-spec record_iterated_items(term(), automation:id(), [automation:item()]) -> ok.
record_iterated_items(TestCaseMarker, AtmTaskSchemaId, NewIteratedItems) ->
    Key = ?CACHE_KEY(TestCaseMarker, AtmTaskSchemaId),
    PrevIteratedItems = node_cache:get(Key, []),
    node_cache:put(Key, NewIteratedItems ++ PrevIteratedItems).


%% @private
-spec get_recorded_iterated_items(term(), automation:id()) -> [automation:item()].
get_recorded_iterated_items(TestCaseMarker, AtmTaskSchemaId) ->
    lists:sort(node_cache:get(?CACHE_KEY(TestCaseMarker, AtmTaskSchemaId), [])).
