%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bases for tests of iterating over list, range, single_value and tree_forest
%%% stores (iteration over audit_log and time_series is not supported in schema).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_iteration_test_base).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    iterate_over_list_store_test/0,
    iterate_over_list_store_with_some_inaccessible_items_test/0,
    iterate_over_list_store_with_all_items_inaccessible_test/0,
    iterate_over_empty_list_store_test/0,

    iterate_over_range_store_test/0,
    iterate_over_empty_range_store_test/0,

    iterate_over_single_value_store_test/0,
    iterate_over_single_value_store_with_all_items_inaccessible_test/0,
    iterate_over_empty_single_value_store_test/0,

    iterate_over_tree_forest_store_test/0,
    iterate_over_tree_forest_store_with_some_inaccessible_items_test/0,
    iterate_over_tree_forest_store_with_all_items_inaccessible_test/0,
    iterate_over_empty_tree_forest_store_test/0
]).


-define(FOREACH_TASK_DRAFT(__ID, __LAMBDA_ID, __LAMBDA_REVISION_NUM), #atm_task_schema_draft{
    id = __ID,
    lambda_id = __LAMBDA_ID,
    lambda_revision_number = __LAMBDA_REVISION_NUM,
    argument_mappings = [#atm_task_schema_argument_mapper{
        argument_name = <<"value">>,
        value_builder = #atm_task_argument_value_builder{
            type = iterated_item,
            recipe = undefined
        }
    }],
    result_mappings = []
}).

-define(FOREACH_WORKFLOW_SCHEMA_DRAFT(
    __STORE_TYPE, __STORE_CONFIG, __DEFAULT_INITIAL_CONTENT, __ITERATED_ITEM_DATA_SPEC
),
    #atm_workflow_schema_dump_draft{
        name = <<"echo">>,
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [
                #atm_store_schema_draft{
                    id = <<"st_1">>,
                    type = __STORE_TYPE,
                    config = __STORE_CONFIG,
                    requires_initial_content = false,
                    default_initial_content = __DEFAULT_INITIAL_CONTENT
                }
            ],
            lanes = [#atm_lane_schema_draft{
                parallel_boxes = [
                    #atm_parallel_box_schema_draft{
                        id = <<"pb1">>,
                        tasks = [
                            ?FOREACH_TASK_DRAFT(<<"t1">>, ?ECHO_LAMBDA_ID, ?ECHO_LAMBDA_REVISION_NUM),
                            ?FOREACH_TASK_DRAFT(<<"t2">>, ?ECHO_LAMBDA_ID, ?ECHO_LAMBDA_REVISION_NUM)
                        ]
                    },
                    #atm_parallel_box_schema_draft{
                        id = <<"pb2">>,
                        tasks = [?FOREACH_TASK_DRAFT(<<"t3">>, ?ECHO_LAMBDA_ID, ?ECHO_LAMBDA_REVISION_NUM)]
                    }
                ],
                store_iterator_spec = #atm_store_iterator_spec_draft{
                    store_schema_id = <<"st_1">>,
                    max_batch_size = ?RAND_INT(5, 8)
                }
            }]
        },
        supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
            ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(__ITERATED_ITEM_DATA_SPEC)
        }}
    }
).

-define(RANGE_FOREACH_WORKFLOW_SCHEMA_DRAFT(__INITIAL_CONTENT), ?FOREACH_WORKFLOW_SCHEMA_DRAFT(
    range,
    #atm_range_store_config{},
    __INITIAL_CONTENT,
    #atm_data_spec{type = atm_integer_type}
)).
-define(RANGE_FOREACH_WORKFLOW_SCHEMA_DRAFT, ?RANGE_FOREACH_WORKFLOW_SCHEMA_DRAFT(undefined)).

-define(FILE_DATA_SPEC, #atm_data_spec{type = atm_file_type}).


-define(NOW(), global_clock:timestamp_seconds()).


%%%===================================================================
%%% Tests
%%%===================================================================


iterate_over_list_store_test() ->
    InitialFiles = create_initial_files(),

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(
        list, InitialFiles, [], InitialFiles
    ).


iterate_over_list_store_with_some_inaccessible_items_test() ->
    InitialFiles = [DirObject | FileObjects] = create_initial_files(),
    FilesToRemove = lists_utils:random_sublist(FileObjects),

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(
        list, InitialFiles, FilesToRemove, [DirObject | FileObjects -- FilesToRemove]
    ).


iterate_over_list_store_with_all_items_inaccessible_test() ->
    InitialFiles = create_initial_files(),

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(
        list, InitialFiles, InitialFiles, []
    ).


iterate_over_empty_list_store_test() ->
    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(
        list, [], [], []
    ).


iterate_over_range_store_test() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?RANGE_FOREACH_WORKFLOW_SCHEMA_DRAFT(
            #{<<"start">> => -100, <<"end">> => 100, <<"step">> => 2}
        ),
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                process_item = #atm_step_mock_spec{
                    before_step_hook = build_validate_iterated_item_batches_fun(
                        ?FUNCTION_NAME,
                        lists:seq(-100, 99, 2)
                    )
                }
            }]
        }]
    }),
    assert_all_items_were_iterated(?FUNCTION_NAME).


iterate_over_empty_range_store_test() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?RANGE_FOREACH_WORKFLOW_SCHEMA_DRAFT(undefined),
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                handle_task_execution_ended = build_handle_task_execution_ended_mock_spec_for_skipped_tasks()
            }]
        }]
    }).


iterate_over_single_value_store_test() ->
    [DirObject | _] = create_initial_files(),

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(
        single_value, DirObject, [], [DirObject]
    ).


iterate_over_single_value_store_with_all_items_inaccessible_test() ->
    [DirObject | _] = create_initial_files(),

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(
        single_value, DirObject, [DirObject], []
    ).


iterate_over_empty_single_value_store_test() ->
    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(
        single_value, undefined, [], []
    ).


iterate_over_tree_forest_store_test() ->
    InitialFiles = [DirObject, A | FileObjects] = create_initial_files(),
    ExpIteratedFiles = lists:flatten([A, DirObject#object.children, DirObject, FileObjects]),

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(
        tree_forest, InitialFiles, [], ExpIteratedFiles
    ).


iterate_over_tree_forest_store_with_some_inaccessible_items_test() ->
    InitialFiles = [DirObject | FileObjects] = create_initial_files(),
    FilesToRemove = [DirObject | lists_utils:random_sublist(FileObjects)],

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(
        tree_forest, InitialFiles, FilesToRemove, InitialFiles -- FilesToRemove
    ).


iterate_over_tree_forest_store_with_all_items_inaccessible_test() ->
    InitialFiles = create_initial_files(),

    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(
        tree_forest, InitialFiles, InitialFiles, []
    ).


iterate_over_empty_tree_forest_store_test() ->
    iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(
        tree_forest, [], [], []
    ).


%% @private
-spec iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(
    automation:store_type(),
    undefined | onenv_file_test_utils:object() | [onenv_file_test_utils:object()],
    [onenv_file_test_utils:object()],
    [onenv_file_test_utils:object()]
) ->
    ok.
iterate_over_file_keeping_store_with_some_inaccessible_files_test_base(
    AtmStoreType,
    InitialFiles,
    FilesToRemove,
    ExpIteratedFiles
) ->
    TestCaseMarker = ?RAND_STR(),
    InitialContent = case InitialFiles of
        undefined -> undefined;
        #object{} -> file_object_to_atm_file_value(InitialFiles);
        _ when is_list(InitialFiles) -> lists:map(fun file_object_to_atm_file_value/1, InitialFiles)
    end,
    ExpIteratedEntries = lists:map(fun file_object_to_atm_file_value/1, ExpIteratedFiles),

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?FOREACH_WORKFLOW_SCHEMA_DRAFT(
            AtmStoreType,
            case AtmStoreType of
                list -> #atm_list_store_config{item_data_spec = ?FILE_DATA_SPEC};
                single_value -> #atm_single_value_store_config{item_data_spec = ?FILE_DATA_SPEC};
                tree_forest -> #atm_tree_forest_store_config{item_data_spec = ?FILE_DATA_SPEC}
            end,
            InitialContent,
            ?FILE_DATA_SPEC
        ),
        workflow_schema_revision_num = 1,
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
                process_item = #atm_step_mock_spec{
                    before_step_hook = build_validate_iterated_item_batches_fun(
                        TestCaseMarker,
                        ExpIteratedEntries,
                        fun filter_out_everything_but_file_id_from_atm_file_value/1
                    )
                },
                handle_task_execution_ended = case ExpIteratedFiles of
                    [] -> build_handle_task_execution_ended_mock_spec_for_skipped_tasks();
                    _ -> #atm_step_mock_spec{}
                end
            }]
        }]
    }),
    assert_all_items_were_iterated(TestCaseMarker).


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
-spec build_handle_task_execution_ended_mock_spec_for_skipped_tasks() ->
    atm_execution_test_runner:step_mock_spec().
build_handle_task_execution_ended_mock_spec_for_skipped_tasks() ->
    #atm_step_mock_spec{
        after_step_exp_state_diff = fun(#atm_mock_call_ctx{
            workflow_execution_exp_state = ExpState0,
            call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
        }) ->
            % No job was ever executed so lane run transit to active status only now
            ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_lane_run_moved_to_active_status_if_was_in_enqueued_status(
                AtmTaskExecutionId, atm_workflow_execution_exp_state_builder:expect_task_skipped(
                    AtmTaskExecutionId, ExpState0
                )
            ),
            {true, case atm_workflow_execution_exp_state_builder:get_task_selector(AtmTaskExecutionId, ExpState1) of
                {_, <<"pb1">>, _} ->
                    % parallel box with 2 tasks: it should transit to active
                    % when first task ended and skipped after second did
                    InferStatusFun = fun
                        (<<"pending">>, [<<"pending">>, <<"skipped">>]) -> <<"active">>;
                        (<<"active">>, [<<"skipped">>]) -> <<"skipped">>
                    end,
                    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_moved_to_inferred_status(
                        AtmTaskExecutionId, InferStatusFun, ExpState1
                    );
                {_, <<"pb2">>, _} ->
                    % parallel box with only 1 task - should transit to skipped status
                    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_moved_to_inferred_status(
                        AtmTaskExecutionId, fun(_, _) -> <<"skipped">> end, ExpState1
                    )
            end}
        end
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
-spec build_validate_iterated_item_batches_fun(term(), [automation:item()]) ->
    atm_workflow_execution_test_runner:hook().
build_validate_iterated_item_batches_fun(TestCaseMarker, ExpItems) ->
    build_validate_iterated_item_batches_fun(TestCaseMarker, ExpItems, fun(Item) -> Item end).


%% @private
-spec build_validate_iterated_item_batches_fun(
    term(),
    [automation:item()],
    fun((automation:item()) -> automation:item())
) ->
    atm_workflow_execution_test_runner:hook().
build_validate_iterated_item_batches_fun(TestCaseMarker, ExpItems, Mapper) ->
    fun(#atm_mock_call_ctx{
        workflow_execution_exp_state = ExpState,
        call_args = [
            _AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId,
            ItemBatch, _ReportResultUrl, _HeartbeatUrl
        ]
    }) ->
        MappedItemBatch = lists:map(Mapper, ItemBatch),
        {_, _, AtmTaskSchemaId} = atm_workflow_execution_exp_state_builder:get_task_selector(
            AtmTaskExecutionId, ExpState
        ),
        CacheKey = {TestCaseMarker, AtmTaskSchemaId},

        LeftoverExpItems = node_cache:get(CacheKey, ExpItems),

        case lists:all(fun(Item) -> lists:member(Item, LeftoverExpItems) end, MappedItemBatch) of
            true ->
                node_cache:put(CacheKey, LeftoverExpItems -- MappedItemBatch);
            false ->
                ct:pal("Expected items: ~p~nGot: ~p", [LeftoverExpItems, MappedItemBatch]),
                ?assert(false)
        end
    end.


%% @private
-spec assert_all_items_were_iterated(term()) -> ok.
assert_all_items_were_iterated(TestCaseMarker) ->
    lists:foreach(fun(AtmTaskSchemaId) ->
        CacheKey = {TestCaseMarker, AtmTaskSchemaId},
        ?assertEqual([], node_cache:get(CacheKey, []))
    end, [<<"t1">>, <<"t2">>, <<"t3">>]).
