%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for use in atm workflow execution tests.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_test_utils).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("modules/automation/atm_execution.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([
    cancel_workflow_execution/1,
    pause_workflow_execution/1,

    delete_offline_session/1,
    report_openfaas_unhealthy/1,
    interrupt_workflow_execution/1,

    stop_workflow_execution/2,

    resume_workflow_execution/1,
    force_continue_workflow_execution/1,
    repeat_workflow_execution/3,

    discard_workflow_execution/1
]).
-export([
    scan_audit_log/3, scan_audit_log/4,
    browse_store/2, browse_store/3,
    get_exception_store_content/2
]).
-export([
    assert_ended_workflow_execution_can_be_neither_stopped_nor_resumed/1,
    assert_not_stopped_workflow_execution_can_not_be_repeated_resumed_nor_discarded/2,
    assert_not_ended_workflow_execution_can_not_be_repeated/2
]).
-export([
    build_task_step_exp_state_diff/1,
    build_task_step_hook/1,
    build_task_step_strategy/1
]).
-export([get_values_batch/1, item_batch_to_json/1, item_to_json/1]).


-define(INFINITE_LOG_BASED_STORES_LISTING_OPTS, #{
    start_from => undefined,
    offset => 0,
    limit => 1000000000
}).


%%%===================================================================
%%% API
%%%===================================================================


-spec cancel_workflow_execution(atm_workflow_execution_test_runner:mock_call_ctx()) ->
    ok | no_return().
cancel_workflow_execution(#atm_mock_call_ctx{
    provider = ProviderSelector,
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, mi_atm:init_cancel_workflow_execution(SessionId, AtmWorkflowExecutionId)).


-spec pause_workflow_execution(atm_workflow_execution_test_runner:mock_call_ctx()) ->
    ok | no_return().
pause_workflow_execution(#atm_mock_call_ctx{
    provider = ProviderSelector,
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, mi_atm:init_pause_workflow_execution(SessionId, AtmWorkflowExecutionId)).


-spec delete_offline_session(atm_workflow_execution_test_runner:mock_call_ctx()) ->
    ok | no_return().
delete_offline_session(#atm_mock_call_ctx{
    provider = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, offline_access_manager:close_session(AtmWorkflowExecutionId)).


-spec report_openfaas_unhealthy(atm_workflow_execution_test_runner:mock_call_ctx()) ->
    ok | no_return().
report_openfaas_unhealthy(#atm_mock_call_ctx{
    provider = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, atm_workflow_execution_handler:on_openfaas_down(
        AtmWorkflowExecutionId, ?ERROR_ATM_OPENFAAS_UNHEALTHY
    )).


-spec interrupt_workflow_execution(atm_workflow_execution_test_runner:mock_call_ctx()) ->
    ok | no_return().
interrupt_workflow_execution(AtmMockCallCtx) ->
    stop_workflow_execution(interrupt, AtmMockCallCtx).


-spec stop_workflow_execution(
    atm_lane_execution:run_stopping_reason(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok | errors:error().
stop_workflow_execution(Reason, #atm_mock_call_ctx{
    provider = ProviderSelector,
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, atm_workflow_execution_handler:init_stop(
        user_ctx:new(SessionId), AtmWorkflowExecutionId, Reason
    )).


-spec resume_workflow_execution(atm_workflow_execution_test_runner:mock_call_ctx()) ->
    ok.
resume_workflow_execution(#atm_mock_call_ctx{
    provider = ProviderSelector,
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, mi_atm:resume_workflow_execution(SessionId, AtmWorkflowExecutionId)).


-spec force_continue_workflow_execution(atm_workflow_execution_test_runner:mock_call_ctx()) ->
    ok.
force_continue_workflow_execution(#atm_mock_call_ctx{
    provider = ProviderSelector,
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, mi_atm:force_continue_workflow_execution(SessionId, AtmWorkflowExecutionId)).


-spec repeat_workflow_execution(
    atm_workflow_execution:repeat_type(),
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok.
repeat_workflow_execution(RepeatType, AtmLaneRunSelector, #atm_mock_call_ctx{
    provider = ProviderSelector,
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, mi_atm:repeat_workflow_execution(
        SessionId, RepeatType, AtmWorkflowExecutionId, AtmLaneRunSelector
    )).


-spec discard_workflow_execution(atm_workflow_execution_test_runner:mock_call_ctx()) ->
    ok | errors:error().
discard_workflow_execution(#atm_mock_call_ctx{
    provider = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, atm_workflow_execution_api:discard(AtmWorkflowExecutionId)).


-spec scan_audit_log(
    automation:id(),
    atm_workflow_execution_test_runner:mock_call_ctx(),
    fun((json_utils:json_term()) -> boolean())
) ->
    boolean().
scan_audit_log(AtmAuditLogStoreSchemaId, AtmMockCallCtx, PredFun) ->
    scan_audit_log(AtmAuditLogStoreSchemaId, undefined, AtmMockCallCtx, PredFun).


-spec scan_audit_log(
    automation:id(),
    undefined | atm_task_execution:id(),
    atm_workflow_execution_test_runner:mock_call_ctx(),
    fun((json_utils:json_term()) -> boolean())
) ->
    boolean().
scan_audit_log(AtmAuditLogStoreSchemaId, AtmWorkflowExecutionComponentSelector, AtmMockCallCtx, PredFun) ->
    #{<<"logEntries">> := LogEntries} = ?assertMatch(
        #{<<"isLast">> := true, <<"logEntries">> := _},
        browse_store(AtmAuditLogStoreSchemaId, AtmWorkflowExecutionComponentSelector, AtmMockCallCtx)
    ),
    lists_utils:foldl_while(fun(Entry, Acc) ->
        case PredFun(Entry) orelse Acc of
            true -> {halt, true};
            false -> {cont, false}
        end
    end, false, LogEntries).


-spec browse_store(automation:id(), atm_workflow_execution_test_runner:mock_call_ctx()) ->
    json_utils:json_term().
browse_store(AtmStoreSchemaId, AtmMockCallCtx) ->
    browse_store(AtmStoreSchemaId, undefined, AtmMockCallCtx).


-spec browse_store
    (exception_store, atm_lane_execution:lane_run_selector(), atm_workflow_execution_test_runner:mock_call_ctx()) ->
        json_utils:json_term();
    (automation:id(), undefined | atm_task_execution:id(), atm_workflow_execution_test_runner:mock_call_ctx()) ->
        json_utils:json_term().
browse_store(AtmStoreSchemaId, AtmWorkflowExecutionComponentSelector, AtmMockCallCtx = #atm_mock_call_ctx{
    provider = ProviderSelector,
    space = SpaceSelector,
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    SpaceId = oct_background:get_space_id(SpaceSelector),
    AtmStoreId = get_store_id(AtmStoreSchemaId, AtmWorkflowExecutionComponentSelector, AtmMockCallCtx),
    ?rpc(ProviderSelector, browse_store(SessionId, SpaceId, AtmWorkflowExecutionId, AtmStoreId)).


-spec get_exception_store_content(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    json_utils:json_term().
get_exception_store_content(AtmLaneRunSelector, AtmMockCallCtx) ->
    #{<<"items">> := Items, <<"isLast">> := true} = browse_store(
        exception_store, AtmLaneRunSelector, AtmMockCallCtx
    ),
    lists:map(fun(#{<<"value">> := Content}) -> Content end, Items).


-spec assert_ended_workflow_execution_can_be_neither_stopped_nor_resumed(
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok.
assert_ended_workflow_execution_can_be_neither_stopped_nor_resumed(AtmMockCallCtx) ->
    lists:foreach(fun(StoppingReason) ->
        ?assertEqual(?ERROR_ATM_WORKFLOW_EXECUTION_ENDED, stop_workflow_execution(StoppingReason, AtmMockCallCtx))
    end, ?STOPPING_REASONS),

    ?assertThrow(?ERROR_ATM_WORKFLOW_EXECUTION_NOT_RESUMABLE, resume_workflow_execution(AtmMockCallCtx)).


-spec assert_not_stopped_workflow_execution_can_not_be_repeated_resumed_nor_discarded(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok.
assert_not_stopped_workflow_execution_can_not_be_repeated_resumed_nor_discarded(AtmLaneRunSelector, AtmMockCallCtx) ->
    ?assertThrow(?ERROR_ATM_WORKFLOW_EXECUTION_NOT_RESUMABLE, resume_workflow_execution(AtmMockCallCtx)),
    assert_not_ended_workflow_execution_can_not_be_repeated(AtmLaneRunSelector, AtmMockCallCtx),
    ?assertEqual(?ERROR_ATM_WORKFLOW_EXECUTION_NOT_STOPPED, discard_workflow_execution(AtmMockCallCtx)).


-spec assert_not_ended_workflow_execution_can_not_be_repeated(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok.
assert_not_ended_workflow_execution_can_not_be_repeated(AtmLaneRunSelector, AtmMockCallCtx) ->
    lists:foreach(fun(RepeatType) ->
        ?assertThrow(?ERROR_ATM_WORKFLOW_EXECUTION_NOT_ENDED, repeat_workflow_execution(
            RepeatType, AtmLaneRunSelector, AtmMockCallCtx
        ))
    end, [rerun, retry]).


%%--------------------------------------------------------------------
%% @doc
%% Builds exp_state_diff for task related steps accounting for various tasks
%% having potentially different expectations. As task execution id is unknown when
%% writing expectations it is allowed to use special placeholders:
%% - ?TASK_ID_PLACEHOLDER instead of concrete task execution id or selector
%% - ?PB_SELECTOR_PLACEHOLDER instead of parallel box selector
%% @end
%%--------------------------------------------------------------------
-spec build_task_step_exp_state_diff(
    #{automation:id() | [automation:id()] => no_diff | atm_workflow_execution_test_runner:exp_state_diff()}
) ->
    atm_workflow_execution_test_runner:exp_state_diff().
build_task_step_exp_state_diff(ExpectationsPerTask) ->
    fun(AtmMockCallCtx = #atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
        AtmTaskExecutionId = get_task_execution_id(AtmMockCallCtx),

        case get_task_expectations(ExpectationsPerTask, AtmTaskExecutionId, ExpState) of
            no_diff ->
                false;
            ExpectationsWithPlaceholders when is_list(ExpectationsWithPlaceholders) ->
                Expectations = substitute_expectation_placeholders(
                    ExpectationsWithPlaceholders, AtmTaskExecutionId
                ),
                {true, atm_workflow_execution_exp_state_builder:expect(ExpState, Expectations)};
            ExpStateDiffFun when is_function(ExpStateDiffFun) ->
                ExpStateDiffFun(AtmMockCallCtx)
        end
    end.


-spec build_task_step_hook(#{automation:id() => atm_workflow_execution_test_runner:hook()}) ->
    atm_workflow_execution_test_runner:hook().
build_task_step_hook(HooksPerTask) ->
    fun(AtmMockCallCtx = #atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
        AtmTaskSchemaId = atm_workflow_execution_exp_state_builder:get_task_schema_id(
            get_task_execution_id(AtmMockCallCtx),
            ExpState
        ),
        TaskStepHook = maps:get(AtmTaskSchemaId, HooksPerTask, fun(_) -> ok end),
        TaskStepHook(AtmMockCallCtx)
    end.


-spec build_task_step_strategy(#{automation:id() => atm_workflow_execution_test_runner:mock_strategy_spec()}) ->
    atm_workflow_execution_test_runner:mock_strategy_spec().
build_task_step_strategy(StrategiesPerTask) ->
    fun(AtmMockCallCtx = #atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
        AtmTaskSchemaId = atm_workflow_execution_exp_state_builder:get_task_schema_id(
            get_task_execution_id(AtmMockCallCtx),
            ExpState
        ),
        case maps:get(AtmTaskSchemaId, StrategiesPerTask, passthrough) of
            TaskStrategyFun when is_function(TaskStrategyFun, 1) -> TaskStrategyFun(AtmMockCallCtx);
            TaskStrategy -> TaskStrategy
        end
    end.


-spec get_values_batch([atm_workflow_execution_handler:item()]) -> [automation:item()].
get_values_batch(ItemBatch) ->
    lists:map(fun(Item) -> Item#atm_item_execution.value end, ItemBatch).


-spec item_batch_to_json([atm_workflow_execution_handler:item()]) -> [json_utils:json_map()].
item_batch_to_json(ItemBatch) ->
    lists:map(fun item_to_json/1, ItemBatch).


-spec item_to_json(atm_workflow_execution_handler:item()) -> json_utils:json_map().
item_to_json(#atm_item_execution{trace_id = TraceId, value = Value}) ->
    #{<<"traceId">> => TraceId, <<"value">> => Value}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_store_id
    (exception_store, atm_lane_execution:lane_run_selector(), atm_workflow_execution_test_runner:mock_call_ctx()) ->
        atm_store:id();
    (automation:id(), undefined | atm_task_execution:id(), atm_workflow_execution_test_runner:mock_call_ctx()) ->
        atm_store:id().
get_store_id(exception_store, AtmLaneRunSelector, #atm_mock_call_ctx{
    provider = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    {ok, #document{value = AtmWorkflowExecution}} = ?rpc(
        ProviderSelector, atm_workflow_execution:get(AtmWorkflowExecutionId)
    ),
    {ok, AtmLaneRun} = atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution),
    AtmLaneRun#atm_lane_execution_run.exception_store_id;

get_store_id(?WORKFLOW_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID, _, #atm_mock_call_ctx{
    provider = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    {ok, #document{value = #atm_workflow_execution{system_audit_log_store_id = AtmStoreId}}} = ?rpc(
        ProviderSelector, atm_workflow_execution:get(AtmWorkflowExecutionId)
    ),
    AtmStoreId;

get_store_id(?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID, AtmTaskExecutionId, #atm_mock_call_ctx{
    provider = ProviderSelector
}) ->
    {ok, #document{value = #atm_task_execution{system_audit_log_store_id = AtmStoreId}}} = ?rpc(
        ProviderSelector, atm_task_execution:get(AtmTaskExecutionId)
    ),
    AtmStoreId;

get_store_id(?CURRENT_TASK_TIME_SERIES_STORE_SCHEMA_ID, AtmTaskExecutionId, #atm_mock_call_ctx{
    provider = ProviderSelector
}) ->
    {ok, #document{value = #atm_task_execution{time_series_store_id = AtmStoreId}}} = ?rpc(
        ProviderSelector, atm_task_execution:get(AtmTaskExecutionId)
    ),
    AtmStoreId;

get_store_id(AtmStoreSchemaId, _, #atm_mock_call_ctx{
    provider = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    {ok, #document{value = #atm_workflow_execution{store_registry = AtmStoreRegistry}}} = ?rpc(
        ProviderSelector, atm_workflow_execution:get(AtmWorkflowExecutionId)
    ),
    maps:get(AtmStoreSchemaId, AtmStoreRegistry).


%% @private
-spec browse_store(session:id(), od_space:id(), atm_workflow_execution:id(), atm_store:id()) ->
    json_utils:json_term().
browse_store(SessionId, SpaceId, AtmWorkflowExecutionId, AtmStoreId) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_auth:build(SpaceId, AtmWorkflowExecutionId, SessionId),

    {ok, AtmStore = #atm_store{container = AtmStoreContainer}} = atm_store_api:get(AtmStoreId),
    AtmStoreBrowseOpts = build_browse_opts(atm_store_container:get_store_type(AtmStoreContainer)),

    AtmStoreContent = atm_store_api:browse_content(AtmWorkflowExecutionAuth, AtmStoreBrowseOpts, AtmStore),
    atm_store_content_browse_result:to_json(AtmStoreContent).


%% @private
-spec build_browse_opts(atm_store:type()) -> atm_store_content_browse_options:record().
build_browse_opts(audit_log) ->
    #atm_audit_log_store_content_browse_options{browse_opts = ?INFINITE_LOG_BASED_STORES_LISTING_OPTS};

build_browse_opts(exception) ->
    #atm_exception_store_content_browse_options{listing_opts = ?INFINITE_LOG_BASED_STORES_LISTING_OPTS};

build_browse_opts(list) ->
    #atm_list_store_content_browse_options{listing_opts = ?INFINITE_LOG_BASED_STORES_LISTING_OPTS};

build_browse_opts(tree_forest) ->
    #atm_tree_forest_store_content_browse_options{listing_opts = ?INFINITE_LOG_BASED_STORES_LISTING_OPTS};

build_browse_opts(range) ->
    #atm_range_store_content_browse_options{};

build_browse_opts(single_value) ->
    #atm_single_value_store_content_browse_options{};

build_browse_opts(time_series) ->
    #atm_time_series_store_content_browse_options{
        request = #time_series_slice_get_request{
            layout = #{?ALL_TIME_SERIES => [?ALL_METRICS]},
            start_timestamp = undefined,
            window_limit = 10000000000000000000000000000000000000000000000000000
        }
    }.


%% @private
get_task_execution_id(#atm_mock_call_ctx{
    step = handle_task_resuming,
    call_args = [AtmTaskExecutionId, _CurrentIncarnation]
}) ->
    AtmTaskExecutionId;

get_task_execution_id(#atm_mock_call_ctx{
    step = handle_task_resumed,
    call_args = [AtmTaskExecutionId]
}) ->
    AtmTaskExecutionId;

get_task_execution_id(#atm_mock_call_ctx{
    step = run_task_for_item,
    call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId, _AtmJobBatchId, _ItemBatch]
}) ->
    AtmTaskExecutionId;

get_task_execution_id(#atm_mock_call_ctx{
    step = process_task_result_for_item,
    call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId, _ItemBatch, _JobBatchResult]
}) ->
    AtmTaskExecutionId;

get_task_execution_id(#atm_mock_call_ctx{
    step = process_streamed_task_data,
    call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId, _StreamedData]
}) ->
    AtmTaskExecutionId;

get_task_execution_id(#atm_mock_call_ctx{
    step = handle_task_results_processed_for_all_items,
    call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
}) ->
    AtmTaskExecutionId;

get_task_execution_id(#atm_mock_call_ctx{
    step = handle_task_execution_stopped,
    call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
}) ->
    AtmTaskExecutionId.


%% @private
get_task_expectations(ExpectationsPerTask, AtmTaskExecutionId, ExpState) ->
    AtmTaskSchemaId = atm_workflow_execution_exp_state_builder:get_task_schema_id(
        AtmTaskExecutionId, ExpState
    ),

    lists_utils:foldl_while(fun
        ({IdGroup, TaskExpectations}, no_diff) when is_list(IdGroup) ->
            case lists:member(AtmTaskSchemaId, IdGroup) of
                true -> {halt, TaskExpectations};
                false -> {cont, no_diff}
            end;
        ({Id, TaskExpectations}, no_diff) when is_binary(Id), Id == AtmTaskSchemaId ->
            {halt, TaskExpectations};
        (_, no_diff) ->
            {cont, no_diff}
    end, no_diff, maps:to_list(ExpectationsPerTask)).


%% @private
substitute_expectation_placeholders(Expectations, AtmTaskExecutionId) ->
    lists:map(fun
        (Expectation) when is_tuple(Expectation), tuple_size(Expectation) > 2 ->
            case {element(1, Expectation), element(2, Expectation)} of
                {task, ?TASK_ID_PLACEHOLDER} ->
                    setelement(2, Expectation, AtmTaskExecutionId);
                _ ->
                    Expectation
            end;
        (Expectation) ->
            Expectation
    end, Expectations).
