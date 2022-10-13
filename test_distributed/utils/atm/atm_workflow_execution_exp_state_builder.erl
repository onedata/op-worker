%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module allowing definition and modification of atm workflow execution
%%% exp state which consists of:
%%% - exp state of atm_workflow_execution model
%%% - exp state of each atm_task_execution model
%%% - exp state of each atm_store model (to be implemented)
%%%
%%% NOTE: exp states are created and stored as json object similar to responses
%%% send to clients via API endpoints. Model records definitions from op are
%%% not reused as they contain many irrelevant (to clients) fields considered
%%% as implementation details and omitted from said responses.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_exp_state_builder).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([
    init/5,

    set_current_lane_run/3,
    expect_lane_run_automatic_retry_scheduled/2,
    expect_lane_run_manual_repeat_scheduled/4,
    expect_lane_run_started_preparing/2,
    expect_current_lane_run_started_preparing/2,
    expect_lane_run_started_preparing_in_advance/2,
    expect_lane_run_created/2,
    expect_lane_run_enqueued/2,
    expect_lane_run_active/2,
    expect_lane_run_stopping/2,
    expect_lane_run_finished/2,
    expect_lane_run_failed/2,
    expect_lane_run_cancelled/2,
    expect_lane_run_crashed/2,
    expect_lane_run_interrupted/2,
    expect_lane_run_paused/2,
    expect_lane_run_resuming/2,
    expect_lane_run_rerunable/2,
    expect_lane_run_retriable/2,
    expect_lane_run_repeatable/4,
    expect_lane_run_num_set/3,
    expect_lane_run_removed/2,

    get_task_selector/2,
    get_task_id/2,
    get_task_stats/2,
    get_task_status/2,
    expect_task_items_in_processing_increased/3,
    expect_task_items_moved_from_processing_to_processed/3,
    expect_task_items_moved_from_processing_to_failed_and_processed/3,
    expect_task_transitioned_to_active_status_if_was_in_pending_status/2,
    expect_task_parallel_box_transitioned_to_active_status_if_was_in_pending_status/2,
    expect_task_lane_run_transitioned_to_active_status_if_was_in_enqueued_status/2,
    expect_task_finished/2,
    expect_task_skipped/2,
    expect_task_failed/2,
    expect_task_interrupted/2,
    expect_task_paused/2,
    expect_task_cancelled/2,
    expect_task_parallel_box_transitioned_to_inferred_status/3,
    expect_all_tasks_pending/2,
    expect_all_tasks_active/2,
    expect_all_tasks_skipped/2,
    expect_all_tasks_paused/2,
    expect_all_tasks_interrupted/2,
    expect_all_tasks_abruptly_interrupted/2,
    expect_all_tasks_cancelled/2,
    expect_all_tasks_failed/2,
    expect_all_tasks_stopping/3,

    expect_workflow_execution_scheduled/1,
    expect_workflow_execution_active/1,
    expect_workflow_execution_stopping/1,
    expect_workflow_execution_finished/1,
    expect_workflow_execution_failed/1,
    expect_workflow_execution_cancelled/1,
    expect_workflow_execution_crashed/1,
    expect_workflow_execution_paused/1,
    expect_workflow_execution_interrupted/1,
    expect_workflow_execution_resuming/1,

    assert_matches_with_backend/2
]).

% json object similar in structure to translations returned via API endpoints
% (has the same keys but as for values instead of concrete values it may contain
% validator functions - e.g. timestamp fields should check approx time rather
% than concrete value)
-type workflow_execution_state() :: json_utils:json_term().
-type lane_run_state() :: json_utils:json_term().
-type parallel_box_execution_state() :: json_utils:json_term().
-type task_execution_state() :: json_utils:json_term().

-record(exp_task_execution_state_ctx, {
    lane_run_selector :: atm_lane_execution:lane_run_selector(),
    parallel_box_schema_id :: automation:id(),
    exp_state :: task_execution_state()
}).
-type exp_task_execution_state_ctx() :: #exp_task_execution_state_ctx{}.
-type exp_task_execution_state_ctx_registry() :: #{atm_task_execution:id() => exp_task_execution_state_ctx()}.

-record(exp_workflow_execution_state_ctx, {
    provider_selector :: oct_background:entity_selector(),
    lane_schemas :: [atm_lane_schema:record()],

    workflow_execution_id :: atm_workflow_execution:id(),
    current_lane_index :: atm_lane_execution:index(),
    current_run_num :: atm_lane_execution:run_num(),

    exp_workflow_execution_state :: workflow_execution_state(),
    exp_task_execution_state_ctx_registry :: exp_task_execution_state_ctx_registry()
}).
-type ctx() :: #exp_workflow_execution_state_ctx{}.

-type task_registry() :: #{AtmTaskSchemaId :: automation:id() => atm_task_execution:id()}.
-type task_selector() :: {atm_lane_execution:lane_run_selector(), automation:id(), automation:id()}.

-type log_fun() :: fun((binary(), [term()]) -> ok).

-export_type([ctx/0, task_selector/0]).


-define(JSON_PATH(__QUERY_BIN), binary:split(__QUERY_BIN, <<".">>, [global])).
-define(JSON_PATH(__FORMAT, __ARGS), ?JSON_PATH(str_utils:format_bin(__FORMAT, __ARGS))).

-define(NOW(), global_clock:timestamp_seconds()).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(
    oct_background:entity_selector(),
    od_space:id(),
    atm_workflow_execution:id(),
    time:seconds(),
    [atm_lane_schema:record()]
) ->
    ctx().
init(
    ProviderSelector,
    SpaceId,
    AtmWorkflowExecutionId,
    ApproxScheduleTime,
    AtmLaneSchemas = [FirstAtmLaneSchema | RestAtmLaneSchemas]
) ->
    ExpFirstAtmLaneExecutionState = #{
        <<"schemaId">> => FirstAtmLaneSchema#atm_lane_schema.id,
        <<"runs">> => [build_initial_regular_lane_run_exp_state(1, <<"scheduled">>)]
    },
    ExpRestAtmLaneExecutionStates = lists:map(fun(#atm_lane_schema{id = AtmLaneSchemaId}) ->
        #{
            <<"schemaId">> => AtmLaneSchemaId,
            <<"runs">> => []
        }
    end, RestAtmLaneSchemas),

    #exp_workflow_execution_state_ctx{
        provider_selector = ProviderSelector,
        lane_schemas = AtmLaneSchemas,

        workflow_execution_id = AtmWorkflowExecutionId,
        current_lane_index = 1,
        current_run_num = 1,

        exp_workflow_execution_state = #{
            <<"spaceId">> => SpaceId,

            <<"lanes">> => [ExpFirstAtmLaneExecutionState | ExpRestAtmLaneExecutionStates],

            <<"status">> => <<"scheduled">>,

            <<"scheduleTime">> => build_timestamp_field_validator(ApproxScheduleTime),
            <<"startTime">> => 0,
            <<"suspendTime">> => 0,
            <<"finishTime">> => 0
        },
        exp_task_execution_state_ctx_registry = #{}
    }.


-spec set_current_lane_run(non_neg_integer(), non_neg_integer(), ctx()) -> ctx().
set_current_lane_run(AtmLaneIndex, AtmRunNum, ExpState) ->
    ExpState#exp_workflow_execution_state_ctx{
        current_lane_index = AtmLaneIndex,
        current_run_num = AtmRunNum
    }.


-spec expect_lane_run_automatic_retry_scheduled(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_lane_run_automatic_retry_scheduled({AtmLaneSelector, _}, ExpStateCtx = #exp_workflow_execution_state_ctx{
    exp_workflow_execution_state = ExpAtmWorkflowExecutionState0
}) ->
    AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpStateCtx),
    Path = ?JSON_PATH("lanes.[~B].runs", [AtmLaneIndex - 1]),

    {ok, PrevAtmLaneExecutionRuns = [FailedLaneRun | _]} = json_utils:query(
        ExpAtmWorkflowExecutionState0, Path
    ),
    NewLaneRun = build_initial_regular_lane_run_exp_state(
        maps:get(<<"runNumber">>, FailedLaneRun) + 1,
        <<"scheduled">>,
        <<"retry">>,
        maps:get(<<"runNumber">>, FailedLaneRun),
        maps:get(<<"exceptionStoreId">>, FailedLaneRun)
    ),
    {ok, ExpAtmWorkflowExecutionState1} = json_utils:insert(
        ExpAtmWorkflowExecutionState0,
        [NewLaneRun | PrevAtmLaneExecutionRuns],
        Path
    ),
    ExpStateCtx#exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState1
    }.


-spec expect_lane_run_manual_repeat_scheduled(
    atm_workflow_execution:repeat_type(),
    atm_lane_execution:lane_run_selector(),
    non_neg_integer(),
    ctx()
) ->
    ctx().
expect_lane_run_manual_repeat_scheduled(
    RepeatType,
    AtmLaneRunSelector,
    NewRunNum,
    ExpStateCtx = #exp_workflow_execution_state_ctx{exp_workflow_execution_state = ExpAtmWorkflowExecutionState0}
) ->
    {AtmLaneSelector, AtmRunSelector} = AtmLaneRunSelector,
    AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpStateCtx),
    AtmRunNum = resolve_run_selector(AtmRunSelector, ExpStateCtx),

    AtmLaneExecutionPath = ?JSON_PATH("lanes.[~B].runs", [AtmLaneIndex - 1]),
    {ok, PrevAtmLaneExecutionRuns} = json_utils:query(ExpAtmWorkflowExecutionState0, AtmLaneExecutionPath),
    {ok, {_, PrevAtmLaneRunState}} = locate_lane_run(AtmLaneRunSelector, ExpStateCtx),

    {RepeatTypeBin, IteratedStoreId} = case RepeatType of
        retry -> {<<"retry">>, maps:get(<<"exceptionStoreId">>, PrevAtmLaneRunState)};
        rerun -> {<<"rerun">>, maps:get(<<"iteratedStoreId">>, PrevAtmLaneRunState)}
    end,
    NewLaneRun = build_initial_regular_lane_run_exp_state(
        NewRunNum,
        <<"scheduled">>,
        RepeatTypeBin,
        AtmRunNum,
        IteratedStoreId
    ),
    {ok, ExpAtmWorkflowExecutionState1} = json_utils:insert(
        ExpAtmWorkflowExecutionState0,
        [NewLaneRun | PrevAtmLaneExecutionRuns],
        AtmLaneExecutionPath
    ),
    ExpStateCtx#exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState1
    }.


-spec expect_lane_run_started_preparing(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_lane_run_started_preparing(AtmLaneRunSelector, ExpStateCtx) ->
    case is_current_lane_run(AtmLaneRunSelector, ExpStateCtx) of
        true -> expect_current_lane_run_started_preparing(AtmLaneRunSelector, ExpStateCtx);
        false -> expect_lane_run_started_preparing_in_advance(AtmLaneRunSelector, ExpStateCtx)
    end.


-spec expect_current_lane_run_started_preparing(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_current_lane_run_started_preparing(AtmLaneRunSelector, ExpStateCtx0) ->
    ExpAtmLaneRunStateDiff = #{<<"status">> => <<"preparing">>},
    ExpStateCtx1 = update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtx0),

    ExpAtmWorkflowExecutionStateDiff = fun(ExpAtmWorkflowExecutionState) ->
        ExpAtmWorkflowExecutionState#{
            <<"status">> => <<"active">>,
            <<"startTime">> => build_timestamp_field_validator(?NOW())
        }
    end,
    update_workflow_execution_exp_state(ExpAtmWorkflowExecutionStateDiff, ExpStateCtx1).


-spec expect_lane_run_started_preparing_in_advance(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_lane_run_started_preparing_in_advance(AtmLaneRunSelector, ExpStateCtx = #exp_workflow_execution_state_ctx{
    exp_workflow_execution_state = ExpAtmWorkflowExecutionState0
}) ->
    {ok, ExpAtmWorkflowExecutionState1} = case locate_lane_run(AtmLaneRunSelector, ExpStateCtx) of
        {ok, {Path, Run = #{<<"status">> := <<"scheduled">>}}} ->
            json_utils:insert(
                ExpAtmWorkflowExecutionState0,
                Run#{<<"status">> => <<"preparing">>},
                Path
            );
        ?ERROR_NOT_FOUND ->
            AtmLaneIndex = resolve_lane_selector(element(1, AtmLaneRunSelector), ExpStateCtx),
            Path = ?JSON_PATH("lanes.[~B].runs", [AtmLaneIndex - 1]),
            {ok, PrevRuns} = json_utils:query(ExpAtmWorkflowExecutionState0, Path),

            json_utils:insert(
                ExpAtmWorkflowExecutionState0,
                [build_initial_regular_lane_run_exp_state(undefined, <<"preparing">>) | PrevRuns],
                Path
            )
    end,
    ExpStateCtx#exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState1
    }.


-spec expect_lane_run_created(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_lane_run_created(AtmLaneRunSelector, ExpStateCtx = #exp_workflow_execution_state_ctx{
    workflow_execution_id = AtmWorkflowExecutionId,
    exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionStateCtxRegistry0
}) ->
    AtmLaneSchema = get_lane_schema(AtmLaneRunSelector, ExpStateCtx),
    {ok, AtmLaneRun} = atm_lane_execution:get_run(AtmLaneRunSelector, fetch_workflow_execution(ExpStateCtx)),

    {ExpAtmParallelBoxExecutionStates, ExpAtmTaskExecutionStateCtxRegistry1} = lists:mapfoldl(
        fun({AtmParallelBoxSchema, AtmParallelBoxExecution}, OuterAcc) ->
            AtmParallelBoxSchemaId = AtmParallelBoxSchema#atm_parallel_box_schema.id,
            AtmTasksRegistry = get_task_registry(AtmParallelBoxExecution, AtmParallelBoxSchema),

            ExpAtmParallelBoxExecutionState = build_exp_initial_parallel_box_execution_state(
                AtmParallelBoxSchemaId,
                AtmTasksRegistry
            ),
            UpdatedOuterAcc = maps:fold(fun(AtmTaskSchemaId, AtmTaskExecutionId, InnerAcc) ->
                InnerAcc#{AtmTaskExecutionId => #exp_task_execution_state_ctx{
                    lane_run_selector = AtmLaneRunSelector,
                    parallel_box_schema_id = AtmParallelBoxSchemaId,
                    exp_state = build_task_execution_initial_exp_state(
                        AtmWorkflowExecutionId, AtmTaskSchemaId
                    )
                }}
            end, OuterAcc, AtmTasksRegistry),

            {ExpAtmParallelBoxExecutionState, UpdatedOuterAcc}
        end,
        ExpAtmTaskExecutionStateCtxRegistry0,
        lists:zip(
            AtmLaneSchema#atm_lane_schema.parallel_boxes,
            AtmLaneRun#atm_lane_execution_run.parallel_boxes
        )
    ),

    ExpAtmLaneRunStateDiff = #{
        <<"iteratedStoreId">> => AtmLaneRun#atm_lane_execution_run.iterated_store_id,
        <<"exceptionStoreId">> => AtmLaneRun#atm_lane_execution_run.exception_store_id,
        <<"parallelBoxes">> => ExpAtmParallelBoxExecutionStates
    },
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtx#exp_workflow_execution_state_ctx{
        exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionStateCtxRegistry1
    }).


-spec expect_lane_run_enqueued(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_lane_run_enqueued(AtmLaneRunSelector, ExpStateCtx) ->
    ExpAtmLaneRunStateDiff = #{<<"status">> => <<"enqueued">>},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtx).


-spec expect_lane_run_active(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_lane_run_active(AtmLaneRunSelector, ExpStateCtx) ->
    ExpAtmLaneRunStateDiff = #{<<"status">> => <<"active">>},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtx).


-spec expect_lane_run_stopping(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_lane_run_stopping(AtmLaneRunSelector, ExpStateCtx) ->
    ExpAtmLaneRunStateDiff = #{<<"status">> => <<"stopping">>},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtx).


-spec expect_lane_run_finished(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_lane_run_finished(AtmLaneRunSelector, ExpStateCtx) ->
    ExpAtmLaneRunStateDiff = #{<<"status">> => <<"finished">>},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtx).


-spec expect_lane_run_failed(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_lane_run_failed(AtmLaneRunSelector, ExpStateCtx) ->
    ExpAtmLaneRunStateDiff = #{<<"status">> => <<"failed">>},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtx).


-spec expect_lane_run_cancelled(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_lane_run_cancelled(AtmLaneRunSelector, ExpStateCtx) ->
    ExpAtmLaneRunStateDiff = #{<<"status">> => <<"cancelled">>},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtx).


-spec expect_lane_run_crashed(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_lane_run_crashed(AtmLaneRunSelector, ExpStateCtx) ->
    ExpAtmLaneRunStateDiff = #{<<"status">> => <<"crashed">>},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtx).


-spec expect_lane_run_interrupted(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_lane_run_interrupted(AtmLaneRunSelector, ExpStateCtx) ->
    ExpAtmLaneRunStateDiff = #{<<"status">> => <<"interrupted">>},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtx).


-spec expect_lane_run_paused(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_lane_run_paused(AtmLaneRunSelector, ExpStateCtx) ->
    ExpAtmLaneRunStateDiff = #{<<"status">> => <<"paused">>},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtx).


-spec expect_lane_run_resuming(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_lane_run_resuming(AtmLaneRunSelector, ExpStateCtx) ->
    ExpAtmLaneRunStateDiff = #{<<"status">> => <<"resuming">>},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtx).


-spec expect_lane_run_rerunable(
    atm_lane_execution:lane_run_selector() | [atm_lane_execution:lane_run_selector()],
    ctx()
) ->
    ctx().
expect_lane_run_rerunable(AtmLaneRunSelectors, ExpStateCtx) ->
    ExpAtmLaneRunStateDiff = #{<<"isRerunable">> => true},
    update_exp_lane_runs_state(as_list(AtmLaneRunSelectors), ExpAtmLaneRunStateDiff, ExpStateCtx).


-spec expect_lane_run_retriable(
    atm_lane_execution:lane_run_selector() | [atm_lane_execution:lane_run_selector()],
    ctx()
) ->
    ctx().
expect_lane_run_retriable(AtmLaneRunSelectors, ExpStateCtx) ->
    ExpAtmLaneRunStateDiff = #{<<"isRetriable">> => true},
    update_exp_lane_runs_state(as_list(AtmLaneRunSelectors), ExpAtmLaneRunStateDiff, ExpStateCtx).


-spec expect_lane_run_repeatable(
    atm_lane_execution:lane_run_selector() | [atm_lane_execution:lane_run_selector()],
    boolean(),
    boolean(),
    ctx()
) ->
    ctx().
expect_lane_run_repeatable(AtmLaneRunSelectors, IsRerunable, IsRepeatable, ExpStateCtx) ->
    ExpAtmLaneRunStateDiff = #{
        <<"isRerunable">> => IsRerunable,
        <<"isRetriable">> => IsRepeatable
    },
    update_exp_lane_runs_state(as_list(AtmLaneRunSelectors), ExpAtmLaneRunStateDiff, ExpStateCtx).


-spec expect_lane_run_num_set(
    atm_lane_execution:lane_run_selector(),
    atm_lane_execution:run_num(),
    ctx()
) ->
    ctx().
expect_lane_run_num_set(AtmLaneRunSelector, RunNum, ExpStateCtx) ->
    ExpAtmLaneRunStateDiff = #{<<"runNumber">> => RunNum},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtx).


-spec expect_lane_run_removed(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_lane_run_removed({AtmLaneSelector, AtmRunSelector}, ExpStateCtx = #exp_workflow_execution_state_ctx{
    current_run_num = CurrentRunNum,
    exp_workflow_execution_state = ExpAtmWorkflowExecutionState = #{<<"lanes">> := AtmLaneExecutions},
    exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
}) ->
    AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpStateCtx),
    TargetRunNum = resolve_run_selector(AtmRunSelector, ExpStateCtx),

    AtmLaneExecutionWithRun = #{<<"runs">> := AtmLaneRuns} = lists:nth(AtmLaneIndex, AtmLaneExecutions),
    {#{<<"parallelBoxes">> := AtmParallelBoxes}, RestAtmLaneRuns} = take_run(
        TargetRunNum, CurrentRunNum, AtmLaneRuns, []
    ),
    AtmLaneExecutionWithoutRun = AtmLaneExecutionWithRun#{<<"runs">> => RestAtmLaneRuns},

    AtmTaskExecutionIds = lists:foldl(fun(#{<<"taskRegistry">> := AtmTaskRegistry}, Acc) ->
        maps:values(AtmTaskRegistry) ++ Acc
    end, [], AtmParallelBoxes),

    ExpStateCtx#exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState#{
            <<"lanes">> => lists_utils:replace_at(
                AtmLaneExecutionWithoutRun, AtmLaneIndex, AtmLaneExecutions
            )
        },
        exp_task_execution_state_ctx_registry = maps:without(
            AtmTaskExecutionIds, ExpAtmTaskExecutionsRegistry
        )
    }.


-spec get_task_selector(atm_task_execution:id(), ctx()) -> task_selector().
get_task_selector(AtmTaskExecutionId, #exp_workflow_execution_state_ctx{
    exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
}) ->
    #exp_task_execution_state_ctx{
        lane_run_selector = AtmLaneRunSelector,
        parallel_box_schema_id = AtmParallelBoxSchemaId,
        exp_state = #{<<"schemaId">> := AtmTaskSchemaId}
    } = maps:get(AtmTaskExecutionId, ExpAtmTaskExecutionsRegistry),

    {AtmLaneRunSelector, AtmParallelBoxSchemaId, AtmTaskSchemaId}.


-spec get_task_id(task_selector(), ctx()) -> atm_task_execution:id().
get_task_id({AtmLaneRunSelector, AtmParallelBoxSchemaId, AtmTaskSchemaId}, ExpStateCtx) ->
    {ok, {_, #{<<"parallelBoxes">> := ExpParallelBoxExecutionStates}}} = locate_lane_run(
        AtmLaneRunSelector, ExpStateCtx
    ),
    {ok, #{<<"taskRegistry">> := AtmTaskRegistry}} = lists_utils:find(
        fun(#{<<"schemaId">> := SchemaId}) -> SchemaId == AtmParallelBoxSchemaId end,
        ExpParallelBoxExecutionStates
    ),
    maps:get(AtmTaskSchemaId, AtmTaskRegistry).


-spec get_task_stats(atm_task_execution:id(), ctx()) -> {integer(), integer(), integer()}.
get_task_stats(AtmTaskExecutionId, #exp_workflow_execution_state_ctx{
    exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
}) ->
    #exp_task_execution_state_ctx{exp_state = #{
        <<"itemsInProcessing">> := IIP,
        <<"itemsFailed">> := IF,
        <<"itemsProcessed">> := IP
    }} = maps:get(AtmTaskExecutionId, ExpAtmTaskExecutionsRegistry),

    {IIP, IF, IP}.


-spec get_task_status(atm_task_execution:id(), ctx()) -> binary().
get_task_status(AtmTaskExecutionId, #exp_workflow_execution_state_ctx{
    exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
}) ->
    #exp_task_execution_state_ctx{exp_state = #{<<"status">> := ExpStatus}} = maps:get(
        AtmTaskExecutionId, ExpAtmTaskExecutionsRegistry
    ),
    ExpStatus.


-spec expect_task_items_in_processing_increased(
    atm_task_execution:id(),
    pos_integer(),
    ctx()
) ->
    ctx().
expect_task_items_in_processing_increased(AtmTaskExecutionId, Inc, ExpStateCtx) ->
    ExpAtmTaskExecutionStateDiff = fun(AtmTaskExecution = #{<<"itemsInProcessing">> := IIP}) ->
        AtmTaskExecution#{<<"itemsInProcessing">> => IIP + Inc}
    end,
    update_task_execution_exp_state(AtmTaskExecutionId, ExpAtmTaskExecutionStateDiff, ExpStateCtx).


-spec expect_task_items_moved_from_processing_to_processed(
    atm_task_execution:id(),
    pos_integer(),
    ctx()
) ->
    ctx().
expect_task_items_moved_from_processing_to_processed(AtmTaskExecutionId, Count, ExpStateCtx) ->
    ExpAtmTaskExecutionStateDiff = fun(ExpAtmTaskExecutionState = #{
        <<"itemsInProcessing">> := IIP,
        <<"itemsProcessed">> := IP
    }) ->
        ExpAtmTaskExecutionState#{
            <<"itemsInProcessing">> => IIP - Count,
            <<"itemsProcessed">> => IP + Count
        }
    end,
    update_task_execution_exp_state(AtmTaskExecutionId, ExpAtmTaskExecutionStateDiff, ExpStateCtx).


-spec expect_task_items_moved_from_processing_to_failed_and_processed(
    atm_task_execution:id(),
    pos_integer(),
    ctx()
) ->
    ctx().
expect_task_items_moved_from_processing_to_failed_and_processed(AtmTaskExecutionId, Count, ExpStateCtx) ->
    ExpAtmTaskExecutionStateDiff = fun(ExpAtmTaskExecutionState = #{
        <<"itemsInProcessing">> := IIP,
        <<"itemsFailed">> := IF,
        <<"itemsProcessed">> := IP
    }) ->
        ExpAtmTaskExecutionState#{
            <<"itemsInProcessing">> => IIP - Count,
            <<"itemsFailed">> => IF + Count,
            <<"itemsProcessed">> => IP + Count
        }
    end,
    update_task_execution_exp_state(AtmTaskExecutionId, ExpAtmTaskExecutionStateDiff, ExpStateCtx).


-spec expect_task_transitioned_to_active_status_if_was_in_pending_status(
    atm_task_execution:id(),
    ctx()
) ->
    ctx().
expect_task_transitioned_to_active_status_if_was_in_pending_status(AtmTaskExecutionId, ExpStateCtx) ->
    update_task_execution_exp_state(
        AtmTaskExecutionId,
        build_transition_to_status_if_in_status_diff(<<"pending">>, <<"active">>),
        ExpStateCtx
    ).


-spec expect_task_parallel_box_transitioned_to_active_status_if_was_in_pending_status(
    atm_task_execution:id(),
    ctx()
) ->
    ctx().
expect_task_parallel_box_transitioned_to_active_status_if_was_in_pending_status(
    AtmTaskExecutionId,
    ExpStateCtx = #exp_workflow_execution_state_ctx{
        exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
    }
) ->
    TaskExecutionExtStateCtx = maps:get(AtmTaskExecutionId, ExpAtmTaskExecutionsRegistry),

    update_exp_parallel_box_execution_state(
        TaskExecutionExtStateCtx#exp_task_execution_state_ctx.lane_run_selector,
        TaskExecutionExtStateCtx#exp_task_execution_state_ctx.parallel_box_schema_id,
        build_transition_to_status_if_in_status_diff(<<"pending">>, <<"active">>),
        ExpStateCtx
    ).


-spec expect_task_lane_run_transitioned_to_active_status_if_was_in_enqueued_status(
    atm_task_execution:id(),
    ctx()
) ->
    ctx().
expect_task_lane_run_transitioned_to_active_status_if_was_in_enqueued_status(
    AtmTaskExecutionId,
    ExpStateCtx = #exp_workflow_execution_state_ctx{
        exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
    }
) ->
    #exp_task_execution_state_ctx{lane_run_selector = AtmLaneRunSelector} = maps:get(
        AtmTaskExecutionId, ExpAtmTaskExecutionsRegistry
    ),
    update_exp_lane_run_state(
        AtmLaneRunSelector,
        build_transition_to_status_if_in_status_diff(<<"enqueued">>, <<"active">>),
        ExpStateCtx
    ).


-spec expect_task_finished(atm_task_execution:id(), ctx()) ->
    ctx().
expect_task_finished(AtmTaskExecutionId, ExpStateCtx) ->
    expect_task_transitioned_to(AtmTaskExecutionId, <<"finished">>, ExpStateCtx).


-spec expect_task_skipped(atm_task_execution:id(), ctx()) ->
    ctx().
expect_task_skipped(AtmTaskExecutionId, ExpStateCtx) ->
    expect_task_transitioned_to(AtmTaskExecutionId, <<"skipped">>, ExpStateCtx).


-spec expect_task_failed(atm_task_execution:id(), ctx()) ->
    ctx().
expect_task_failed(AtmTaskExecutionId, ExpStateCtx) ->
    expect_task_transitioned_to(AtmTaskExecutionId, <<"failed">>, ExpStateCtx).


-spec expect_task_interrupted(atm_task_execution:id(), ctx()) ->
    ctx().
expect_task_interrupted(AtmTaskExecutionId, ExpStateCtx) ->
    expect_task_transitioned_to(AtmTaskExecutionId, <<"interrupted">>, ExpStateCtx).


-spec expect_task_paused(atm_task_execution:id(), ctx()) -> ctx().
expect_task_paused(AtmTaskExecutionId, ExpStateCtx) ->
    expect_task_transitioned_to(AtmTaskExecutionId, <<"paused">>, ExpStateCtx).


-spec expect_task_cancelled(atm_task_execution:id(), ctx()) ->
    ctx().
expect_task_cancelled(AtmTaskExecutionId, ExpStateCtx) ->
    expect_task_transitioned_to(AtmTaskExecutionId, <<"cancelled">>, ExpStateCtx).


-spec expect_task_parallel_box_transitioned_to_inferred_status(
    atm_task_execution:id(),
    fun((CurrentParallelBoxStatus :: binary(), [AtmTaskStatus :: binary()]) -> binary()),
    ctx()
) ->
    ctx().
expect_task_parallel_box_transitioned_to_inferred_status(AtmTaskExecutionId, InferStatusFun, ExpStateCtx) ->
    Diff = fun(ExpParallelBoxState = #{<<"status">> := CurrentStatus}) ->
        ExpParallelBoxState#{<<"status">> => InferStatusFun(CurrentStatus, get_parallel_box_tasks_statuses(
            ExpParallelBoxState, ExpStateCtx
        ))}
    end,
    update_exp_task_parallel_box_execution_state(AtmTaskExecutionId, Diff, ExpStateCtx).


-spec expect_all_tasks_pending(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_all_tasks_pending(AtmLaneRunSelector, ExpStateCtx) ->
    expect_all_tasks_transitioned_to(AtmLaneRunSelector, <<"pending">>, ExpStateCtx).


-spec expect_all_tasks_active(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_all_tasks_active(AtmLaneRunSelector, ExpStateCtx) ->
    expect_all_tasks_transitioned_to(AtmLaneRunSelector, <<"active">>, ExpStateCtx).


-spec expect_all_tasks_skipped(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_all_tasks_skipped(AtmLaneRunSelector, ExpStateCtx) ->
    expect_all_tasks_transitioned_to(AtmLaneRunSelector, <<"skipped">>, ExpStateCtx).


-spec expect_all_tasks_paused(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_all_tasks_paused(AtmLaneRunSelector, ExpStateCtx) ->
    expect_all_tasks_transitioned_to(AtmLaneRunSelector, <<"paused">>, ExpStateCtx).


-spec expect_all_tasks_interrupted(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_all_tasks_interrupted(AtmLaneRunSelector, ExpStateCtx) ->
    expect_all_tasks_transitioned_to(AtmLaneRunSelector, <<"interrupted">>, ExpStateCtx).


-spec expect_all_tasks_abruptly_interrupted(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_all_tasks_abruptly_interrupted(AtmLaneRunSelector, ExpStateCtx) ->
    ExpAtmTaskExecutionStatusChangeFun = fun(ExpAtmTaskExecution = #exp_task_execution_state_ctx{
        exp_state = ExpState
    }) ->
        ExpAtmTaskExecution#exp_task_execution_state_ctx{exp_state = handle_abruptly_stopped_task_stats(ExpState#{
            <<"status">> => <<"interrupted">>
        })}
    end,
    ExpAtmParallelBoxExecutionStatusChangeFun = fun(ExpAtmParallelBoxExecutionState, _) ->
        ExpAtmParallelBoxExecutionState#{<<"status">> => <<"interrupted">>}
    end,
    expect_all_tasks_transitioned(
        AtmLaneRunSelector,
        ExpAtmTaskExecutionStatusChangeFun,
        ExpAtmParallelBoxExecutionStatusChangeFun,
        ExpStateCtx
    ).


%% @private
handle_abruptly_stopped_task_stats(ExpAtmTaskExecutionState = #{
    <<"itemsInProcessing">> := ItemsInProcessing,
    <<"itemsProcessed">> := ItemsProcessed,
    <<"itemsFailed">> := ItemsFailed
}) ->
    ExpAtmTaskExecutionState#{
        <<"itemsInProcessing">> => 0,
        <<"itemsProcessed">> => ItemsProcessed + ItemsInProcessing,
        <<"itemsFailed">> => ItemsFailed + ItemsInProcessing
    }.


-spec expect_all_tasks_cancelled(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_all_tasks_cancelled(AtmLaneRunSelector, ExpStateCtx) ->
    expect_all_tasks_transitioned_to(AtmLaneRunSelector, <<"cancelled">>, ExpStateCtx).


-spec expect_all_tasks_failed(atm_lane_execution:lane_run_selector(), ctx()) ->
    ctx().
expect_all_tasks_failed(AtmLaneRunSelector, ExpStateCtx) ->
    expect_all_tasks_transitioned_to(AtmLaneRunSelector, <<"failed">>, ExpStateCtx).


-spec expect_all_tasks_stopping(
    atm_lane_execution:lane_run_selector(),
    atm_lane_execution:run_stopping_reason(),
    ctx()
) ->
    ctx().
expect_all_tasks_stopping(AtmLaneRunSelector, Reason, ExpStateCtx) ->
    Status = case Reason of
        pause -> <<"paused">>;
        interrupt -> <<"interrupted">>;
        cancel -> <<"cancelled">>
    end,
    ExpAtmTaskExecutionStatusChangeFun = fun(ExpAtmTaskExecution = #exp_task_execution_state_ctx{
        exp_state = ExpState
    }) ->
        ExpAtmTaskExecution#exp_task_execution_state_ctx{exp_state = ExpState#{
            <<"status">> => case maps:get(<<"status">>, ExpState) of
                <<"pending">> -> Status;
                <<"active">> -> <<"stopping">>;
                <<"stopping">> -> <<"stopping">>;
                Status -> Status
            end
        }}
    end,
    ExpAtmParallelBoxExecutionStatusChangeFun = fun(ExpAtmParallelBoxExecutionState, AtmTaskExecutionStatuses) ->
        ExpAtmParallelBoxExecutionState#{
            <<"status">> => case lists:usort(AtmTaskExecutionStatuses) of
                [Status] -> Status;
                _ -> <<"stopping">>
            end
        }
    end,
    expect_all_tasks_transitioned(
        AtmLaneRunSelector,
        ExpAtmTaskExecutionStatusChangeFun,
        ExpAtmParallelBoxExecutionStatusChangeFun,
        ExpStateCtx
    ).


-spec expect_workflow_execution_scheduled(ctx()) -> ctx().
expect_workflow_execution_scheduled(ExpStateCtx) ->
    ExpAtmWorkflowExecutionStateDiff = fun(ExpAtmWorkflowExecutionState) ->
        ExpAtmWorkflowExecutionState#{
            <<"status">> => <<"scheduled">>,
            <<"scheduleTime">> => build_timestamp_field_validator(?NOW())
        }
    end,
    update_workflow_execution_exp_state(ExpAtmWorkflowExecutionStateDiff, ExpStateCtx).


-spec expect_workflow_execution_active(ctx()) -> ctx().
expect_workflow_execution_active(ExpStateCtx) ->
    ExpAtmWorkflowExecutionStateDiff = fun(ExpAtmWorkflowExecutionState) ->
        ExpAtmWorkflowExecutionState#{
            <<"status">> => <<"active">>,
            <<"startTime">> => build_timestamp_field_validator(?NOW())
        }
    end,
    update_workflow_execution_exp_state(ExpAtmWorkflowExecutionStateDiff, ExpStateCtx).


-spec expect_workflow_execution_stopping(ctx()) -> ctx().
expect_workflow_execution_stopping(ExpStateCtx) ->
    ExpAtmWorkflowExecutionStateDiff = fun(ExpAtmWorkflowExecutionState) ->
        ExpAtmWorkflowExecutionState#{
            <<"status">> => <<"stopping">>,
            <<"startTime">> => build_timestamp_field_validator(?NOW())
        }
    end,
    update_workflow_execution_exp_state(ExpAtmWorkflowExecutionStateDiff, ExpStateCtx).


-spec expect_workflow_execution_finished(ctx()) -> ctx().
expect_workflow_execution_finished(ExpStateCtx) ->
    ExpAtmWorkflowExecutionStateDiff = #{
        <<"status">> => <<"finished">>,
        <<"finishTime">> => build_timestamp_field_validator(?NOW())
    },
    update_workflow_execution_exp_state(ExpAtmWorkflowExecutionStateDiff, ExpStateCtx).


-spec expect_workflow_execution_failed(ctx()) -> ctx().
expect_workflow_execution_failed(ExpStateCtx) ->
    ExpAtmWorkflowExecutionStateDiff = #{
        <<"status">> => <<"failed">>,
        <<"finishTime">> => build_timestamp_field_validator(?NOW())
    },
    update_workflow_execution_exp_state(ExpAtmWorkflowExecutionStateDiff, ExpStateCtx).


-spec expect_workflow_execution_cancelled(ctx()) -> ctx().
expect_workflow_execution_cancelled(ExpStateCtx) ->
    ExpAtmWorkflowExecutionStateDiff = #{
        <<"status">> => <<"cancelled">>,
        <<"finishTime">> => build_timestamp_field_validator(?NOW())
    },
    update_workflow_execution_exp_state(ExpAtmWorkflowExecutionStateDiff, ExpStateCtx).


-spec expect_workflow_execution_crashed(ctx()) -> ctx().
expect_workflow_execution_crashed(ExpStateCtx) ->
    ExpAtmWorkflowExecutionStateDiff = #{
        <<"status">> => <<"crashed">>,
        <<"finishTime">> => build_timestamp_field_validator(?NOW())
    },
    update_workflow_execution_exp_state(ExpAtmWorkflowExecutionStateDiff, ExpStateCtx).


-spec expect_workflow_execution_paused(ctx()) -> ctx().
expect_workflow_execution_paused(ExpStateCtx) ->
    ExpAtmWorkflowExecutionStateDiff = #{
        <<"status">> => <<"paused">>,
        <<"suspendTime">> => build_timestamp_field_validator(?NOW())
    },
    update_workflow_execution_exp_state(ExpAtmWorkflowExecutionStateDiff, ExpStateCtx).


-spec expect_workflow_execution_interrupted(ctx()) -> ctx().
expect_workflow_execution_interrupted(ExpStateCtx) ->
    ExpAtmWorkflowExecutionStateDiff = #{
        <<"status">> => <<"interrupted">>,
        <<"suspendTime">> => build_timestamp_field_validator(?NOW())
    },
    update_workflow_execution_exp_state(ExpAtmWorkflowExecutionStateDiff, ExpStateCtx).


-spec expect_workflow_execution_resuming(ctx()) -> ctx().
expect_workflow_execution_resuming(ExpStateCtx) ->
    ExpAtmWorkflowExecutionStateDiff = fun(ExpAtmWorkflowExecutionState) ->
        ExpAtmWorkflowExecutionState#{
            <<"status">> => <<"resuming">>,
            <<"scheduleTime">> => build_timestamp_field_validator(?NOW())
        }
    end,
    update_workflow_execution_exp_state(ExpAtmWorkflowExecutionStateDiff, ExpStateCtx).


-spec assert_matches_with_backend(ctx(), non_neg_integer()) -> boolean().
assert_matches_with_backend(ExpStateCtx, 0) ->
    assert_matches_with_backend_internal(ExpStateCtx, fun ct:pal/2);

assert_matches_with_backend(ExpStateCtx, Retries) ->
    case assert_matches_with_backend_internal(ExpStateCtx, fun(_, _) -> ok end) of
        true ->
            true;
        false ->
            timer:sleep(timer:seconds(1)),
            assert_matches_with_backend(ExpStateCtx, Retries - 1)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec fetch_workflow_execution(ctx()) -> atm_workflow_execution:record().
fetch_workflow_execution(#exp_workflow_execution_state_ctx{
    provider_selector = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    {ok, #document{value = AtmWorkflowExecution}} = opw_test_rpc:call(
        ProviderSelector, atm_workflow_execution, get, [AtmWorkflowExecutionId]
    ),
    AtmWorkflowExecution.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Task registry must be fetched from model stored in op as it is not
%% possible to tell beforehand (and later assert) what ids will be generated
%% for task executions.
%% @end
%%--------------------------------------------------------------------
-spec get_task_registry(atm_parallel_box_execution:record(), atm_parallel_box_schema:record()) ->
    task_registry().
get_task_registry(AtmParallelBoxExecution, #atm_parallel_box_schema{tasks = AtmTaskSchemas}) ->
    AtmTasksRegistry = maps:get(
        <<"taskRegistry">>,
        atm_parallel_box_execution:to_json(AtmParallelBoxExecution)
    ),

    % Assert task executions are generated for all task schemas
    ExpTaskSchemaIds = lists:sort(lists:map(
        fun(#atm_task_schema{id = AtmTaskSchemaId}) -> AtmTaskSchemaId end,
        AtmTaskSchemas
    )),
    case lists:sort(maps:keys(AtmTasksRegistry)) of
        ExpTaskSchemaIds ->
            ok;
        _ ->
            ct:pal("ERROR - task executions not generated for every task schema"),
            ?assert(false)
    end,

    AtmTasksRegistry.


%% @private
-spec get_lane_schema(atm_lane_execution:lane_run_selector(), ctx()) ->
    atm_lane_schema:record().
get_lane_schema({AtmLaneSelector, _}, ExpStateCtx = #exp_workflow_execution_state_ctx{
    lane_schemas = AtmLaneSchemas
}) ->
    AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpStateCtx),
    lists:nth(AtmLaneIndex, AtmLaneSchemas).


%% @private
-spec build_transition_to_status_if_in_status_diff(binary(), binary()) ->
    fun((json_utils:json_map()) -> json_utils:json_map()).
build_transition_to_status_if_in_status_diff(RequiredStatus, NewStatus) ->
    fun
        (ExpState = #{<<"status">> := Status}) when Status =:= RequiredStatus ->
            ExpState#{<<"status">> => NewStatus};
        (ExpState) ->
            ExpState
    end.


%% @private
-spec update_workflow_execution_exp_state(
    json_utils:json_map() | fun((workflow_execution_state()) -> workflow_execution_state()),
    ctx()
) ->
    ctx().
update_workflow_execution_exp_state(Diff, ExpStateCtx) when is_map(Diff) ->
    update_workflow_execution_exp_state(
        fun(ExpAtmWorkflowExecutionState) -> maps:merge(ExpAtmWorkflowExecutionState, Diff) end,
        ExpStateCtx
    );

update_workflow_execution_exp_state(Diff, ExpStateCtx = #exp_workflow_execution_state_ctx{
    exp_workflow_execution_state = ExpAtmWorkflowExecutionState
}) ->
    ExpStateCtx#exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = Diff(ExpAtmWorkflowExecutionState)
    }.


%% @private
-spec update_exp_lane_runs_state(
    [atm_lane_execution:lane_run_selector()],
    json_utils:json_map() | fun((lane_run_state()) -> lane_run_state()),
    ctx()
) ->
    ctx().
update_exp_lane_runs_state(AtmLaneRunSelectors, ExpAtmLaneRunStateDiff, ExpStateCtx) ->
    lists:foldl(fun(AtmLaneRunSelector, ExpStateCtxAcc) ->
        update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtxAcc)
    end, ExpStateCtx, AtmLaneRunSelectors).


%% @private
as_list(Term) when is_list(Term) -> Term;
as_list(Term) -> [Term].


%% @private
-spec update_exp_lane_run_state(
    atm_lane_execution:lane_run_selector(),
    json_utils:json_map() | fun((lane_run_state()) -> lane_run_state()),
    ctx()
) ->
    ctx().
update_exp_lane_run_state(AtmLaneRunSelector, Diff, ExpStateCtx) when is_map(Diff) ->
    update_exp_lane_run_state(
        AtmLaneRunSelector,
        fun(ExpAtmLaneRunState) -> maps:merge(ExpAtmLaneRunState, Diff) end,
        ExpStateCtx
    );

update_exp_lane_run_state(AtmLaneRunSelector, Diff, ExpStateCtx = #exp_workflow_execution_state_ctx{
    exp_workflow_execution_state = ExpAtmWorkflowExecutionState0
}) ->
    {ok, {AtmLaneRunPath, AtmLaneRun}} = locate_lane_run(AtmLaneRunSelector, ExpStateCtx),
    {ok, ExpAtmWorkflowExecutionState1} = json_utils:insert(
        ExpAtmWorkflowExecutionState0,
        Diff(AtmLaneRun),
        AtmLaneRunPath
    ),
    ExpStateCtx#exp_workflow_execution_state_ctx{exp_workflow_execution_state = ExpAtmWorkflowExecutionState1}.


%% @private
-spec get_parallel_box_tasks_statuses(parallel_box_execution_state(), ctx()) ->
    [binary()].
get_parallel_box_tasks_statuses(#{<<"taskRegistry">> := AtmTasksRegistry}, #exp_workflow_execution_state_ctx{
    exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
}) ->
    lists:usort(lists:map(fun(ExpTaskExecutionId) ->
        ExpAtmTaskExecutionStateCtx = maps:get(ExpTaskExecutionId, ExpAtmTaskExecutionsRegistry),
        maps:get(<<"status">>, ExpAtmTaskExecutionStateCtx#exp_task_execution_state_ctx.exp_state)
    end, maps:values(AtmTasksRegistry))).


%% @private
-spec update_exp_task_parallel_box_execution_state(
    atm_task_execution:id(),
    json_utils:json_map() | fun((parallel_box_execution_state()) -> parallel_box_execution_state()),
    ctx()
) ->
    ctx().
update_exp_task_parallel_box_execution_state(AtmTaskExecutionId, Diff, ExpStateCtx = #exp_workflow_execution_state_ctx{
    exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
}) ->
    #exp_task_execution_state_ctx{
        lane_run_selector = AtmLaneRunSelector,
        parallel_box_schema_id = AtmParallelBoxSchemaId
    } = maps:get(AtmTaskExecutionId, ExpAtmTaskExecutionsRegistry),

    update_exp_parallel_box_execution_state(AtmLaneRunSelector, AtmParallelBoxSchemaId, Diff, ExpStateCtx).


%% @private
-spec update_exp_parallel_box_execution_state(
    atm_lane_execution:lane_run_selector(),
    automation:id(),
    json_utils:json_map() | fun((parallel_box_execution_state()) -> parallel_box_execution_state()),
    ctx()
) ->
    ctx().
update_exp_parallel_box_execution_state(AtmLaneRunSelector, AtmParallelBoxSchemaId, Diff, ExpStateCtx) when
    is_map(Diff)
->
    update_exp_parallel_box_execution_state(
        AtmLaneRunSelector,
        AtmParallelBoxSchemaId,
        fun(ExpAtmTaskExecutionState) -> maps:merge(ExpAtmTaskExecutionState, Diff) end,
        ExpStateCtx
    );

update_exp_parallel_box_execution_state(
    AtmLaneRunSelector,
    AtmParallelBoxSchemaId,
    ExpParallelBoxStateDiff,
    ExpStateCtx
) ->
    ExpAtmLaneRunStateDiff = fun(ExpAtmLaneRunState = #{<<"parallelBoxes">> := ExpAtmParallelBoxes}) ->
        ExpAtmLaneRunState#{<<"parallelBoxes">> => lists:map(fun
            (ExpAtmParallelBoxState = #{<<"schemaId">> := Id}) when Id =:= AtmParallelBoxSchemaId ->
                ExpParallelBoxStateDiff(ExpAtmParallelBoxState);
            (ExpAtmParallelBoxState) ->
                ExpAtmParallelBoxState
        end, ExpAtmParallelBoxes)}
    end,
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtx).


%% @private
-spec expect_all_tasks_transitioned_to(atm_lane_execution:lane_run_selector(), binary(), ctx()) ->
    ctx().
expect_all_tasks_transitioned_to(AtmLaneRunSelector, Status, ExpStateCtx) ->
    ExpAtmTaskExecutionStatusChangeFun = fun(ExpAtmTaskExecution = #exp_task_execution_state_ctx{exp_state = ExpState}) ->
        ExpAtmTaskExecution#exp_task_execution_state_ctx{exp_state = ExpState#{
            <<"status">> => Status
        }}
    end,
    ExpAtmParallelBoxExecutionStatusChangeFun = fun(ExpAtmParallelBoxExecutionState, _AtmTaskExecutionStatuses) ->
        ExpAtmParallelBoxExecutionState#{<<"status">> => Status}
    end,
    expect_all_tasks_transitioned(
        AtmLaneRunSelector,
        ExpAtmTaskExecutionStatusChangeFun,
        ExpAtmParallelBoxExecutionStatusChangeFun,
        ExpStateCtx
    ).


%% @private
-spec expect_all_tasks_transitioned(
    atm_lane_execution:lane_run_selector(),
    fun((exp_task_execution_state_ctx()) -> exp_task_execution_state_ctx()),
    fun((json_utils:json_map()) -> json_utils:json_map()),
    ctx()
) ->
    ctx().
expect_all_tasks_transitioned(
    AtmLaneRunSelector,
    ExpAtmTaskExecutionStatusChangeFun,
    ExpAtmParallelBoxExecutionStatusChangeFun,
    ExpStateCtx = #exp_workflow_execution_state_ctx{
        exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionStateCtxRegistry0
    }
) ->
    {ok, {_AtmLaneRunPath, ExpAtmLaneRunState}} = locate_lane_run(AtmLaneRunSelector, ExpStateCtx),

    {ExpAtmParallelBoxExecutionStates, ExpAtmTaskExecutionStateCtxRegistry1} = lists:mapfoldl(
        fun(ExpAtmParallelBoxExecutionState = #{<<"taskRegistry">> := AtmTaskRegistry}, OuterAcc) ->
            UpdatedOuterAcc = maps:fold(fun(_AtmTaskSchemaId, AtmTaskExecutionId, InnerAcc) ->
                maps:update_with(AtmTaskExecutionId, ExpAtmTaskExecutionStatusChangeFun, InnerAcc)
            end, OuterAcc, AtmTaskRegistry),

            AtmTaskExecutionStatuses = maps:values(maps:map(fun(_AtmTaskSchemaId, AtmTaskExecutionId) ->
                #exp_task_execution_state_ctx{exp_state = ExpState} = maps:get(AtmTaskExecutionId, UpdatedOuterAcc),
                maps:get(<<"status">>, ExpState)
            end, AtmTaskRegistry)),

            UpdatedExpAtmParallelBoxExecutionState = ExpAtmParallelBoxExecutionStatusChangeFun(
                ExpAtmParallelBoxExecutionState, AtmTaskExecutionStatuses
            ),

            {UpdatedExpAtmParallelBoxExecutionState, UpdatedOuterAcc}
        end,
        ExpAtmTaskExecutionStateCtxRegistry0,
        maps:get(<<"parallelBoxes">>, ExpAtmLaneRunState)
    ),

    ExpAtmLaneRunStateDiff = #{<<"parallelBoxes">> => ExpAtmParallelBoxExecutionStates},

    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpStateCtx#exp_workflow_execution_state_ctx{
        exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionStateCtxRegistry1
    }).


%% @private
-spec expect_task_transitioned_to(atm_task_execution:id(), binary(), ctx()) ->
    ctx().
expect_task_transitioned_to(AtmTaskExecutionId, EndedStatus, ExpStateCtx) ->
    ExpAtmTaskExecutionStateDiff = fun(ExpAtmTaskExecutionState) ->
        ExpAtmTaskExecutionState#{<<"status">> => EndedStatus}
    end,
    update_task_execution_exp_state(AtmTaskExecutionId, ExpAtmTaskExecutionStateDiff, ExpStateCtx).


%% @private
-spec update_task_execution_exp_state(
    atm_task_execution:id(),
    json_utils:json_map() | fun((task_execution_state()) -> task_execution_state()),
    ctx()
) ->
    ctx().
update_task_execution_exp_state(AtmTaskExecutionId, Diff, ExpStateCtx) when is_map(Diff) ->
    update_task_execution_exp_state(
        AtmTaskExecutionId,
        fun(ExpAtmTaskExecutionState) -> maps:merge(ExpAtmTaskExecutionState, Diff) end,
        ExpStateCtx
    );

update_task_execution_exp_state(AtmTaskExecutionId, ExpStateDiff, ExpStateCtx = #exp_workflow_execution_state_ctx{
    exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
}) ->
    Diff = fun(ExpAtmTaskExecution = #exp_task_execution_state_ctx{exp_state = ExpState}) ->
        ExpAtmTaskExecution#exp_task_execution_state_ctx{exp_state = ExpStateDiff(ExpState)}
    end,
    ExpStateCtx#exp_workflow_execution_state_ctx{exp_task_execution_state_ctx_registry = maps:update_with(
        AtmTaskExecutionId, Diff, ExpAtmTaskExecutionsRegistry
    )}.


%% @private
take_run(_TargetRunNum, _CurrentRunNum, [], _) ->
    error;

take_run(TargetRunNum, CurrentRunNum, [Run = #{<<"runNumber">> := null} | RestRuns], NewerRunsReversed) when
    CurrentRunNum =< TargetRunNum
->
    {Run, lists:reverse(NewerRunsReversed) ++ RestRuns};

take_run(TargetRunNum, _CurrentRunNum, [Run = #{<<"runNumber">> := TargetRunNum} | RestRuns], NewerRunsReversed) ->
    {Run, lists:reverse(NewerRunsReversed) ++ RestRuns};

take_run(TargetRunNum, CurrentRunNum, [Run | RestRuns], NewerRunsReversed) ->
    take_run(TargetRunNum, CurrentRunNum, RestRuns, [Run | NewerRunsReversed]).


%% @private
-spec locate_lane_run(atm_lane_execution:lane_run_selector(), ctx()) ->
    {ok, {json_utils:query(), json_utils:json_map()}} | ?ERROR_NOT_FOUND.
locate_lane_run({AtmLaneSelector, AtmRunSelector}, ExpStateCtx = #exp_workflow_execution_state_ctx{
    current_run_num = CurrentRunNum,
    exp_workflow_execution_state = #{<<"lanes">> := AtmLaneExecutions}
}) ->
    AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpStateCtx),
    TargetRunNum = resolve_run_selector(AtmRunSelector, ExpStateCtx),

    SearchResult = lists_utils:foldl_while(fun
        (Run = #{<<"runNumber">> := null}, Acc) when CurrentRunNum =< TargetRunNum ->
            {halt, {ok, Acc + 1, Run}};
        (Run = #{<<"runNumber">> := RunNum}, Acc) when RunNum =:= TargetRunNum ->
            {halt, {ok, Acc + 1, Run}};
        (#{<<"runNumber">> := RunNum}, _) when RunNum < TargetRunNum ->
            {halt, ?ERROR_NOT_FOUND};
        (_, Acc) ->
            {cont, Acc + 1}
    end, 0, maps:get(<<"runs">>, lists:nth(AtmLaneIndex, AtmLaneExecutions))),

    case SearchResult of
        {ok, AtmRunIndex, AtmLaneRun} ->
            Path = ?JSON_PATH("lanes.[~B].runs.[~B]", [AtmLaneIndex - 1, AtmRunIndex - 1]),
            {ok, {Path, AtmLaneRun}};
        _ ->
            ?ERROR_NOT_FOUND
    end.


%% @private
-spec is_current_lane_run(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:record()
) ->
    boolean().
is_current_lane_run({AtmLaneSelector, AtmRunSelector}, ExpStateCtx = #exp_workflow_execution_state_ctx{
    current_lane_index = CurrentAtmLaneIndex,
    current_run_num = CurrentRunNum
}) ->
    CurrentAtmLaneIndex == resolve_lane_selector(AtmLaneSelector, ExpStateCtx) andalso
        CurrentRunNum == resolve_run_selector(AtmRunSelector, ExpStateCtx).


%% @private
-spec resolve_lane_selector(atm_lane_execution:selector(), ctx()) ->
    atm_lane_execution:index().
resolve_lane_selector(current, #exp_workflow_execution_state_ctx{current_lane_index = CurrentAtmLaneIndex}) ->
    CurrentAtmLaneIndex;
resolve_lane_selector(AtmLaneIndex, _) ->
    AtmLaneIndex.


%% @private
-spec resolve_run_selector(atm_lane_execution:run_selector(), ctx()) ->
    atm_lane_execution:run_num().
resolve_run_selector(current, #exp_workflow_execution_state_ctx{current_run_num = CurrentRunNum}) ->
    CurrentRunNum;
resolve_run_selector(RunNum, _) ->
    RunNum.


%% @private
-spec build_timestamp_field_validator(non_neg_integer()) ->
    fun((non_neg_integer()) -> boolean()).
build_timestamp_field_validator(ApproxTime) ->
    fun(RecordedTime) -> abs(RecordedTime - ApproxTime) < 10 end.


%% @private
-spec build_initial_regular_lane_run_exp_state(
    undefined | atm_lane_execution:run_num(),
    binary()
) ->
    lane_run_state().
build_initial_regular_lane_run_exp_state(ExpRunNum, ExpInitialStatus) ->
    build_initial_regular_lane_run_exp_state(
        ExpRunNum, ExpInitialStatus, <<"regular">>, undefined, undefined
    ).


%% @private
-spec build_initial_regular_lane_run_exp_state(
    undefined | atm_lane_execution:run_num(),
    binary(),
    binary(),
    undefined | atm_lane_execution:run_num(),
    undefined | atm_store:id()
) ->
    lane_run_state().
build_initial_regular_lane_run_exp_state(
    ExpRunNum,
    ExpInitialStatus,
    ExpRunType,
    ExpOriginalRunNum,
    ExpIteratedStoreId
) ->
    #{
        <<"runNumber">> => utils:undefined_to_null(ExpRunNum),
        <<"originRunNumber">> => utils:undefined_to_null(ExpOriginalRunNum),
        <<"status">> => ExpInitialStatus,
        <<"iteratedStoreId">> => utils:undefined_to_null(ExpIteratedStoreId),
        <<"exceptionStoreId">> => null,
        <<"parallelBoxes">> => [],
        <<"runType">> => ExpRunType,
        <<"isRetriable">> => false,
        <<"isRerunable">> => false
    }.


%% @private
-spec build_exp_initial_parallel_box_execution_state(automation:id(), task_registry()) ->
    parallel_box_execution_state().
build_exp_initial_parallel_box_execution_state(AtmParallelBoxSchemaId, AtmTasksRegistry) ->
    #{
        <<"schemaId">> => AtmParallelBoxSchemaId,
        <<"status">> => <<"pending">>,
        <<"taskRegistry">> => AtmTasksRegistry
    }.


%% @private
-spec build_task_execution_initial_exp_state(atm_workflow_execution:id(), automation:id()) ->
    task_execution_state().
build_task_execution_initial_exp_state(AtmWorkflowExecutionId, AtmTaskSchemaId) ->
    #{
        <<"atmWorkflowExecutionId">> => AtmWorkflowExecutionId,
        <<"schemaId">> => AtmTaskSchemaId,
        <<"status">> => <<"pending">>,
        <<"itemsInProcessing">> => 0,
        <<"itemsProcessed">> => 0,
        <<"itemsFailed">> => 0
    }.


%% @private
-spec assert_matches_with_backend_internal(ctx(), log_fun()) -> boolean().
assert_matches_with_backend_internal(ExpStateCtx, LogFun) ->
    assert_workflow_execution_expectations(ExpStateCtx, LogFun) and
        assert_task_execution_expectations(ExpStateCtx, LogFun).


%% @private
-spec assert_workflow_execution_expectations(ctx(), log_fun()) -> boolean().
assert_workflow_execution_expectations(ExpStateCtx = #exp_workflow_execution_state_ctx{
    exp_workflow_execution_state = ExpAtmWorkflowExecutionState
}, LogFun) ->
    AtmWorkflowExecutionState = atm_workflow_execution_to_json(fetch_workflow_execution(ExpStateCtx)),

    case catch assert_json_expectations(
        <<"atmWorkflowExecution">>, ExpAtmWorkflowExecutionState, AtmWorkflowExecutionState, LogFun
    ) of
        ok ->
            true;
        badmatch ->
            LogFun(
                "Error: mismatch between exp workflow execution state: ~n~p~n~nand model stored in op: ~n~p",
                [ExpAtmWorkflowExecutionState, AtmWorkflowExecutionState]
            ),
            false
    end.


%% @private
-spec assert_task_execution_expectations(ctx(), log_fun()) -> boolean().
assert_task_execution_expectations(#exp_workflow_execution_state_ctx{
    provider_selector = ProviderSelector,
    exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionStateCtxRegistry
}, LogFun) ->
    maps_utils:fold_while(fun(AtmTaskExecutionId, ExpAtmTaskExecution, true) ->
        {ok, #document{value = AtmTaskExecution}} = opw_test_rpc:call(
            ProviderSelector, atm_task_execution, get, [AtmTaskExecutionId]
        ),
        AtmTaskExecutionState = atm_task_execution_to_json(AtmTaskExecution),
        ExpAtmTaskExecutionState = ExpAtmTaskExecution#exp_task_execution_state_ctx.exp_state,

        case catch assert_json_expectations(
            <<"atmTaskExecution">>, ExpAtmTaskExecutionState, AtmTaskExecutionState, LogFun
        ) of
            ok ->
                {cont, true};
            badmatch ->
                LogFun(
                    "Error: mismatch between exp task execution state: ~n~p~n~nand model stored in op: ~n~p",
                    [ExpAtmTaskExecutionState, AtmTaskExecutionState]
                ),
                {halt, false}
        end
    end, true, ExpAtmTaskExecutionStateCtxRegistry).


%% @private
-spec atm_workflow_execution_to_json(atm_workflow_execution:record()) ->
    workflow_execution_state().
atm_workflow_execution_to_json(AtmWorkflowExecution = #atm_workflow_execution{
    space_id = SpaceId,

    lanes_count = AtmLanesCount,

    status = Status,

    schedule_time = ScheduleTime,
    start_time = StartTime,
    suspend_time = SuspendTime,
    finish_time = FinishTime
}) ->
    #{
        <<"spaceId">> => SpaceId,

        <<"lanes">> => lists:map(
            fun(AtmLaneIndex) -> atm_lane_execution:to_json(AtmLaneIndex, AtmWorkflowExecution) end,
            lists:seq(1, AtmLanesCount)
        ),

        <<"status">> => atom_to_binary(Status, utf8),

        <<"scheduleTime">> => ScheduleTime,
        <<"startTime">> => StartTime,
        <<"suspendTime">> => SuspendTime,
        <<"finishTime">> => FinishTime
    }.


%% @private
-spec atm_task_execution_to_json(atm_task_execution:record()) ->
    task_execution_state().
atm_task_execution_to_json(#atm_task_execution{
    workflow_execution_id = AtmWorkflowExecutionId,
    schema_id = AtmTaskSchemaId,

    status = AtmTaskExecutionStatus,

    items_in_processing = ItemsInProcessing,
    items_processed = ItemsProcessed,
    items_failed = ItemsFailed
}) ->
    #{
        <<"atmWorkflowExecutionId">> => AtmWorkflowExecutionId,
        <<"schemaId">> => AtmTaskSchemaId,

        <<"status">> => atom_to_binary(AtmTaskExecutionStatus, utf8),

        <<"itemsInProcessing">> => ItemsInProcessing,
        <<"itemsProcessed">> => ItemsProcessed,
        <<"itemsFailed">> => ItemsFailed
    }.


%% private
-spec assert_json_expectations(
    binary(),
    json_utils:json_term(),
    json_utils:json_term(),
    log_fun()
) ->
    ok | no_return().
assert_json_expectations(Path, Expected, Value, LogFun) when is_map(Expected), is_map(Value) ->
    ExpectedKeys = lists:sort(maps:keys(Expected)),
    ValueKeys = lists:sort(maps:keys(Value)),

    case ExpectedKeys == ValueKeys of
        true ->
            ok;
        false ->
            LogFun("Error: unmatching keys in objects at '~p'.~nExpected: ~p~nGot: ~p", [
                Path, Expected, Value
            ]),
            throw(badmatch)
    end,

    maps:foreach(fun(Key, ExpectedField) ->
        ValueField = maps:get(Key, Value),
        assert_json_expectations(<<Path/binary, ".", Key/binary>>, ExpectedField, ValueField, LogFun)
    end, Expected);

assert_json_expectations(Path, Expected, Value, LogFun) when is_list(Expected), is_list(Value) ->
    case length(Expected) == length(Value) of
        true ->
            lists:foreach(fun({Index, {ExpectedItem, ValueItem}}) ->
                assert_json_expectations(
                    str_utils:format_bin("~s.[~B]", [Path, Index - 1]),
                    ExpectedItem,
                    ValueItem,
                    LogFun
                )
            end, lists_utils:enumerate(lists:zip(Expected, Value)));
        false ->
            LogFun("Error: unmatching arrays at '~p'.~nExpected: ~p~nGot: ~p", [
                Path, Expected, Value
            ]),
            throw(badmatch)
    end;

assert_json_expectations(Path, Expected, Value, LogFun) when is_function(Expected, 1) ->
    case Expected(Value) of
        true ->
            ok;
        false ->
            LogFun("Error: predicate for '~p' failed.~nGot: ~p", [Path, Value]),
            throw(badmatch)
    end;

assert_json_expectations(Path, Expected, Value, LogFun) ->
    case Expected == Value of
        true ->
            ok;
        false ->
            LogFun("Error: unmatching items at '~p'.~nExpected: ~p~nGot: ~p", [
                Path, Expected, Value
            ]),
            throw(badmatch)
    end.
