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

    expect_lane_run_started_preparing/2,
    expect_current_lane_run_started_preparing/2,
    expect_lane_run_started_preparing_in_advance/2,
    expect_lane_run_created/2,
    expect_lane_run_enqueued/2,
    expect_lane_run_aborting/2,
    expect_lane_run_finished/2,
    expect_lane_run_failed/2,
    expect_lane_run_cancelled/2,
    expect_lane_run_interrupted/2,
    expect_lane_run_num_set/3,

    expect_task_items_in_processing_increased/3,
    expect_task_items_transit_from_processing_to_processed/3,
    expect_task_transit_to_active_status_if_in_pending_status/2,
    expect_task_parallel_box_transit_to_active_status_if_in_pending_status/2,
    expect_task_lane_run_transit_to_active_status_if_in_enqueued_status/2,
    expect_task_finished/2,
    expect_task_parallel_box_finished_if_other_tasks_finished/2,
    expect_all_tasks_skipped/2,

    expect_workflow_execution_aborting/1,
    expect_workflow_execution_finished/1,
    expect_workflow_execution_failed/1,
    expect_workflow_execution_cancelled/1,

    assert_matches_with_backend/1
]).

% json object similar in structure to translations returned via API endpoints
% (has the same keys but as for values instead of concrete values it may contain
% validator functions - e.g. timestamp fields should check approx time rather
% than concrete value)
-type exp_workflow_execution_state() :: json_utils:json_term().
-type exp_task_execution_state() :: json_utils:json_term().

-record(exp_task_execution, {
    lane_run_selector :: atm_lane_execution:lane_run_selector(),
    parallel_box_schema_id :: automation:id(),
    exp_state :: exp_task_execution_state()
}).
-type exp_task_execution() :: #exp_task_execution{}.

-type exp_task_executions_registry() :: #{atm_task_execution:id() => exp_task_execution()}.

-record(exp_state, {
    provider_selector :: oct_background:entity_selector(),
    lane_schemas :: [atm_lane_schema:record()],

    workflow_execution_id :: atm_workflow_execution:id(),
    current_lane_index :: atm_lane_execution:index(),
    current_run_num :: atm_lane_execution:run_num(),

    exp_workflow_execution_state :: exp_workflow_execution_state(),
    exp_task_executions_registry :: exp_task_executions_registry()
}).
-type exp_state() :: #exp_state{}.

-type task_registry() :: #{AtmTaskSchemaId :: automation:id() => atm_task_execution:id()}.

-export_type([exp_state/0]).


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
    exp_state().
init(
    ProviderSelector,
    SpaceId,
    AtmWorkflowExecutionId,
    ApproxScheduleTime,
    AtmLaneSchemas = [FirstAtmLaneSchema | RestAtmLaneSchemas]
) ->
    ExpFirstAtmLaneExecutionState = #{
        <<"schemaId">> => FirstAtmLaneSchema#atm_lane_schema.id,
        <<"runs">> => [build_exp_initial_regular_lane_run(1, <<"scheduled">>)]
    },
    ExpRestAtmLaneExecutionStates = lists:map(fun(#atm_lane_schema{id = AtmLaneSchemaId}) ->
        #{
            <<"schemaId">> => AtmLaneSchemaId,
            <<"runs">> => []
        }
    end, RestAtmLaneSchemas),

    #exp_state{
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
            <<"finishTime">> => 0
        },
        exp_task_executions_registry = #{}
    }.


-spec expect_lane_run_started_preparing(atm_lane_execution:lane_run_selector(), exp_state()) ->
    exp_state().
expect_lane_run_started_preparing(AtmLaneRunSelector, ExpState) ->
    case is_current_lane_run(AtmLaneRunSelector, ExpState) of
        true -> expect_current_lane_run_started_preparing(AtmLaneRunSelector, ExpState);
        false -> expect_lane_run_started_preparing_in_advance(AtmLaneRunSelector, ExpState)
    end.


-spec expect_current_lane_run_started_preparing(
    atm_lane_execution:lane_run_selector(),
    exp_state()
) ->
    exp_state().
expect_current_lane_run_started_preparing(AtmLaneRunSelector, ExpState0) ->
    ExpAtmLaneRunStateDiff = #{<<"status">> => <<"preparing">>},
    ExpState1 = update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpState0),

    ExpAtmWorkflowExecutionStateDiff = fun
        (ExpAtmWorkflowExecutionState = #{<<"status">> := <<"scheduled">>}) ->
            ExpAtmWorkflowExecutionState#{
                <<"status">> => <<"active">>,
                <<"startTime">> => build_timestamp_field_validator(?NOW())
            };
        (ExpAtmWorkflowExecutionState) ->
            ExpAtmWorkflowExecutionState
    end,
    update_exp_workflow_execution_state(ExpAtmWorkflowExecutionStateDiff, ExpState1).


-spec expect_lane_run_started_preparing_in_advance(
    atm_lane_execution:lane_run_selector(),
    exp_state()
) ->
    exp_state().
expect_lane_run_started_preparing_in_advance(AtmLaneRunSelector, ExpState = #exp_state{
    exp_workflow_execution_state = ExpWorkflowExecutionState0
}) ->
    {AtmLaneRunPath, ExpAtmLaneRunState} = case locate_lane_run(AtmLaneRunSelector, ExpState) of
        {ok, {Path, Run = #{<<"status">> := <<"scheduled">>}}} ->
            {Path, Run#{<<"status">> => <<"preparing">>}};
        ?ERROR_NOT_FOUND ->
            AtmLaneIndex = resolve_lane_selector(element(1, AtmLaneRunSelector), ExpState),
            Path = ?JSON_PATH("lanes.[~B].runs.[0]", [AtmLaneIndex - 1]),
            {Path, build_exp_initial_regular_lane_run(undefined, <<"preparing">>)}
    end,
    {ok, ExpWorkflowExecutionState1} = json_utils:insert(
        ExpWorkflowExecutionState0,
        ExpAtmLaneRunState,
        AtmLaneRunPath
    ),
    ExpState#exp_state{exp_workflow_execution_state = ExpWorkflowExecutionState1}.


-spec expect_lane_run_created(atm_lane_execution:lane_run_selector(), exp_state()) ->
    exp_state().
expect_lane_run_created(AtmLaneRunSelector, ExpState = #exp_state{
    workflow_execution_id = AtmWorkflowExecutionId,
    exp_task_executions_registry = ExpAtmTaskExecutionRegistry0
}) ->
    AtmLaneSchema = get_lane_schema(AtmLaneRunSelector, ExpState),
    {ok, AtmLaneRun} = atm_lane_execution:get_run(AtmLaneRunSelector, fetch_workflow_execution(ExpState)),

    {ExpAtmParallelBoxExecutionStates, ExpAtmTaskExecutionRegistry1} = lists:mapfoldl(
        fun({AtmParallelBoxSchema, AtmParallelBoxExecution}, OuterAcc) ->
            AtmParallelBoxSchemaId = AtmParallelBoxSchema#atm_parallel_box_schema.id,
            AtmTasksRegistry = get_task_registry(AtmParallelBoxExecution, AtmParallelBoxSchema),

            ExpAtmParallelBoxExecutionState = build_exp_initial_parallel_box_execution_state(
                AtmParallelBoxSchemaId,
                AtmTasksRegistry
            ),
            UpdatedOuterAcc = maps:fold(fun(AtmTaskSchemaId, AtmTaskExecutionId, InnerAcc) ->
                InnerAcc#{AtmTaskExecutionId => #exp_task_execution{
                    lane_run_selector = AtmLaneRunSelector,
                    parallel_box_schema_id = AtmParallelBoxSchemaId,
                    exp_state = build_exp_initial_task_execution_state(
                        AtmWorkflowExecutionId, AtmTaskSchemaId
                    )
                }}
            end, OuterAcc, AtmTasksRegistry),

            {ExpAtmParallelBoxExecutionState, UpdatedOuterAcc}
        end,
        ExpAtmTaskExecutionRegistry0,
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
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpState#exp_state{
        exp_task_executions_registry = ExpAtmTaskExecutionRegistry1
    }).


-spec expect_lane_run_enqueued(atm_lane_execution:lane_run_selector(), exp_state()) ->
    exp_state().
expect_lane_run_enqueued(AtmLaneRunSelector, ExpState) ->
    ExpAtmLaneRunStateDiff = #{<<"status">> => <<"enqueued">>},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpState).


-spec expect_lane_run_aborting(atm_lane_execution:lane_run_selector(), exp_state()) ->
    exp_state().
expect_lane_run_aborting(AtmLaneRunSelector, ExpState) ->
    ExpAtmLaneRunStateDiff = #{<<"status">> => <<"aborting">>},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpState).


-spec expect_lane_run_finished(atm_lane_execution:lane_run_selector(), exp_state()) ->
    exp_state().
expect_lane_run_finished(AtmLaneRunSelector, ExpState) ->
    ExpAtmLaneRunStateDiff = #{
        <<"status">> => <<"finished">>,
        <<"isRerunable">> => true
    },
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpState).


-spec expect_lane_run_failed(atm_lane_execution:lane_run_selector(), exp_state()) ->
    exp_state().
expect_lane_run_failed(AtmLaneRunSelector, ExpState) ->
    ExpAtmLaneRunStateDiff = #{
        <<"status">> => <<"failed">>,
        <<"isRerunable">> => true
    },
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpState).


-spec expect_lane_run_cancelled(atm_lane_execution:lane_run_selector(), exp_state()) ->
    exp_state().
expect_lane_run_cancelled(AtmLaneRunSelector, ExpState) ->
    ExpAtmLaneRunStateDiff = #{
        <<"status">> => <<"cancelled">>,
        <<"isRerunable">> => true
    },
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpState).


-spec expect_lane_run_interrupted(atm_lane_execution:lane_run_selector(), exp_state()) ->
    exp_state().
expect_lane_run_interrupted(AtmLaneRunSelector, ExpState) ->
    ExpAtmLaneRunStateDiff = #{<<"status">> => <<"interrupted">>},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpState).


-spec expect_lane_run_num_set(
    atm_lane_execution:lane_run_selector(),
    atm_lane_execution:run_num(),
    exp_state()
) ->
    exp_state().
expect_lane_run_num_set(AtmLaneRunSelector, RunNum, ExpState) ->
    ExpAtmLaneRunStateDiff = #{<<"runNumber">> => RunNum},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpState).


-spec expect_task_items_in_processing_increased(
    atm_task_execution:id(),
    pos_integer(),
    exp_state()
) ->
    exp_state().
expect_task_items_in_processing_increased(AtmTaskExecutionId, Inc, ExpState) ->
    ExpAtmTaskExecutionStateDiff = fun(AtmTaskExecution = #{<<"itemsInProcessing">> := IIP}) ->
        AtmTaskExecution#{<<"itemsInProcessing">> => IIP + Inc}
    end,
    update_exp_task_execution_state(AtmTaskExecutionId, ExpAtmTaskExecutionStateDiff, ExpState).


-spec expect_task_items_transit_from_processing_to_processed(
    atm_task_execution:id(),
    pos_integer(),
    exp_state()
) ->
    exp_state().
expect_task_items_transit_from_processing_to_processed(AtmTaskExecutionId, Count, ExpState) ->
    ExpAtmTaskExecutionStateDiff = fun(ExpAtmTaskExecutionState = #{
        <<"itemsInProcessing">> := IIP,
        <<"itemsProcessed">> := IP
    }) ->
        ExpAtmTaskExecutionState#{
            <<"itemsInProcessing">> => IIP - Count,
            <<"itemsProcessed">> => IP + Count
        }
    end,
    update_exp_task_execution_state(AtmTaskExecutionId, ExpAtmTaskExecutionStateDiff, ExpState).


-spec expect_task_transit_to_active_status_if_in_pending_status(
    atm_task_execution:id(),
    exp_state()
) ->
    exp_state().
expect_task_transit_to_active_status_if_in_pending_status(AtmTaskExecutionId, ExpState) ->
    update_exp_task_execution_state(
        AtmTaskExecutionId,
        build_transit_to_status_if_in_status_diff(<<"pending">>, <<"active">>),
        ExpState
    ).


-spec expect_task_parallel_box_transit_to_active_status_if_in_pending_status(
    atm_task_execution:id(),
    exp_state()
) ->
    exp_state().
expect_task_parallel_box_transit_to_active_status_if_in_pending_status(AtmTaskExecutionId, ExpState = #exp_state{
    exp_task_executions_registry = ExpAtmTaskExecutionsRegistry
}) ->
    ExpTaskExecution = maps:get(AtmTaskExecutionId, ExpAtmTaskExecutionsRegistry),

    update_exp_parallel_box_execution_state(
        ExpTaskExecution#exp_task_execution.lane_run_selector,
        ExpTaskExecution#exp_task_execution.parallel_box_schema_id,
        build_transit_to_status_if_in_status_diff(<<"pending">>, <<"active">>),
        ExpState
    ).


-spec expect_task_lane_run_transit_to_active_status_if_in_enqueued_status(
    atm_task_execution:id(),
    exp_state()
) ->
    exp_state().
expect_task_lane_run_transit_to_active_status_if_in_enqueued_status(AtmTaskExecutionId, ExpState = #exp_state{
    exp_task_executions_registry = ExpAtmTaskExecutionsRegistry
}) ->
    #exp_task_execution{lane_run_selector = AtmLaneRunSelector} = maps:get(
        AtmTaskExecutionId, ExpAtmTaskExecutionsRegistry
    ),
    update_exp_lane_run_state(
        AtmLaneRunSelector,
        build_transit_to_status_if_in_status_diff(<<"enqueued">>, <<"active">>),
        ExpState
    ).


-spec expect_task_finished(atm_task_execution:id(), exp_state()) ->
    exp_state().
expect_task_finished(AtmTaskExecutionId, ExpState) ->
    ExpAtmTaskExecutionStateDiff = fun(ExpAtmTaskExecutionState) ->
        ExpAtmTaskExecutionState#{<<"status">> => <<"finished">>}
    end,
    update_exp_task_execution_state(AtmTaskExecutionId, ExpAtmTaskExecutionStateDiff, ExpState).


-spec expect_task_parallel_box_finished_if_other_tasks_finished(
    atm_task_execution:id(),
    exp_state()
) ->
    exp_state().
expect_task_parallel_box_finished_if_other_tasks_finished(AtmTaskExecutionId, ExpState = #exp_state{
    exp_task_executions_registry = ExpAtmTaskExecutionsRegistry
}) ->
    #exp_task_execution{
        lane_run_selector = AtmLaneRunSelector,
        parallel_box_schema_id = AtmParallelBoxSchemaId
    } = maps:get(AtmTaskExecutionId, ExpAtmTaskExecutionsRegistry),

    Diff = fun(ExpParallelBoxState = #{<<"taskRegistry">> := AtmTasksRegistry}) ->
        ExpTaskExecutionStatuses = lists:usort(lists:map(fun(ExpTaskExecutionId) ->
            ExpTaskExecution = maps:get(ExpTaskExecutionId, ExpAtmTaskExecutionsRegistry),
            maps:get(<<"status">>, ExpTaskExecution#exp_task_execution.exp_state)
        end, maps:values(AtmTasksRegistry))),

        case ExpTaskExecutionStatuses of
            [<<"finished">>] -> ExpParallelBoxState#{<<"status">> => <<"finished">>};
            _ -> ExpParallelBoxState
        end
    end,

    update_exp_parallel_box_execution_state(AtmLaneRunSelector, AtmParallelBoxSchemaId, Diff, ExpState).


-spec expect_all_tasks_skipped(atm_lane_execution:lane_run_selector(), exp_state()) ->
    exp_state().
expect_all_tasks_skipped(AtmLaneRunSelector, ExpState = #exp_state{
    exp_task_executions_registry = ExpAtmTaskExecutionRegistry0
}) ->
    ExpTaskExecutionDiff = fun(ExpAtmTaskExecution = #exp_task_execution{exp_state = ExpState}) ->
        ExpAtmTaskExecution#exp_task_execution{exp_state = ExpState#{
            <<"status">> => <<"skipped">>
        }}
    end,
    {ok, {_AtmLaneRunPath, ExpAtmLaneRunState}} = locate_lane_run(AtmLaneRunSelector, ExpState),

    {ExpAtmParallelBoxExecutionStates, ExpAtmTaskExecutionRegistry1} = lists:mapfoldl(
        fun(ExpAtmParallelBoxExecutionState = #{<<"taskRegistry">> := AtmTaskRegistry}, OuterAcc) ->
            UpdatedExpAtmParallelBoxExecutionState = ExpAtmParallelBoxExecutionState#{
                <<"status">> => <<"skipped">>
            },
            UpdatedOuterAcc = maps:fold(fun(_AtmTaskSchemaId, AtmTaskExecutionId, InnerAcc) ->
                maps:update_with(AtmTaskExecutionId, ExpTaskExecutionDiff, InnerAcc)
            end, OuterAcc, AtmTaskRegistry),

            {UpdatedExpAtmParallelBoxExecutionState, UpdatedOuterAcc}
        end,
        ExpAtmTaskExecutionRegistry0,
        maps:get(<<"parallelBoxes">>, ExpAtmLaneRunState)
    ),

    ExpAtmLaneRunStateDiff = #{<<"parallelBoxes">> => ExpAtmParallelBoxExecutionStates},
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpState#exp_state{
        exp_task_executions_registry = ExpAtmTaskExecutionRegistry1
    }).


-spec expect_workflow_execution_aborting(exp_state()) -> exp_state().
expect_workflow_execution_aborting(ExpState) ->
    ExpAtmWorkflowExecutionStateDiff = fun
        (ExpAtmWorkflowExecutionState = #{<<"startTime">> := 0}) ->
            % atm workflow execution failure/cancel while in schedule status
            ExpAtmWorkflowExecutionState#{
                <<"status">> => <<"aborting">>,
                <<"startTime">> => build_timestamp_field_validator(?NOW())
            };
        (ExpAtmWorkflowExecutionState) ->
            ExpAtmWorkflowExecutionState#{<<"status">> => <<"aborting">>}
    end,
    update_exp_workflow_execution_state(ExpAtmWorkflowExecutionStateDiff, ExpState).


-spec expect_workflow_execution_finished(exp_state()) -> exp_state().
expect_workflow_execution_finished(ExpState) ->
    ExpAtmWorkflowExecutionStateDiff = #{
        <<"status">> => <<"finished">>,
        <<"finishTime">> => build_timestamp_field_validator(?NOW())
    },
    update_exp_workflow_execution_state(ExpAtmWorkflowExecutionStateDiff, ExpState).


-spec expect_workflow_execution_failed(exp_state()) -> exp_state().
expect_workflow_execution_failed(ExpState) ->
    ExpAtmWorkflowExecutionStateDiff = #{
        <<"status">> => <<"failed">>,
        <<"finishTime">> => build_timestamp_field_validator(?NOW())
    },
    update_exp_workflow_execution_state(ExpAtmWorkflowExecutionStateDiff, ExpState).


-spec expect_workflow_execution_cancelled(exp_state()) -> exp_state().
expect_workflow_execution_cancelled(ExpState) ->
    ExpAtmWorkflowExecutionStateDiff = #{
        <<"status">> => <<"cancelled">>,
        <<"finishTime">> => build_timestamp_field_validator(?NOW())
    },
    update_exp_workflow_execution_state(ExpAtmWorkflowExecutionStateDiff, ExpState).


-spec assert_matches_with_backend(exp_state()) -> boolean().
assert_matches_with_backend(ExpState) ->
    assert_workflow_execution_expectations(ExpState) and assert_task_execution_expectations(ExpState).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec fetch_workflow_execution(exp_state()) -> atm_workflow_execution:id().
fetch_workflow_execution(#exp_state{
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
-spec get_lane_schema(atm_lane_execution:lane_run_selector(), exp_state()) ->
    atm_lane_schema:record().
get_lane_schema({AtmLaneSelector, _}, ExpState = #exp_state{lane_schemas = AtmLaneSchemas}) ->
    AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpState),
    lists:nth(AtmLaneIndex, AtmLaneSchemas).


%% @private
-spec build_transit_to_status_if_in_status_diff(binary(), binary()) ->
    fun((json_utils:json_map()) -> json_utils:json_map()).
build_transit_to_status_if_in_status_diff(RequiredStatus, NewStatus) ->
    fun
        (ExpState = #{<<"status">> := Status}) when Status =:= RequiredStatus ->
            ExpState#{<<"status">> => NewStatus};
        (ExpState) ->
            ExpState
    end.


%% @private
-spec update_exp_workflow_execution_state(
    json_utils:json_map() | fun((json_utils:json_map()) -> json_utils:json_map()),
    exp_state()
) ->
    exp_state().
update_exp_workflow_execution_state(Diff, ExpState) when is_map(Diff) ->
    update_exp_workflow_execution_state(
        fun(ExpAtmWorkflowExecutionState) -> maps:merge(ExpAtmWorkflowExecutionState, Diff) end,
        ExpState
    );

update_exp_workflow_execution_state(Diff, ExpState = #exp_state{
    exp_workflow_execution_state = ExpWorkflowExecutionState
}) ->
    ExpState#exp_state{exp_workflow_execution_state = Diff(ExpWorkflowExecutionState)}.


%% @private
-spec update_exp_lane_run_state(
    atm_lane_execution:lane_run_selector(),
    json_utils:json_map() | fun((json_utils:json_map()) -> json_utils:json_map()),
    exp_state()
) ->
    exp_state().
update_exp_lane_run_state(AtmLaneRunSelector, Diff, ExpState) when is_map(Diff) ->
    update_exp_lane_run_state(
        AtmLaneRunSelector,
        fun(ExpAtmLaneRunState) -> maps:merge(ExpAtmLaneRunState, Diff) end,
        ExpState
    );

update_exp_lane_run_state(AtmLaneRunSelector, Diff, ExpState = #exp_state{
    exp_workflow_execution_state = ExpWorkflowExecutionState0
}) ->
    {ok, {AtmLaneRunPath, AtmLaneRun}} = locate_lane_run(AtmLaneRunSelector, ExpState),
    {ok, ExpWorkflowExecutionState1} = json_utils:insert(
        ExpWorkflowExecutionState0,
        Diff(AtmLaneRun),
        AtmLaneRunPath
    ),
    ExpState#exp_state{exp_workflow_execution_state = ExpWorkflowExecutionState1}.


%% @private
-spec update_exp_parallel_box_execution_state(
    atm_lane_execution:lane_run_selector(),
    automation:id(),
    json_utils:json_map() | fun((json_utils:json_map()) -> json_utils:json_map()),
    exp_state()
) ->
    exp_state().
update_exp_parallel_box_execution_state(AtmLaneRunSelector, AtmParallelBoxSchemaId, Diff, ExpState) when
    is_map(Diff)
->
    update_exp_parallel_box_execution_state(
        AtmLaneRunSelector,
        AtmParallelBoxSchemaId,
        fun(ExpAtmTaskExecutionState) -> maps:merge(ExpAtmTaskExecutionState, Diff) end,
        ExpState
    );

update_exp_parallel_box_execution_state(
    AtmLaneRunSelector,
    AtmParallelBoxSchemaId,
    ExpParallelBoxStateDiff,
    ExpState
) ->
    ExpAtmLaneRunStateDiff = fun(ExpAtmLaneRunState = #{<<"parallelBoxes">> := ExpAtmParallelBoxes}) ->
        ExpAtmLaneRunState#{<<"parallelBoxes">> => lists:map(fun
            (ExpAtmParallelBoxState = #{<<"schemaId">> := Id}) when Id =:= AtmParallelBoxSchemaId ->
                ExpParallelBoxStateDiff(ExpAtmParallelBoxState);
            (ExpAtmParallelBoxState) ->
                ExpAtmParallelBoxState
        end, ExpAtmParallelBoxes)}
    end,
    update_exp_lane_run_state(AtmLaneRunSelector, ExpAtmLaneRunStateDiff, ExpState).


%% @private
-spec update_exp_task_execution_state(
    atm_task_execution:id(),
    json_utils:json_map() | fun((json_utils:json_map()) -> json_utils:json_map()),
    exp_state()
) ->
    exp_state().
update_exp_task_execution_state(AtmTaskExecutionId, Diff, ExpState) when is_map(Diff) ->
    update_exp_task_execution_state(
        AtmTaskExecutionId,
        fun(ExpAtmTaskExecutionState) -> maps:merge(ExpAtmTaskExecutionState, Diff) end,
        ExpState
    );

update_exp_task_execution_state(AtmTaskExecutionId, ExpStateDiff, ExpState = #exp_state{
    exp_task_executions_registry = ExpAtmTaskExecutionsRegistry
}) ->
    Diff = fun(ExpAtmTaskExecution = #exp_task_execution{exp_state = ExpState}) ->
        ExpAtmTaskExecution#exp_task_execution{exp_state = ExpStateDiff(ExpState)}
    end,
    ExpState#exp_state{exp_task_executions_registry = maps:update_with(
        AtmTaskExecutionId, Diff, ExpAtmTaskExecutionsRegistry
    )}.


%% @private
-spec locate_lane_run(atm_lane_execution:lane_run_selector(), exp_state()) ->
    {ok, {json_utils:query(), json_utils:json_map()}} | ?ERROR_NOT_FOUND.
locate_lane_run({AtmLaneSelector, AtmRunSelector}, ExpState = #exp_state{
    current_run_num = CurrentRunNum,
    exp_workflow_execution_state = #{<<"lanes">> := AtmLaneExecutions}
}) ->
    AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpState),
    TargetRunNum = resolve_run_selector(AtmRunSelector, ExpState),

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
is_current_lane_run({AtmLaneSelector, AtmRunSelector}, ExpState = #exp_state{
    current_lane_index = CurrentAtmLaneIndex,
    current_run_num = CurrentRunNum
}) ->
    CurrentAtmLaneIndex == resolve_lane_selector(AtmLaneSelector, ExpState) andalso
        CurrentRunNum == resolve_run_selector(AtmRunSelector, ExpState).


%% @private
-spec resolve_lane_selector(atm_lane_execution:selector(), exp_state()) ->
    atm_lane_execution:index().
resolve_lane_selector(current, #exp_state{current_lane_index = CurrentAtmLaneIndex}) ->
    CurrentAtmLaneIndex;
resolve_lane_selector(AtmLaneIndex, _) ->
    AtmLaneIndex.


%% @private
-spec resolve_run_selector(atm_lane_execution:run_selector(), exp_state()) ->
    atm_lane_execution:run_num().
resolve_run_selector(current, #exp_state{current_run_num = CurrentRunNum}) ->
    CurrentRunNum;
resolve_run_selector(RunNum, _) ->
    RunNum.


%% @private
-spec build_timestamp_field_validator(non_neg_integer()) ->
    fun((non_neg_integer()) -> boolean()).
build_timestamp_field_validator(ApproxTime) ->
    fun(RecordedTime) -> abs(RecordedTime - ApproxTime) < 10 end.


%% @private
-spec build_exp_initial_regular_lane_run(
    undefined | atm_lane_execution:run_num(),
    binary()
) ->
    json_utils:json_map().
build_exp_initial_regular_lane_run(ExpRunNum, ExpInitialStatus) ->
    #{
        <<"runNumber">> => utils:undefined_to_null(ExpRunNum),
        <<"originRunNumber">> => null,
        <<"status">> => ExpInitialStatus,
        <<"iteratedStoreId">> => null,
        <<"exceptionStoreId">> => null,
        <<"parallelBoxes">> => [],
        <<"runType">> => <<"regular">>,
        <<"isRetriable">> => false,
        <<"isRerunable">> => false
    }.


%% @private
-spec build_exp_initial_parallel_box_execution_state(automation:id(), task_registry()) ->
    json_utils:json_map().
build_exp_initial_parallel_box_execution_state(AtmParallelBoxSchemaId, AtmTasksRegistry) ->
    #{
        <<"schemaId">> => AtmParallelBoxSchemaId,
        <<"status">> => <<"pending">>,
        <<"taskRegistry">> => AtmTasksRegistry
    }.


%% @private
-spec build_exp_initial_task_execution_state(atm_workflow_execution:id(), automation:id()) ->
    json_utils:json_map().
build_exp_initial_task_execution_state(AtmWorkflowExecutionId, AtmTaskSchemaId) ->
    #{
        <<"atmWorkflowExecutionId">> => AtmWorkflowExecutionId,
        <<"schemaId">> => AtmTaskSchemaId,
        <<"status">> => <<"pending">>,
        <<"itemsInProcessing">> => 0,
        <<"itemsProcessed">> => 0,
        <<"itemsFailed">> => 0
    }.


%% @private
-spec assert_workflow_execution_expectations(exp_state()) -> boolean().
assert_workflow_execution_expectations(ExpState = #exp_state{
    exp_workflow_execution_state = ExpWorkflowExecutionJson
}) ->
    AtmWorkflowExecution = fetch_workflow_execution(ExpState),
    AtmWorkflowExecutionJson = atm_workflow_execution_to_json(AtmWorkflowExecution),

    case catch assert_json_expectations(
        <<"atmWorkflowExecution">>, ExpWorkflowExecutionJson, AtmWorkflowExecutionJson
    ) of
        ok ->
            true;
        badmatch ->
            ct:pal(
                "Error: mismatch between exp workflow execution state: ~n~p~n~nand model stored in op: ~n~p",
                [ExpWorkflowExecutionJson, AtmWorkflowExecutionJson]
            ),
            false
    end.


%% @private
-spec assert_task_execution_expectations(exp_state()) -> boolean().
assert_task_execution_expectations(#exp_state{
    provider_selector = ProviderSelector,
    exp_task_executions_registry = ExpTaskExecutionStatesRegistry
}) ->
    maps_utils:fold_while(fun(AtmTaskExecutionId, ExpAtmTaskExecution, true) ->
        {ok, #document{value = AtmTaskExecution}} = opw_test_rpc:call(
            ProviderSelector, atm_task_execution, get, [AtmTaskExecutionId]
        ),
        AtmTaskExecutionJson = atm_task_execution_to_json(AtmTaskExecution),
        ExpAtmTaskExecutionJson = ExpAtmTaskExecution#exp_task_execution.exp_state,

        case catch assert_json_expectations(
            <<"atmTaskExecution">>, ExpAtmTaskExecutionJson, AtmTaskExecutionJson
        ) of
            ok ->
                {cont, true};
            badmatch ->
                ct:pal(
                    "Error: mismatch between exp task execution state: ~n~p~n~nand model stored in op: ~n~p",
                    [ExpAtmTaskExecutionJson, AtmTaskExecutionJson]
                ),
                {halt, false}
        end
    end, true, ExpTaskExecutionStatesRegistry).


%% @private
-spec atm_workflow_execution_to_json(atm_workflow_execution:record()) ->
    json_utils:json_map().
atm_workflow_execution_to_json(#atm_workflow_execution{
    space_id = SpaceId,

    lanes = AtmLaneExecutions,
    lanes_count = AtmLanesCount,

    status = Status,

    schedule_time = ScheduleTime,
    start_time = StartTime,
    finish_time = FinishTime
}) ->
    #{
        <<"spaceId">> => SpaceId,

        <<"lanes">> => lists:map(
            fun(LaneIndex) -> atm_lane_execution:to_json(maps:get(LaneIndex, AtmLaneExecutions)) end,
            lists:seq(1, AtmLanesCount)
        ),

        <<"status">> => atom_to_binary(Status, utf8),

        <<"scheduleTime">> => ScheduleTime,
        <<"startTime">> => StartTime,
        <<"finishTime">> => FinishTime
    }.


%% @private
-spec atm_task_execution_to_json(atm_task_execution:record()) ->
    json_utils:json_map().
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
-spec assert_json_expectations(binary(), json_utils:json_term(), json_utils:json_term()) ->
    ok | no_return().
assert_json_expectations(Path, Expected, Value) when is_map(Expected), is_map(Value) ->
    ExpectedKeys = lists:sort(maps:keys(Expected)),
    ValueKeys = lists:sort(maps:keys(Value)),

    case ExpectedKeys == ValueKeys of
        true ->
            ok;
        false ->
            ct:pal("Error: unmatching keys in objects at '~p'.~nExpected: ~p~nGot: ~p", [
                Path, Expected, Value
            ]),
            throw(badmatch)
    end,

    maps:foreach(fun(Key, ExpectedField) ->
        ValueField = maps:get(Key, Value),
        assert_json_expectations(<<Path/binary, ".", Key/binary>>, ExpectedField, ValueField)
    end, Expected);

assert_json_expectations(Path, Expected, Value) when is_list(Expected), is_list(Value) ->
    case length(Expected) == length(Value) of
        true ->
            lists:foreach(fun({Index, {ExpectedItem, ValueItem}}) ->
                assert_json_expectations(
                    str_utils:format_bin("~s.[~B]", [Path, Index - 1]),
                    ExpectedItem,
                    ValueItem
                )
            end, lists_utils:enumerate(lists:zip(Expected, Value)));
        false ->
            ct:pal("Error: unmatching arrays at '~p'.~nExpected: ~p~nGot: ~p", [
                Path, Expected, Value
            ]),
            throw(badmatch)
    end;

assert_json_expectations(Path, Expected, Value) when is_function(Expected, 1) ->
    case Expected(Value) of
        true ->
            ok;
        false ->
            ct:pal("Error: predicate for '~p' failed.~nGot: ~p", [Path, Value]),
            throw(badmatch)
    end;

assert_json_expectations(Path, Expected, Value) ->
    case Expected == Value of
        true ->
            ok;
        false ->
            ct:pal("Error: unmatching items at '~p'.~nExpected: ~p~nGot: ~p", [
                Path, Expected, Value
            ]),
            throw(badmatch)
    end.
