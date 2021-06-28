%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements callbacks for handling automation workflow
%%% execution process.
%%% @end
%%%--------------------------------------------------------------------
-module(atm_workflow_execution_handler).
-author("Bartosz Walkowicz").

-behaviour(workflow_handler).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/logging.hrl").

% workflow_handler callbacks
-export([
    prepare/2,
    get_lane_spec/3,

    process_item/6,
    process_result/4,

    handle_task_execution_ended/3,
    handle_lane_execution_ended/3,
    handle_workflow_execution_ended/2
]).


%%%===================================================================
%%% workflow_handler callbacks
%%%===================================================================


-spec prepare(atm_workflow_execution:id(), atm_workflow_execution_env:record()) ->
    ok | error.
prepare(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv) ->
    try
        prepare_internal(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO PREPARE WORKFLOW DUE TO: ~p", [
            AtmWorkflowExecutionId, Reason
        ]),
        error
    end.


-spec get_lane_spec(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    non_neg_integer()
) ->
    {ok, workflow_engine:lane_spec()} | error.
get_lane_spec(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmLaneIndex) ->
    try
        {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
        AtmLaneSchema = get_lane_schema(AtmLaneIndex, AtmWorkflowExecutionDoc),
        AtmLaneExecution = get_lane_execution(AtmLaneIndex, AtmWorkflowExecutionDoc),

        freeze_lane_iteration_store(AtmWorkflowExecutionEnv, AtmLaneSchema),

        {ok, #{
            parallel_boxes => atm_lane_execution:get_parallel_box_execution_specs(
                AtmLaneExecution
            ),
            iterator => acquire_iterator_for_lane(AtmWorkflowExecutionEnv, AtmLaneSchema),
            is_last => is_last_lane(AtmLaneIndex, AtmWorkflowExecutionDoc)
        }}
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO GET LANE ~p SPEC DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmLaneIndex, Reason
        ]),
        error
    end.


-spec process_item(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    automation:item(),
    binary(),
    binary()
) ->
    ok | error.
process_item(
    AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId,
    Item, ReportResultUrl, HeartbeatUrl
) ->
    try
        ok = atm_task_execution_api:run(
            AtmWorkflowExecutionEnv, AtmTaskExecutionId, Item,
            ReportResultUrl, HeartbeatUrl
        )
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO RUN TASK ~p DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmTaskExecutionId, Reason
        ]),
        report_task_execution_failed(AtmWorkflowExecutionEnv, AtmTaskExecutionId),
        error
    end.


-spec process_result(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    {error, term()} | json_utils:json_map()
) ->
    ok | error.
process_result(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId, {error, _} = Error) ->
    % TODO VFS-7637 use audit log
    ?error("[~p] ASYNC TASK EXECUTION ~p FAILED DUE TO: ~p", [
        AtmWorkflowExecutionId, AtmTaskExecutionId, Error
    ]),
    report_task_execution_failed(AtmWorkflowExecutionEnv, AtmTaskExecutionId),
    error;

process_result(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId, Results) ->
    try
        atm_task_execution_api:handle_results(AtmWorkflowExecutionEnv, AtmTaskExecutionId, Results)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO PROCESS RESULTS FOR TASK EXECUTION ~p DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmTaskExecutionId, Reason
        ]),
        report_task_execution_failed(AtmWorkflowExecutionEnv, AtmTaskExecutionId),
        error
    end.


-spec handle_task_execution_ended(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id()
) ->
    ok.
handle_task_execution_ended(AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId) ->
    try
        ok = atm_task_execution_api:mark_ended(AtmTaskExecutionId)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO MARK TASK EXECUTION ~p AS ENDED DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmTaskExecutionId, Reason
        ])
    end.


-spec handle_lane_execution_ended(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    non_neg_integer()
) ->
    ok.
handle_lane_execution_ended(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmLaneIndex) ->
    try
        {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
        AtmLaneSchema = get_lane_schema(AtmLaneIndex, AtmWorkflowExecutionDoc),

        unfreeze_lane_iteration_store(AtmWorkflowExecutionEnv, AtmLaneSchema)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO MARK LANE EXECUTION ~p AS ENDED DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmLaneIndex, Reason
        ])
    end.


-spec handle_workflow_execution_ended(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record()
) ->
    ok.
handle_workflow_execution_ended(AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv) ->
    try
        %% TODO uncomment after enabling other users to see stores content
%%        atm_workflow_execution_session:terminate(AtmWorkflowExecutionId)
        ok
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO MARK WORKFLOW EXECUTION AS ENDED DUE TO: ~p", [
            AtmWorkflowExecutionId, Reason
        ])
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec prepare_internal(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record()
) ->
    ok | no_return().
prepare_internal(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv) ->
    AtmWorkflowExecutionCtx = atm_workflow_execution_env:acquire_workflow_execution_ctx(
        AtmWorkflowExecutionEnv
    ),
    {ok, #document{value = #atm_workflow_execution{
        lanes = AtmLaneExecutions
    }}} = transition_to_preparing_status(AtmWorkflowExecutionId),

    try
        atm_lane_execution:prepare_all(AtmWorkflowExecutionCtx, AtmLaneExecutions)
    catch Type:Reason ->
        atm_workflow_execution_status:handle_transition_to_failed_status_from_waiting_phase(
            AtmWorkflowExecutionId
        ),
        erlang:Type(Reason)
    end,

    transition_to_enqueued_status(AtmWorkflowExecutionId).


%% @private
-spec transition_to_preparing_status(atm_workflow_execution:id()) ->
    {ok, atm_workflow_execution:doc()} | no_return().
transition_to_preparing_status(AtmWorkflowExecutionId) ->
    {ok, _} = atm_workflow_execution_status:handle_transition_in_waiting_phase(
        AtmWorkflowExecutionId, ?PREPARING_STATUS
    ).


%% @private
-spec transition_to_enqueued_status(atm_workflow_execution:id()) -> ok | no_return().
transition_to_enqueued_status(AtmWorkflowExecutionId) ->
    {ok, _} = atm_workflow_execution_status:handle_transition_in_waiting_phase(
        AtmWorkflowExecutionId, ?ENQUEUED_STATUS
    ),
    ok.


%% @private
-spec is_last_lane(non_neg_integer(), atm_workflow_execution:doc()) ->
    boolean().
is_last_lane(AtmLaneIndex, #document{value = #atm_workflow_execution{
    lanes = AtmLaneExecutions
}}) ->
    AtmLaneIndex == length(AtmLaneExecutions).


%% @private
-spec get_lane_execution(non_neg_integer(), atm_workflow_execution:doc()) ->
    atm_lane_execution:record().
get_lane_execution(AtmLaneIndex, #document{value = #atm_workflow_execution{
    lanes = AtmLaneExecutions
}}) ->
    lists:nth(AtmLaneIndex, AtmLaneExecutions).


%% @private
-spec get_lane_schema(non_neg_integer(), atm_workflow_execution:doc()) ->
    atm_lane_schema:record().
get_lane_schema(AtmLaneIndex, #document{value = #atm_workflow_execution{
    schema_snapshot_id = AtmWorkflowSchemaSnapshotId
}}) ->
    {ok, #document{value = #atm_workflow_schema_snapshot{
        lanes = AtmLaneSchemas
    }}} = atm_workflow_schema_snapshot:get(AtmWorkflowSchemaSnapshotId),

    lists:nth(AtmLaneIndex, AtmLaneSchemas).


%% @private
-spec freeze_lane_iteration_store(atm_workflow_execution_env:record(), atm_lane_schema:record()) ->
    ok | no_return().
freeze_lane_iteration_store(AtmWorkflowExecutionEnv, AtmLaneSchema) ->
    AtmStoreId = get_lane_iteration_store_id(AtmWorkflowExecutionEnv, AtmLaneSchema),
    ok = atm_store_api:freeze(AtmStoreId).


%% @private
-spec unfreeze_lane_iteration_store(atm_workflow_execution_env:record(), atm_lane_schema:record()) ->
    ok | no_return().
unfreeze_lane_iteration_store(AtmWorkflowExecutionEnv, AtmLaneSchema) ->
    AtmStoreId = get_lane_iteration_store_id(AtmWorkflowExecutionEnv, AtmLaneSchema),
    ok = atm_store_api:unfreeze(AtmStoreId).


%% @private
-spec get_lane_iteration_store_id(atm_workflow_execution_env:record(), atm_lane_schema:record()) ->
    atm_store:id().
get_lane_iteration_store_id(AtmWorkflowExecutionEnv, #atm_lane_schema{
    store_iterator_spec = #atm_store_iterator_spec{store_schema_id = AtmStoreSchemaId}
}) ->
    atm_workflow_execution_env:get_store_id(AtmStoreSchemaId, AtmWorkflowExecutionEnv).


%% @private
-spec acquire_iterator_for_lane(atm_workflow_execution_env:record(), atm_lane_schema:record()) ->
    atm_store_iterator:record() | no_return().
acquire_iterator_for_lane(AtmWorkflowExecutionEnv, #atm_lane_schema{
    store_iterator_spec = AtmStoreIteratorSpec
}) ->
    atm_store_api:acquire_iterator(AtmWorkflowExecutionEnv, AtmStoreIteratorSpec).


%% @private
-spec report_task_execution_failed(atm_workflow_execution_env:record(), atm_task_execution:id()) ->
    ok.
report_task_execution_failed(AtmWorkflowExecutionEnv, AtmTaskExecutionId) ->
    catch atm_task_execution_api:handle_results(AtmWorkflowExecutionEnv, AtmTaskExecutionId, error),
    ok.
