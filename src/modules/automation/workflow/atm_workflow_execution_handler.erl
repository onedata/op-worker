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

% TODO VFS-7551 substitute behaviour module
%%-behaviour(workflow_handler).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/logging.hrl").

% workflow_handler callbacks
-export([
    prepare/2,
    get_lane_spec/3,
    process_item/6,
    process_result/4,
    handle_task_execution_ended/3,
    handle_lane_execution_ended/3
]).


%%%===================================================================
%%% workflow_handler callbacks
%%%===================================================================


-spec prepare(atm_workflow_execution:id(), atm_workflow_execution_env:record()) ->
    ok | error.
prepare(AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv) ->
    try
        prepare_internal(AtmWorkflowExecutionId)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("FAILED TO PREPARE WORKFLOW ~p DUE TO: ~p", [
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
        ?error("[~p] FAILED TO GET LANE ~p SPEC ~p DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmLaneIndex, Reason
        ]),
        error
    end.


-spec process_item(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    atm_api:item(),
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
        catch atm_task_execution_api:handle_results(
            AtmWorkflowExecutionEnv, AtmTaskExecutionId, error
        ),
        error
    end.


-spec process_result(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    {error, term()} | json_utils:json_map()
) ->
    ok | error.
process_result(_, AtmWorkflowExecutionEnv, AtmTaskExecutionId, {error, _} = Error) ->
    % TODO VFS-7637 use audit log
    ?error("ASYNC TASK EXECUTION ~p FAILED DUE TO: ~p", [
        AtmTaskExecutionId, Error
    ]),
    catch atm_task_execution_api:handle_results(AtmWorkflowExecutionEnv, AtmTaskExecutionId, error),
    error;

process_result(_AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId, Results) ->
    try
        atm_task_execution_api:handle_results(AtmWorkflowExecutionEnv, AtmTaskExecutionId, Results)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("FAILED TO PROCESS RESULT FOR TASK EXECUTION ~p DUE TO: ~p", [
            AtmTaskExecutionId, Reason
        ]),
        catch atm_task_execution_api:handle_results(AtmWorkflowExecutionEnv, AtmTaskExecutionId, error)
    end,
    ok.


-spec handle_task_execution_ended(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id()
) ->
    ok.
handle_task_execution_ended(_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId) ->
    try
        ok = atm_task_execution_api:mark_ended(AtmTaskExecutionId)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("FAILED TO MARK TASK EXECUTION ~p AS ENDED DUE TO: ~p", [
            AtmTaskExecutionId, Reason
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
        ?error("FAILED TO MARK LANE EXECUTION ~p AS ENDED DUE TO: ~p", [
            AtmLaneIndex, Reason
        ])
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec prepare_internal(atm_workflow_execution:id()) -> ok | no_return().
prepare_internal(AtmWorkflowExecutionId) ->
    {ok, #document{value = #atm_workflow_execution{
        lanes = AtmLaneExecutions
    }}} = transition_to_preparing_status(AtmWorkflowExecutionId),

    try
        atm_lane_execution:prepare_all(AtmLaneExecutions)
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
