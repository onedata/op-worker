%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([prepare/3]).

-type create_ctx() :: #atm_lane_execution_create_ctx{}.

-export_type([create_ctx/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec prepare(pos_integer(), atm_workflow_execution:id(), atm_workflow_execution_ctx:record()) ->
    ok.
prepare(AtmLaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecutionDoc = mark_as_preparing(AtmLaneIndex, AtmWorkflowExecutionId),

    AtmLaneExecutionCreateCtx = create_execution_elements(build_lane_execution_create_ctx(
        AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
    )),

    ok.


%%-spec prepare_all(atm_workflow_execution_ctx:record(), [record()]) -> ok | no_return().
%%prepare_all(AtmWorkflowExecutionCtx, AtmLaneExecutions) ->
%%    atm_parallel_runner:foreach(fun(#atm_lane_execution{schema_id = AtmLaneSchemaId} = AtmLaneExecution) ->
%%        try
%%            prepare(AtmWorkflowExecutionCtx, AtmLaneExecution)
%%        catch _:Reason ->
%%            throw(?ERROR_ATM_LANE_EXECUTION_PREPARATION_FAILED(AtmLaneSchemaId, Reason))
%%        end
%%    end, AtmLaneExecutions).
%%
%%
%%-spec prepare(atm_workflow_execution_ctx:record(), record()) -> ok | no_return().
%%prepare(AtmWorkflowExecutionCtx, #atm_lane_execution{parallel_boxes = AtmParallelBoxExecutions}) ->
%%    atm_parallel_box_execution:setup_all(AtmWorkflowExecutionCtx, AtmParallelBoxExecutions),
%%    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec mark_as_preparing(pos_integer(), atm_workflow_execution:id()) ->
    atm_workflow_execution:doc() | no_return().
mark_as_preparing(AtmLaneIndex, AtmWorkflowExecutionId) ->
    Diff = fun(#atm_workflow_execution{lanes = AtmLaneExecutions} = AtmWorkflowExecution) ->
        AtmLaneExecution = lists:nth(AtmLaneIndex, AtmLaneExecutions),

        case mark_current_run_as_preparing(AtmLaneExecution#atm_lane_execution_rec.runs) of
            {ok, UpdatedRuns} ->
                NewAtmLaneExecution = AtmLaneExecution#atm_lane_execution_rec{runs = UpdatedRuns},
                NewAtmLaneExecutions = lists_utils:replace_at(
                    NewAtmLaneExecution, AtmLaneIndex, AtmLaneExecutions
                ),
                {ok, AtmWorkflowExecution#atm_workflow_execution{lanes = NewAtmLaneExecutions}};
            {error, _} = Error ->
                Error
        end
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} ->
            AtmWorkflowExecutionDoc;
        ?ERROR_ATM_INVALID_STATUS_TRANSITION(_, _) = Error ->
            throw(Error)
    end.


%% @private
-spec mark_current_run_as_preparing([atm_lane_execution:run()]) ->
    {ok, [atm_lane_execution:run()]} | errors:error().
mark_current_run_as_preparing([]) ->
    % preparing in advance
    {ok, [#atm_lane_execution_run{run_no = undefined, status = ?PREPARING_STATUS}]};

mark_current_run_as_preparing([
    #atm_lane_execution_run{status = ?SCHEDULED_STATUS} = CurrentRun
    | RestRuns
]) ->
    {ok, [CurrentRun#atm_lane_execution_run{status = ?PREPARING_STATUS} | RestRuns]};

mark_current_run_as_preparing([#atm_lane_execution_run{status = Status} | _] = PreviousRuns) ->
    case status_to_phase(Status) of
        ?ENDED_PHASE ->
            % preparing in advance
            NewRun = #atm_lane_execution_run{run_no = undefined, status = ?PREPARING_STATUS},
            {ok, [NewRun | PreviousRuns]};
        _ ->
            ?ERROR_ATM_INVALID_STATUS_TRANSITION(Status, ?PREPARING_STATUS)
    end.


%% @private
-spec build_lane_execution_create_ctx(
    pos_integer(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    create_ctx().
build_lane_execution_create_ctx(AtmLaneIndex, AtmWorkflowExecutionDoc = #document{
    value = #atm_workflow_execution{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
        lanes = AtmLaneExecutions
    }
}, AtmWorkflowExecutionCtx) ->
    #atm_lane_execution_rec{runs = [
        #atm_lane_execution_run{iterated_store_id = IteratedStoreId}
        | _
    ]} = lists:nth(AtmLaneIndex, AtmLaneExecutions),

    {ok, AtmWorkflowSchemaSnapshotDoc = #document{value = #atm_workflow_schema_snapshot{
        lanes = AtmLaneSchemas
    }}} = atm_workflow_schema_snapshot:get(AtmWorkflowSchemaSnapshotId),

    AtmLaneSchema = #atm_lane_schema{store_iterator_spec = #atm_store_iterator_spec{
        store_schema_id = AtmStoreSchemaId
    }} = lists:nth(AtmLaneIndex, AtmLaneSchemas),

    #atm_lane_execution_create_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        workflow_execution_env = atm_workflow_execution_ctx:get_env(AtmWorkflowExecutionCtx),

        workflow_schema_snapshot_doc = AtmWorkflowSchemaSnapshotDoc,
        workflow_execution_doc = AtmWorkflowExecutionDoc,

        lane_index = AtmLaneIndex,
        lane_schema = AtmLaneSchema,

        iterated_store_id = case IteratedStoreId of
            undefined ->
                atm_workflow_execution_ctx:get_workflow_store_id(
                    AtmStoreSchemaId, AtmWorkflowExecutionCtx
                );
            _ ->
                IteratedStoreId
        end,
        exception_store_id = undefined,

        parallel_boxes = undefined
    }.


%% @private
-spec create_execution_elements(create_ctx()) ->
    create_ctx() | no_return().
create_execution_elements(AtmLaneExecutionCreateCtx) ->
    lists:foldl(fun(CreateExecutionElementFun, NewAtmLaneExecutionCreateCtx) ->
        try
            CreateExecutionElementFun(NewAtmLaneExecutionCreateCtx)
        catch Type:Reason ->
            delete_execution_elements(NewAtmLaneExecutionCreateCtx),
            erlang:Type(Reason)
        end
    end, AtmLaneExecutionCreateCtx, [
        fun create_exception_store/1,
        fun atm_parallel_box_execution:create_all/1
    ]).


%% @private
-spec create_exception_store(create_ctx()) ->
    create_ctx().
create_exception_store(AtmLaneExecutionCreateCtx = #atm_lane_execution_create_ctx{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
    workflow_execution_env = AtmWorkflowExecutionEnv,
    iterated_store_id = AtmIteratedStoreId
}) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),
    {ok, #atm_store{container = AtmStoreContainer}} = atm_store_api:get(AtmIteratedStoreId),

    {ok, #document{
        key = AtmLaneExceptionStoreId,
        value = #atm_store{container = AtmLaneExceptionStoreContainer}
    }} = atm_store_api:create(
        AtmWorkflowExecutionAuth, undefined, #atm_store_schema{
            id = ?CURRENT_LANE_EXCEPTION_STORE_SCHEMA_ID,
            name = ?CURRENT_LANE_EXCEPTION_STORE_SCHEMA_ID,
            description = <<>>,
            type = list,
            data_spec = atm_store_container:get_data_spec(AtmStoreContainer),
            requires_initial_value = false
        }
    ),

    AtmLaneExecutionCreateCtx#atm_lane_execution_create_ctx{
        workflow_execution_env = atm_workflow_execution_env:set_lane_exception_store_container(
            AtmLaneExceptionStoreContainer, AtmWorkflowExecutionEnv
        ),
        exception_store_id = AtmLaneExceptionStoreId
    }.


%% @private
-spec delete_execution_elements(create_ctx()) -> ok.
delete_execution_elements(AtmLaneExecutionCreateCtx = #atm_lane_execution_create_ctx{
    exception_store_id = AtmLaneExceptionStoreId
}) when AtmLaneExceptionStoreId /= undefined ->
    catch atm_store_api:delete(AtmLaneExceptionStoreId),

    delete_execution_elements(AtmLaneExecutionCreateCtx#atm_lane_execution_create_ctx{
        exception_store_id = undefined
    });

delete_execution_elements(AtmLaneExecutionCreateCtx = #atm_lane_execution_create_ctx{
    parallel_boxes = AtmParallelBoxExecutions
}) when AtmParallelBoxExecutions /= undefined ->
    catch atm_parallel_box_execution:delete_all(AtmParallelBoxExecutions),

    delete_execution_elements(AtmLaneExecutionCreateCtx#atm_lane_execution_create_ctx{
        parallel_boxes = undefined
    });

delete_execution_elements(_) ->
    ok.










%% @private
-spec status_to_phase(atm_workflow_execution:status()) ->
    atm_workflow_execution:phase().
status_to_phase(?SCHEDULED_STATUS) -> ?WAITING_PHASE;
status_to_phase(?PREPARING_STATUS) -> ?WAITING_PHASE;
status_to_phase(?ENQUEUED_STATUS) -> ?WAITING_PHASE;
status_to_phase(?ACTIVE_STATUS) -> ?ONGOING_PHASE;
status_to_phase(?ABORTING_STATUS) -> ?ONGOING_PHASE;
status_to_phase(?FINISHED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?CANCELLED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?FAILED_STATUS) -> ?ENDED_PHASE.
