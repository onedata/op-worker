%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles creation of all documents associated with automation
%%% lane execution (e.g. exception store, etc.). If creation of any element
%%% fails then ones created before are deleted.
%%%
%%%                              !!! NOTE !!!
%%% This module creates lane execution elements and saves them into latest
%%% lane execution run which embryo (record with 'undefined' elements and
%%% ?PREPARING_STATUS) must have been prepared beforehand.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution_factory).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([create/3]).

-type create_ctx() :: #atm_lane_execution_create_ctx{}.

-export_type([create_ctx/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(pos_integer(), atm_workflow_execution:doc(), atm_workflow_execution_ctx:record()) ->
    {atm_workflow_execution:doc(), atm_workflow_execution_env:record()} | no_return().
create(AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    try
        create_internal(AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx)
    catch _:Reason ->
        #atm_lane_execution{schema_id = AtmLaneSchemaId} = lists:nth(
            AtmLaneIndex, AtmWorkflowExecutionDoc#document.value#atm_workflow_execution.lanes
        ),
        throw(?ERROR_ATM_LANE_EXECUTION_CREATION_FAILED(AtmLaneSchemaId, Reason))
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_internal(
    pos_integer(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    {atm_workflow_execution:doc(), atm_workflow_execution_env:record()} | no_return().
create_internal(AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    AtmLaneExecutionCreateCtx = #atm_lane_execution_create_ctx{
        workflow_execution_env = AtmWorkflowExecutionEnv,
        iterated_store_id = IteratedStoreId,
        exception_store_id = ExceptionStoreId,
        parallel_boxes = AtmParallelBoxExecutions
    } = create_execution_elements(build_lane_execution_create_ctx(
        AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
    )),

    Diff = fun(#atm_workflow_execution{lanes = AtmLaneExecutions} = AtmWorkflowExecution) ->
        #atm_lane_execution{runs = [CurrRun | PrevRuns]} = AtmLaneExecution = lists:nth(
            AtmLaneIndex, AtmLaneExecutions
        ),
        case CurrRun of
            #atm_lane_execution_run{
                status = ?PREPARING_STATUS,
                exception_store_id = undefined,
                parallel_boxes = []
            } ->
                UpdatedCurrRun = CurrRun#atm_lane_execution_run{
                    iterated_store_id = IteratedStoreId,
                    exception_store_id = ExceptionStoreId,
                    parallel_boxes = AtmParallelBoxExecutions
                },
                NewAtmLaneExecution = AtmLaneExecution#atm_lane_execution{
                    runs = [UpdatedCurrRun | PrevRuns]
                },
                NewAtmLaneExecutions = lists_utils:replace_at(
                    NewAtmLaneExecution, AtmLaneIndex, AtmLaneExecutions
                ),
                {ok, AtmWorkflowExecution#atm_workflow_execution{lanes = NewAtmLaneExecutions}};
            _ ->
                ?ERROR_ALREADY_EXISTS
        end
    end,
    case atm_workflow_execution:update(AtmWorkflowExecutionDoc#document.key, Diff) of
        {ok, NewAtmWorkflowExecutionDoc} ->
            {NewAtmWorkflowExecutionDoc, AtmWorkflowExecutionEnv};
        ?ERROR_ALREADY_EXISTS = Error ->
            delete_execution_elements(AtmLaneExecutionCreateCtx),
            throw(Error)
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
    #atm_lane_execution{runs = [
        #atm_lane_execution_run{status = ?PREPARING_STATUS, iterated_store_id = IteratedStoreId}
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
                % If not explicitly set then take designated by schema store
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
-spec create_execution_elements(create_ctx()) -> create_ctx() | no_return().
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
-spec create_exception_store(create_ctx()) -> create_ctx().
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
