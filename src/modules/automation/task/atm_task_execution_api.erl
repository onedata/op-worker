%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for performing operations on automation task execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_api).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    create_all/4, create/4,
    prepare_all/2, prepare/2,
    clean_all/1, clean/1,
    delete_all/1, delete/1,

    get_spec/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_all(
    atm_workflow_execution:creation_ctx(),
    non_neg_integer(),
    non_neg_integer(),
    [atm_task_schema:record()]
) ->
    [atm_task_execution:doc()] | no_return().
create_all(AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, AtmTaskSchemas) ->
    lists:reverse(lists:foldl(fun(#atm_task_schema{id = AtmTaskSchemaId} = AtmTaskSchema, Acc) ->
        try
            AtmTaskExecutionDoc = create(
                AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, AtmTaskSchema
            ),
            [AtmTaskExecutionDoc | Acc]
        catch _:Reason ->
            catch delete_all([Doc#document.key || Doc <- Acc]),
            throw(?ERROR_ATM_TASK_EXECUTION_CREATION_FAILED(AtmTaskSchemaId, Reason))
        end
    end, [], AtmTaskSchemas)).


-spec create(
    atm_workflow_execution:creation_ctx(),
    non_neg_integer(),
    non_neg_integer(),
    atm_task_schema:record()
) ->
    atm_task_execution:doc() | no_return().
create(AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, AtmTaskSchema) ->
    atm_task_execution_factory:create(
        AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, AtmTaskSchema
    ).


-spec prepare_all(atm_workflow_execution_auth:record(), [atm_task_execution:id()]) ->
    ok | no_return().
prepare_all(AtmWorkflowExecutionAuth, AtmTaskExecutionIds) ->
    atm_parallel_runner:foreach(fun(AtmTaskExecutionId) ->
        {ok, AtmTaskExecutionDoc = #document{value = #atm_task_execution{
            schema_id = AtmTaskSchemaId
        }}} = atm_task_execution:get(AtmTaskExecutionId),

        try
            prepare(AtmWorkflowExecutionAuth, AtmTaskExecutionDoc)
        catch _:Reason ->
            throw(?ERROR_ATM_TASK_EXECUTION_PREPARATION_FAILED(AtmTaskSchemaId, Reason))
        end
    end, AtmTaskExecutionIds).


-spec prepare(
    atm_workflow_execution_auth:record(),
    atm_task_execution:id() | atm_task_execution:doc()
) ->
    ok | no_return().
prepare(AtmWorkflowExecutionAuth, AtmTaskExecutionIdOrDoc) ->
    #document{value = #atm_task_execution{executor = AtmTaskExecutor}} = ensure_atm_task_execution_doc(
        AtmTaskExecutionIdOrDoc
    ),
    atm_task_executor:prepare(AtmWorkflowExecutionAuth, AtmTaskExecutor).


-spec clean_all([atm_task_execution:id()]) -> ok.
clean_all(AtmTaskExecutionIds) ->
    lists:foreach(
        fun(AtmTaskExecutionId) -> catch clean(AtmTaskExecutionId) end,
        AtmTaskExecutionIds
    ).


-spec clean(atm_task_execution:id()) -> ok | no_return().
clean(AtmTaskExecutionId) ->
    #document{value = #atm_task_execution{executor = AtmTaskExecutor}} = ensure_atm_task_execution_doc(
        AtmTaskExecutionId
    ),
    atm_task_executor:clean(AtmTaskExecutor).


-spec delete_all([atm_task_execution:id()]) -> ok.
delete_all(AtmTaskExecutionIds) ->
    lists:foreach(fun delete/1, AtmTaskExecutionIds).


-spec delete(atm_task_execution:id()) -> ok | {error, term()}.
delete(AtmTaskExecutionId) ->
    case atm_task_execution:get(AtmTaskExecutionId) of
        {ok, #document{value = #atm_task_execution{system_audit_log_id = AtmSystemAuditLogId}}} ->
            atm_store_api:delete(AtmSystemAuditLogId),
            atm_task_execution:delete(AtmTaskExecutionId);
        ?ERROR_NOT_FOUND ->
            ok
    end.


-spec get_spec(atm_task_execution:id()) -> workflow_engine:task_spec().
get_spec(AtmTaskExecutionId) ->
    #document{value = #atm_task_execution{executor = AtmTaskExecutor}} = ensure_atm_task_execution_doc(
        AtmTaskExecutionId
    ),
    atm_task_executor:get_spec(AtmTaskExecutor).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ensure_atm_task_execution_doc(atm_task_execution:id() | atm_task_execution:doc()) ->
    atm_task_execution:doc().
ensure_atm_task_execution_doc(#document{value = #atm_task_execution{}} = AtmTaskExecutionDoc) ->
    AtmTaskExecutionDoc;
ensure_atm_task_execution_doc(AtmTaskExecutionId) ->
    {ok, AtmTaskExecutionDoc = #document{}} = atm_task_execution:get(AtmTaskExecutionId),
    AtmTaskExecutionDoc.
