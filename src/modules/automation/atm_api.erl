%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% TODO VFS-7674 Describe automation workflow execution machinery
%%% @end
%%%--------------------------------------------------------------------
-module(atm_api).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("modules/fslogic/fslogic_common.hrl").

-export([start/4]).


% TODO VFS-7660 mv to automation erl
-type item() :: json_utils:json_term().

-type initial_values() :: #{AtmStoreSchemaId :: automation:id() => atm_store_api:initial_value()}.

-export_type([item/0, initial_values/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec start(user_ctx:ctx(), od_space:id(), od_atm_workflow_schema:id(), initial_values()) ->
    {ok, atm_workflow_execution:id()} | no_return().
start(UserCtx, SpaceId, AtmWorkflowSchemaId, InitialValues) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    {ok, AtmWorkflowSchemaDoc} = atm_workflow_schema_logic:get(SessionId, AtmWorkflowSchemaId),

    AtmWorkflowExecutionId = datastore_key:new(),
    ok = atm_workflow_execution_session:init(AtmWorkflowExecutionId, UserCtx),

    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:build(
        SpaceId, AtmWorkflowExecutionId
    ),

    {ok, #document{
        value = #atm_workflow_execution{
            store_registry = AtmStoreRegistry
        }
    }} = atm_workflow_execution_api:create(#atm_workflow_execution_creation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        workflow_schema_doc = AtmWorkflowSchemaDoc,
        initial_values = InitialValues
    }),

    % TODO uncomment after MW implements it
%%    AtmWorkflowExecutionEnv = #atm_workflow_execution_env{
%%        space_id = SpaceId,
%%        workflow_execution_id = AtmWorkflowExecutionId,
%%        store_registry = AtmStoreRegistry
%%    },
%%    workflow_engine:start(
%%        atm_workflow_execution_handler, AtmWorkflowExecutionId, AtmWorkflowExecutionEnv
%%    ),

    {ok, AtmWorkflowExecutionId}.
