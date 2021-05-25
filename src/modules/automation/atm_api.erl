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

-export([start/3]).


% TODO VFS-7660 mv to automation erl
-type item() :: json_utils:json_term().

-type initial_values() :: #{AtmStoreSchemaId :: automation:id() => atm_store_api:initial_value()}.

-type creation_ctx() :: #atm_execution_creation_ctx{}.

-export_type([item/0, initial_values/0, creation_ctx/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec start(od_space:id(), od_atm_workflow_schema:id(), initial_values()) ->
    {ok, atm_workflow_execution:id()} | no_return().
start(SpaceId, AtmWorkflowSchemaId, InitialValues) ->
    %% TODO VFS-7671 use user session
    {ok, AtmWorkflowSchemaDoc} = atm_workflow_schema_logic:get(?ROOT_SESS_ID, AtmWorkflowSchemaId),

    AtmWorkflowExecutionId = datastore_key:new(),

    {ok, _} = atm_workflow_execution_api:create(#atm_execution_creation_ctx{
        space_id = SpaceId,
        workflow_execution_id = AtmWorkflowExecutionId,
        workflow_schema_doc = AtmWorkflowSchemaDoc,
        initial_values = InitialValues
    }),

    {ok, AtmWorkflowExecutionId}.
