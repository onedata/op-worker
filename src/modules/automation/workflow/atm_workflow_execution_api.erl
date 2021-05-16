%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for performing operations on automation workflow executions.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_api).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_wokflow_execution.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([create/3, init/1, delete/1]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(od_space:id(), atm_workflow_schema(), atm_store_api:initial_values()) ->
    {ok, atm_workflow_execution:id()} | no_return().
create(SpaceId, #atm_workflow_schema{
    id = AtmWorkflowSchemaId,
    state = AtmWorkflowSchemaState,
    name = AtmWorkflowSchemaName,
    description = AtmWorkflowSchemaDescription,
    stores = AtmStoreSchemas,
    lanes = AtmLaneSchemas
}, InitialValues) ->
    AtmWorkflowExecutionId = datastore_key:new(),

    AtmStoreRegistry = atm_store_api:create_all(AtmStoreSchemas, InitialValues),

    AtmLaneExecutions = try
        atm_lane_execution:create_all(AtmWorkflowExecutionId, AtmStoreRegistry, AtmLaneSchemas)
    catch Type:Reason ->
        atm_store_api:delete_all(maps:values(AtmStoreRegistry)),
        erlang:Type(Reason)
    end,

    % TODO try cache ??
    ok = atm_workflow_execution:create(#document{
        key = AtmWorkflowExecutionId,
        value = #atm_workflow_execution{
            schema_id = AtmWorkflowSchemaId,
            schema_state = AtmWorkflowSchemaState,
            name = AtmWorkflowSchemaName,
            description = AtmWorkflowSchemaDescription,

            space_id = SpaceId,
            stores = maps:values(AtmStoreRegistry),
            lanes = AtmLaneExecutions,

            status = ?SCHEDULED_STATUS,
            schedule_time = global_clock:timestamp_seconds(),
            start_time = 0,
            finish_time = 0
        }
    }),
    {ok, AtmWorkflowExecutionId}.


-spec init(atm_workflow_execution:id()) -> ok | no_return().
init(AtmWorkflowExecutionId) ->
    {ok, #atm_workflow_execution{lanes = AtmLaneExecutions}} = atm_workflow_execution:get(
        AtmWorkflowExecutionId
    ),
    atm_lane_execution:init_all(AtmLaneExecutions).


-spec delete(atm_workflow_execution:id()) -> ok | no_return().
delete(AtmWorkflowExecutionId) ->
    {ok, #atm_workflow_execution{lanes = AtmLaneExecutions}} = atm_workflow_execution:get(
        AtmWorkflowExecutionId
    ),
    atm_lane_execution:delete_all(AtmLaneExecutions).
