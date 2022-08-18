%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Module operating on collection of suspended automation workflow executions.
%%% TODO add tests
%%% @end
%%%-------------------------------------------------------------------
-module(atm_suspended_workflow_executions).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([list/2, list/3, add/1, delete/1]).

-define(FOREST(__SPACE_ID), <<"ATM_SUSPENDED_WORKFLOW_EXECUTIONS_FOREST_", __SPACE_ID/binary>>).


%%%===================================================================
%%% API
%%%===================================================================


-spec list(od_space:id(), atm_workflow_executions_forest:listing_opts()) ->
    atm_workflow_executions_forest:entries().
list(SpaceId, ListingOpts) ->
    list(SpaceId, all, ListingOpts).


-spec list(
    od_space:id(),
    atm_workflow_executions_forest:tree_ids(),
    atm_workflow_executions_forest:listing_opts()
) ->
    atm_workflow_executions_forest:entries().
list(SpaceId, AtmInventoryIds, ListingOpts) ->
    atm_workflow_executions_forest:list(?FOREST(SpaceId), AtmInventoryIds, ListingOpts).


-spec add(atm_workflow_execution:doc()) -> ok.
add(#document{key = AtmWorkflowExecutionId, value = #atm_workflow_execution{
    space_id = SpaceId,
    atm_inventory_id = AtmInventoryId,
    finish_time = FinishTime
}}) ->
    atm_workflow_executions_forest:add(
        ?FOREST(SpaceId), AtmInventoryId, AtmWorkflowExecutionId, FinishTime
    ).


-spec delete(atm_workflow_execution:doc()) -> ok.
delete(#document{key = AtmWorkflowExecutionId, value = #atm_workflow_execution{
    space_id = SpaceId,
    atm_inventory_id = AtmInventoryId,
    finish_time = FinishTime
}}) ->
    atm_workflow_executions_forest:delete(
        ?FOREST(SpaceId), AtmInventoryId, AtmWorkflowExecutionId, FinishTime
    ).
