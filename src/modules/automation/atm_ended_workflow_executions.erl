%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Module operating on collection of ended automation workflow executions.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_ended_workflow_executions).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_wokflow_execution.hrl").

%% API
-export([list/2, add/1, delete/1]).


%%%===================================================================
%%% API
%%%===================================================================


-spec list(od_space:id(), atm_workflow_executions_collection:listing_opts()) ->
    [{atm_workflow_execution:id(), atm_workflow_executions_collection:index()}].
list(SpaceId, ListingOpts) ->
    atm_workflow_executions_collection:list(SpaceId, ?ENDED_STATE, ListingOpts).


-spec add(atm_workflow_execution:doc()) -> ok.
add(#document{key = AtmWorkflowExecutionId, value = #atm_workflow_execution{
    space_id = SpaceId,
    finish_time = FinishTime
}}) ->
    atm_workflow_executions_collection:add(
        SpaceId, ?ENDED_STATE, AtmWorkflowExecutionId, FinishTime
    ).


-spec delete(atm_workflow_execution:doc()) -> ok.
delete(#document{key = AtmWorkflowExecutionId, value = #atm_workflow_execution{
    space_id = SpaceId,
    finish_time = FinishTime
}}) ->
    atm_workflow_executions_collection:delete(
        SpaceId, ?ENDED_STATE, AtmWorkflowExecutionId, FinishTime
    ).
