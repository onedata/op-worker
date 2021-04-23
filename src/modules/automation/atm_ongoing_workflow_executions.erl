%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Module operating on collection of ongoing automation workflow executions.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_ongoing_workflow_executions).
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
    atm_workflow_executions_collection:list(SpaceId, ?ONGOING_STATE, ListingOpts).


-spec add(atm_workflow_execution:doc()) -> ok.
add(#document{key = AtmWorkflowExecutionId, value = #atm_workflow_execution{
    space_id = SpaceId,
    start_time = StartTime
}}) ->
    atm_workflow_executions_collection:add(
        SpaceId, ?ONGOING_STATE, AtmWorkflowExecutionId, StartTime
    ).


-spec delete(atm_workflow_execution:doc()) -> ok.
delete(#document{key = AtmWorkflowExecutionId, value = #atm_workflow_execution{
    space_id = SpaceId,
    start_time = StartTime
}}) ->
    atm_workflow_executions_collection:delete(
        SpaceId, ?ONGOING_STATE, AtmWorkflowExecutionId, StartTime
    ).
