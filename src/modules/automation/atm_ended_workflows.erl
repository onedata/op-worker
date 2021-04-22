%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Module operating on collection of ended workflows.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_ended_workflows).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_wokflow.hrl").

%% API
-export([list/2, add/1, delete/1]).


%%%===================================================================
%%% API
%%%===================================================================


-spec list(od_space:id(), atm_workflow_collection:listing_opts()) ->
    [{atm_workflow:id(), atm_workflow_collection:index()}].
list(SpaceId, ListingOpts) ->
    atm_workflow_collection:list(SpaceId, ?ENDED_WORKFLOWS_STATE, ListingOpts).


-spec add(atm_workflow:doc()) -> ok.
add(#document{key = WorkflowId, value = #atm_workflow{
    space_id = SpaceId,
    finish_time = FinishTime
}}) ->
    atm_workflow_collection:add(SpaceId, ?ENDED_WORKFLOWS_STATE, WorkflowId, FinishTime).


-spec delete(atm_workflow:doc()) -> ok.
delete(#document{key = WorkflowId, value = #atm_workflow{
    space_id = SpaceId,
    finish_time = FinishTime
}}) ->
    atm_workflow_collection:delete(SpaceId, ?ENDED_WORKFLOWS_STATE, WorkflowId, FinishTime).
