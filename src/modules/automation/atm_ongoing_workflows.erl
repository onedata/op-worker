%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Module operating on collection of ongoing workflows.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_ongoing_workflows).
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
    atm_workflow_collection:list(SpaceId, ?ONGOING_WORKFLOWS_STATE, ListingOpts).


-spec add(atm_workflow:doc()) -> ok.
add(#document{key = WorkflowId, value = #atm_workflow{
    space_id = SpaceId,
    start_time = StartTime
}}) ->
    atm_workflow_collection:add(SpaceId, ?ONGOING_WORKFLOWS_STATE, WorkflowId, StartTime).


-spec delete(atm_workflow:doc()) -> ok.
delete(#document{key = WorkflowId, value = #atm_workflow{
    space_id = SpaceId,
    start_time = StartTime
}}) ->
    atm_workflow_collection:delete(SpaceId, ?ONGOING_WORKFLOWS_STATE, WorkflowId, StartTime).
