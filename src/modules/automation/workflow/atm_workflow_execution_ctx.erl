%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utility functions for management of automation
%%% workflow execution ctx. The ctx contains information about space and
%%% user in context of who the workflow execution is performed.
%%% It mustn't be cached or saved to db but rather recreated on demand to
%%% periodically refresh user offline session used by execution machinery.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_ctx).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([build/2]).
-export([get_space_id/1, get_workflow_execution_id/1, get_session_id/1]).

-type record() :: #atm_workflow_execution_ctx{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(od_space:id(), atm_workflow_execution:id()) -> record().
build(SpaceId, AtmWorkflowExecutionId) ->
    #atm_workflow_execution_ctx{
        space_id = SpaceId,
        workflow_execution_id = AtmWorkflowExecutionId,
        session_id = atm_workflow_execution_session:acquire(AtmWorkflowExecutionId)
    }.


-spec get_space_id(record()) -> od_space:id().
get_space_id(#atm_workflow_execution_ctx{space_id = SpaceId}) ->
    SpaceId.


-spec get_workflow_execution_id(record()) -> atm_workflow_execution:id().
get_workflow_execution_id(#atm_workflow_execution_ctx{
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    AtmWorkflowExecutionId.


-spec get_session_id(record()) -> session:id().
get_session_id(#atm_workflow_execution_ctx{session_id = SessionId}) ->
    SessionId.
