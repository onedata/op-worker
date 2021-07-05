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
%%% user in context of which the workflow execution is performed or accessed.
%%% The context may change during an execution, as it includes the user session
%%% that is refreshed periodically. For this reason, it must be rebuilt every
%%% time it is needed.
%%% Main uses of automation workflow context are:
%%% 1) iteration (e.g. when iterating file or dataset tree forest store
%%%    it is necessary to check user access privileges).
%%% 2) data validation (e.g. when adding file to some store it is necessary
%%%    to assert files existence within this space and user's access to them).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_ctx).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([build/3]).
-export([
    get_space_id/1,
    get_workflow_execution_id/1,
    get_session_id/1, get_access_token/1
]).


-record(atm_workflow_execution_ctx, {
    space_id :: od_space:id(),
    workflow_execution_id :: atm_workflow_execution:id(),
    user_ctx :: user_ctx:ctx()
}).
-type record() :: #atm_workflow_execution_ctx{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(od_space:id(), atm_workflow_execution:id(), session:id() | user_ctx:ctx()) ->
    record().
build(SpaceId, AtmWorkflowExecutionId, SessionId) when is_binary(SessionId) ->
    build(SpaceId, AtmWorkflowExecutionId, user_ctx:new(SessionId));
build(SpaceId, AtmWorkflowExecutionId, UserCtx) ->
    #atm_workflow_execution_ctx{
        space_id = SpaceId,
        workflow_execution_id = AtmWorkflowExecutionId,
        user_ctx = UserCtx
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
get_session_id(#atm_workflow_execution_ctx{user_ctx = UserCtx}) ->
    user_ctx:get_session_id(UserCtx).


-spec get_access_token(record()) -> auth_manager:access_token().
get_access_token(#atm_workflow_execution_ctx{user_ctx = UserCtx}) ->
    TokenCredentials = user_ctx:get_credentials(UserCtx),
    auth_manager:get_access_token(TokenCredentials).
