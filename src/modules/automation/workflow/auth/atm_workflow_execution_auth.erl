%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utility functions for management of automation
%%% workflow execution auth. The auth contains information about space and
%%% user in context of which the workflow execution is performed or accessed.
%%%
%%%                              !!! NOTE !!!
%%% The auth used by internal atm workflow execution machinery is created using
%%% creator's offline session. Because of that it must not be cached but rather
%%% recreated each time it is needed (this causes necessary periodic offline
%%% session refresh).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_auth).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([build/3]).
-export([
    get_space_id/1,
    get_workflow_execution_id/1,
    get_user_id/1, get_session_id/1, get_access_token/1, get_user_ctx/1
]).


-record(atm_workflow_execution_auth, {
    workflow_execution_id :: atm_workflow_execution:id(),
    space_id :: od_space:id(),
    user_ctx :: user_ctx:ctx()
}).
-type record() :: #atm_workflow_execution_auth{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(od_space:id(), atm_workflow_execution:id(), session:id() | user_ctx:ctx()) ->
    record().
build(SpaceId, AtmWorkflowExecutionId, SessionId) when is_binary(SessionId) ->
    build(SpaceId, AtmWorkflowExecutionId, user_ctx:new(SessionId));
build(SpaceId, AtmWorkflowExecutionId, UserCtx) ->
    #atm_workflow_execution_auth{
        workflow_execution_id = AtmWorkflowExecutionId,
        space_id = SpaceId,
        user_ctx = UserCtx
    }.


-spec get_space_id(record()) -> od_space:id().
get_space_id(#atm_workflow_execution_auth{space_id = SpaceId}) ->
    SpaceId.


-spec get_workflow_execution_id(record()) -> atm_workflow_execution:id().
get_workflow_execution_id(#atm_workflow_execution_auth{
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    AtmWorkflowExecutionId.


-spec get_user_id(record()) -> od_user:id().
get_user_id(#atm_workflow_execution_auth{user_ctx = UserCtx}) ->
    user_ctx:get_user_id(UserCtx).


-spec get_session_id(record()) -> session:id().
get_session_id(#atm_workflow_execution_auth{user_ctx = UserCtx}) ->
    user_ctx:get_session_id(UserCtx).


-spec get_access_token(record()) -> auth_manager:access_token().
get_access_token(#atm_workflow_execution_auth{user_ctx = UserCtx}) ->
    TokenCredentials = user_ctx:get_credentials(UserCtx),
    auth_manager:get_access_token(TokenCredentials).


-spec get_user_ctx(record()) -> user_ctx:ctx().
get_user_ctx(#atm_workflow_execution_auth{user_ctx = UserCtx}) ->
    UserCtx.
