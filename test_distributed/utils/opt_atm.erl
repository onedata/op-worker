%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for manipulating automation in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(opt_atm).
-author("Bartosz Walkowicz").


-export([
    schedule_workflow_execution/5, schedule_workflow_execution/7,
    cancel_workflow_execution/3,
    repeat_workflow_execution/5
]).

-define(CALL(NodeSelector, Args),
    try opw_test_rpc:call(NodeSelector, mi_atm, ?FUNCTION_NAME, Args) of
        ok -> ok;
        __RESULT -> {ok, __RESULT}
    catch throw:__ERROR ->
        __ERROR
    end
).


%%%===================================================================
%%% API
%%%===================================================================


-spec schedule_workflow_execution(
    oct_background:node_selector(),
    session:id(),
    od_space:id(),
    od_atm_workflow_schema:id(),
    atm_workflow_schema_revision:revision_number()
) ->
    {atm_workflow_execution:id(), atm_workflow_execution:record()} | no_return().
schedule_workflow_execution(
    NodeSelector,
    SessionId,
    SpaceId,
    AtmWorkflowSchemaId,
    AtmWorkflowSchemaRevisionNum
) ->
    schedule_workflow_execution(
        NodeSelector, SessionId, SpaceId, AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum,
        #{}, <<>>
    ).


-spec schedule_workflow_execution(
    oct_background:node_selector(),
    session:id(),
    od_space:id(),
    od_atm_workflow_schema:id(),
    atm_workflow_schema_revision:revision_number(),
    atm_workflow_execution_api:store_initial_values(),
    undefined | http_client:url()
) ->
    {atm_workflow_execution:id(), atm_workflow_execution:record()} | no_return().
schedule_workflow_execution(
    NodeSelector,
    SessionId,
    SpaceId,
    AtmWorkflowSchemaId,
    AtmWorkflowSchemaRevisionNum,
    AtmStoreInitialValues,
    CallbackUrl
) ->
    ?CALL(NodeSelector, [
        SessionId, SpaceId, AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum,
        AtmStoreInitialValues, CallbackUrl
    ]).


-spec cancel_workflow_execution(
    oct_background:node_selector(),
    session:id(),
    atm_workflow_execution:id()
) ->
    ok | no_return().
cancel_workflow_execution(NodeSelector, SessionId, AtmWorkflowExecutionId) ->
    ?CALL(NodeSelector, [SessionId, AtmWorkflowExecutionId]).


-spec repeat_workflow_execution(
    oct_background:node_selector(),
    session:id(),
    atm_workflow_execution:repeat_type(),
    atm_workflow_execution:id(),
    atm_lane_execution:lane_run_selector()
) ->
    ok | errors:error().
repeat_workflow_execution(
    NodeSelector,
    SessionId,
    RepeatType,
    AtmWorkflowExecutionId,
    AtmLaneRunSelector
) ->
    ?CALL(NodeSelector, [SessionId, RepeatType, AtmWorkflowExecutionId, AtmLaneRunSelector]).
