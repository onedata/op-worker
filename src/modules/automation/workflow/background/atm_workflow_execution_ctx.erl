%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utility functions for management of automation
%%% workflow execution ctx which consists of information necessary to
%%% validate data, access stores or perform logging.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_ctx).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([build/3]).
-export([
    get_auth/1,
    get_logger/1,
    get_workflow_store_id/2
]).


-record(atm_workflow_execution_ctx, {
    auth :: atm_workflow_execution_auth:record(),
    logger :: atm_workflow_execution_logger:record(),
    workflow_store_registry :: atm_workflow_execution:store_registry()
}).
-type record() :: #atm_workflow_execution_ctx{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(
    atm_workflow_execution_auth:record(),
    atm_workflow_execution_logger:record(),
    atm_workflow_execution:store_registry()
) ->
    record().
build(AtmWorkflowExecutionAuth, AtmWorkflowExecutionLogger, AtmWorkflowStoreRegistry) ->
    #atm_workflow_execution_ctx{
        auth = AtmWorkflowExecutionAuth,
        logger = AtmWorkflowExecutionLogger,
        workflow_store_registry = AtmWorkflowStoreRegistry
    }.


-spec get_auth(record()) -> atm_workflow_execution_auth:record().
get_auth(#atm_workflow_execution_ctx{auth = AtmWorkflowExecutionAuth}) ->
    AtmWorkflowExecutionAuth.


-spec get_logger(record()) -> atm_workflow_execution_logger:record().
get_logger(#atm_workflow_execution_ctx{logger = AtmWorkflowExecutionLogger}) ->
    AtmWorkflowExecutionLogger.


-spec get_workflow_store_id(automation:id(), record()) -> atm_store:id() | no_return().
get_workflow_store_id(AtmStoreSchemaId, #atm_workflow_execution_ctx{
    workflow_store_registry = AtmStoreRegistry
}) ->
    case maps:get(AtmStoreSchemaId, AtmStoreRegistry, undefined) of
        undefined ->
            throw(?ERROR_ATM_REFERENCED_NONEXISTENT_STORE(AtmStoreSchemaId));
        AtmStoreId ->
            AtmStoreId
    end.
