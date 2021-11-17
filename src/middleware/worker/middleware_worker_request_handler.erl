%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations.
%%% @end
%%%-------------------------------------------------------------------
-module(middleware_worker_request_handler).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([handle_request/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec handle_request(user_ctx:ctx(), file_ctx:ctx(), middleware_worker:operation()) ->
    ok | {ok, term()} | no_return().
handle_request(UserCtx, SpaceDirCtx, #schedule_atm_workflow_execution{
    atm_workflow_schema_id = AtmWorkflowSchemaId,
    store_initial_values = AtmStoreInitialValues,
    callback_url = CallbackUrl
}) ->
    {ok, atm_workflow_execution_api:schedule(
        UserCtx, file_ctx:get_space_id_const(SpaceDirCtx),
        AtmWorkflowSchemaId, AtmStoreInitialValues, CallbackUrl
    )};

handle_request(_UserCtx, _SpaceDirCtx, #cancel_atm_workflow_execution{
    atm_workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ok = atm_workflow_execution_api:cancel(AtmWorkflowExecutionId).
