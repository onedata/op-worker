%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module routes middleware operations to corresponding handler modules.
%%% @end
%%%-------------------------------------------------------------------
-module(middleware_worker_request_router).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([route/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec route(user_ctx:ctx(), file_ctx:ctx(), middleware_worker:operation()) ->
    ok | {ok, term()} | no_return().
route(UserCtx, SpaceDirCtx, #schedule_atm_workflow_execution{
    atm_workflow_schema_id = AtmWorkflowSchemaId,
    store_initial_values = AtmStoreInitialValues,
    callback_url = CallbackUrl
}) ->
    {ok, atm_workflow_execution_api:schedule(
        UserCtx, file_ctx:get_space_id_const(SpaceDirCtx),
        AtmWorkflowSchemaId, AtmStoreInitialValues, CallbackUrl
    )};

route(_UserCtx, _SpaceDirCtx, #cancel_atm_workflow_execution{
    atm_workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ok = atm_workflow_execution_api:cancel(AtmWorkflowExecutionId).