%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% The module handles streaming atm store dump via REST API.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_dump_download_handler).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include("modules/automation/atm_execution.hrl").

-export([handle/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec handle(middleware:req(), cowboy_req:req()) -> cowboy_req:req().
handle(#op_req{auth = AaiAuth = ?USER, gri = #gri{id = AtmStoreId}}, Req) ->
    try
        handle_download(Req, AaiAuth, AtmStoreId)
    catch Class:Reason:Stacktrace ->
        Error = case request_error_handler:infer_error(Reason) of
            {true, KnownError} -> KnownError;
            false -> ?examine_exception(Class, Reason, Stacktrace)
        end,
        http_req:send_error(Error, Req)
    end;

handle(#op_req{auth = ?GUEST}, Req) ->
    http_req:send_error(?ERROR_UNAUTHORIZED, Req).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec handle_download(cowboy_req:req(), aai:auth(), atm_store:id()) ->
    cowboy_req:req() | no_return().
handle_download(Req, AaiAuth = ?USER(_, SessionId), AtmStoreId) ->
    AtmStoreCtx = ?check(atm_store_api:get_ctx(AtmStoreId)),

    authorize_download(AaiAuth, AtmStoreCtx),
    atm_store_dump_download_utils:assert_operation_supported(AtmStoreCtx),

    atm_store_dump_download_utils:handle_download(Req, SessionId, AtmStoreCtx).


%% @private
-spec authorize_download(aai:auth(), atm_store:ctx()) -> true | no_return().
authorize_download(Auth, #atm_store_ctx{workflow_execution = AtmWorkflowExecution}) ->
    atm_workflow_execution_middleware_plugin:has_access_to_workflow_execution_details(
        Auth, AtmWorkflowExecution
    ) orelse throw(?ERROR_FORBIDDEN).
