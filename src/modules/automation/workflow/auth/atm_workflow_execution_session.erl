%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for management of user offline session used by
%%% automation workflow execution machinery.
%%% For each automation workflow execution a new session should be initialized
%%% at the beginning of processing and terminated at the end of it. Session
%%% acquisition, on the other hand, should be performed each time session is
%%% used (this checks if session is still valid and refreshes credentials needed).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_session).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([init/2, acquire/1, terminate/1]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec init(atm_workflow_execution:id(), user_ctx:ctx()) -> ok | {error, term()}.
init(AtmWorkflowExecutionId, UserCtx) ->
    UserCredentials = user_ctx:get_credentials(UserCtx),

    case offline_access_manager:init_session(AtmWorkflowExecutionId, UserCredentials) of
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.


-spec acquire(atm_workflow_execution:id()) -> user_ctx:ctx() | no_return().
acquire(AtmWorkflowExecutionId) ->
    case offline_access_manager:get_session_id(AtmWorkflowExecutionId) of
        {ok, SessionId} ->
            user_ctx:new(SessionId);
        {error, _} = Error ->
            throw({session_acquisition_failed, Error})
    end.


-spec terminate(atm_workflow_execution:id()) -> ok.
terminate(AtmWorkflowExecutionId) ->
    offline_access_manager:close_session(AtmWorkflowExecutionId).
