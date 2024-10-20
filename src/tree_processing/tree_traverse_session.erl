%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for tree_traverse mechanism.
%%% It is responsible for setting up and acquiring session, suitable for
%%% scheduling user. The session is used to perform possibly
%%% long-lasting tasks which may continue even after restart of
%%% the provider.
%%% Setup should be performed once per task.
%%% Acquisition should be performed each time session is needed
%%% @end
%%%-------------------------------------------------------------------
-module(tree_traverse_session).
-author("Jakub Kudzia").

%% API
-export([
    setup_for_task/2,
    close_for_task/1,
    acquire_for_task/3
]).

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

-define(LOG_THROTTLING_INTERVAL, 600). % 10 minutes

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function initializes session suitable for user who scheduled
%% given task.
%% For normal user offline session is initialized.
%% For special user (root, guest) such initialization is not performed as
%% its session will be returned from acquire_session_for_task/3 function.
%% @end
%%--------------------------------------------------------------------
-spec setup_for_task(user_ctx:ctx(), tree_traverse:id()) -> ok | {error, term()}.
setup_for_task(UserCtx, TaskId) ->
    case user_ctx:is_root(UserCtx) orelse user_ctx:is_guest(UserCtx) of
        true ->
            ok;
        false ->
            case offline_access_manager:init_session(TaskId, user_ctx:get_credentials(UserCtx)) of
                {ok, _} -> ok;
                {error, _} = Error -> Error
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% This function closes session associated with user who scheduled
%% given task.
%% For ?ROOT_USER this operation is a NOOP as ?ROOT_SESS_ID
%% is not an offline session.
%% @end
%%--------------------------------------------------------------------
-spec close_for_task(tree_traverse:id()) -> ok.
close_for_task(TaskId) ->
    offline_access_manager:close_session(TaskId).


%%--------------------------------------------------------------------
%% @doc
%% This function acquires session suitable for user who scheduled
%% given task.
%% For normal user offline session is acquired while for special 
%% user (root, guest) its session is returned.
%% Returned sessions are wrapped in user_ctx:ctx().
%% @end
%%--------------------------------------------------------------------
-spec acquire_for_task(od_user:id(), tree_traverse:pool(), tree_traverse:id()) ->
    {ok, user_ctx:ctx()} | {error, term()}.
acquire_for_task(?ROOT_USER_ID, _Pool, _TaskId) ->
    {ok, user_ctx:new(?ROOT_SESS_ID)};
acquire_for_task(?GUEST_USER_ID, _Pool, _TaskId) ->
    {ok, user_ctx:new(?GUEST_SESS_ID)};
acquire_for_task(_UserId, Pool, TaskId) ->
    case offline_access_manager:get_session_id(TaskId) of
        {ok, SessionId} ->
            {ok, user_ctx:new(SessionId)};
        {error, _} = Error ->
            utils:throttle(?LOG_THROTTLING_INTERVAL, fun() ->
                ?error(
                    "Traverse ~ts performed by pool ~ts failed to acquire offline session due to ~tp. "
                    "Traverse will be cancelled.", [TaskId, Pool, Error])
            end),
            tree_traverse:cancel(Pool, TaskId),
            {error, ?EACCES}
    end.
