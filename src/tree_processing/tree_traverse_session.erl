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
%%% scheduling user.
%%% THe session is used to perform possibly long-lasting tasks which may
%%% continue even after restart of the provider.
%%% @end
%%%-------------------------------------------------------------------
-module(tree_traverse_session).
-author("Jakub Kudzia").

%% API
-export([
    setup_session_for_task/2,
    acquire_session_for_task/3
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
%% This function initializes offline session for user
%% who scheduled given task.
%% Initialization is performed only for normal user as it's not needed
%% for ?ROOT_USER.
%% @end
%%--------------------------------------------------------------------
-spec setup_session_for_task(user_ctx:ctx(), tree_traverse:id()) -> ok | {error, term()}.
setup_session_for_task(UserCtx, TaskId) ->
    case user_ctx:is_normal_user(UserCtx) of
        true ->
            case offline_access_manager:init_session(TaskId, user_ctx:get_credentials(UserCtx)) of
                {ok, _} -> ok;
                {error, _} = Error -> Error
            end;
        false ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% This function acquires session suitable for user who scheduled
%% given task.
%% For normal user offline session is acquired while for ?ROOT_USER
%% ?ROOT_SESS_ID is returned.
%% Returned essions are wrapped in user_ctx:ctx().
%% @end
%%--------------------------------------------------------------------
-spec acquire_session_for_task(od_user:id(), tree_traverse:traverse_info(), tree_traverse:id()) ->
    {ok, user_ctx:ctx()} | {error, term()}.
acquire_session_for_task(?ROOT_USER_ID, _TraverseInfo, _TaskId) ->
    {ok, user_ctx:new(?ROOT_SESS_ID)};
acquire_session_for_task(_UserId, TraverseInfo, TaskId) ->
    Pool = maps:get(pool, TraverseInfo),
    case offline_access_manager:get_session_id(TaskId) of
        {ok, SessionId} ->
            {ok, user_ctx:new(SessionId)};
        {error, _} = Error ->
            utils:throttle(?LOG_THROTTLING_INTERVAL, fun() ->
                ?error(
                    "Traverse ~s performed by pool ~s failed to acquire offline session due to ~p. "
                    "Traverse will be cancelled.", [TaskId, Pool, Error])
            end),
            tree_traverse:cancel(Pool, TaskId),
            {error, ?EACCES}
    end.
