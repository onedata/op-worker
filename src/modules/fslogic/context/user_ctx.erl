%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides access to user context information, such as user's
%%% credentials, along with context management functions.
%%% @end
%%%--------------------------------------------------------------------
-module(user_ctx).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% Context definition
-record(user_ctx, {
    session :: session:doc()
}).

-type ctx() :: #user_ctx{}.
-export_type([ctx/0]).

%% API
-export([new/1]).
-export([
    get_user/1, get_user_id/1,
    get_eff_spaces/1, get_session_id/1,
    get_credentials/1, get_data_constraints/1
]).
-export([is_space_owner/2]).
-export([is_root/1, is_guest/1, is_normal_user/1, is_direct_io/2]).
-export([is_in_open_handle_mode/1, set_mode/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns newly created user context for given session ID.
%% @end
%%--------------------------------------------------------------------
-spec new(session:id()) -> ctx() | no_return().
new(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{type = rest, accessed = LastAccess}} = Session} ->
            Now = global_clock:timestamp_seconds(),
            {ok, TTL} = application:get_env(?APP_NAME, rest_session_grace_period_seconds),
            % TODO VFS-6586 - refactor rest session expiration
            {ok, UpdatedSession} = case Now > LastAccess + 0.6 * TTL of
                true ->
                    session:update_doc_and_time(SessId, fun(Rec) -> {ok, Rec} end);
                false ->
                    {ok, Session}
            end,
            #user_ctx{session = UpdatedSession};
        {ok, #document{value = #session{type = gui, accessed = LastAccess}} = Session} ->
            Now = global_clock:timestamp_seconds(),
            {ok, TTL} = application:get_env(?APP_NAME, gui_session_grace_period_seconds),
            % TODO VFS-6586 - refactor gui session expiration
            {ok, UpdatedSession} = case Now > LastAccess + 0.6 * TTL of
                true ->
                    session:update_doc_and_time(SessId, fun(Rec) -> {ok, Rec} end);
                false ->
                    {ok, Session}
            end,
            #user_ctx{session = UpdatedSession};
        {ok, Session} ->
            #user_ctx{session = Session};
        {error, not_found} ->
            throw(?EACCES)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets user from request's context.
%% @end
%%--------------------------------------------------------------------
-spec get_user(ctx()) -> od_user:doc().
get_user(#user_ctx{session = #document{key = SessId, value = #session{
    identity = ?SUB(Type, UserId)
}}}) when
    (Type =:= root andalso UserId =:= ?ROOT_USER_ID);
    (Type =:= nobody andalso UserId =:= ?GUEST_USER_ID);
    Type =:= user
->
    % TODO VFS-7313 get rid of this hacked cache
%%    case get(user_ctx_cache) of
%%        undefined ->
            {ok, User} = user_logic:get(SessId, UserId),
%%            put(user_ctx_cache, User),
            User.
%%        CachedUser ->
%%            CachedUser
%%    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets UserId from user context.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(ctx()) -> od_user:id().
get_user_id(#user_ctx{session = Session}) ->
    {ok, UserId} = session:get_user_id(Session),
    UserId.

%%--------------------------------------------------------------------
%% @doc
%% Gets effective spaces from user context.
%% @end
%%--------------------------------------------------------------------
-spec get_eff_spaces(ctx()) -> [od_space:id()].
get_eff_spaces(UserCtx) ->
    #document{value = #od_user{eff_spaces = Spaces}} = user_ctx:get_user(UserCtx),
    Spaces.

%%--------------------------------------------------------------------
%% @doc
%% Gets SessionId from user context.
%% @end
%%--------------------------------------------------------------------
-spec get_session_id(ctx()) -> session:id().
get_session_id(#user_ctx{session = #document{key = SessId}}) ->
    SessId.

%%--------------------------------------------------------------------
%% @doc
%% Gets session's credentials from user context.
%% @end
%%--------------------------------------------------------------------
-spec get_credentials(ctx()) -> auth_manager:credentials().
get_credentials(#user_ctx{session = Session}) ->
    session:get_credentials(Session).

%%--------------------------------------------------------------------
%% @doc
%% Gets session's data constraints from user context.
%% @end
%%--------------------------------------------------------------------
-spec get_data_constraints(ctx()) -> data_constraints:constraints().
get_data_constraints(#user_ctx{session = Session}) ->
    session:get_data_constraints(Session).

-spec is_space_owner(ctx(), od_space:id()) -> boolean().
is_space_owner(#user_ctx{session = SessionDoc}, SpaceId) ->
    session:is_space_owner(SessionDoc, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Checks if context represents root user.
%% @end
%%--------------------------------------------------------------------
-spec is_root(ctx()) -> boolean().
is_root(#user_ctx{session = #document{key = SessId}}) ->
    session_utils:is_root(SessId).

%%--------------------------------------------------------------------
%% @doc
%% Checks if context represents guest user.
%% @end
%%--------------------------------------------------------------------
-spec is_guest(ctx()) -> boolean().
is_guest(#user_ctx{session = #document{key = SessId}}) ->
    session_utils:is_guest(SessId).

%%--------------------------------------------------------------------
%% @doc
%% Checks if context represents normal user.
%% @end
%%--------------------------------------------------------------------
-spec is_normal_user(ctx()) -> boolean().
is_normal_user(#user_ctx{session = #document{key = SessId}}) ->
    not session_utils:is_special(SessId).

%%--------------------------------------------------------------------
%% @doc
%% Checks if session uses direct_io.
%% @end
%%--------------------------------------------------------------------
-spec is_direct_io(ctx(), od_space:id()) -> boolean().
is_direct_io(#user_ctx{session = #document{
    value = #session{direct_io = DirectIO, type = fuse}
}}, SpaceId) ->
    maps:get(SpaceId, DirectIO, true);
is_direct_io(_, _) ->
    false.

-spec is_in_open_handle_mode(ctx()) -> boolean().
is_in_open_handle_mode(#user_ctx{session = #document{
    value = #session{mode = open_handle}
}}) ->
    true;
is_in_open_handle_mode(_) ->
    false.

-spec set_mode(ctx(), session:mode()) -> ctx().
set_mode(#user_ctx{session = #document{value = SessRec} = SessDoc} = UserCtx, SessMode) ->
    UserCtx#user_ctx{session = SessDoc#document{
        value = SessRec#session{mode = SessMode}
    }}.
