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

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
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
    get_session_id/1,
    get_auth/1, get_data_constraints/1
]).
-export([is_root/1, is_guest/1, is_normal_user/1, is_direct_io/1]).

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
        {ok, Session} -> #user_ctx{session = Session};
        {error, not_found} -> throw(?EACCES)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets user from request's context.
%% @end
%%--------------------------------------------------------------------
-spec get_user(ctx()) -> od_user:doc().
get_user(#user_ctx{session = #document{key = SessId, value = #session{
    identity = #user_identity{user_id = UserId}
}}}) ->
    case get(user_ctx_cache) of
        undefined ->
            {ok, User} = user_logic:get(SessId, UserId),
            put(user_ctx_cache, User),
            User;
        CachedUser ->
            CachedUser
    end.

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
%% Gets SessionId from user context.
%% @end
%%--------------------------------------------------------------------
-spec get_session_id(ctx()) -> session:id().
get_session_id(#user_ctx{session = #document{key = SessId}}) ->
    SessId.

%%--------------------------------------------------------------------
%% @doc
%% Gets session's auth from user context.
%% @end
%%--------------------------------------------------------------------
-spec get_auth(ctx()) -> session:auth().
get_auth(#user_ctx{session = Session}) ->
    session:get_auth(Session).

%%--------------------------------------------------------------------
%% @doc
%% Gets session's data constraints.
%% @end
%%--------------------------------------------------------------------
-spec get_data_constraints(ctx()) -> token_utils:data_constraints().
get_data_constraints(#user_ctx{session = Session}) ->
    session:get_data_constraints(Session).

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
-spec is_direct_io(ctx()) -> boolean().
is_direct_io(#user_ctx{session = #document{
    value = #session{direct_io = DirectIO}
}}) ->
    DirectIO.
