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
-include_lib("ctool/include/logging.hrl").

%% Context definition
-record(user_ctx, {
    session :: session:doc(),
    user_doc :: od_user:doc()
}).

-type ctx() :: #user_ctx{}.

%% API
-export([new/1]).
-export([get_user/1, get_user_id/1, get_session_id/1, get_auth/1]).
-export([is_root/1, is_guest/1, is_normal_user/1]).

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
    {ok, Session = #document{value = #session{
        auth = Auth,
        identity = #user_identity{user_id = UserId}
    }}} = session:get(SessId),
    {ok, User} = od_user:get_or_fetch(Auth, UserId),
    #user_ctx{
        session = Session,
        user_doc = User
    }.

%%--------------------------------------------------------------------
%% @doc
%% Gets user from request's context.
%% @end
%%--------------------------------------------------------------------
-spec get_user(ctx()) -> od_user:doc().
get_user(#user_ctx{user_doc = User}) ->
    User.

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
%% Checks if context represents root user.
%% @end
%%--------------------------------------------------------------------
-spec is_root(ctx()) -> boolean().
is_root(#user_ctx{session = #document{key = SessId}}) ->
    session:is_root(SessId).

%%--------------------------------------------------------------------
%% @doc
%% Checks if context represents guest user.
%% @end
%%--------------------------------------------------------------------
-spec is_guest(ctx()) -> boolean().
is_guest(#user_ctx{session = #document{key = SessId}}) ->
    session:is_guest(SessId).

%%--------------------------------------------------------------------
%% @doc
%% Checks if context represents normal user.
%% @end
%%--------------------------------------------------------------------
-spec is_normal_user(ctx()) -> boolean().
is_normal_user(#user_ctx{session = #document{key = SessId}}) ->
    not session:is_special(SessId).