%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides and manages fslogic context information
%%% such user's credentials.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_context).
-author("Rafal Slota").

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% Context definition
-record(fslogic_context, {
    session :: session:doc(),
    user_doc :: undefined | od_user:doc()
}).

-type ctx() :: #fslogic_context{}.

%% API
-export([new/1]).
-export([get_user/1, get_user_id/1, get_session_id/1, get_auth/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns newly created fslogic Ctx for given session ID
%% @end
%%--------------------------------------------------------------------
-spec new(session:id()) -> ctx() | no_return().
new(SessId) ->
    {ok, Session = #document{value = #session{
        auth = Auth,
        identity = #user_identity{user_id = UserId}
    }}} = session:get(SessId),
%%    {ok, User} = od_user:get_or_fetch(Auth, UserId), %todo enable after fixing race
    #fslogic_context{session = Session}.

%%--------------------------------------------------------------------
%% @doc
%% Get user from request's context
%% @end
%%--------------------------------------------------------------------
-spec get_user(ctx()) -> od_user:doc().
get_user(Ctx) ->
    Auth = get_auth(Ctx),
    UserId = get_user_id(Ctx),
    {ok, User} = od_user:get_or_fetch(Auth, UserId), %todo remove after fixing race
    User.

%%--------------------------------------------------------------------
%% @doc Retrieves user ID from fslogic context.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(ctx()) -> od_user:id().
get_user_id(#fslogic_context{session = #document{value = #session{identity = #user_identity{user_id = UserId}}}}) ->
    UserId.

%%--------------------------------------------------------------------
%% @doc Retrieves SessionID from fslogic context.
%% @end
%%--------------------------------------------------------------------
-spec get_session_id(ctx()) -> session:id().
get_session_id(#fslogic_context{session = #document{key = SessId}}) ->
    SessId.

%%--------------------------------------------------------------------
%% @doc Retrieves session's auth from fslogic context.
%% @end
%%--------------------------------------------------------------------
-spec get_auth(ctx()) -> session:auth().
get_auth(#fslogic_context{session = #document{value = #session{auth = Auth}}}) ->
    Auth.