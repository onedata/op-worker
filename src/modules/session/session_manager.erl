%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for creating and forwarding requests to
%%% session worker.
%%% @end
%%%-------------------------------------------------------------------
-module(session_manager).
-author("Krzysztof Trzepla").

-include("cluster/worker/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").

%% API
-export([reuse_or_create_session/3, update_session_auth/2, remove_session/1]).
-export([create_gui_session/1]).

-define(TIMEOUT, timer:seconds(20)).
-define(SESSION_WORKER, session_manager_worker).

%%%===================================================================
%%% API
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% Reuses active session or creates one for user with given identity.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_session(SessId :: session:id(),
    Iden :: session:identity(), Con :: pid()) ->
    {ok, reused | created} |{error, Reason :: term()}.
reuse_or_create_session(SessId, Iden, Con) ->
    worker_proxy:call(
        ?SESSION_WORKER,
        {reuse_or_create_session, SessId, Iden, Con},
        ?TIMEOUT
    ).


%%--------------------------------------------------------------------
%% @doc
%% Updates the #auth{} record in given session (asynchronous).
%% @end
%%--------------------------------------------------------------------
-spec update_session_auth(SessId :: session:id(), Auth :: #auth{}) -> ok.
update_session_auth(SessId, #auth{} = Auth) ->
    worker_proxy:cast(
        ?SESSION_WORKER,
        {update_session_auth, SessId, Auth}
    ).


%%--------------------------------------------------------------------
%% @doc
%% Removes session identified by session ID.
%% @end
%%--------------------------------------------------------------------
-spec remove_session(SessId :: session:id()) -> ok | {error, Reason :: term()}.
remove_session(SessId) ->
    worker_proxy:call(
        ?SESSION_WORKER,
        {remove_session, SessId},
        ?TIMEOUT
    ).


% @todo Below function must be integrated with current session logic.
% For now, this is only a stub used in GUI.

-spec create_gui_session(Auth :: #auth{}) ->
    {ok, session:id()} | {error, Reason :: term()}.
create_gui_session(Auth) ->
    SessionId = datastore_utils:gen_uuid(),
    {ok, #document{value = #identity{} = Iden}} = identity:get_or_fetch(Auth),
    SessionRec = #session{
        identity = Iden,
        type = gui,
        auth = Auth
    },
    SessionDoc = #document{
        key = SessionId,
        value = SessionRec
    },
    case session:save(SessionDoc) of
        {ok, _} -> {ok, SessionId};
        {error, _} = Error -> Error
    end.
