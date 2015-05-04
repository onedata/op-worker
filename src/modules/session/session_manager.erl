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

%% API
-export([reuse_or_create_session/3, remove_session/1]).

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
        ?TIMEOUT,
        prefer_local
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
        ?TIMEOUT,
        prefer_local
    ).

%%%===================================================================
%%% Internal functions
%%%===================================================================
