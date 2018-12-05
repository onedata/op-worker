%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utility functions for session management.
%%% @end
%%%-------------------------------------------------------------------
-module(session_manager).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([reuse_or_create_fuse_session/3, reuse_or_create_fuse_session/4]).
-export([reuse_or_create_rest_session/1, reuse_or_create_rest_session/2]).
-export([reuse_or_create_provider_session/4, reuse_or_create_proxy_session/4]).
-export([create_gui_session/2, create_root_session/0, create_guest_session/0]).
-export([remove_session/1]).

%% Test API
-export([reuse_or_create_session/5]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv reuse_or_create_fuse_session(SessId, Iden, undefined, Con)
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_fuse_session(SessId :: session:id(), Iden :: session:identity(),
    Con :: pid()) -> {ok, SessId :: session:id()} | {error, Reason :: term()}.
reuse_or_create_fuse_session(SessId, Iden, Con) ->
    reuse_or_create_fuse_session(SessId, Iden, undefined, Con).

%%--------------------------------------------------------------------
%% @doc
%% Creates FUSE session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_fuse_session(SessId :: session:id(), Iden :: session:identity(),
    Auth :: session:auth() | undefined, Con :: pid()) ->
    {ok, SessId :: session:id()} | {error, Reason :: term()}.
reuse_or_create_fuse_session(SessId, Iden, Auth, Con) ->
    reuse_or_create_session(SessId, fuse
        , Iden, Auth, [Con]).

%%--------------------------------------------------------------------
%% @doc
%% Creates provider's session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_provider_session(SessId :: session:id(),
    SessionType :: session:type(), Iden :: session:identity(), Con :: pid()) ->
    {ok, SessId :: session:id()} | {error, Reason :: term()}.
reuse_or_create_provider_session(SessId, SessionType, Iden, Con) ->
    reuse_or_create_session(SessId, SessionType, Iden, undefined, [Con]).

%%--------------------------------------------------------------------
%% @doc
%% @equiv reuse_or_create_rest_session(Iden, undefined)
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_rest_session(Iden :: session:identity()) ->
    {ok, SessId :: session:id()} | {error, Reason :: term()}.
reuse_or_create_rest_session(Iden) ->
    reuse_or_create_rest_session(Iden, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Creates REST session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_rest_session(Iden :: session:identity(),
    Auth :: session:auth() | undefined) ->
    {ok, SessId :: session:id()} | {error, Reason :: term()}.
reuse_or_create_rest_session(Iden = #user_identity{user_id = UserId}, Auth) ->
    SessId = session_utils:get_rest_session_id(Iden),
    case user_logic:exists(?ROOT_SESS_ID, UserId) of
        true ->
            reuse_or_create_session(SessId, rest, Iden, Auth, []);
        false ->
            {error, {invalid_identity, Iden}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates or reuses proxy session and starts session supervisor.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_proxy_session(SessId :: session:id(), ProxyVia :: oneprovider:id(),
    Auth :: session:auth(), SessionType :: atom()) ->
    {ok, SessId :: session:id()} | {error, Reason :: term()}.
reuse_or_create_proxy_session(SessId, ProxyVia, Auth, SessionType) ->
    {ok, #document{value = #user_identity{} = Iden}} = user_identity:get_or_fetch(Auth),
    reuse_or_create_session(SessId, SessionType, Iden, Auth, [], ProxyVia).


%%--------------------------------------------------------------------
%% @doc
%% Creates GUI session and starts session supervisor.
%% @end
%%--------------------------------------------------------------------
-spec create_gui_session(Iden :: session:identity(), Auth :: session:auth()) ->
    {ok, SessId :: session:id()} | {error, Reason :: term()}.
create_gui_session(Iden, Auth) ->
    SessId = datastore_utils:gen_key(),
    Sess = #session{status = active, identity = Iden, auth = Auth, type = gui,
        connections = []},
    start_session(#document{key = SessId, value = Sess}, gui).

%%--------------------------------------------------------------------
%% @doc
%% Creates root session and starts session supervisor.
%% @end
%%--------------------------------------------------------------------
-spec create_root_session() -> {ok, SessId :: session:id()} |
{error, Reason :: term()}.
create_root_session() ->
    Sess = #session{status = active, type = root, connections = [],
        identity = #user_identity{user_id = ?ROOT_USER_ID},
        auth = ?ROOT_AUTH
    },
    start_session(#document{key = ?ROOT_SESS_ID, value = Sess}, root).

%%--------------------------------------------------------------------
%% @doc
%% Creates guest session and starts session supervisor.
%% @end
%%--------------------------------------------------------------------
-spec create_guest_session() -> {ok, SessId :: session:id()} |
    {error, Reason :: term()}.
create_guest_session() ->
    Sess = #session{status = active, type = guest, connections = [],
        identity = #user_identity{user_id = ?GUEST_USER_ID}, auth = ?GUEST_AUTH},
    start_session(#document{key = ?GUEST_SESS_ID, value = Sess}, guest).

%%--------------------------------------------------------------------
%% @doc
%% Removes session from cache, stops session supervisor and disconnects remote
%% client.
%% @end
%%--------------------------------------------------------------------
-spec remove_session(SessId :: session:id()) -> ok | {error, Reason :: term()}.
remove_session(SessId) ->
    worker_proxy:call(?SESSION_MANAGER_WORKER, {remove_session, SessId}).

%%%===================================================================
%%% Internal functions exported for tests
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @private
%% Creates session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_session(SessId :: session:id(), SessType :: session:type(),
    Iden :: session:identity(), Auth :: session:auth() | undefined,
    NewCons :: list()) ->
    {ok, SessId :: session:id()} | {error, Reason :: term()}.
reuse_or_create_session(SessId, SessType, Iden, Auth, NewCons) ->
    reuse_or_create_session(SessId, SessType, Iden, Auth, NewCons, undefined).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @private
%% Creates session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_session(SessId :: session:id(), SessType :: session:type(),
    Iden :: session:identity(), Auth :: session:auth() | undefined,
    NewCons :: list(), ProxyVia :: session:id() | undefined) ->
    {ok, SessId :: session:id()} | {error, Reason :: term()}.
reuse_or_create_session(SessId, SessType, Iden, Auth, NewCons, ProxyVia) ->
    {Sess, Diff} = session_connections:get_new_record_and_update_fun(
        NewCons, ProxyVia, SessType, Auth, Iden),
    case session:update(SessId, Diff) of
        {ok, SessId} ->
            {ok, SessId};
        {error, not_found} ->
            case start_session(#document{key = SessId, value = Sess}, SessType) of
                {error, already_exists} ->
                    reuse_or_create_session(SessId, SessType, Iden, Auth, NewCons, ProxyVia);
                Other ->
                    Other
            end;
        {error, Reason} ->
            {error, Reason}
    end.

start_session(Doc, SessType) ->
    case session:create(Doc) of
        {ok, SessId} ->
            supervisor:start_child(?SESSION_MANAGER_WORKER_SUP, [SessId, SessType]),
            {ok, SessId};
        Error ->
            Error
    end.