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

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([reuse_or_create_fuse_session/3, reuse_or_create_fuse_session/4]).
-export([reuse_or_create_rest_session/1, reuse_or_create_rest_session/2]).
-export([create_gui_session/2]).
-export([remove_session/1]).
-export([get_provider_session_id/2, session_id_to_provider_id/1]).
-export([reuse_or_create_provider_session/4]).

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
reuse_or_create_rest_session(Iden, Auth) ->
    SessId = session:get_rest_session_id(Iden),
    reuse_or_create_session(SessId, rest, Iden, Auth, []).

%%--------------------------------------------------------------------
%% @doc
%% Creates GUI session and starts session supervisor.
%% @end
%%--------------------------------------------------------------------
-spec create_gui_session(Iden :: session:identity(), Auth :: session:auth()) ->
    {ok, SessId :: session:id()} | {error, Reason :: term()}.
create_gui_session(Iden, Auth) ->
    SessId = datastore_utils:gen_uuid(),
    Sess = #session{status = active, identity = Iden, auth = Auth, type = gui},
    case session:create(#document{key = SessId, value = Sess}) of
        {ok, SessId} ->
            supervisor:start_child(?SESSION_MANAGER_WORKER_SUP, [SessId, gui]),
            subscribe_user(Iden),
            {ok, SessId};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes session from cache, stops session supervisor and disconnects remote
%% client.
%% @end
%%--------------------------------------------------------------------
-spec remove_session(SessId :: session:id()) -> ok | {error, Reason :: term()}.
remove_session(SessId) ->
    worker_proxy:call(?SESSION_MANAGER_WORKER, {remove_session, SessId}).


%%--------------------------------------------------------------------
%% @doc
%% Generates session id for given provider.
%% @end
%%--------------------------------------------------------------------
-spec get_provider_session_id(Type :: incoming | outgoing, oneprovider:id()) -> session:id().
get_provider_session_id(Type, ProviderId) ->
    base64:encode(term_to_binary({Type, provider, ProviderId})).


%%--------------------------------------------------------------------
%% @doc
%% Gets provider's id from given session (assumes that the session was created for provider).
%% @end
%%--------------------------------------------------------------------
-spec session_id_to_provider_id(session:id()) -> oneprovider:id().
session_id_to_provider_id(SessId) ->
    {_, provider, ProviderId} = binary_to_term(base64:decode(SessId)),
    ProviderId.

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
    NewCons :: list()) -> {ok, SessId :: session:id()} | {error, Reason :: term()}.
reuse_or_create_session(SessId, SessType, Iden, Auth, NewCons) ->
    Sess = #session{status = active, identity = Iden, auth = Auth,
        connections = NewCons, type = SessType},
    Diff = fun
        (#session{status = inactive}) ->
            {error, {not_found, session}};
        (#session{identity = ValidIden, connections = Cons} = ExistingSess) ->
            case Iden of
                ValidIden ->
                    {ok, ExistingSess#session{connections = NewCons ++ Cons}};
                _ ->
                    {error, {invalid_identity, Iden}}
            end
    end,
    case session:update(SessId, Diff) of
        {ok, SessId} ->
            subscribe_user(Iden),
            {ok, SessId};
        {error, {not_found, _}} ->
            case session:create(#document{key = SessId, value = Sess}) of
                {ok, SessId} ->
                    supervisor:start_child(?SESSION_MANAGER_WORKER_SUP, [SessId, SessType]),
                    subscribe_user(Iden),
                    {ok, SessId};
                {error, already_exists} ->
                    reuse_or_create_session(SessId, SessType, Iden, Auth, NewCons);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc @private
%% Includes user in subscription (if identity belongs to an user).
%% @end
%%--------------------------------------------------------------------
-spec subscribe_user(Iden :: session:identity()) -> ok.
subscribe_user(Iden) ->
    UID = Iden#identity.user_id,
    case UID of
        undefined -> ok;
        _ ->
            subscriptions:put_user(UID),
            subscriptions_worker:refresh_subscription()
    end.