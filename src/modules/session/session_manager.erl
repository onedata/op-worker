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
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    reuse_or_create_fuse_session/3,
    reuse_or_create_rest_session/2,
    reuse_or_create_incoming_provider_session/2,
    reuse_or_create_outgoing_provider_session/2,
    reuse_or_create_proxied_session/4,
    reuse_or_create_gui_session/2,
    create_root_session/0, create_guest_session/0
]).
-export([remove_session/1]).

-type error() :: {error, Reason :: term()}.


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates FUSE session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_fuse_session(Nonce :: binary(), session:identity(),
    session:auth()) -> {ok, session:id()} | error().
reuse_or_create_fuse_session(Nonce, Iden, Auth) ->
    SessId = datastore_utils:gen_key(<<"">>, term_to_binary({fuse, Nonce, Auth})),
    reuse_or_create_session(SessId, fuse, Iden, Auth).


%%--------------------------------------------------------------------
%% @doc
%% Creates incoming provider's session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_incoming_provider_session(session:id(),
    session:identity()) -> {ok, session:id()} | error().
reuse_or_create_incoming_provider_session(SessId, Iden) ->
    reuse_or_create_session(SessId, provider_incoming, Iden, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Creates outgoing provider's session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_outgoing_provider_session(session:id(),
    session:identity()) -> {ok, session:id()} | error().
reuse_or_create_outgoing_provider_session(SessId, Iden) ->
    reuse_or_create_session(SessId, provider_outgoing, Iden, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Creates or reuses proxy session and starts session supervisor.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_proxied_session(SessId :: session:id(),
    ProxyVia :: oneprovider:id(),
    session:auth(), SessionType :: atom()) ->
    {ok, session:id()} | error().
reuse_or_create_proxied_session(SessId, ProxyVia, Auth, SessionType) ->
    case user_identity:get_or_fetch(Auth) of
        {ok, #document{value = #user_identity{} = Iden}} ->
            reuse_or_create_session(SessId, SessionType, Iden, Auth, ProxyVia);
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates REST session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_rest_session(session:identity(),
    session:auth() | undefined) -> {ok, session:id()} | error().
reuse_or_create_rest_session(Iden = #user_identity{user_id = UserId}, Auth) ->
    SessId = datastore_utils:gen_key(<<"">>, term_to_binary({rest, Auth})),
    case user_logic:exists(?ROOT_SESS_ID, UserId) of
        true ->
            reuse_or_create_session(SessId, rest, Iden, Auth);
        false ->
            {error, {invalid_identity, Iden}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates GUI session and starts session supervisor.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_gui_session(session:identity(), session:auth()) ->
    {ok, session:id()} | error().
reuse_or_create_gui_session(Iden, Auth) ->
    SessId = datastore_utils:gen_key(<<"">>, term_to_binary({gui, Auth})),
    reuse_or_create_session(SessId, gui, Iden, Auth).


%%--------------------------------------------------------------------
%% @doc
%% Creates root session and starts session supervisor.
%% @end
%%--------------------------------------------------------------------
-spec create_root_session() -> {ok, session:id()} | error().
create_root_session() ->
    start_session(#document{
        key = ?ROOT_SESS_ID,
        value = #session{
            type = root,
            status = active,
            identity = #user_identity{user_id = ?ROOT_USER_ID},
            auth = ?ROOT_AUTH,
            allowed_paths = any,
            guid_constraints = any
        }
    }).


%%--------------------------------------------------------------------
%% @doc
%% Creates guest session and starts session supervisor.
%% @end
%%--------------------------------------------------------------------
-spec create_guest_session() -> {ok, session:id()} | error().
create_guest_session() ->
    start_session(#document{
        key = ?GUEST_SESS_ID,
        value = #session{
            type = guest,
            status = active,
            identity = #user_identity{user_id = ?GUEST_USER_ID},
            auth = ?GUEST_AUTH,
            allowed_paths = any,
            guid_constraints = any
        }
    }).


%%--------------------------------------------------------------------
%% @doc
%% Removes session from cache, stops session supervisor and disconnects remote
%% client.
%% @end
%%--------------------------------------------------------------------
-spec remove_session(session:id()) -> ok | error().
remove_session(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{supervisor = undefined, connections = Cons}}} ->
            session:delete(SessId),
            % VFS-5155 Should connections be closed before session document is deleted?
            close_connections(Cons);
        {ok, #document{value = #session{supervisor = Sup, node = Node, connections = Cons}}} ->
            try
                supervisor:terminate_child({?SESSION_MANAGER_WORKER_SUP, Node}, Sup)
            catch
                exit:{noproc, _} -> ok;
                exit:{shutdown, _} -> ok
            end,
            session:delete(SessId),
            close_connections(Cons);
        {error, not_found} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv reuse_or_create_session(SessId, SessType, Iden, Auth, undefined).
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_session(session:id(), session:type(),
    session:identity(), session:auth() | undefined) ->
    {ok, SessId :: session:id()} | error().
reuse_or_create_session(SessId, SessType, Iden, Auth) ->
    reuse_or_create_session(SessId, SessType, Iden, Auth, undefined).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_session(session:id(), session:type(),
    session:identity(), session:auth() | undefined,
    ProxyVia :: oneprovider:id() | undefined) ->
    {ok, SessId :: session:id()} | error().
reuse_or_create_session(SessId, SessType, Iden, Auth, ProxyVia) ->
    Caveats = case Auth of
        #token_auth{token = SerializedToken} ->
            {ok, Token} = tokens:deserialize(SerializedToken),
            tokens:get_caveats(Token);
        _ ->
            []
    end,
    case token_utils:get_data_constraints(Caveats) of
        {[], _} ->
            {error, invalid_token};
        {_, []} ->
            {error, invalid_token};
        {AllowedPaths, GuidConstraints} ->
            critical_section:run([?MODULE, SessId], fun() ->
                reuse_or_create_session(
                    SessId, SessType, Iden, Auth,
                    AllowedPaths, GuidConstraints, ProxyVia
                )
            end)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session or if session exists reuses it.
%% NOTE !!!
%% To avoid races during session creation this function should be run in
%% critical section.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_session(SessId :: session:id(), SessType :: session:type(),
    Iden :: session:identity(), Auth :: session:auth() | undefined,
    token_utils:allowed_paths(), token_utils:guid_constraints(),
    ProxyVia :: undefined | oneprovider:id()
) ->
    {ok, SessId :: session:id()} | error().
reuse_or_create_session(SessId, SessType, Iden, Auth,
    AllowedPaths, GuidConstraints, ProxyVia
) ->
    Sess = #session{
        type = SessType,
        status = initializing,
        identity = Iden,
        auth = Auth,
        allowed_paths = AllowedPaths,
        guid_constraints = GuidConstraints,
        proxy_via = ProxyVia
    },
    Diff = fun
        (#session{status = inactive}) ->
            % TODO VFS-5126 - possible race with closing (creation when cleanup
            % is not finished)
            {error, not_found};
        (#session{identity = ValidIden} = ExistingSess) ->
            case Iden of
                ValidIden ->
                    {ok, ExistingSess};
                _ ->
                    {error, {invalid_identity, Iden}}
            end
    end,
    case session:update(SessId, Diff) of
        {ok, SessId} ->
            {ok, SessId};
        {error, not_found} ->
            case start_session(#document{key = SessId, value = Sess}) of
                {error, already_exists} ->
                    reuse_or_create_session(
                        SessId, SessType, Iden, Auth,
                        AllowedPaths, GuidConstraints, ProxyVia
                    );
                Other ->
                    Other
            end;
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session doc and starts supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_session(session:doc()) -> {ok, session:id()} | error().
start_session(#document{value = #session{type = SessType}} = Doc) ->
    case session:create(Doc) of
        {ok, SessId} ->
            supervisor:start_child(?SESSION_MANAGER_WORKER_SUP, [SessId, SessType]),
            {ok, SessId};
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Closes connections to remote client.
%% @end
%%--------------------------------------------------------------------
-spec close_connections(Cons :: [pid()]) -> ok.
close_connections(Cons) ->
    lists:foreach(fun(Conn) ->
        connection:close(Conn)
    end, Cons).
