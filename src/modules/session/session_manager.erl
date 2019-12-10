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
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    reuse_or_create_fuse_session/3,
    reuse_or_create_rest_session/2,
    reuse_or_create_incoming_provider_session/1,
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
-spec reuse_or_create_fuse_session(Nonce :: binary(), aai:subject(),
    session:auth()) -> {ok, session:id()} | error().
reuse_or_create_fuse_session(Nonce, Identity, Auth) ->
    SessId = datastore_utils:gen_key(<<"">>, term_to_binary({fuse, Nonce})),
    reuse_or_create_session(SessId, fuse, Identity, Auth).


%%--------------------------------------------------------------------
%% @doc
%% Creates incoming provider's session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_incoming_provider_session(aai:subject()) ->
    {ok, session:id()} | error().
reuse_or_create_incoming_provider_session(?SUB(?ONEPROVIDER, ProviderId) = Identity) ->
    SessId = session_utils:get_provider_session_id(incoming, ProviderId),
    reuse_or_create_session(SessId, provider_incoming, Identity, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Creates outgoing provider's session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_outgoing_provider_session(session:id(),
    aai:subject()) -> {ok, session:id()} | error().
reuse_or_create_outgoing_provider_session(SessId, Identity) ->
    reuse_or_create_session(SessId, provider_outgoing, Identity, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Creates or reuses proxy session and starts session supervisor.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_proxied_session(session:id(),
    ProxyVia :: oneprovider:id(),
    session:auth(), SessionType :: atom()) ->
    {ok, session:id()} | error().
reuse_or_create_proxied_session(SessId, ProxyVia, Auth, SessionType) ->
    case auth_manager:verify(Auth) of
        {ok, #auth{subject = ?SUB(user, _) = Identity}, _TokenValidUntil} ->
            reuse_or_create_session(
                SessId, SessionType, Identity, Auth, ProxyVia
            );
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates REST session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_rest_session(aai:subject(), session:auth()) ->
    {ok, session:id()} | error().
reuse_or_create_rest_session(?SUB(user, UserId) = Identity, Auth) ->
    SessId = datastore_utils:gen_key(<<"">>, term_to_binary({rest, Auth})),
    case user_logic:exists(?ROOT_SESS_ID, UserId) of
        true ->
            reuse_or_create_session(SessId, rest, Identity, Auth);
        false ->
            {error, {invalid_identity, Identity}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates GUI session and starts session supervisor.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_gui_session(aai:subject(), session:auth()) ->
    {ok, session:id()} | error().
reuse_or_create_gui_session(Identity, Auth) ->
    SessId = datastore_utils:gen_key(<<"">>, term_to_binary({gui, Auth})),
    reuse_or_create_session(SessId, gui, Identity, Auth).


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
            identity = ?SUB(root, ?ROOT_USER_ID),
            auth = ?ROOT_AUTH,
            data_constraints = data_constraints:get_allow_all_constraints()
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
            identity = ?SUB(nobody, ?GUEST_USER_ID),
            auth = ?GUEST_AUTH,
            data_constraints = data_constraints:get_allow_all_constraints()
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
    aai:subject(), session:auth() | undefined) ->
    {ok, SessId :: session:id()} | error().
reuse_or_create_session(SessId, SessType, Identity, Auth) ->
    reuse_or_create_session(SessId, SessType, Identity, Auth, undefined).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_session(session:id(), session:type(),
    aai:subject(), session:auth() | undefined,
    ProxyVia :: oneprovider:id() | undefined) ->
    {ok, SessId :: session:id()} | error().
reuse_or_create_session(SessId, SessType, Identity, Auth, ProxyVia) ->
    Caveats = case Auth of
        % Providers sessions are not constrained by any caveats
        undefined ->
            [];
        TokenAuth ->
            {ok, Token} = tokens:deserialize(auth_manager:get_access_token(TokenAuth)),
            tokens:get_caveats(Token)
    end,

    case data_constraints:get(Caveats) of
        {ok, DataConstraints} ->
            % TODO VFS-5895 check if this critical section is still necessary
            critical_section:run([?MODULE, SessId], fun() ->
                reuse_or_create_session(
                    SessId, SessType, Identity, Auth,
                    DataConstraints, ProxyVia
                )
            end);
        {error, invalid_constraints} ->
            {error, invalid_token}
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
    Identity :: aai:subject(), Auth :: session:auth() | undefined,
    DataConstraints :: data_constraints:constraints(),
    ProxyVia :: undefined | oneprovider:id()
) ->
    {ok, SessId :: session:id()} | error().
reuse_or_create_session(SessId, SessType, Identity, Auth, DataConstraints, ProxyVia) ->
    Sess = #session{
        type = SessType,
        status = initializing,
        identity = Identity,
        auth = Auth,
        data_constraints = DataConstraints,
        proxy_via = ProxyVia
    },
    Diff = fun
        (#session{status = inactive}) ->
            % TODO VFS-5126 - possible race with closing (creation when cleanup
            % is not finished)
            {error, not_found};
        (#session{identity = ValidIdentity} = ExistingSess) ->
            case Identity of
                ValidIdentity ->
                    {ok, ExistingSess};
                _ ->
                    {error, {invalid_identity, Identity}}
            end
    end,
    case session:update(SessId, Diff) of
        {ok, SessId} ->
            {ok, SessId};
        {error, not_found} ->
            case start_session(#document{key = SessId, value = Sess}) of
                {error, already_exists} ->
                    reuse_or_create_session(
                        SessId, SessType, Identity, Auth,
                        DataConstraints, ProxyVia
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
    lists:foreach(fun(Conn) -> connection:close(Conn) end, Cons).
