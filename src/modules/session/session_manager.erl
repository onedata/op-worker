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
-export([
    restart_dead_sessions/0,
    maybe_restart_session/1,
    stop_session/1,
    clean_stopped_session/1,
    reset_session_record/3
]).

-type error() :: {error, Reason :: term()}.

% Macros used when process is waiting for other process to init session
-define(SESSION_INITIALISATION_CHECK_PERIOD_BASE, 100).
-define(SESSION_INITIALISATION_RETRIES, application:get_env(?APP_NAME, session_initialisation_retries, 8)).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates FUSE session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_fuse_session(Nonce :: binary(), aai:subject(),
    auth_manager:credentials()) -> {ok, session:id()} | error().
reuse_or_create_fuse_session(Nonce, Identity, Credentials) ->
    SessId = datastore_key:new_from_digest([<<"fuse">>, Nonce]),
    reuse_or_create_session(SessId, fuse, Identity, Credentials).


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
-spec reuse_or_create_proxied_session(session:id(), ProxyVia :: oneprovider:id(),
    auth_manager:credentials(), SessionType :: atom()) ->
    {ok, session:id()} | error().
reuse_or_create_proxied_session(SessId, ProxyVia, Credentials, SessionType) ->
    case auth_manager:verify_credentials(Credentials) of
        {ok, #auth{subject = ?SUB(user, _) = Identity}, _TokenValidUntil} ->
            reuse_or_create_session(
                SessId, SessionType, Identity, Credentials, ProxyVia
            );
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates REST session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_rest_session(aai:subject(), auth_manager:credentials()) ->
    {ok, session:id()} | error().
reuse_or_create_rest_session(?SUB(user, UserId) = Identity, Credentials) ->
    SessId = datastore_key:new_from_digest([<<"rest">>, Credentials]),
    case provider_logic:has_eff_user(UserId) of
        true ->
            reuse_or_create_session(SessId, rest, Identity, Credentials);
        false ->
            {error, {invalid_identity, Identity}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates GUI session and starts session supervisor.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_gui_session(aai:subject(), auth_manager:credentials()) ->
    {ok, session:id()} | error().
reuse_or_create_gui_session(Identity, Credentials) ->
    SessId = datastore_key:new_from_digest([<<"gui">>, Credentials]),
    reuse_or_create_session(SessId, gui, Identity, Credentials).


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
            identity = ?ROOT_IDENTITY,
            credentials = ?ROOT_CREDENTIALS,
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
            identity = ?GUEST_IDENTITY,
            credentials = ?GUEST_CREDENTIALS,
            data_constraints = data_constraints:get_allow_all_constraints()
        }
    }).


-spec restart_dead_sessions() -> ok.
restart_dead_sessions() ->
    {ok, AllSessions} = session:list(),

    lists:foreach(fun(#document{key = SessId}) ->
        maybe_restart_session(SessId)
    end, AllSessions).


%%--------------------------------------------------------------------
%% @doc
%% Checks if session processes are still alive and if not restarts them.
%% @end
%%--------------------------------------------------------------------
-spec maybe_restart_session(SessId) ->
    {ok, SessId} | error() when SessId :: session:id().
maybe_restart_session(SessId) ->
    case session:update_doc_and_time(SessId, fun maybe_clear_session_record/1) of
        {ok, #document{key = SessId}} ->
            {ok, SessId};
        {error, update_not_needed} ->
            {ok, SessId};
        {error, {supervsior_is_not_alive, SessType}} ->
            restart_session(SessId, SessType),
            {ok, SessId};
        {error, _Reason} = Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if session supervisor is alive and if not clears entries in
%% session doc and sets new one.
%% @end
%%--------------------------------------------------------------------
-spec reset_session_record(session:record(), pid(), node()) ->
    {ok, session:record()} | {error, update_not_needed}.
reset_session_record(#session{connections = Cons} = Sess, Sup, SupNode) ->
    case is_alive(Sup) of
        true ->
            {error, supervsior_is_alive};
        false ->
            % All session processes but connection ones are on the same
            % node as supervisor. If supervisor is dead so they are.
            {ok, Sess#session{
                node = SupNode,
                supervisor = Sup,
                event_manager = undefined,
                watcher = undefined,
                sequencer_manager = undefined,
                async_request_manager = undefined,
                connections = lists:filter(fun is_alive/1, Cons)
            }}
    end.

-spec stop_session(session:id()) -> ok | error().
stop_session(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{supervisor = Sup, node = Node}}} ->
            try
                supervisor:terminate_child({?SESSION_MANAGER_WORKER_SUP, Node}, Sup)
            catch
                exit:{noproc, _} -> ok;
                exit:{shutdown, _} -> ok
            end;
        {error, not_found} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

-spec clean_stopped_session(session:id()) -> ok | error().
clean_stopped_session(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{connections = Cons}}} ->
            session:delete(SessId),
            % VFS-5155 Should connections be closed before session document is deleted?
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
%% @equiv reuse_or_create_session(SessId, SessType, Iden, Credentials, undefined).
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_session(SessId, session:type(),
    aai:subject(), undefined | auth_manager:credentials()) ->
    {ok, SessId} | error() when SessId :: session:id().
reuse_or_create_session(SessId, SessType, Identity, Credentials) ->
    reuse_or_create_session(SessId, SessType, Identity, Credentials, undefined).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session or if session exists reuses it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_session(
    SessId,
    session:type(),
    Identity :: aai:subject(),
    Credentials :: undefined | auth_manager:credentials(),
    ProxyVia :: oneprovider:id() | undefined
) ->
    {ok, SessId} | error() when SessId :: session:id().
reuse_or_create_session(SessId, SessType, Identity, Credentials, ProxyVia) ->
    Caveats = case Credentials of
        % Providers sessions are not constrained by any caveats
        undefined ->
            [];
        _ ->
            {ok, SessionCaveats} = auth_manager:get_caveats(Credentials),
            SessionCaveats
    end,

    case data_constraints:get(Caveats) of
        {ok, DataConstraints} ->
            reuse_or_create_session(
                SessId, SessType, Identity, Credentials,
                DataConstraints, ProxyVia
            );
        {error, invalid_constraints} ->
            {error, invalid_token}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv reuse_or_create_session(SessId, SessType, Identity, Credentials, DataConstraints, ProxyVia,
%%        ?SESSION_INITIALISATION_CHECK_PERIOD_BASE, 0)
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_session(
    SessId,
    SessType :: session:type(),
    Identity :: aai:subject(),
    Credentials :: undefined | auth_manager:credentials(),
    DataConstraints :: data_constraints:constraints(),
    ProxyVia :: undefined | oneprovider:id()
) ->
    {ok, SessId} | error() when SessId :: session:id().
reuse_or_create_session(SessId, SessType, Identity, Credentials, DataConstraints, ProxyVia) ->
    reuse_or_create_session(SessId, SessType, Identity, Credentials, DataConstraints, ProxyVia,
        ?SESSION_INITIALISATION_CHECK_PERIOD_BASE, 0).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session or if session exists reuses it.
%% NOTE !!!
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_session(
    SessId,
    SessType :: session:type(),
    Identity :: aai:subject(),
    Credentials :: undefined | auth_manager:credentials(),
    DataConstraints :: data_constraints:constraints(),
    ProxyVia :: undefined | oneprovider:id(),
    ErrorSleep:: non_neg_integer(),
    Retries :: non_neg_integer()
) ->
    {ok, SessId} | error() when SessId :: session:id().
reuse_or_create_session(SessId, SessType, Identity, Credentials, DataConstraints, ProxyVia, ErrorSleep, RetryNum) ->
    Sess = #session{
        type = SessType,
        status = initializing,
        identity = Identity,
        credentials = Credentials,
        data_constraints = DataConstraints,
        proxy_via = ProxyVia
    },
    Diff = fun
        (#session{status = inactive}) ->
            % TODO VFS-5126 - possible race with closing (creation when cleanup
            % is not finished)
            {error, not_found};
        (#session{status = initializing}) ->
            {error, initializing};
        (#session{identity = ValidIdentity} = ExistingSess) ->
            case Identity of
                ValidIdentity ->
                    case maybe_clear_session_record(ExistingSess) of
                        {error, update_not_needed} -> {ok, ExistingSess};
                        Other -> Other
                    end;
                _ ->
                    {error, {invalid_identity, Identity}}
            end
    end,
    case session:update_doc_and_time(SessId, Diff) of
        {ok, #document{key = SessId}} ->
            {ok, SessId};
        {error, not_found} = Error ->
            case start_session(#document{key = SessId, value = Sess}) of
                {error, already_exists} ->
                    maybe_retry_session_init(SessId, SessType, Identity, Credentials,
                        DataConstraints, ProxyVia, ErrorSleep, RetryNum, Error);
                Other ->
                    Other
            end;
        {error, {supervsior_is_not_alive, SessType}} ->
            case restart_session(SessId, SessType)of
                {error, already_exists} = Error ->
                    maybe_retry_session_init(SessId, SessType, Identity, Credentials,
                        DataConstraints, ProxyVia, ErrorSleep, RetryNum, Error);
                Other ->
                    Other
            end;
        {error, initializing} = Error ->
            maybe_retry_session_init(SessId, SessType, Identity, Credentials,
                DataConstraints, ProxyVia, ErrorSleep, RetryNum, Error);
        {error, Reason} ->
            {error, Reason}
    end.

-spec maybe_retry_session_init(
    SessId,
    SessType :: session:type(),
    Identity :: aai:subject(),
    Credentials :: undefined | auth_manager:credentials(),
    DataConstraints :: data_constraints:constraints(),
    ProxyVia :: undefined | oneprovider:id(),
    ErrorSleep:: non_neg_integer(),
    Retries :: non_neg_integer(),
    Error :: {error, term()}
) ->
    {ok, SessId} | error() when SessId :: session:id().
maybe_retry_session_init(SessId, SessType, Identity, Credentials, DataConstraints, ProxyVia, ErrorSleep, RetryNum, Error) ->
    MaxRetries = ?SESSION_INITIALISATION_RETRIES,
    case RetryNum of
        MaxRetries ->
            % Process that is initializing session probably hangs - return error
            Error;
        _ ->
            % Other process is initializing session - wait
            timer:sleep(ErrorSleep),
            ?debug("Waiting for session ~p init", [SessId]),
            reuse_or_create_session(SessId, SessType, Identity, Credentials,
                DataConstraints, ProxyVia, ErrorSleep * 2, MaxRetries + 1)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if session processes are still alive and if not clears entries in
%% session doc.
%% @end
%%--------------------------------------------------------------------
-spec maybe_clear_session_record(session:record()) -> {ok, session:record()} | {error, update_not_needed}.
maybe_clear_session_record(#session{supervisor = Sup, connections = Cons, type = SessType} = Sess) ->
    case is_alive(Sup) of
        true ->
            case lists:partition(fun is_alive/1, Cons) of
                {_, []} ->
                    {error, update_not_needed};
                {AliveCons, _DeadCons} ->
                    {ok, Sess#session{connections = AliveCons}}
            end;
        false ->
            {error, {supervsior_is_not_alive, SessType}}
    end.

%% @private
-spec start_session(session:doc()) -> {ok, session:id()} | error().
start_session(#document{key = SessId, value = #session{type = SessType}} = Doc) ->
    case supervisor:start_child(?SESSION_MANAGER_WORKER_SUP, [Doc, SessType]) of
        {ok, undefined} ->
            {error, already_exists};
        {ok, _} ->
            {ok, SessId};
        {error, already_present} ->
            ?warning("Session ~p supervisor already exists", [SessId]),
            {ok, SessId};
        {error, {already_started, _}} ->
            ?warning("Session ~p supervisor already exists", [SessId]),
            {ok, SessId};
        Error ->
            ?error("Session ~p start error: ~p", [SessId, Error]),
            session:delete_doc(SessId),
            Error
    end.

%% @private
-spec restart_session(session:id(), session:type()) -> {ok, session:id()} | error().
restart_session(SessId, SessType) ->
    case supervisor:start_child(?SESSION_MANAGER_WORKER_SUP, [SessId, SessType]) of
        {ok, undefined} ->
            {error, already_exists};
        {ok, _} ->
            {ok, SessId};
        {error, already_present} ->
            ?warning("Session ~p restart: supervisor already exists", [SessId]),
            {ok, SessId};
        {error, {already_started, _}} ->
            ?warning("Session ~p restart: supervisor already exists", [SessId]),
            {ok, SessId};
        Error ->
            ?error("Session ~p restart error: ~p", [SessId, Error]),
            Error
    end.

%% @private
-spec is_alive(pid()) -> boolean().
is_alive(Pid) ->
    try rpc:pinfo(Pid, [status]) of
        [{status, _}] -> true;
        _ -> false
    catch _:_ ->
        false
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
