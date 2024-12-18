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
    reuse_or_create_fuse_session/3, reuse_or_create_fuse_session/4,
    reuse_or_create_rest_session/2,
    reuse_or_create_incoming_provider_session/1,
    reuse_or_create_outgoing_provider_session/2,
    reuse_or_create_proxied_session/5,
    reuse_or_create_gui_session/2,
    reuse_or_create_offline_session/3,
    create_root_session/0, create_guest_session/0
]).
-export([
    restart_dead_sessions/0,
    restart_session_if_dead/1,
    terminate_session/1,
    clean_terminated_session/1,
    restore_session_on_slave_node/3
]).

-type error() :: {error, Reason :: term()}.

% Macros used when process is waiting for other process to init session
-define(SESSION_INITIALIZATION_CHECK_PERIOD_BASE, 100).
-define(SESSION_INITIALIZATION_RETRIES, op_worker:get_env(session_initialization_retries, 8)).

% Log not more often than once every 5 min
-define(THROTTLE_LOG(SessId, Log), utils:throttle({?MODULE, ?FUNCTION_NAME, SessId}, 300, fun() -> Log end)).

%%%===================================================================
%%% API
%%%===================================================================


-spec reuse_or_create_fuse_session(Nonce :: binary(), aai:subject(),
    auth_manager:credentials()) -> {ok, session:id()} | error().
reuse_or_create_fuse_session(Nonce, Identity, Credentials) ->
    reuse_or_create_fuse_session(Nonce, Identity, normal, Credentials).


-spec reuse_or_create_fuse_session(Nonce :: binary(), aai:subject(), session:mode(),
    auth_manager:credentials()) -> {ok, session:id()} | error().
reuse_or_create_fuse_session(Nonce, Identity, SessMode, Credentials) ->
    SessId = datastore_key:new_from_digest([<<"fuse">>, Nonce]),
    reuse_or_create_session(SessId, fuse, SessMode, Identity, Credentials).


-spec reuse_or_create_incoming_provider_session(aai:subject()) ->
    {ok, session:id()} | error().
reuse_or_create_incoming_provider_session(?SUB(?ONEPROVIDER, ProviderId) = Identity) ->
    SessId = session_utils:get_provider_session_id(incoming, ProviderId),
    reuse_or_create_session(SessId, provider_incoming, Identity, undefined).


-spec reuse_or_create_outgoing_provider_session(session:id(),
    aai:subject()) -> {ok, session:id()} | error().
reuse_or_create_outgoing_provider_session(SessId, Identity) ->
    reuse_or_create_session(SessId, provider_outgoing, Identity, undefined).


-spec reuse_or_create_proxied_session(
    session:id(),
    ProxyVia :: oneprovider:id(),
    auth_manager:credentials(),
    session:type(),
    session:mode()
) ->
    {ok, session:id()} | error().
reuse_or_create_proxied_session(SessId, ProxyVia, Credentials, SessionType, SessMode) ->
    case auth_manager:verify_credentials(Credentials) of
        {ok, #auth{subject = ?SUB(user, _) = Identity}, _TokenValidUntil} ->
            reuse_or_create_session(
                SessId, SessionType, SessMode, Identity, Credentials, ProxyVia
            );
        Error ->
            Error
    end.


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


-spec reuse_or_create_gui_session(aai:subject(), auth_manager:credentials()) ->
    {ok, session:id()} | error().
reuse_or_create_gui_session(Identity, Credentials) ->
    SessId = datastore_key:new_from_digest([<<"gui">>, Credentials]),
    reuse_or_create_session(SessId, gui, Identity, Credentials).


-spec reuse_or_create_offline_session(session:id(), aai:subject(), auth_manager:credentials()) ->
    {ok, session:id()} | error().
reuse_or_create_offline_session(SessId, Identity, Credentials) ->
    reuse_or_create_session(SessId, offline, Identity, Credentials).


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
        restart_session_if_dead(SessId)
    end, AllSessions).


-spec restart_session_if_dead(SessId :: session:id()) -> ok.
restart_session_if_dead(SessId) ->
    case session:update_doc_and_time(SessId, fun try_to_clear_dead_connections/1) of
        {ok, #document{key = SessId}} ->
            ok;
        {error, update_not_needed} ->
            ok;
        {error, {supervisor_dead, SessType}} ->
            restart_session(SessId, SessType),
            ok;
        {error, internal_call} ->
            ?THROTTLE_LOG(SessId, ?warning("Internal call cleaning dead connections for session ~tp", [SessId])),
            % Fix session document async as it cannot be done from the inside of tp process
            spawn(fun() ->
                restart_session_if_dead(SessId)
            end),
            ok;
        {error, Reason} ->
            ?error("Unexpected error cleaning dead connections for session ~tp: ~tp", [SessId, Reason]),
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks if session supervisor is alive and if not clears entries in
%% session doc and sets new one.
%% @end
%%--------------------------------------------------------------------
-spec restore_session_on_slave_node(session:id(), pid(), node()) ->
    {ok, session:doc()} | {error, supervisor_alive}.
restore_session_on_slave_node(SessId, NewSup, NewSupNode) ->
    session:update(SessId, fun(#session{supervisor = Sup, connections = Cons} = Sess) ->
        case is_pid_alive(Sup) of
            true ->
                {error, supervisor_alive};
            false ->
                % All session processes but connection ones are on the same
                % node as supervisor. If supervisor is dead so they are.
                {ok, Sess#session{
                    node = NewSupNode,
                    supervisor = NewSup,
                    event_manager = undefined,
                    watcher = undefined,
                    sequencer_manager = undefined,
                    async_request_manager = undefined,
                    connections = lists:filter(fun is_pid_alive/1, Cons)
                }}
        end
    end).


-spec terminate_session(session:id()) -> ok | error().
terminate_session(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{supervisor = Sup, event_manager = EventManager, node = Node}}} ->
            try
                event_manager:handle_session_termination(EventManager),
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


-spec clean_terminated_session(session:id()) -> ok | error().
clean_terminated_session(SessId) ->
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


%% @private
-spec reuse_or_create_session(
    session:id(),
    session:type(),
    aai:subject(),
    undefined | auth_manager:credentials()
) ->
    {ok, SessId} | error() when SessId :: session:id().
reuse_or_create_session(SessId, SessType, Identity, Credentials) ->
    reuse_or_create_session(SessId, SessType, normal, Identity, Credentials, undefined).


%% @private
-spec reuse_or_create_session(
    session:id(),
    session:type(),
    session:mode(),
    aai:subject(),
    undefined | auth_manager:credentials()
) ->
    {ok, SessId} | error() when SessId :: session:id().
reuse_or_create_session(SessId, SessType, SessMode, Identity, Credentials) ->
    reuse_or_create_session(SessId, SessType, SessMode, Identity, Credentials, undefined).


%% @private
-spec reuse_or_create_session(
    session:id(),
    session:type(),
    session:mode(),
    aai:subject(),
    undefined | auth_manager:credentials(),
    ProxyVia :: oneprovider:id() | undefined
) ->
    {ok, SessId} | error() when SessId :: session:id().
reuse_or_create_session(SessId, SessType, SessMode, Identity, Credentials, ProxyVia) ->
    case get_caveats(Credentials) of
        {ok, Caveats} ->
            case data_constraints:get(Caveats) of
                {ok, DataConstraints} ->
                    case {SessMode, data_constraints:has_no_constraints(DataConstraints)} of
                        {open_handle, false} ->
                            % Data constraints are not allowed in 'open_handle' mode
                            {error, invalid_token};
                        _ ->
                            reuse_or_create_session(
                                SessId, SessType, SessMode, Identity,
                                Credentials, DataConstraints, ProxyVia
                            )
                    end;
                {error, invalid_constraints} ->
                    {error, invalid_token}
            end;
        {error, _} = Error ->
            Error
    end.


%% @private
-spec get_caveats(undefined | auth_manager:credentials()) ->
    {ok, [caveats:caveat()]} | errors:error().
get_caveats(undefined) ->
    % Providers sessions are not constrained by any caveats
    {ok, []};
get_caveats(Credentials) ->
    auth_manager:get_caveats(Credentials).


%%--------------------------------------------------------------------
%% @private
-spec reuse_or_create_session(
    session:id(),
    session:type(),
    session:mode(),
    aai:subject(),
    undefined | auth_manager:credentials(),
    DataConstraints :: data_constraints:constraints(),
    ProxyVia :: undefined | oneprovider:id()
) ->
    {ok, SessId} | error() when SessId :: session:id().
reuse_or_create_session(SessId, SessType, SessMode, Identity, Credentials, DataConstraints, ProxyVia) ->
    reuse_or_create_session(
        SessId, SessType, SessMode, Identity, Credentials, DataConstraints, ProxyVia,
        ?SESSION_INITIALIZATION_CHECK_PERIOD_BASE, 0
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session or reuses it if session exists. Session creation results in
%% starting of session's supervisor together with its children and saving session document to datastore
%% (document is created during initialization of supervisor).
%% If session is corrupted after failure of node that hosted session's supervisor,
%% this function recreates supervisor together with its children.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_session(
    session:id(),
    session:type(),
    session:mode(),
    aai:subject(),
    undefined | auth_manager:credentials(),
    DataConstraints :: data_constraints:constraints(),
    ProxyVia :: undefined | oneprovider:id(),
    ErrorSleep:: non_neg_integer(),
    Retries :: non_neg_integer()
) ->
    {ok, SessId} | error() when SessId :: session:id().
reuse_or_create_session(
    SessId, SessType, SessMode, Identity, Credentials, DataConstraints, ProxyVia, ErrorSleep, RetryNum
) ->
    Sess = #session{
        type = SessType,
        mode = SessMode,
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
                    case try_to_clear_dead_connections(ExistingSess) of
                        {error, update_not_needed} -> {ok, ExistingSess};
                        Other -> Other
                    end;
                _ ->
                    {error, {invalid_identity, Identity}}
            end
    end,
    case session:update_doc_and_time(SessId, Diff) of
        {ok, #document{key = SessId, value = UpdatedSession}} ->
            update_credentials_if_needed(SessType, Credentials, UpdatedSession),
            renew_connection_if_needed(SessType, UpdatedSession),
            {ok, SessId};
        {error, not_found} = Error ->
            case start_session(#document{key = SessId, value = Sess}) of
                {error, already_exists} ->
                    maybe_retry_session_init(
                        SessId, SessType, SessMode, Identity, Credentials,
                        DataConstraints, ProxyVia, ErrorSleep, RetryNum, Error
                    );
                Other ->
                    Other
            end;
        {error, {supervisor_dead, SessType}} ->
            case restart_session(SessId, SessType) of
                {error, already_exists} = Error ->
                    maybe_retry_session_init(
                        SessId, SessType, SessMode, Identity, Credentials,
                        DataConstraints, ProxyVia, ErrorSleep, RetryNum, Error
                    );
                Other ->
                    Other
            end;
        {error, initializing} = Error ->
            maybe_retry_session_init(
                SessId, SessType, SessMode, Identity, Credentials,
                DataConstraints, ProxyVia, ErrorSleep, RetryNum, Error
            );
        {error, Reason} ->
            {error, Reason}
    end.


%% @private
-spec update_credentials_if_needed(
    session:type(),
    auth_manager:token_credentials(),
    session:record()
) ->
    ok.
update_credentials_if_needed(_SessType, Credentials, #session{credentials = Credentials}) ->
    ok;
update_credentials_if_needed(offline, NewCredentials, Session) ->
    update_credentials(NewCredentials, Session);
update_credentials_if_needed(fuse, NewCredentials, #session{proxy_via = <<_/binary>>} = Session) ->
    update_credentials(NewCredentials, Session);
update_credentials_if_needed(_, _, _) ->
    % neither offline nor a proxy fuse session
    ok.


%% @private
-spec update_credentials(auth_manager:token_credentials(), session:record()) ->
    ok.
update_credentials(NewCredentials, #session{watcher = SessionWatcher}) ->
    incoming_session_watcher:update_credentials(
        SessionWatcher,
        auth_manager:get_access_token(NewCredentials),
        auth_manager:get_consumer_token(NewCredentials)
    ).


%% @private
-spec renew_connection_if_needed(session:type(), session:record()) -> ok.
renew_connection_if_needed(provider_outgoing, #session{watcher = ConnManagerPid}) ->
    outgoing_connection_manager:renew_connection(ConnManagerPid);
renew_connection_if_needed(_, _) ->
    ok.


%% @private
-spec maybe_retry_session_init(
    session:id(),
    session:type(),
    session:mode(),
    aai:subject(),
    undefined | auth_manager:credentials(),
    DataConstraints :: data_constraints:constraints(),
    ProxyVia :: undefined | oneprovider:id(),
    ErrorSleep:: non_neg_integer(),
    Retries :: non_neg_integer(),
    Error :: {error, term()}
) ->
    {ok, SessId} | error() when SessId :: session:id().
maybe_retry_session_init(
    SessId, SessType, SessMode, Identity, Credentials, DataConstraints, ProxyVia, ErrorSleep, RetryNum, Error
) ->
    MaxRetries = ?SESSION_INITIALIZATION_RETRIES,
    case RetryNum of
        MaxRetries ->
            % Process that is initializing session probably hangs - return error
            Error;
        _ ->
            % Other process is initializing session - wait
            timer:sleep(ErrorSleep),
            ?debug("Waiting for session ~tp init", [SessId]),
            reuse_or_create_session(
                SessId, SessType, SessMode, Identity, Credentials,
                DataConstraints, ProxyVia, ErrorSleep * 2, RetryNum + 1
            )
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if session connections are still alive and if not clears entries in
%% session doc.
%% @end
%%--------------------------------------------------------------------
-spec try_to_clear_dead_connections(session:record()) -> {ok, session:record()} | {error, update_not_needed}.
try_to_clear_dead_connections(#session{supervisor = Sup, connections = Cons, type = SessType} = Sess) ->
    case is_pid_alive(Sup) of
        true ->
            case lists:partition(fun is_pid_alive/1, Cons) of
                {_, []} ->
                    {error, update_not_needed};
                {AliveCons, _DeadCons} ->
                    {ok, Sess#session{connections = AliveCons}}
            end;
        false ->
            {error, {supervisor_dead, SessType}}
    end.


%% @private
-spec start_session(session:doc()) -> {ok, session:id()} | error().
start_session(#document{key = SessId, value = #session{type = SessType}} = Doc) ->
    case supervisor:start_child(?SESSION_MANAGER_WORKER_SUP, [Doc, SessType]) of
        {ok, undefined} ->
            {error, already_exists};
        {ok, _} ->
            {ok, SessId};
        Error ->
            ?error("Session ~tp start error: ~tp", [SessId, Error]),
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
        Error ->
            ?error("Session ~tp restart error: ~tp", [SessId, Error]),
            Error
    end.


%% @private
-spec is_pid_alive(pid()) -> boolean().
is_pid_alive(Pid) ->
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
