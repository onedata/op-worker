%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible for
%%% management of connections for outgoing provider session. It starts
%%% such connections, monitors them and restarts if they fail due to
%%% reasons other than timeout.
%%% Connection is always randomly selected for sending msg it is unlikely
%%% that for active session any of the connections will be idle for more
%%% than 24h (connection timeout). That is why when any connection dies out
%%% due to timeout entire session is terminated (session is inactive).
%%% Failed connection attempts are repeated after a backoff period that grows
%%% with every failure. Mentioned period is reset every time that any
%%% connection reports successful handshake.
%%% If backoff reaches ?MAX_RENEWAL_INTERVAL and manager still fails to
%%% connect to at least one peer host, then it is assumed that the peer is
%%% down/offline and the session is terminated.
%%% Next time any process tries to send a msg to the peer, it will first call
%%% `session_connections:ensure_connected` to create the session again.
%%% @end
%%%-------------------------------------------------------------------
-module(outgoing_connection_manager).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/onedata.hrl").

%% API
-export([
    start_link/1,
    report_successful_handshake/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

% Definitions of renewal intervals for provider connections.
-define(INITIAL_RENEWAL_INTERVAL, 2000).  % 2 seconds
-define(RENEWAL_INTERVAL_BACKOFF_RATE, 1.2).
-define(MAX_RENEWAL_INTERVAL, 180000).  % 3 minutes

% how often logs appear when waiting for peer provider connection
-define(CONNECTION_AWAIT_LOG_INTERVAL_SEC, 21600).  % 6 hours

-define(RENEW_CONNECTIONS_REQ, renew_connections).
-define(HANDSHAKE_SUCCEEDED(__PID), {handshake_succeeded, __PID}).


-record(state, {
    session_id :: session:id(),
    peer_id :: od_provider:id(),

    connecting = #{} :: #{pid() => Hostname :: binary()},
    connected = #{} :: #{pid() => Hostname :: binary()},

    renewal_timer = undefined :: undefined | reference(),
    renewal_interval = ?INITIAL_RENEWAL_INTERVAL :: pos_integer(),

    is_stopping = false :: boolean()
}).

-type state() :: #state{}.
-type error() :: {error, Reason :: term()}.


%%%===================================================================
%%% API
%%%===================================================================


-spec start_link(session:id()) -> {ok, pid()} | ignore | error().
start_link(SessId) ->
    gen_server:start_link(?MODULE, [SessId], []).


-spec report_successful_handshake(pid()) -> ok.
report_successful_handshake(ConnManager) ->
    gen_server:cast(ConnManager, ?HANDSHAKE_SUCCEEDED(self())).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> {ok, state()}.
init([SessionId]) ->
    process_flag(trap_exit, true),
    self() ! ?RENEW_CONNECTIONS_REQ,

    {ok, _} = session:update(SessionId, fun(Session = #session{}) ->
        {ok, Session#session{status = active}}
    end),

    {ok, #state{
        session_id = SessionId,
        peer_id = session_utils:session_id_to_provider_id(SessionId)
    }}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), NewState :: state()}.
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, {error, wrong_request}, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), state()) ->
    {noreply, NewState :: state()}.
handle_cast(?HANDSHAKE_SUCCEEDED(ConnPid), #state{
    connecting = Connecting,
    connected = Connected
} = State) ->
    NewState = case maps:take(ConnPid, Connecting) of
        {Host, LeftoverConnecting} ->
            State#state{
                connecting = LeftoverConnecting,
                connected = Connected#{ConnPid => Host},
                renewal_interval = ?INITIAL_RENEWAL_INTERVAL
            };
        error ->
            State
    end,
    {noreply, NewState};

handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), state()) ->
    {noreply, NewState :: state()}.
handle_info(_, #state{is_stopping = true} = State) ->
    {noreply, State};

handle_info(?RENEW_CONNECTIONS_REQ, State0) ->
    NewState = case renew_connections(State0#state{renewal_timer = undefined}) of
        {ok, State1} -> State1;
        {error, peer_offline} -> terminate_session(State0)
    end,
    {noreply, NewState};

handle_info({'EXIT', _ConnPid, timeout}, State) ->
    {noreply, terminate_session(State)};

handle_info({'EXIT', ConnPid, _Reason}, #state{
    connecting = Connecting,
    connected = Connected
} = State) ->
    case maps:take(ConnPid, Connecting) of
        {_Host, LeftoverConnecting} ->
            handle_failed_connection_attempt(State#state{
                connecting = LeftoverConnecting
            });
        error ->
            handle_connection_death(State#state{
                connected = maps:remove(ConnPid, Connected)
            })
    end;

handle_info(Info, State) ->
    ?log_bad_request(Info),
    {noreply, State}.


%% @private
-spec handle_failed_connection_attempt(state()) -> {noreply, state()}.
handle_failed_connection_attempt(State) ->
    NewState = case failed_connection_attempts_exceeded(State) of
        true -> terminate_session(State);
        false -> schedule_next_renewal(State)
    end,
    {noreply, NewState}.


%% @private
-spec handle_connection_death(state()) -> {noreply, state()}.
handle_connection_death(State0) ->
    NewState = case renew_connections(State0) of
        {ok, State1} -> State1;
        {error, peer_offline} -> terminate_session(State0)
    end,
    {noreply, NewState}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()), state()) ->
    term().
terminate(Reason, #state{session_id = SessionId} = State) ->
    ?log_terminate(Reason, State),
    session_manager:clean_terminated_session(SessionId).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, state(), Extra :: term()) ->
    {ok, NewState :: state()} | error().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Connects to peer nodes with which provider is not yet connected if next
%% renewal is not scheduled.
%% In case of errors while connecting schedules next renewal with increased
%% interval than before (not larger than ?MAX_RENEWAL_INTERVAL though).
%% If it is already another failed attempt and no connection is alive,
%% meaning that peer is probably down/offline, returns error.
%% @end
%%--------------------------------------------------------------------
-spec renew_connections(state()) -> {ok, state()} | {error, peer_offline}.
renew_connections(#state{
    peer_id = PeerId,
    session_id = SessionId,
    renewal_timer = undefined,
    renewal_interval = Interval
} = State) ->
    NewState = try
        renew_connections_insecure(State)
    catch Type:Reason ->
        ?debug("Failed to establish connection with provider ~ts due to ~p:~p.~n"
               "Next retry not sooner than ~B s.", [
            provider_logic:to_printable(PeerId), Type, Reason,
            Interval div 1000
        ]),
        utils:throttle({?MODULE, SessionId}, ?CONNECTION_AWAIT_LOG_INTERVAL_SEC, fun() ->
            log_error(State, Type, Reason)
        end),
        schedule_next_renewal(State)
    end,

    case failed_connection_attempts_exceeded(NewState) of
        true -> {error, peer_offline};
        false -> {ok, NewState}
    end;
renew_connections(State) ->
    {ok, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Verifies peer identity and compatibility. Then, if everything is alright,
%% attempts to connects to those peer nodes with which he is not yet connected.
%% In case of errors schedules next connections renewal.
%% @end
%%--------------------------------------------------------------------
-spec renew_connections_insecure(state()) -> state().
renew_connections_insecure(#state{
    session_id = SessionId,
    peer_id = ProviderId,
    connecting = Connecting,
    connected = Connected
} = State) ->
    case provider_logic:verify_provider_identity(ProviderId) of
        ok -> ok;
        Error1 -> throw({cannot_verify_peer_op_identity, Error1})
    end,
    {ok, Domain} = provider_logic:get_domain(ProviderId),
    provider_logic:assert_provider_compatibility(Domain),

    Port = https_listener:port(),
    {ok, [_ | _] = Hosts} = provider_logic:get_nodes(ProviderId),

    lists:foldl(fun(Host, #state{connecting = ConnsAcc} = AccState) ->
        case connection:start_link(ProviderId, SessionId, Domain, Host, Port) of
            {ok, Pid} ->
                AccState#state{connecting = ConnsAcc#{Pid => Host}};
            Error2 ->
                ?warning("Failed to connect to host ~p of provider ~ts due to ~p. ", [
                    Host, provider_logic:to_printable(ProviderId), Error2
                ]),
                schedule_next_renewal(AccState)
        end
    end, State, (Hosts -- maps:values(Connecting)) -- maps:values(Connected)).


%% @private
-spec schedule_next_renewal(state()) -> state().
schedule_next_renewal(#state{
    renewal_timer = undefined,
    renewal_interval = Interval
} = State) when Interval =< ?MAX_RENEWAL_INTERVAL ->
    TimerRef = erlang:send_after(Interval, self(), ?RENEW_CONNECTIONS_REQ),
    NextRenewalInterval = case Interval of
        ?MAX_RENEWAL_INTERVAL ->
            ?MAX_RENEWAL_INTERVAL + 1;
        _ ->
            min(round(Interval * ?RENEWAL_INTERVAL_BACKOFF_RATE), ?MAX_RENEWAL_INTERVAL)
    end,
    State#state{
        renewal_timer = TimerRef,
        renewal_interval = NextRenewalInterval
    };
schedule_next_renewal(State) ->
    State.


%% @private
-spec failed_connection_attempts_exceeded(state()) -> boolean().
failed_connection_attempts_exceeded(#state{
    connecting = Connecting,
    connected = Connected,
    renewal_timer = undefined,
    renewal_interval = Interval
}) when map_size(Connecting) == 0, map_size(Connected) == 0, Interval > ?MAX_RENEWAL_INTERVAL ->
    true;
failed_connection_attempts_exceeded(_) ->
    false.


%% @private
-spec terminate_session(state()) -> state().
terminate_session(#state{session_id = SessionId} = State) ->
    spawn(fun() ->
        session_manager:terminate_session(SessionId)
    end),
    State#state{is_stopping = true}.


%% @private
-spec log_error(state(), Type :: throw | error | exit, Reason :: term()) ->
    ok.
log_error(State, throw, {cannot_verify_peer_op_identity, Reason}) ->
    log_error(State, str_utils:format("peer identity cannot be verified due to ~w", [
        Reason
    ]));
log_error(State, throw, {cannot_check_peer_op_version, HTTPErrorCode}) ->
    log_error(State, str_utils:format("peer version cannot be determined (HTTP ~B)", [
        HTTPErrorCode
    ]));
log_error(State, throw, {incompatible_peer_op_version, PeerOpVersion, PeerCompOpVersions}) ->
    Version = op_worker:get_release_version(),
    {ok, CompatibleOpVersions} = compatibility:get_compatible_versions(
        ?ONEPROVIDER, Version, ?ONEPROVIDER
    ),
    log_error(State, str_utils:format(
        "peer is of incompatible version.~n"
        "Local version: ~s, supports providers: ~s~n"
        "Remote version: ~s, supports providers: ~s", [
            Version, str_utils:join_binary(CompatibleOpVersions, <<", ">>),
            PeerOpVersion, str_utils:join_binary(PeerCompOpVersions, <<", ">>)
        ]
    ));
log_error(State, Type, Reason) ->
    log_error(State, str_utils:format("~w:~p", [Type, Reason])).


%% @private
-spec log_error(state(), ReasonString :: string()) -> ok.
log_error(#state{peer_id = PeerId}, ReasonString) ->
    ?warning(
        "Failed to renew connections to provider ~ts, retrying in the background...~n"
        "Last error was: ~ts.", [
            provider_logic:to_printable(PeerId),
            ReasonString
        ]
    ).
