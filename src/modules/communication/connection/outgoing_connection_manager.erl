%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
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

-include("timeouts.hrl").
-include("modules/communication/connection.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").
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
-define(SUCCESSFUL_HANDSHAKE, handshake_succeeded).


-record(state, {
    session_id :: session:id(),
    peer_id :: od_provider:id(),
    connections = #{} :: #{pid() => Hostname :: binary()},

    renewal_timer = undefined :: undefined | reference(),
    renewal_interval = ?INITIAL_RENEWAL_INTERVAL :: pos_integer(),

    is_stopping = false :: boolean()
}).

-type state() :: #state{}.
-type error() :: {error, Reason :: term()}.


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts the outgoing connection_manager server for specified session.
%% @end
%%--------------------------------------------------------------------
-spec start_link(session:id()) -> {ok, pid()} | ignore | error().
start_link(SessId) ->
    gen_server:start_link(?MODULE, [SessId], []).


%%--------------------------------------------------------------------
%% @doc
%% Informs connection manager about successful handshake.
%% @end
%%--------------------------------------------------------------------
-spec report_successful_handshake(pid()) -> ok.
report_successful_handshake(ConnManager) ->
    gen_server:cast(ConnManager, ?SUCCESSFUL_HANDSHAKE).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, state()} | {ok, state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
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
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
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
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast(?SUCCESSFUL_HANDSHAKE, State) ->
    {noreply, State#state{renewal_interval = ?INITIAL_RENEWAL_INTERVAL}};
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
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info(_, #state{is_stopping = true} = State) ->
    {noreply, State};
handle_info(?RENEW_CONNECTIONS_REQ, #state{session_id = SessionId} = State) ->
    case renew_connections(State#state{renewal_timer = undefined}) of
        {ok, NewState} ->
            {noreply, NewState};
        {error, peer_offline} ->
            terminate_session(SessionId, State)
    end;

handle_info({'EXIT', _ConnPid, timeout}, #state{session_id = SessionId} = State) ->
    terminate_session(SessionId, State);

handle_info({'EXIT', ConnPid, connection_attempt_failed}, #state{
    connections = AllConnections,
    renewal_interval = Interval,
    session_id = SessionId
} = State) ->
    WorkingConnections = maps:remove(ConnPid, AllConnections),
    AfterLastConnectionRetry = Interval > ?MAX_RENEWAL_INTERVAL,

    case {map_size(WorkingConnections), AfterLastConnectionRetry} of
        {0, true} ->
            terminate_session(SessionId, State);
        _ ->
            NewState = State#state{connections = WorkingConnections},
            {noreply, schedule_next_renewal(NewState)}
    end;

handle_info({'EXIT', ConnPid, _Reason}, #state{connections = Cons, session_id = SessionId} = State0) ->
    State1 = State0#state{connections = maps:remove(ConnPid, Cons)},
    case renew_connections(State1) of
        {ok, State2} ->
            {noreply, State2};
        {error, peer_offline} ->
            terminate_session(SessionId, State1)
    end;

handle_info(Info, State) ->
    ?log_bad_request(Info),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    state()) -> term().
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

%% @private
-spec terminate_session(session:id(), state()) -> {noreply, state()}.
terminate_session(SessionId, State) ->
    spawn(fun() ->
        session_manager:terminate_session(SessionId)
    end),
    {noreply, State#state{is_stopping = true}}.

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

    case NewState of
        #state{renewal_timer = undefined} ->
            {ok, NewState};
        #state{connections = Cons} when map_size(Cons) > 0 ->
            {ok, NewState};
        _ when not (Interval > ?MAX_RENEWAL_INTERVAL) ->   % not last retry
            {ok, NewState};
        _ ->
            {error, peer_offline}
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
    connections = Cons
} = State) ->
    case provider_logic:verify_provider_identity(ProviderId) of
        ok -> ok;
        Error1 -> throw({cannot_verify_peer_op_identity, Error1})
    end,
    {ok, Domain} = provider_logic:get_domain(ProviderId),
    provider_logic:assert_provider_compatibility(Domain),

    Port = https_listener:port(),
    {ok, [_ | _] = Hosts} = provider_logic:get_nodes(ProviderId),

    lists:foldl(fun(Host, #state{connections = AccCons} = AccState) ->
        case connection:start_link(ProviderId, SessionId, Domain, Host, Port) of
            {ok, Pid} ->
                AccState#state{connections = AccCons#{Pid => Host}};
            Error2 ->
                ?warning("Failed to connect to host ~p of provider ~ts "
                         "due to ~p. ", [
                    Host, provider_logic:to_printable(ProviderId), Error2
                ]),
                schedule_next_renewal(AccState)
        end
    end, State, Hosts -- maps:values(Cons)).


%% @private
-spec schedule_next_renewal(state()) -> state().
schedule_next_renewal(#state{
    renewal_timer = undefined,
    renewal_interval = Interval
} = State) ->
    TimerRef = erlang:send_after(Interval, self(), ?RENEW_CONNECTIONS_REQ),
    NextRenewalInterval = case Interval of
        ?MAX_RENEWAL_INTERVAL ->
            round(Interval * ?RENEWAL_INTERVAL_BACKOFF_RATE);
        _ ->
            min(round(Interval * ?RENEWAL_INTERVAL_BACKOFF_RATE),
                ?MAX_RENEWAL_INTERVAL)
    end,
    State#state{
        renewal_timer = TimerRef,
        renewal_interval = NextRenewalInterval
    };
schedule_next_renewal(State) ->
    State.


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
    Resolver = compatibility:build_resolver(consistent_hashing:get_all_nodes(), oneprovider:trusted_ca_certs()),
    Version = op_worker:get_release_version(),
    {ok, CompatibleOpVersions} = compatibility:get_compatible_versions(
        Resolver, ?ONEPROVIDER, Version, ?ONEPROVIDER
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
