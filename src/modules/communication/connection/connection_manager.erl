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
%%% such connections, monitors them and restarts if they fail.
%%% Outgoing provider sessions should be created immediately after discovering
%%% new peer (provider supporting the same space as this provider) and
%%% terminated when when there is no more cooperation point left
%%% (peer unsupports spaces that we support).
%%% @end
%%%-------------------------------------------------------------------
-module(connection_manager).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("timeouts.hrl").
-include("modules/communication/connection.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

% Definitions of renewal intervals for provider connections.
-define(INITIAL_RENEWAL_INTERVAL, timer:seconds(2)).
-define(RENEWAL_INTERVAL_INCREASE_RATE, 2).
-define(MAX_RENEWAL_INTERVAL, timer:minutes(15)).

-define(RENEW_CONNECTIONS_REQ, renew_connections).

-record(state, {
    session_id :: session:id(),
    peer_id :: od_provider:id(),
    connections = #{} :: #{pid() => binary()},

    renewal_timer = undefined :: undefined | reference(),
    renewal_interval = ?INITIAL_RENEWAL_INTERVAL :: pos_integer()
}).

-type state() :: #state{}.
-type error() :: {error, Reason :: term()}.


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts the connection_manager server for specified session.
%% @end
%%--------------------------------------------------------------------
-spec start_link(session:id()) -> {ok, pid()} | ignore | error().
start_link(SessId) ->
    gen_server:start_link(?MODULE, [SessId], []).


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
    schedule_keepalive_msg(),

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
handle_info(?RENEW_CONNECTIONS_REQ, State) ->
    {noreply, renew_connections(State#state{renewal_timer = undefined})};
handle_info({'EXIT', ConnPid, _Reason}, #state{connections = Cons} = State) ->
    {noreply, renew_connections(State#state{
        connections = maps:remove(ConnPid, Cons)
    })};
handle_info(keepalive, #state{session_id = SessionId} = State) ->
    case session_connections:list(SessionId) of
        {ok, Cons} ->
            lists:foreach(fun(Conn) -> 
                connection:send_keepalive(Conn) 
            end, Cons);
        Error ->
            ?error("Connection manager for session ~p failed to send "
                   "keepalives due to: ~p", [
                SessionId, Error
            ])
    end,
    schedule_keepalive_msg(),
    {noreply, State};
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
terminate(_Reason, #state{connections = Cons}) ->
    lists:foreach(fun(Conn) ->
        connection:close(Conn)
    end, maps:keys(Cons)).


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
%% Connects to peer nodes with witch he is not yet connected if next
%% renewal is not scheduled. Otherwise does nothing.
%% @end
%%--------------------------------------------------------------------
-spec renew_connections(state()) -> state().
renew_connections(#state{renewal_timer = undefined} = State) ->
    try
        renew_connections_insecure(State)
    catch
        Type:Reason ->
            ?warning("Failed to renew connections to peer provider(~p) "
                     "due to ~p:~p. Next retry not sooner than ~p s.", [
                State#state.peer_id, Type, Reason,
                State#state.renewal_interval / 1000
            ]),
            schedule_next_renewal(State)
    end;
renew_connections(State) ->
    State.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Verifies peer identity and compatibility. Then, if everything is alright,
%% attempts to connects to those peer nodes with which he is not yet connected.
%% In case of errors while connecting to nodes schedules next renewal and if
%% no such errors occurred resets renewal interval.
%% @end
%%--------------------------------------------------------------------
-spec renew_connections_insecure(state()) -> state().
renew_connections_insecure(#state{
    session_id = SessionId,
    peer_id = ProviderId,
    connections = Cons
} = State0) ->
    case provider_logic:verify_provider_identity(ProviderId) of
        ok -> ok;
        Error1 -> throw({cannot_verify_peer_op_identity, Error1})
    end,

    {ok, Domain} = provider_logic:get_domain(ProviderId),
    provider_logic:assert_provider_compatibility(ProviderId, Domain, Domain),

    Port = https_listener:port(),
    {ok, [_ | _] = Hosts} = provider_logic:get_nodes(ProviderId),

    State1 = lists:foldl(fun(Host, #state{connections = AccCons} = AccState) ->
        case connection:start_link(ProviderId, SessionId, Domain, Host, Port) of
            {ok, Pid} ->
                AccState#state{connections = AccCons#{Pid => Host}};
            Error2 ->
                ?warning("Failed to connect to host ~p of provider ~p "
                         "due to ~p. ", [Host, ProviderId, Error2]),
                schedule_next_renewal(AccState)
        end
    end, State0, Hosts -- maps:values(Cons)),

    case State1#state.renewal_timer of
        undefined ->
            State1#state{renewal_interval = ?INITIAL_RENEWAL_INTERVAL};
        _ ->
            State1
    end.


%% @private
-spec schedule_next_renewal(state()) -> state().
schedule_next_renewal(#state{renewal_timer = undefined} = State) ->
    Interval = State#state.renewal_interval,
    TimerRef = erlang:send_after(Interval, self(), ?RENEW_CONNECTIONS_REQ),
    NextRenewalInterval = min(
        Interval * ?RENEWAL_INTERVAL_INCREASE_RATE,
        ?MAX_RENEWAL_INTERVAL
    ),
    State#state{
        renewal_timer = TimerRef,
        renewal_interval = NextRenewalInterval
    };
schedule_next_renewal(State) ->
    State.


%% @private
-spec schedule_keepalive_msg() -> TimerRef :: reference().
schedule_keepalive_msg() ->
    erlang:send_after(?KEEPALIVE_TIMEOUT, self(), keepalive).
