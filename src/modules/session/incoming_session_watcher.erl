%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible for removal
%%% of incoming session (gui/rest/fuse/incoming provider) that has been
%%% inactive longer that allowed grace period.
%%% @end
%%%-------------------------------------------------------------------
-module(incoming_session_watcher).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2, update_credentials/3]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-record(state, {
    session_id :: session:id(),
    session_type :: session:type(),
    session_grace_period :: undefined | session:grace_period(),
    identity :: aai:subject(),
    % Possible auth values:
    % auth_manager:token_credentials() -> for user sessions (gui, rest, fuse).
    %                                     It needs to be periodically verified
    %                                     whether it's still valid (it could
    %                                     be revoked)
    % undefined  -> for provider_incoming sessions. No periodic peer
    %               verification is needed.
    credentials :: undefined | auth_manager:credentials(),
    validity_checkup_timer :: undefined | reference()
}).
-type state() :: #state{}.


-define(REMOVE_SESSION, remove_session).
-define(CHECK_SESSION_ACTIVITY, check_session_activity).
-define(CHECK_SESSION_VALIDITY, check_session_validity).
-define(UPDATE_CLIENT_TOKENS_REQ(__AccessToken, __ConsumerToken),
    {update_credentials, __AccessToken, __ConsumerToken}
).

-define(SESSION_REMOVAL_RETRY_DELAY, 15).   % in seconds
-define(SESSION_VALIDITY_CHECK_INTERVAL, application:get_env(
    ?APP_NAME, session_validity_check_interval_seconds, 15
)).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts the session watcher.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SessId :: session:id(), SessType :: session:type()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SessId, SessType) ->
    gen_server2:start_link(?MODULE, [SessId, SessType], []).


-spec update_credentials(pid() | session:id(), auth_manager:access_token(),
    auth_manager:consumer_token()) -> ok.
update_credentials(SessionWatcher, AccessToken, ConsumerToken) when is_pid(SessionWatcher) ->
    gen_server2:call(
        SessionWatcher,
        ?UPDATE_CLIENT_TOKENS_REQ(AccessToken, ConsumerToken)
    );
update_credentials(SessionId, AccessToken, ConsumerToken) ->
    case session:get(SessionId) of
        {ok, #document{value = #session{watcher = SessionWatcher}}} ->
            update_credentials(SessionWatcher, AccessToken, ConsumerToken);
        _ ->
            ok
    end.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the session watcher.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> {ok, state()}.
init([SessionId, SessionType]) ->
    process_flag(trap_exit, true),

    {ok, #document{
        value = #session{
            identity = Identity,
            credentials = Credentials
        }
    }} = mark_session_as_active(SessionId),

    {ok, #state{
        session_id = SessionId,
        session_type = SessionType,
        identity = Identity,
        credentials = Credentials,
        session_grace_period = register_session_activity_checkup_if_not_offline_session(
            SessionType
        ),
        validity_checkup_timer = register_auth_validity_checkup_if_user_session(
            SessionType, Credentials
        )
    }}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate}.
handle_call(?UPDATE_CLIENT_TOKENS_REQ(AccessToken, ConsumerToken), _From, #state{
    session_id = SessionId,
    session_type = SessionType,
    identity = Identity,
    credentials = OldTokenCredentials,
    validity_checkup_timer = OldTimer
} = State) when
    SessionType == fuse;
    SessionType == offline
->
    cancel_auth_validity_checkup(OldTimer),

    NewTokenCredentials = auth_manager:update_client_tokens(
        OldTokenCredentials, AccessToken, ConsumerToken
    ),
    case check_auth_validity(NewTokenCredentials, Identity) of
        {true, NewTokenTTL} ->
            {ok, TokenCaveats} = auth_manager:get_caveats(NewTokenCredentials),
            {ok, DataConstraints} = data_constraints:get(TokenCaveats),
            {ok, _} = session:update(SessionId, fun(Session) ->
                {ok, Session#session{
                    credentials = NewTokenCredentials,
                    data_constraints = DataConstraints
                }}
            end),
            NewState = State#state{
                credentials = NewTokenCredentials,
                validity_checkup_timer = schedule_auth_validity_checkup(NewTokenTTL)
            },
            {reply, ok, NewState, hibernate};
        false ->
            mark_session_as_inactive(SessionId),
            schedule_session_removal(0),
            {reply, ok, State}
    end;
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, ok, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate}.
handle_info(?REMOVE_SESSION, #state{session_id = SessionId} = State) ->
    async_terminate_session(SessionId),
    schedule_session_removal(?SESSION_REMOVAL_RETRY_DELAY),
    {noreply, State, hibernate};

handle_info(?CHECK_SESSION_ACTIVITY, #state{session_type = offline} = State) ->
    {noreply, State, hibernate};
handle_info(?CHECK_SESSION_ACTIVITY, #state{
    session_id = SessionId,
    session_grace_period = GracePeriod
} = State) ->
    case is_session_inactive(SessionId, GracePeriod) of
        true ->
            schedule_session_removal(0),
            {noreply, State};
        {false, RemainingTime} when is_integer(RemainingTime) ->
            schedule_session_activity_checkup(RemainingTime),
            {noreply, State, hibernate};
        {false, {error, Reason}} ->
            ?error("Checking session ~p activity failed with error: ~p", [
                SessionId, Reason
            ]),
            async_terminate_session(SessionId),
            {noreply, State, hibernate}
    end;

handle_info(?CHECK_SESSION_VALIDITY, #state{session_type = provider_incoming} = State) ->
    {noreply, State, hibernate};
handle_info(?CHECK_SESSION_VALIDITY, #state{
    session_id = SessionId,
    session_type = SessionType,
    identity = Identity,
    credentials = TokenCredentials0,
    validity_checkup_timer = OldTimer
} = State) ->
    cancel_auth_validity_checkup(OldTimer),

    TokenCredentials1 = ensure_credentials_up_to_date_if_offline_session(
        SessionType, TokenCredentials0
    ),
    case check_auth_validity(TokenCredentials1, Identity) of
        {true, TokenTTL} ->
            NewState = State#state{
                validity_checkup_timer = schedule_auth_validity_checkup(TokenTTL)
            },
            {noreply, NewState, hibernate};
        false ->
            mark_session_as_inactive(SessionId),
            schedule_session_removal(0),
            {noreply, State}
    end;

handle_info({'EXIT', _, shutdown}, #state{session_id = SessionId} = State) ->
    async_terminate_session(SessionId),
    {noreply, State, hibernate};

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
    State :: state()) -> term().
terminate(Reason, #state{session_id = SessId} = State) ->
    ?log_terminate(Reason, State),
    session_manager:clean_terminated_session(SessId).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) -> {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec mark_session_as_active(session:id()) -> {ok, session:doc()} | {error, term()}.
mark_session_as_active(SessionId) ->
    Self = self(),

    session:update(SessionId, fun(#session{} = SessionRec) ->
        {ok, SessionRec#session{status = active, watcher = Self}}
    end).


%% @private
-spec register_session_activity_checkup_if_not_offline_session(session:type()) ->
    undefined | session:grace_period().
register_session_activity_checkup_if_not_offline_session(offline) ->
    undefined;
register_session_activity_checkup_if_not_offline_session(SessionType) ->
    GracePeriod = get_session_grace_period(SessionType),
    schedule_session_activity_checkup(GracePeriod),
    GracePeriod.


%% @private
-spec get_session_grace_period(session:type()) -> session:grace_period().
get_session_grace_period(gui) ->
    op_worker:get_env(gui_session_grace_period_seconds);
get_session_grace_period(rest) ->
    op_worker:get_env(rest_session_grace_period_seconds);
get_session_grace_period(provider_incoming) ->
    op_worker:get_env(provider_session_grace_period_seconds);
get_session_grace_period(fuse) ->
    op_worker:get_env(fuse_session_grace_period_seconds).


%% @private
-spec schedule_session_activity_checkup(Delay :: time:seconds()) ->
    TimeRef :: reference().
schedule_session_activity_checkup(Delay) ->
    erlang:send_after(timer:seconds(Delay), self(), ?CHECK_SESSION_ACTIVITY).


%% @private
-spec is_session_inactive(session:id(), session:grace_period()) ->
    true | {false, session:grace_period() | {error, term()}}.
is_session_inactive(SessionId, GracePeriod) ->
    case session:get(SessionId) of
        {ok, #document{value = #session{status = inactive}}} ->
            true;
        {ok, #document{value = #session{connections = [_ | _]}}} ->
            {false, GracePeriod};
        {ok, #document{value = #session{status = active}}} ->
            mark_session_as_inactive_if_grace_period_has_passed(SessionId, GracePeriod);
        {error, _} = Error ->
            {false, Error}
    end.


%% @private
-spec mark_session_as_inactive(session:id()) -> ok.
mark_session_as_inactive(SessionId) ->
    {ok, _} = session:update(SessionId, fun(#session{} = Session) ->
        {ok, Session#session{status = inactive}}
    end),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether session inactivity period exceeds grace period. If
%% session grace period is exceeded, session is marked as 'inactive'
%% for later removal.
%% Returns false and remaining time to removal for session that does not
%% exceed grace period and true for inactive session that exceeded it.
%% @end
%%--------------------------------------------------------------------
-spec mark_session_as_inactive_if_grace_period_has_passed(session:id(), session:grace_period()) ->
    true | {false, RemainingTime :: time:seconds()}.
mark_session_as_inactive_if_grace_period_has_passed(SessionId, GracePeriod) ->
    Diff = fun
        (#session{status = active, accessed = LastAccessTimestamp} = Sess) ->
            Now = global_clock:timestamp_seconds(),
            case Now >= LastAccessTimestamp of
                true ->
                    InactivityPeriod = Now - LastAccessTimestamp,
                    case InactivityPeriod >= GracePeriod of
                        true ->
                            {ok, Sess#session{status = inactive}};
                        false ->
                            {error, {grace_period_not_exceeded, GracePeriod - InactivityPeriod}}
                    end;
                false ->
                    % backward time warp has happened and it is impossible to tell if grace period
                    % has passed or not. To not let session unnecessarily exist until global time
                    % catches to previous value, set `accessed` field to Now and check once again
                    % later.
                    {ok, Sess#session{accessed = Now}}
            end;
        (#session{} = Sess) ->
            {ok, Sess#session{status = inactive}}
    end,
    case session:update(SessionId, Diff) of
        {ok, #document{value = #session{status = inactive}}} ->
            true;
        {ok, _} ->
            {false, GracePeriod};
        {error, {grace_period_not_exceeded, RemainingTime}} ->
            {false, RemainingTime};
        {error, _} ->
            true
    end.


%% @private
-spec register_auth_validity_checkup_if_user_session(
    session:type(),
    undefined | auth_manager:credentials()
) ->
    TimeRef :: reference().
register_auth_validity_checkup_if_user_session(provider_incoming, undefined) ->
    undefined;
register_auth_validity_checkup_if_user_session(_UserSession, TokenCredentials) ->
    AccessTokenBin = auth_manager:get_access_token(TokenCredentials),
    {ok, AccessToken} = tokens:deserialize(AccessTokenBin),
    TokenTTL = caveats:infer_ttl(tokens:get_caveats(AccessToken)),

    schedule_auth_validity_checkup(TokenTTL).


%% @private
-spec schedule_auth_validity_checkup(TokenTTL :: undefined | time:seconds()) ->
    TimeRef :: reference().
schedule_auth_validity_checkup(TokenTTL) ->
    Delay = case TokenTTL of
        undefined -> ?SESSION_VALIDITY_CHECK_INTERVAL;
        TokenTTL -> min(?SESSION_VALIDITY_CHECK_INTERVAL, TokenTTL)
    end,
    erlang:send_after(timer:seconds(Delay), self(), ?CHECK_SESSION_VALIDITY).


%% @private
-spec cancel_auth_validity_checkup(undefined | reference()) -> ok.
cancel_auth_validity_checkup(undefined) ->
    ok;
cancel_auth_validity_checkup(ValidityCheckupTimer) ->
    erlang:cancel_timer(ValidityCheckupTimer, [{async, true}, {info, false}]).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Ensures consumer token (this provider identity token) is up to date in case
%% of 'offline' session. In other cases the responsibility to keep credentials
%% up to date belongs to remote peer.
%% @end
%%--------------------------------------------------------------------
ensure_credentials_up_to_date_if_offline_session(offline, TokenCredentials) ->
    offline_access_manager:ensure_consumer_token_up_to_date(TokenCredentials);
ensure_credentials_up_to_date_if_offline_session(_SessionType, TokenCredentials) ->
    TokenCredentials.


%% @private
-spec check_auth_validity(auth_manager:credentials(), aai:subject()) ->
    {true, TokenTTL :: undefined | time:seconds()} | false.
check_auth_validity(TokenCredentials, Identity) ->
    case auth_manager:verify_credentials(TokenCredentials) of
        {ok, #auth{subject = Identity}, undefined} ->
            {true, undefined};
        {ok, #auth{subject = Identity}, TokenValidUntil} ->
            {true, max(0, TokenValidUntil - global_clock:timestamp_seconds())};
        {ok, #auth{subject = Subject}, _} ->
            ?warning("Token identity verification failure.~nExpected ~p.~nGot: ~p", [
                Identity, Subject
            ]),
            false;
        {error, Reason} ->
            ?warning("Token auth verification failure: ~p", [Reason]),
            false
    end.


%% @private
-spec schedule_session_removal(time:seconds()) -> TimeRef :: reference().
schedule_session_removal(Delay) ->
    erlang:send_after(timer:seconds(Delay), self(), ?REMOVE_SESSION).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Spawns process to terminate session. This can't be done by ?MODULE
%% as it is part of session supervision tree and would result in deadlock.
%% @end
%%--------------------------------------------------------------------
async_terminate_session(SessionId) ->
    spawn(fun() ->
        session_manager:terminate_session(SessionId)
    end).
