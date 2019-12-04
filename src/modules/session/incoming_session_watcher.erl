%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
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
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-record(state, {
    session_id :: session:id(),
    session_grace_period :: session:grace_period()
}).

-define(REMOVE_SESSION, remove_session).
-define(CHECK_SESSION_ACTIVITY, check_session_activity).

-define(SESSION_REMOVAL_RETRY_DELAY, 15).   % in seconds

-type state() :: #state{}.
-type seconds() :: non_neg_integer().


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


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the session watcher.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SessId, SessType]) ->
    process_flag(trap_exit, true),
    Self = self(),
    {ok, _} = session:update(SessId, fun(Session = #session{}) ->
        {ok, Session#session{
            status = active,
            watcher = Self
        }}
    end),
    GracePeriod = get_session_grace_period(SessType),
    schedule_session_activity_checkup(GracePeriod),
    {ok, #state{
        session_id = SessId,
        session_grace_period = GracePeriod
    }}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
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
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info(?REMOVE_SESSION, #state{session_id = SessionId} = State) ->
    spawn(fun() ->
        session_manager:remove_session(SessionId)
    end),
    schedule_session_removal(?SESSION_REMOVAL_RETRY_DELAY),
    {noreply, State, hibernate};

handle_info(?CHECK_SESSION_ACTIVITY, #state{
    session_id = SessionId,
    session_grace_period = GracePeriod
} = State) ->
    IsSessionInactive = case session:get(SessionId) of
        {ok, #document{value = #session{status = inactive}}} ->
            true;
        {ok, #document{value = #session{connections = [_ | _]}}} ->
            {false, GracePeriod};
        {ok, #document{value = #session{status = active}}} ->
            has_grace_period_passed(SessionId, GracePeriod);
        {error, _} = Error ->
            {false, Error}
    end,

    case IsSessionInactive of
        true ->
            schedule_session_removal(0),
            {noreply, State};
        {false, RemainingTime} when is_integer(RemainingTime) ->
            schedule_session_activity_checkup(RemainingTime),
            {noreply, State, hibernate};
        {false, {error, Reason}} ->
            {stop, Reason, State}
    end;

handle_info({'EXIT', _, shutdown}, State) ->
    {stop, shutdown, State};

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
    spawn(fun() ->
        session_manager:remove_session(SessId)
    end).


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
-spec get_session_grace_period(session:type()) -> session:grace_period().
get_session_grace_period(gui) ->
    {ok, Period} = application:get_env(?APP_NAME, gui_session_grace_period_seconds),
    Period;
get_session_grace_period(rest) ->
    {ok, Period} = application:get_env(?APP_NAME, rest_session_grace_period_seconds),
    Period;
get_session_grace_period(provider_incoming) ->
    {ok, Period} = application:get_env(?APP_NAME, provider_session_grace_period_seconds),
    Period;
get_session_grace_period(_) ->
    {ok, Period} = application:get_env(?APP_NAME, fuse_session_grace_period_seconds),
    Period.


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
-spec has_grace_period_passed(session:id(), session:grace_period()) ->
    true | {false, RemainingTime :: seconds()}.
has_grace_period_passed(SessionId, GracePeriod) ->
    Diff = fun
        (#session{status = active, accessed = Accessed} = Sess) ->
            InactivityPeriod = time_utils:cluster_time_seconds() - Accessed,
            case InactivityPeriod >= GracePeriod of
                true ->
                    {ok, Sess#session{status = inactive}};
                false ->
                    {error, {grace_period_not_exceeded, GracePeriod - InactivityPeriod}}
            end;
        (#session{} = Sess) ->
            {ok, Sess#session{status = inactive}}
    end,
    case session:update(SessionId, Diff) of
        {ok, SessionId} ->
            true;
        {error, {grace_period_not_exceeded, RemainingTime}} ->
            {false, RemainingTime};
        {error, _} ->
            true
    end.


%% @private
-spec schedule_session_activity_checkup(Delay :: seconds()) ->
    TimeRef :: reference().
schedule_session_activity_checkup(Delay) ->
    erlang:send_after(timer:seconds(Delay), self(), ?CHECK_SESSION_ACTIVITY).


%% @private
-spec schedule_session_removal(Delay :: seconds()) ->
    TimeRef :: reference().
schedule_session_removal(Delay) ->
    erlang:send_after(timer:seconds(Delay), self(), ?REMOVE_SESSION).
