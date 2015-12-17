%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible for removal
%%% of session that has been inactive longer that allowed period.
%%% @end
%%%-------------------------------------------------------------------
-module(session_watcher).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% session watcher state:
%% session_id - ID of session associated with sequencer manager
-record(state, {
    session_id :: session:id(),
    max_inactivity_period :: non_neg_integer()
}).

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
    gen_server:start_link(?MODULE, [SessId, SessType], []).

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
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SessId, SessType]) ->
    process_flag(trap_exit, true),
    {ok, SessId} = session:update(SessId, #{watcher => self()}),
    Period = get_max_inactivity_period(SessType),
    {ok, #state{session_id = SessId, max_inactivity_period = Period}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_cast(schedule_session_status_checkup,
    #state{max_inactivity_period = Period} = Status) ->
    schedule_session_status_checkup(Period),
    {noreply, Status, hibernate};

handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_info(session_status_checkup, #state{session_id = SessId,
    max_inactivity_period = Period} = State) ->
    case session:const_get(SessId) of
        {ok, #document{value = #session{status = active}}} ->
            {noreply, State, hibernate};
        {ok, #document{value = #session{status = inactive}}} ->
            case is_max_inactivity_period_exceeded(SessId, Period) of
                true ->
                    {stop, normal, State};
                false ->
                    {noreply, State, hibernate};
                {false, RemainingTime} ->
                    schedule_session_status_checkup(RemainingTime),
                    {noreply, State, hibernate}
            end;
        {ok, #document{value = #session{status = phantom}}} ->
            {stop, normal, State};
        {error, Reason} ->
            {stop, Reason, State}
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
    State :: #state{}) -> term().
terminate(Reason, #state{session_id = SessId} = State) ->
    ?log_terminate(Reason, State),
    session:update(SessId, #{watcher => undefined}),
    worker_proxy:cast(?SESSION_MANAGER_WORKER, {remove_session, SessId}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) -> {ok, NewState :: #state{}} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns max inactivity period for given session type.
%% @end
%%--------------------------------------------------------------------
-spec get_max_inactivity_period(SessType :: session:type()) ->
    Milliseconds :: non_neg_integer().
get_max_inactivity_period(gui) ->
    {ok, Period} = application:get_env(?APP_NAME,
        gui_session_max_inactivity_period_in_seconds_before_removal),
    timer:seconds(Period);
get_max_inactivity_period(fuse) ->
    {ok, Period} = application:get_env(?APP_NAME,
        fuse_session_max_inactivity_period_in_seconds_before_removal),
    timer:seconds(Period);
get_max_inactivity_period(rest) ->
    {ok, Period} = application:get_env(?APP_NAME,
        rest_session_max_inactivity_period_in_seconds_before_removal),
    timer:seconds(Period).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether session inactivity period exceeds maximal allowed inactivity
%% period for session type. If maximal period is exceeded session is marked
%% as 'phantom' for later removal. Returns false for active session, false and
%% remaining time to removal for inactive session that does not exceed allowed
%% inactivity period and true for inactive session that exceeded maximal allowed
%% inactivity period.
%% @end
%%--------------------------------------------------------------------
-spec is_max_inactivity_period_exceeded(SessId :: session:id(),
    MaxInactivityPeriod :: non_neg_integer()) ->
    boolean() | {false, RemainingTime :: non_neg_integer()}.
is_max_inactivity_period_exceeded(SessId, MaxInactivityPeriod) ->
    Diff = fun
        (#session{status = active}) ->
            {error, active_session};
        (#session{status = inactive, accessed = Accessed} = Sess) ->
            InactivityPeriod = timer:now_diff(os:timestamp(), Accessed) div 1000,
            case InactivityPeriod >= MaxInactivityPeriod of
                true ->
                    {ok, Sess#session{status = phantom}};
                false ->
                    {error, {period_not_exceeded, MaxInactivityPeriod - InactivityPeriod}}
            end;
        (#session{} = Sess) ->
            {ok, Sess#session{status = phantom}}
    end,
    case session:update(SessId, Diff) of
        {ok, SessId} -> true;
        {error, active_session} -> false;
        {error, {period_not_exceeded, RemainingTime}} -> {false, RemainingTime};
        {error, _} -> true
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules session status checkup that should take place after 'Delay'
%% milliseconds.
%% @end
%%--------------------------------------------------------------------
-spec schedule_session_status_checkup(Delay :: non_neg_integer()) ->
    TimeRef :: reference().
schedule_session_status_checkup(Delay) ->
    erlang:send_after(Delay, self(), session_status_checkup).
