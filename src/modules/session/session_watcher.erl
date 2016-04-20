%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
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
    session_ttl :: non_neg_integer()
}).

-define(SESSION_REMOVAL_RETRY_DELAY, timer:seconds(5)).

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
    TTL = get_session_ttl(SessType),
    schedule_session_status_checkup(TTL),
    {ok, #state{session_id = SessId, session_ttl = TTL}}.

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
handle_cast(schedule_session_status_checkup, #state{session_ttl = TTL} = Status) ->
    schedule_session_status_checkup(TTL),
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
handle_info(remove_session, #state{session_id = SessId} = State) ->
    worker_proxy:cast(?SESSION_MANAGER_WORKER, {remove_session, SessId}),
    schedule_session_removal(?SESSION_REMOVAL_RETRY_DELAY),
    {noreply, State, hibernate};

handle_info(check_session_status, #state{session_id = SessId,
    session_ttl = TTL} = State) ->
    RemoveSession = case session:const_get(SessId) of
        {ok, #document{value = #session{status = inactive}}} ->
            true;
        {ok, #document{value = #session{connections = [_ | _]}}} ->
            {false, TTL};
        {ok, #document{value = #session{status = active}}} ->
            is_session_ttl_exceeded(SessId, TTL);
        {error, _} = Error ->
            {false, Error}
    end,

    case RemoveSession of
        true ->
            schedule_session_removal(0),
            {noreply, State};
        {false, {error, Reason} } ->
            {stop, Reason, State};         
        {false, RemainingTime} ->
            schedule_session_status_checkup(RemainingTime),
            {noreply, State, hibernate}
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
%% Returns session TTL for given session type.
%% @end
%%--------------------------------------------------------------------
-spec get_session_ttl(SessType :: session:type()) ->
    Milliseconds :: non_neg_integer().
get_session_ttl(gui) ->
    {ok, Period} = application:get_env(?APP_NAME, gui_session_ttl_seconds),
    timer:seconds(Period);
get_session_ttl(fuse) ->
    {ok, Period} = application:get_env(?APP_NAME, fuse_session_ttl_seconds),
    timer:seconds(Period);
get_session_ttl(rest) ->
    {ok, Period} = application:get_env(?APP_NAME, rest_session_ttl_seconds),
    timer:seconds(Period);
get_session_ttl(provider) ->
    {ok, Period} = application:get_env(?APP_NAME, provider_session_ttl_seconds),
    timer:seconds(Period);
get_session_ttl(provider_outgoing) ->
    {ok, Period} = application:get_env(?APP_NAME, provider_session_ttl_seconds),
    timer:seconds(Period).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether session inactivity period exceeds TTL. If session TTL is
%% exceeded, session is marked as 'inactive' for later removal. Returns false and
%% remaining time to removal for session that does not exceed TTL
%% and true for inactive session that exceeded TTL.
%% @end
%%--------------------------------------------------------------------
-spec is_session_ttl_exceeded(SessId :: session:id(), TTL :: session:ttl()) ->
    true | {false, RemainingTime :: non_neg_integer()}.
is_session_ttl_exceeded(SessId, TTL) ->
    Diff = fun
        (#session{status = active, accessed = Accessed} = Sess) ->
            InactivityPeriod = timer:now_diff(os:timestamp(), Accessed) div 1000,
            case InactivityPeriod >= TTL of
                true -> {ok, Sess#session{status = inactive}};
                false -> {error, {ttl_not_exceeded, TTL - InactivityPeriod}}
            end;
        (#session{} = Sess) ->
            {ok, Sess#session{status = inactive}}
    end,
    case session:update(SessId, Diff) of
        {ok, SessId} -> true;
        {error, {ttl_not_exceeded, RemainingTime}} -> {false, RemainingTime};
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
    erlang:send_after(Delay, self(), check_session_status).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules session removal that should take place after 'Delay'
%% milliseconds.
%% @end
%%--------------------------------------------------------------------
-spec schedule_session_removal(Delay :: non_neg_integer()) ->
    TimeRef :: reference().
schedule_session_removal(Delay) ->
    erlang:send_after(Delay, self(), remove_session).