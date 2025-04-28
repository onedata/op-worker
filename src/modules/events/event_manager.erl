%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for dispatching events to event streams. Whenever an event arrives it is
%%% forwarded to an associated event stream. Event manager is supervised by
%%% event manager supervisor and initialized on session creation.
%%% @end
%%%-------------------------------------------------------------------
-module(event_manager).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/events/definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/common/handshake_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2, handle/2, handle_session_termination/1]).

%% gen_server callbacks
-export([
    init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3
]).

-type data_type() :: streams | subscriptions.

%% event manager state:
%% session_id        - ID of a session associated with this event manager
%% event_manager_sup - pid of an event manager supervisor
%% event_stream_sup  - pid of an event stream supervisor
%% event_streams     - mapping from a subscription ID to an event stream pid
-record(state, {
    session_id :: undefined | session:id(),
    manager_sup :: undefined | pid(),
    streams_sup :: undefined | pid()
}).

-define(STATE_ID, session).
-define(INITIALIZATION_STATUS_KEY, initialization_status).
-define(INITIALIZATION_STATUS_FINISHED_VALUE, initialization_finished).
-define(CALL_TIMEOUT, timer:minutes(1)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the event manager.
%% @end
%%--------------------------------------------------------------------
-spec start_link(MgrSup :: pid(), SessionId :: session:id()) ->
    {ok, Mgr :: pid()} | ignore | {error, Reason :: term()}.
start_link(MgrSup, SessionId) ->
    gen_server2:start_link(?MODULE, [MgrSup, SessionId], []).

%%--------------------------------------------------------------------
%% @doc
%% Handles message or sends to event_manager.
%% @end
%%--------------------------------------------------------------------
-spec handle(pid(), term()) -> ok.
handle(Manager, Message) ->
    handle(Manager, Message, 1).

%% @private
-spec handle(pid(), term(), integer()) -> ok.
handle(_Manager, Request, -1) ->
    case op_worker:get_env(log_event_manager_errors, false) of
        true -> ?error("Max retries for request: ~tp", [Request]);
        false -> ?debug("Max retries for request: ~tp", [Request])
    end,
    ok;
handle(Manager, Request, RetryCounter) ->
    try
        handle_insecure(Request, Manager)
    catch
        exit:{noproc, _} ->
            ?debug("No proc to handle request ~tp, retry", [Request]),
            handle(Manager, Request, RetryCounter - 1);
        exit:{normal, _} ->
            ?debug("Exit of stream process for request ~tp, retry", [Request]),
            handle(Manager, Request, RetryCounter - 1);
        exit:{timeout, _} ->
            ?debug("Timeout of stream process for request ~tp, retry", [Request]),
            handle(Manager, Request, RetryCounter - 1);
        exit:Reason:Stacktrace ->
            ?error_stacktrace("Cannot process request ~tp due to: exit:~tp", [Request, Reason], Stacktrace),
            % Stream process crashed - wait and ping supervisor to wait for restart to stream by supervisor
            timer:sleep(50),
            call_manager(Manager, ping_stream_sup),
            handle(Manager, Request, RetryCounter - 1);
        Reason1:Reason2:Stacktrace ->
            ?error_stacktrace("Cannot process request ~tp due to: ~tp:~tp", [Request, Reason1, Reason2], Stacktrace),
            handle(Manager, Request, RetryCounter - 1)
    end.


-spec handle_session_termination(pid()) -> ok.
handle_session_termination(Manager) ->
    call_manager(Manager, handle_session_termination).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the event manager. Returns timeout equal to zero, so that
%% event manager receives 'timeout' message in handle_info immediately after
%% initialization. This mechanism is introduced in order to avoid deadlock
%% when asking event manager supervisor for event stream supervisor pid during
%% supervision tree creation.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([MgrSup, SessionId]) ->
    ?debug("Initializing event manager for session ~tp", [SessionId]),
    process_flag(trap_exit, true),
    init_memory(),
    Self = self(),
    {ok, #document{key = SessionId}} = session:update(SessionId, fun(Session = #session{}) ->
        {ok, Session#session{event_manager = Self}}
    end),
    {ok, #state{manager_sup = MgrSup, session_id = SessionId}, 0}.

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
handle_call(ping_stream_sup, _From, #state{streams_sup = StmsSup} = State) ->
    event_stream_sup:ping(StmsSup),
    {reply, ok, State};
handle_call(handle_session_termination, _From, #state{} = State) ->
    case get_collection_keys_from_memory(subscriptions) of
        {ok, Subscriptions} ->
            lists:foreach(fun(SubId) ->
                cancel_subscription(#subscription_cancellation{id = SubId}, State, termination)
            end, Subscriptions);
        error ->
            ok
    end,
    {reply, ok, State};
handle_call(Request, _From, State) ->
    Retries = op_worker:get_env(event_manager_retries, 1),
    handle_in_process(Request, State, Retries),
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Wraps cast messages' handlers.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_cast({register_stream, StmKey, Stm}, State) ->
    add_to_memory(streams, StmKey, Stm),
    {noreply, State};

handle_cast({unregister_stream, StmKey}, State) ->
    remove_from_memory(streams, StmKey),
    {noreply, State};

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
handle_info({'EXIT', MgrSup, shutdown}, #state{manager_sup = MgrSup} = State) ->
    {stop, normal, State};

handle_info(timeout, State) ->
    State2 = start_event_streams(State),
    {noreply, State2};

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
terminate(Reason, #state{session_id = SessionId} = State) ->
    ?log_terminate(Reason, State),
    delete_memory(),
    session:update(SessionId, fun(Session = #session{}) ->
        {ok, Session#session{event_manager = undefined}}
    end).

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
%% Handles request locally (in caller process) or delegates it to manager
%% if manager has not finished initialization.
%% @end
%%--------------------------------------------------------------------
-spec handle_insecure(Request :: term(), Manager :: pid()) -> ok.
handle_insecure(#event{} = Evt, Manager) ->
    handle_event(Evt, Manager, true);
handle_insecure(#flush_events{} = FlushRequest, Manager) ->
    handle_flush(FlushRequest, Manager, true);
handle_insecure(Request, Manager) ->
    call_manager(Manager, Request).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles event locally (in caller process) or delegates it to manager
%% if manager has not finished initialization.
%% @end
%%--------------------------------------------------------------------
-spec handle_event(Evt :: event:base(), Manager :: pid(), VerifyManager :: boolean()) -> ok.
handle_event(Evt, Manager, VerifyManager) ->
    StmKey = event_type:get_stream_key(Evt),
    case {get_from_memory(Manager, streams, StmKey), VerifyManager} of
        {{ok, Stm}, _} ->
            ok = event_stream:send(Stm, Evt);
        {_, true} ->
            case ets_state:get(?STATE_ID, Manager, ?INITIALIZATION_STATUS_KEY) of
                {ok, ?INITIALIZATION_STATUS_FINISHED_VALUE} -> handle_event(Evt, Manager, false);
                _ -> call_manager(Manager, Evt)
            end,
            ok;
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles flush request locally (in caller process) or delegates it to manager
%% if manager has not finished initialization.
%% @end
%%--------------------------------------------------------------------
-spec handle_flush(FlushRequest :: #flush_events{}, Manager :: pid(),
    VerifyManager :: boolean()) -> ok.
handle_flush(#flush_events{subscription_id = SubId, notify = NotifyFun} = FlushRequest,
    Manager, VerifyManager) ->
    case get_from_memory(Manager, subscriptions, SubId) of
        {ok, StmKey} ->
            case get_from_memory(Manager, streams, StmKey) of
                {ok, Stm} ->
                    ok = event_stream:send(Stm, {flush, NotifyFun});
                _ ->
                    maybe_retry_flush(FlushRequest, Manager, VerifyManager)
            end;
        _ ->
            maybe_retry_flush(FlushRequest, Manager, VerifyManager)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Verifies if flush request handling should be retried.
%% @end
%%--------------------------------------------------------------------
-spec maybe_retry_flush(FlushRequest :: #flush_events{}, Manager :: pid(),
    VerifyManager :: boolean()) -> ok.
maybe_retry_flush(_FlushRequest, _Manager, false) ->
    ok;
maybe_retry_flush(FlushRequest, Manager, true) ->
    case ets_state:get(?STATE_ID, Manager, ?INITIALIZATION_STATUS_KEY) of
        {ok, ?INITIALIZATION_STATUS_FINISHED_VALUE} -> handle_flush(FlushRequest, Manager, false);
        _ -> call_manager(Manager, FlushRequest)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles request in manager process. Repeats handling in case of error.
%% @end
%%--------------------------------------------------------------------
-spec handle_in_process(Request :: term(), State :: #state{}, non_neg_integer()) -> ok.
handle_in_process(Request, #state{streams_sup = StmsSup} = State, RetryCounter) ->
    try
        handle_in_process(Request, State)
    catch
        exit:{noproc, _} ->
            ?debug("No proc to handle request ~tp, retry", [Request]),
            retry_handle(State, Request, RetryCounter);
        exit:{normal, _} ->
            ?debug("Exit of stream process for request ~tp, retry", [Request]),
            retry_handle(State, Request, RetryCounter);
        exit:{timeout, _} ->
            ?debug("Timeout of stream process for request ~tp, retry", [Request]),
            retry_handle(State, Request, RetryCounter);
        exit:Reason:Stacktrace ->
            ?error_stacktrace("Cannot process request ~tp due to: exit:~tp", [Request, Reason], Stacktrace),
            % Stream process crashed - wait and ping supervisor to wait for restart to stream by supervisor
            timer:sleep(50),
            event_stream_sup:ping(StmsSup),
            retry_handle(State, Request, RetryCounter);
        Reason1:Reason2:Stacktrace ->
            ?error_stacktrace("Cannot process request ~tp due to: ~tp", [Request, {Reason1, Reason2}], Stacktrace),
            retry_handle(State, Request, RetryCounter)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles request in manager process.
%% @end
%%--------------------------------------------------------------------
-spec handle_in_process(Request :: term(), State :: #state{}) -> ok.
handle_in_process(#subscription{} = Sub, #state{} = State) ->
    add_subscription(Sub, State);

handle_in_process(#subscription_cancellation{} = Request, #state{} = State) ->
    cancel_subscription(Request, State, request);

handle_in_process(#event{} = Evt, _State) ->
    handle_event(Evt, self(), false);

handle_in_process(#flush_events{} = FlushRequest, _State) ->
    handle_flush(FlushRequest, self(), false);

handle_in_process(Request, _State) ->
    ?log_bad_request(Request),
    ok.


%% @private
-spec add_subscription(#subscription{}, #state{}) -> ok.
add_subscription(#subscription{id = Id} = Sub, #state{} = State) ->
    #state{
        streams_sup = StmsSup,
        session_id = SessionId
    } = State,
    StmKey = subscription_type:get_stream_key(Sub),
    case get_from_memory(streams, StmKey) of
        {ok, Stm} ->
            ok = event_stream:send(Stm, {add_subscription, Sub});
        error ->
            {ok, Stm} = event_stream_sup:start_stream(StmsSup, self(), Sub, SessionId),
            add_to_memory(streams, StmKey, Stm)
    end,
    add_to_memory(subscriptions, Id, StmKey).


%% @private
-spec cancel_subscription(#subscription_cancellation{}, #state{}, CancellationReason :: request | termination) -> ok.
cancel_subscription(#subscription_cancellation{id = SubId}, _State, CancellationReason) ->
    case get_from_memory(subscriptions, SubId) of
        {ok, StmKey} ->
            case get_from_memory(streams, StmKey) of
                {ok, Stm} ->
                    case CancellationReason of
                        request -> ok = event_stream:send(Stm, {remove_subscription, SubId});
                        % Ignore stream errors on termination
                        termination -> event_stream:send(Stm, {remove_subscription, SubId})
                    end,
                    remove_from_memory(subscriptions, SubId);
                _ ->
                    ok
            end;
        _ ->
            ok
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts event streams for durable subscriptions.
%% @end
%%--------------------------------------------------------------------
-spec start_event_streams(#state{}) -> #state{}.
start_event_streams(#state{streams_sup = undefined, manager_sup = MgrSup} = State) ->
    {ok, StmsSup} = event_manager_sup:get_event_stream_sup(MgrSup),
    start_event_streams(State#state{streams_sup = StmsSup});
start_event_streams(#state{streams_sup = StmsSup, session_id = SessionId} = State) ->
    {ok, Docs} = subscription:list_durable_subscriptions(),

    lists:foreach(fun(#document{value = #subscription{id = Id} = Sub}) ->
        StmKey = subscription_type:get_stream_key(Sub),
        {ok, Stm} = event_stream_sup:start_stream(StmsSup, self(), Sub, SessionId),
        add_to_memory(subscriptions, Id, StmKey),
        add_to_memory(streams, StmKey, Stm)
    end, Docs),

    ets_state:save(?STATE_ID, self(), ?INITIALIZATION_STATUS_KEY, ?INITIALIZATION_STATUS_FINISHED_VALUE),
    State#state{
        streams_sup = StmsSup
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retries to handle request if counter is not 0.
%% @end
%%--------------------------------------------------------------------
-spec retry_handle(#state{}, Request :: term(), RetryCounter :: non_neg_integer()) -> ok.
retry_handle(_State, Request, 0) ->
    case op_worker:get_env(log_event_manager_errors, false) of
        true -> ?error("Max retries for request: ~tp", [Request]);
        false -> ?debug("Max retries for request: ~tp", [Request])
    end,
    ok;
retry_handle(State, Request, RetryCounter) ->
    check_streams(),
    handle_in_process(Request, State, RetryCounter - 1).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if any stream registration/unregistration happened.
%% @end
%%--------------------------------------------------------------------
-spec check_streams() -> ok.
check_streams() ->
    receive
        {'$gen_cast',{unregister_stream, StmKey}} ->
            remove_from_memory(streams, StmKey),
            check_streams();
        {'$gen_cast',{register_stream, StmKey, Stm}} ->
            add_to_memory(streams, StmKey, Stm),
            check_streams()
    after
        50 -> ok
    end.

%%%===================================================================
%%% Internal functions for caching in ets state
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves value in state.
%% @end
%%--------------------------------------------------------------------
-spec add_to_memory(data_type(), term(), term()) -> ok.
add_to_memory(DataType, Key, Value) ->
    ets_state:add_to_collection(?STATE_ID, DataType, Key, Value).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes value from state.
%% @end
%%--------------------------------------------------------------------
-spec remove_from_memory(data_type(), term()) -> ok.
remove_from_memory(DataType, Key) ->
    ets_state:remove_from_collection(?STATE_ID, DataType, Key).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets value from state.
%% @end
%%--------------------------------------------------------------------
-spec get_from_memory(data_type(), term()) -> {ok, term()} | error.
get_from_memory(DataType, Key) ->
    ets_state:get_from_collection(?STATE_ID, DataType, Key).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets value from state.
%% @end
%%--------------------------------------------------------------------
-spec get_from_memory(Manager :: pid(), data_type(), term()) -> {ok, term()} | error.
get_from_memory(Manager, DataType, Key) ->
    ets_state:get_from_collection(?STATE_ID, Manager, DataType, Key).

%% @private
-spec get_collection_keys_from_memory(data_type()) -> {ok, list()} | error.
get_collection_keys_from_memory(DataType) ->
    case ets_state:get_collection(?STATE_ID, self(), DataType) of
        {ok, Collection} -> {ok, maps:keys(Collection)};
        error -> error
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes state.
%% @end
%%--------------------------------------------------------------------
-spec init_memory() -> ok.
init_memory() ->
    ets_state:init_collection(?STATE_ID, streams),
    ets_state:init_collection(?STATE_ID, subscriptions).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes all data from state.
%% @end
%%--------------------------------------------------------------------
-spec delete_memory() -> ok.
delete_memory() ->
    ets_state:delete(?STATE_ID, self(), ?INITIALIZATION_STATUS_KEY),
    ets_state:delete_collection(?STATE_ID, streams),
    ets_state:delete_collection(?STATE_ID, subscriptions).

%% @private
-spec call_manager(pid(), term()) -> term().
call_manager(Manager, Request) ->
    gen_server2:call(Manager, Request, ?CALL_TIMEOUT).
