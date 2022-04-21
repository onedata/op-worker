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
-export([start_link/2, handle/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-type ctx() :: event_type:ctx() | subscription_type:ctx().
-type data_type() :: streams | subscriptions | guid_to_provider | sub_to_guid.
-type provider() :: oneprovider:id() | self.

%% event manager state:
%% session_id        - ID of a session associated with this event manager
%% event_manager_sup - pid of an event manager supervisor
%% event_stream_sup  - pid of an event stream supervisor
%% event_streams     - mapping from a subscription ID to an event stream pid
%% providers         - cache that maps file to provider that shall handle the event
-record(state, {
    session_id :: undefined | session:id(),
    manager_sup :: undefined | pid(),
    streams_sup :: undefined | pid()
}).

-define(STATE_ID, session).
-define(INITIALIZATION_STATUS_KEY, initialization_status).
-define(INITIALIZATION_STATUS_FINISHED_VALUE, initialization_finished).
-define(LOCAL_CALL_TIMEOUT, timer:minutes(1)).
-define(REMOTE_CALL_TIMEOUT, timer:minutes(10)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the event manager.
%% @end
%%--------------------------------------------------------------------
-spec start_link(MgrSup :: pid(), SessId :: session:id()) ->
    {ok, Mgr :: pid()} | ignore | {error, Reason :: term()}.
start_link(MgrSup, SessId) ->
    gen_server2:start_link(?MODULE, [MgrSup, SessId], []).

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
        true -> ?error("Max retries for request: ~p", [Request]);
        false -> ?debug("Max retries for request: ~p", [Request])
    end,
    ok;
handle(Manager, Request, RetryCounter) ->
    try
        case get_provider(Request, Manager) of
            self ->
                handle_locally(Request, Manager);
            ignore ->
                ?debug("Ignore request: ~p", [Request]), % Manager is closing
                ok;
            ProviderId ->
                Self = oneprovider:get_id_or_undefined(),
                case ProviderId of
                    Self -> % For requests with proxy filed set
                        handle_locally(Request, Manager);
                    _ ->
                        case Request of
                            #subscription{} ->
                                call_manager(Manager, {remote_subscription, ProviderId, Request}),
                                {ok, SessId} = ets_state:get(?STATE_ID, Manager, session_id),
                                handle_remotely(Request, ProviderId, SessId);
                            _->
                                {ok, SessId} = ets_state:get(?STATE_ID, Manager, session_id),
                                handle_remotely(Request, ProviderId, SessId)
                        end
                end
        end
    catch
        exit:{noproc, _} ->
            ?debug("No proc to handle request ~p, retry", [Request]),
            handle(Manager, Request, RetryCounter - 1);
        exit:{normal, _} ->
            ?debug("Exit of stream process for request ~p, retry", [Request]),
            handle(Manager, Request, RetryCounter - 1);
        exit:{timeout, _} ->
            ?debug("Timeout of stream process for request ~p, retry", [Request]),
            handle(Manager, Request, RetryCounter - 1);
        exit:Reason:Stacktrace ->
            ?error_stacktrace("Cannot process request ~p due to: exit:~p", [Request, Reason], Stacktrace),
            % Stream process crashed - wait and ping supervisor to wait for restart to stream by supervisor
            timer:sleep(50),
            call_manager(Manager, ping_stream_sup),
            handle(Manager, Request, RetryCounter - 1);
        Reason1:Reason2:Stacktrace ->
            ?error_stacktrace("Cannot process request ~p due to: ~p:~p", [Request, Reason1, Reason2], Stacktrace),
            handle(Manager, Request, RetryCounter - 1)
    end.

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
init([MgrSup, SessId]) ->
    ?debug("Initializing event manager for session ~p", [SessId]),
    process_flag(trap_exit, true),
    init_memory(SessId),
    Self = self(),
    {ok, #document{key = SessId}} = session:update(SessId, fun(Session = #session{}) ->
        {ok, Session#session{event_manager = Self}}
    end),
    {ok, #state{manager_sup = MgrSup, session_id = SessId}, 0}.

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
terminate(Reason, #state{session_id = SessId} = State) ->
    ?log_terminate(Reason, State),
    delete_memory(),
    session:update(SessId, fun(Session = #session{}) ->
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
%% Returns ID of a provider responsible for request handling in given context.
%% Handles cases of events that should be handled even if file is not supported
%% locally
%% @end
%%--------------------------------------------------------------------
-spec get_provider(Request :: term(), Manager :: pid()) ->
    provider() | ignore | no_return().
get_provider(#flush_events{provider_id = ProviderId}, _Manager) ->
    ProviderId;
get_provider(#event{type = Type}, Manager)
    when is_record(Type, file_attr_changed_event)
    orelse is_record(Type, file_location_changed_event)
    orelse is_record(Type, file_perm_changed_event)
    orelse is_record(Type, file_removed_event)
    orelse is_record(Type, file_renamed_event)
    orelse is_record(Type, quota_exceeded_event) ->
    case ets_state:get(session, Manager, proxy_via) of
        {ok, ProxyVia} -> ProxyVia;
        _ -> ignore
    end;
get_provider(Request, Manager) ->
    case get_context(Request) of
        undefined ->
            self;
        {file, FileGuid} -> % TODO VFS-7448 - test production of events for hardlinks
            case get_from_memory(Manager, guid_to_provider, FileGuid) of
                {ok, ID} ->
                    ID;
                _ ->
                    case ets_state:get(session, Manager, session_id) of
                        {ok, SessId} -> get_provider(Request, SessId, FileGuid);
                        _ -> ignore
                    end
            end
    end.

-spec get_provider(Request :: term(), session:id(), file_id:file_guid()) ->
    provider() | no_return().
get_provider(_, SessId, FileGuid) ->
    case fslogic_uuid:is_root_dir_guid(FileGuid) of
        true ->
            self;
        false ->
            SpaceId = file_id:guid_to_space_id(FileGuid),
            case provider_logic:supports_space(SpaceId) of
                true ->
                    self;
                false ->
                    case space_logic:get_provider_ids(SessId, SpaceId) of
                        {ok, []} -> throw(unsupported_space);
                        {ok, [RemoteProviderId | _]} -> RemoteProviderId
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles request locally (in caller process) or delegates it to manager
%% if manager has not finished initialization.
%% @end
%%--------------------------------------------------------------------
-spec handle_locally(Request :: term(), Manager :: pid()) -> ok.
handle_locally(#event{} = Evt, Manager) ->
    handle_event(Evt, Manager, true);
handle_locally(#flush_events{} = FlushRequest, Manager) ->
    handle_flush(FlushRequest, Manager, true);
handle_locally(Request, Manager) ->
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
            ?debug("No proc to handle request ~p, retry", [Request]),
            retry_handle(State, Request, RetryCounter);
        exit:{normal, _} ->
            ?debug("Exit of stream process for request ~p, retry", [Request]),
            retry_handle(State, Request, RetryCounter);
        exit:{timeout, _} ->
            ?debug("Timeout of stream process for request ~p, retry", [Request]),
            retry_handle(State, Request, RetryCounter);
        exit:Reason:Stacktrace ->
            ?error_stacktrace("Cannot process request ~p due to: exit:~p", [Request, Reason], Stacktrace),
            % Stream process crashed - wait and ping supervisor to wait for restart to stream by supervisor
            timer:sleep(50),
            event_stream_sup:ping(StmsSup),
            retry_handle(State, Request, RetryCounter);
        Reason1:Reason2:Stacktrace ->
            ?error_stacktrace("Cannot process request ~p due to: ~p", [Request, {Reason1, Reason2}], Stacktrace),
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
    add_subscription(Sub, State),
    cache_provider(Sub, self);

handle_in_process({remote_subscription, ProviderId, Sub}, #state{} = State) ->
    add_subscription(Sub, State),
    cache_provider(Sub, ProviderId);

handle_in_process(#subscription_cancellation{id = SubId} = Request, #state{session_id = SessId}) ->
    case get_from_memory(subscriptions, SubId) of
        {ok, StmKey} ->
            case get_from_memory(streams, StmKey) of
                {ok, Stm} ->
                    ok = event_stream:send(Stm, {remove_subscription, SubId}),
                    remove_from_memory(subscriptions, SubId),
                    {FileGuid, ProviderId} = get_and_clean_subscription_cache(SubId),

                    Self = oneprovider:get_id_or_undefined(),
                    case is_binary(ProviderId) andalso ProviderId =/= Self of
                        true ->
                            % Cancel subscription also on remote provider
                            % Spawn as sending request could block manager for long time
                            spawn(fun() ->
                                stream_to_provider(Request, ProviderId, SessId, FileGuid, undefined)
                            end),
                            ok;
                        false ->
                            ok
                    end;
                _ ->
                    ok
            end;
        _ ->
            ok
    end;

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
        session_id = SessId
    } = State,
    StmKey = subscription_type:get_stream_key(Sub),
    case get_from_memory(streams, StmKey) of
        {ok, Stm} ->
            ok = event_stream:send(Stm, {add_subscription, Sub});
        error ->
            {ok, Stm} = event_stream_sup:start_stream(StmsSup, self(), Sub, SessId),
            add_to_memory(streams, StmKey, Stm)
    end,
    add_to_memory(subscriptions, Id, StmKey).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Forwards request to the remote provider.
%% @end
%%--------------------------------------------------------------------
-spec handle_remotely(Request :: term(), ProviderId :: oneprovider:id(),
    session:id()) -> ok.
handle_remotely(#flush_events{} = Request, ProviderId, SessId) ->
    #flush_events{context = Context, notify = Notify} = Request,
    {ok, SessDoc} = session:get(SessId),
    Credentials = session:get_credentials(SessDoc),
    {ok, SessMode} = session:get_mode(SessDoc),
    StreamId = sequencer:binary_to_stream_id(Context),
    ClientMsg = #client_message{
        message_stream = #message_stream{stream_id = StreamId},
        message_body = Request,
        effective_session_id = SessId,
        effective_client_tokens = auth_manager:get_client_tokens(Credentials),
        effective_session_mode = SessMode
    },
    Ref = session_utils:get_provider_session_id(outgoing, ProviderId),
    RequestTranslator = spawn(fun() ->
        receive
            % VFS-5206 - handle heartbeats
            #server_message{message_body = #status{}} = Msg ->
                Notify(Msg)
        after ?REMOTE_CALL_TIMEOUT ->
            Notify(#server_message{message_body = #status{code = ?EAGAIN}})
        end
    end),
    communicator:stream_to_provider(Ref, ClientMsg, StreamId, RequestTranslator),
    ok;

handle_remotely(#event{} = Evt, ProviderId, SessId) ->
    handle_remotely(#events{events = [Evt]}, ProviderId, SessId);

handle_remotely(#subscription{} = Sub, ProviderId, SessId) ->
    stream_to_provider(Sub, ProviderId, SessId, self()),

    receive
        % VFS-5206 - handle heartbeats
        #server_message{message_body = #status{code = ?OK}} ->
            ok;
        #server_message{message_body = #status{} = Status} ->
            ?error("Remote subscription ~p (provider ~p, seesion ~p) failed with status: ~p",
                [Sub, ProviderId, SessId, Status])
    after
        ?REMOTE_CALL_TIMEOUT ->
            ?error("Remote subscription ~p (provider ~p, seesion ~p) failed timeout", [Sub, ProviderId, SessId])
    end;

handle_remotely(Request, ProviderId, SessId) ->
    stream_to_provider(Request, ProviderId, SessId, undefined).


%% @private
-spec stream_to_provider(Request :: term(), ProviderId :: oneprovider:id(), session:id(), undefined | pid()) -> ok.
stream_to_provider(Request, ProviderId, SessId, RecipientPid) ->
    {file, FileGuid} = get_context(Request),
    stream_to_provider(Request, ProviderId, SessId, FileGuid, RecipientPid).


%% @private
-spec stream_to_provider(Request :: term(), ProviderId :: oneprovider:id(), session:id(),
    file_id:file_guid(), undefined | pid()) -> ok.
stream_to_provider(Request, ProviderId, SessId, FileGuid, RecipientPid) ->
    StreamId = sequencer:binary_to_stream_id(FileGuid),
    {ok, SessDoc} = session:get(SessId),
    Credentials = session:get_credentials(SessDoc),
    {ok, SessMode} = session:get_mode(SessDoc),
    communicator:stream_to_provider(
        session_utils:get_provider_session_id(outgoing, ProviderId),
        #client_message{
            message_body = Request,
            effective_session_id = SessId,
            effective_client_tokens = auth_manager:get_client_tokens(Credentials),
            effective_session_mode = SessMode
        },
        StreamId, RecipientPid
    ),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Caches information about provider that handles request connected with guid.
%% @end
%%--------------------------------------------------------------------
-spec cache_provider(#subscription{}, ProviderId :: provider()) -> ok.
cache_provider(#subscription{id = Id} = Sub, Provider) ->
    case get_context(Sub) of
        undefined ->
            ok;
        {file, FileGuid} ->
            add_to_memory(guid_to_provider, FileGuid, Provider),
            add_to_memory(sub_to_guid, Id, FileGuid)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes from cache information about provider that handles request
%% connected with guid.
%% @end
%%--------------------------------------------------------------------
-spec get_and_clean_subscription_cache(subscription:id()) ->
    {file_id:file_guid() | undefined, oneprovider:id() | undefined}.
get_and_clean_subscription_cache(SubId) ->
    case get_from_memory(sub_to_guid, SubId) of
        {ok, FileGuid} ->
            remove_from_memory(sub_to_guid, SubId),
            case get_from_memory(guid_to_provider, FileGuid) of
                {ok, Provider} ->
                    remove_from_memory(guid_to_provider, FileGuid),
                    {FileGuid, Provider};
                _ ->
                    {FileGuid, undefined}
            end;
        _ ->
            {undefined, undefined}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns request context.
%% @end
%%--------------------------------------------------------------------
-spec get_context(Request :: term()) -> Ctx :: ctx().
get_context(#event{} = Evt) ->
    event_type:get_context(Evt);

get_context(#events{events = [Evt]}) ->
    get_context(Evt);

get_context(#subscription{} = Sub) ->
    subscription_type:get_context(Sub);

get_context(_) ->
    undefined.

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
start_event_streams(#state{streams_sup = StmsSup, session_id = SessId} = State) ->
    {ok, Docs} = subscription:list_durable_subscriptions(),

    lists:foreach(fun(#document{value = #subscription{id = Id} = Sub}) ->
        StmKey = subscription_type:get_stream_key(Sub),
        {ok, Stm} = event_stream_sup:start_stream(StmsSup, self(), Sub, SessId),
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
        true -> ?error("Max retries for request: ~p", [Request]);
        false -> ?debug("Max retries for request: ~p", [Request])
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes state.
%% @end
%%--------------------------------------------------------------------
-spec init_memory(session:id()) -> ok.
init_memory(SessionID) ->
    ets_state:init_collection(?STATE_ID, streams),
    ets_state:init_collection(?STATE_ID, subscriptions),
    ets_state:init_collection(?STATE_ID, guid_to_provider),
    ets_state:init_collection(?STATE_ID, sub_to_guid),
    ets_state:save(?STATE_ID, self(), session_id, SessionID),
    {ok, #document{value = #session{proxy_via = ProxyVia}}} = session:get(SessionID),
    ets_state:save(?STATE_ID, self(), proxy_via, utils:ensure_defined(ProxyVia, self)).

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
    ets_state:delete_collection(?STATE_ID, subscriptions),
    ets_state:delete_collection(?STATE_ID, guid_to_provider),
    ets_state:delete_collection(?STATE_ID, sub_to_guid),
    ets_state:delete(?STATE_ID, self(), session_id),
    ets_state:delete(?STATE_ID, self(), proxy_via).

%% @private
-spec call_manager(pid(), term()) -> term().
call_manager(Manager, Request) ->
    gen_server2:call(Manager, Request, ?LOCAL_CALL_TIMEOUT).