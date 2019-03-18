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

% TODO - ogarnac to co Bartek mowil (ze wcale nie pada sesja z managerem i test jest zly)

-include("global_definitions.hrl").
-include("modules/events/definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/common/handshake_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2, send/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-type ctx() :: event_type:ctx() | subscription_type:ctx().

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
%% Sends message to event_manager.
%% @end
%%--------------------------------------------------------------------
-spec send(pid(), term()) -> ok.
% TODO - zmienic nazwe na handle
send(Manager, Message) ->
    send_internal(Manager, Message, 1).

send_internal(_Manager, Request, -1) ->
    case application:get_env(?APP_NAME, log_event_manager_errors, false) of
        true -> ?error("Max retries for request: ~p", [Request]);
        false -> ?debug("Max retries for request: ~p", [Request])
    end,
    ok;
send_internal(Manager, Request, RetryCounter) ->
    try
        % sprawdzic czy istnieje stream dla eventu - jak nie to wtedy pobierac providera,
        % jak istnieje to walimy lokalnie
        case {get_provider(Request, Manager), Request} of
            {self, _} ->
                handle_locally(Request, Manager);
            {RemoteProviderId, _} ->
                {ok, SessId} = ets_state:get(session, Manager, session_id),
                handle_remotely(Request, RemoteProviderId, SessId);
            {RemoteProviderId, #subscription{} = Sub} ->
                gen_server2:cast(Manager, {cache_provider, Sub, RemoteProviderId}),
                {ok, SessId} = ets_state:get(session, Manager, session_id),
                handle_remotely(Request, RemoteProviderId, SessId);
            {RemoteProviderId, #subscription_cancellation{id = SubId}} ->
                gen_server2:cast(Manager, {remove_provider_cache, SubId}),
                {ok, SessId} = ets_state:get(session, Manager, session_id),
                handle_remotely(Request, RemoteProviderId, SessId)
        end
    catch
        exit:{noproc, _} ->
            ?debug("No proc to handle request ~p, retry", [Request]),
            send_internal(Manager, Request, RetryCounter - 1);
        exit:{normal, _} ->
            ?debug("Exit of stream process for request ~p, retry", [Request]),
            send_internal(Manager, Request, RetryCounter - 1);
        exit:{timeout, _} ->
            ?debug("Timeout of stream process for request ~p, retry", [Request]),
            send_internal(Manager, Request, RetryCounter - 1);
        Reason1:Reason2 ->
            ?error_stacktrace("Cannot process request ~p due to: ~p", [Request, {Reason1, Reason2}]),
            send_internal(Manager, Request, RetryCounter - 1)
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
    init_memory(),
    Self = self(),
    {ok, SessId} = session:update(SessId, fun(Session = #session{}) ->
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
handle_call(Request, _From, State) ->
    Retries = application:get_env(?APP_NAME, event_manager_retries, 1),
    handle_in_process(Request, State, Retries).

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

handle_cast({cache_provider, Sub, Provider}, State) ->
    cache_provider(Sub, Provider),
    {noreply, State};

handle_cast({remove_provider_cache, SubID}, State) ->
    remove_provider_cache(SubID),
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
%% @todo remove and use get_provider/2
%% @private
%% @doc
%% Returns ID of a provider responsible for request handling in given context.
%% Handles cases of events that should be handled even if file is not supported
%% locally
%% @end
%%--------------------------------------------------------------------
-spec get_provider(Request :: term(), State :: #state{}, oneprovider:id() | undefined) ->
    {ProviderId :: oneprovider:id(), NewState :: #state{}} |
    no_return().
get_provider(#flush_events{provider_id = ProviderId}, _Manager) ->
    ProviderId;
get_provider(Request, Manager) ->
    RequestCtx = get_context(Request),
    case RequestCtx of
        undefined ->
            oneprovider:get_id_or_undefined();
        {file, FileCtx} ->
            FileGuid = file_ctx:get_guid_const(FileCtx),
            case get_from_memory(Manager, guid_to_provider, FileGuid) of
                {ok, ID} ->
                    {ID, FileCtx};
                _ ->
                    {ok, SessId} = ets_state:get(session, Manager, session_id),
                    get_provider(Request, SessId, FileCtx)
            end
    end.

get_provider(#event{type = Type}, SessId, _FileCtx)
    when is_record(Type, file_attr_changed_event)
    orelse is_record(Type, file_location_changed_event)
    orelse is_record(Type, file_perm_changed_event)
    orelse is_record(Type, file_removed_event)
    orelse is_record(Type, file_renamed_event)
    orelse is_record(Type, quota_exceeded_event) ->
    {ok, #document{value = #session{proxy_via = ProxyVia}}} = session:get(SessId),
    utils:ensure_defined(ProxyVia, undefined, oneprovider:get_id_or_undefined());
get_provider(_, SessId, FileCtx) ->
    ProviderId = oneprovider:get_id(),
    case file_ctx:is_root_dir_const(FileCtx) of
        true ->
            ProviderId;
        false ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            {ok, ProviderIds} = space_logic:get_provider_ids(SessId, SpaceId),
            case {ProviderIds, lists:member(ProviderId, ProviderIds)} of
                {_, true} -> self;
                {[RemoteProviderId | _], _} -> RemoteProviderId;
                {[], _} ->
                    % TODO - logowanie i handlowanie bledow
                    throw(unsupported_space)
            end
    end.

handle_locally(#event{} = Evt, Manager) ->
    StmKey = event_type:get_stream_key(Evt),
    case get_from_memory(Manager, streams, StmKey) of
        {ok, Stm} ->
            ok = event_stream:send(Stm, Evt);
        _ ->
            ok
    end;

handle_locally(#flush_events{subscription_id = SubId, notify = NotifyFun}, Manager) ->
    case get_from_memory(Manager, subscriptions, SubId) of
        {ok, StmKey} ->
            case get_from_memory(Manager, streams, StmKey) of
                {ok, Stm} ->
                    ok = event_stream:send(Stm, {flush, NotifyFun});
                _ ->
                    ok
            end;
        _ ->
            ok
    end;

handle_locally(Request, Manager) ->
    gen_server2:call(Manager, Request, timer:minutes(1)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles request locally.
%% @end
%%--------------------------------------------------------------------
-spec handle_locally(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}}.
handle_in_process(#subscription{id = Id} = Sub, #state{} = State) ->
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
    add_to_memory(subscriptions, Id, StmKey),
    cache_provider(Sub, self),

    {reply, ok, State};

handle_in_process(#subscription_cancellation{id = SubId}, #state{} = State) ->
    State,
    % TODO - zalatwic kwestie defaultow
    case get_from_memory(subscriptions, SubId) of
        {ok, StmKey} ->
            case get_from_memory(streams, StmKey) of
                {ok, Stm} ->
                    ok = event_stream:send(Stm, {remove_subscription, SubId}),
                    remove_from_memory(subscriptions, SubId),
                    remove_provider_cache(SubId);
                _ ->
                    ok
            end;
        _ ->
            ok
    end,
    {reply, ok, State};

handle_in_process(Request, State) ->
    ?log_bad_request(Request),
    {reply, ok, State}.

handle_in_process(Request, State, RetryCounter) ->
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
        Reason1:Reason2 ->
            ?error_stacktrace("Cannot process request ~p due to: ~p", [Request, {Reason1, Reason2}]),
            retry_handle(State, Request, RetryCounter)
    end.

cache_provider(#subscription{id = Id} = Sub, Provider) ->
    RequestCtx = get_context(Sub),
    case RequestCtx of
        undefined ->
            ok;
        {file, FileCtx} ->
            FileGuid = file_ctx:get_guid_const(FileCtx),
            add_to_memory(guid_to_provider, FileGuid, Provider),
            add_to_memory(sub_to_guid, Id, FileGuid)
    end.

remove_provider_cache(SubId) ->
    case get_from_memory(sub_to_guid, SubId) of
        {ok, FileGuid} ->
            remove_from_memory(sub_to_guid, SubId),
            remove_from_memory(guid_to_provider, FileGuid);
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Forwards request to the remote provider.
%% @end
%%--------------------------------------------------------------------
-spec handle_remotely(Request :: term(), ProviderId :: oneprovider:id(),
    State :: #state{}) -> {noreply, NewState :: #state{}}.
handle_remotely(#flush_events{} = Request, ProviderId, SessId) ->
    #flush_events{context = Context, notify = Notify} = Request,
    {ok, Auth} = session:get_auth(SessId),
    ClientMsg = #client_message{
        message_stream = #message_stream{
            stream_id = sequencer:term_to_stream_id(Context)
        },
        message_body = Request,
        proxy_session_id = SessId,
        proxy_session_auth = Auth
    },
    Ref = session_utils:get_provider_session_id(outgoing, ProviderId),
    RequestTranslator = spawn(fun() ->
        receive
            % VFS-5206 - handle heartbeats
            #server_message{message_body = #status{}} = Msg ->
                Notify(Msg)
        after timer:minutes(10) ->
            Notify(#server_message{message_body = #status{code = ?EAGAIN}})
        end
    end),
    communicator:communicate_with_provider(ClientMsg, Ref, RequestTranslator),
    ok;

handle_remotely(#event{} = Evt, ProviderId, SessId) ->
    handle_remotely(#events{events = [Evt]}, ProviderId, SessId);

handle_remotely(Request, ProviderId, SessId) ->
    {file, FileUuid} = get_context(Request),
    StreamId = sequencer:term_to_stream_id(FileUuid),
    {ok, Auth} = session:get_auth(SessId),
    communicator:stream_to_provider(#client_message{
        message_body = Request,
        proxy_session_id = SessId,
        proxy_session_auth = Auth
    }, session_utils:get_provider_session_id(outgoing, ProviderId), StreamId),
    ok.

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

    State#state{
        streams_sup = StmsSup
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retries to handle request if counter is not 0.
%% @end
%%--------------------------------------------------------------------
-spec retry_handle(#state{}, Request :: term(), RetryCounter :: non_neg_integer()) -> {noreply, NewState :: #state{}}.
retry_handle(State, Request, 0) ->
    case application:get_env(?APP_NAME, log_event_manager_errors, false) of
        true -> ?error("Max retries for request: ~p", [Request]);
        false -> ?debug("Max retries for request: ~p", [Request])
    end,
    {reply, ok, State};
retry_handle(State, Request, RetryCounter) ->
    State2 = check_streams(State),
    handle_in_process(Request, State2, RetryCounter - 1).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if any stream registration/unregistration happened.
%% @end
%%--------------------------------------------------------------------
-spec check_streams(#state{}) -> NewState :: #state{}.
check_streams(State) ->
    receive
        {'$gen_cast',{unregister_stream, _} = Request} ->
            {noreply, State2} = handle_locally(Request, State),
            check_streams(State2);
        {'$gen_cast',{register_stream, _, _} = Request} ->
            {noreply, State2} = handle_locally(Request, State),
            check_streams(State2)
    after
        50 -> State
    end.


add_to_memory(StreamType, StmId, SeqStm) ->
    Manager = self(),
    {ok, Stms} = ets_state:get(session, Manager, StreamType),
    ets_state:save(session, Manager, {StreamType, StmId}, SeqStm),
    ets_state:save(session, Manager, StreamType, maps:put(StmId, SeqStm, Stms)).

remove_from_memory(StreamType, StmId) ->
    Manager = self(),
    {ok, Stms} = ets_state:get(session, Manager, StreamType),
    ets_state:delete(session, Manager, {StreamType, StmId}),
    ets_state:save(session, Manager, StreamType, maps:remove(StmId, Stms)).

get_from_memory(StreamType, StmId) ->
    ets_state:get(session, self(), {StreamType, StmId}).

get_from_memory(Manager, StreamType, StmId) ->
    ets_state:get(session, Manager, {StreamType, StmId}).

init_memory() ->
    Manager = self(),
    ets_state:save(session, Manager, streams, #{}),
    ets_state:save(session, Manager, subscriptions, #{}),
    ets_state:save(session, Manager, sub_to_provider, #{}),
    ets_state:save(session, Manager, guid_to_provider, #{}),
    ets_state:save(session, Manager, session_id, #{}).

delete_memory() ->
    delete_data(streams),
    delete_data(subscriptions),
    delete_data(sub_to_provider),
    delete_data(guid_to_provider),
    ets_state:delete(session, self(), session_id).

delete_data(StreamType) ->
    Manager = self(),
    {ok, Stms} = ets_state:get(session, Manager, StreamType),
    maps:map(fun(StmId, _) ->
        ets_state:delete(session, Manager, {StreamType, StmId})
             end, Stms),
    ets_state:delete(session, Manager, StreamType).