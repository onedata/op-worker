%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for dispatching events to event streams. Whenever an event arrives it it
%%% forwarded to all registered event streams. Event manager is supervised by
%%% event manager supervisor and initialized on session creation.
%%% @end
%%%-------------------------------------------------------------------
-module(event_manager).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("modules/events/definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-type streams() :: #{subscription:id() => pid()}.

%% event manager state:
%% session_id               - ID of a session associated with this event manager
%% event_manager_sup        - pid of an event manager supervisor
%% event_stream_sup         - pid of an event stream supervisor
%% event_streams            - mapping from a subscription ID to an event stream pid
%% entry_to_provider_map    - cache that maps file to provider that shall handle the event
-record(state, {
    session_id :: session:id(),
    event_manager_sup :: pid(),
    event_stream_sup :: pid(),
    event_streams = #{} :: streams(),
    entry_to_provider_map = #{} :: #{}
}).

%% Lifetime in seconds of #state.entry_to_provider_map entry
-define(ENTRY_TO_PROVIDER_MAPPING_CACHE_LIFETIME_SECONDS, 5).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the event manager.
%% @end
%%--------------------------------------------------------------------
-spec start_link(EvtManSup :: pid(), SessId :: session:id()) ->
    {ok, EvtMan :: pid()} | ignore | {error, Reason :: term()}.
start_link(EvtManSup, SessId) ->
    gen_server:start_link(?MODULE, [EvtManSup, SessId], []).

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
init([EvtManSup, SessId]) ->
    ?debug("Initializing event manager for session ~p", [SessId]),
    process_flag(trap_exit, true),
    {ok, SessId} = session:update(SessId, #{event_manager => self()}),
    {ok, #state{event_manager_sup = EvtManSup, session_id = SessId}, 0}.

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
handle_cast({register_stream, SubId, EvtStm}, #state{event_streams = EvtStms} = State) ->
    {noreply, State#state{event_streams = maps:put(SubId, EvtStm, EvtStms)}};

handle_cast({unregister_stream, SubId}, #state{event_streams = EvtStms} = State) ->
    {noreply, State#state{event_streams = maps:remove(SubId, EvtStms)}};

handle_cast({flush_stream, SubId, Notify}, #state{event_streams = Stms,
    session_id = SessId} = State) ->
    case maps:find(SubId, Stms) of
        {ok, Stm} ->
            gen_server:cast(Stm, {flush, Notify});
        error ->
            ?warning("Event stream flush error: stream for subscription ~p and "
            "session ~p not found", [SubId, SessId])
    end,
    {noreply, State};

handle_cast(#event{} = Evt, #state{session_id = SessId, event_streams = EvtStms, entry_to_provider_map = ProvMap} = State) ->
    ?debug("Handling event ~p in session ~p", [Evt, SessId]),
    HandleLocally =
        fun
            (#events{events = [Event]}, NewProvMap, false) -> %% Request should be handled locally only
                {noreply, State#state{entry_to_provider_map = NewProvMap, event_streams = maps:map(
                    fun(_, EvtStm) ->
                        gen_server:cast(EvtStm, Event),
                        EvtStm
                    end, EvtStms)}};
            (_, NewProvMap, true) -> %% Request was already handled remotely
                {noreply, State#state{entry_to_provider_map = NewProvMap}}
        end,

    handle_or_reroute(#events{events = [Evt]}, request_to_file_entry_or_provider(Evt), SessId, HandleLocally, ProvMap);

handle_cast(#flush_events{provider_id = ProviderId, subscription_id = SubId, notify = NotifyFun} = Evt,
    #state{session_id = SessId, event_streams = EvtStms, entry_to_provider_map = ProvMap} = State) ->
    ?debug("Handling event ~p in session ~p", [Evt, SessId]),
    HandleLocally =
        fun
            (_, NewProvMap, false) -> %% Request should be handled locally only
                case maps:find(SubId, EvtStms) of
                    {ok, Stm} ->
                        gen_server:cast(Stm, {flush, NotifyFun});
                    error ->
                        ?warning("Event stream flush error: stream for subscription ~p and "
                        "session ~p not found", [SubId, SessId])
                end,
                {noreply, State#state{entry_to_provider_map = NewProvMap}};
            (_, NewProvMap, true) -> %% Request was already handled remotely
                {noreply, State#state{entry_to_provider_map = NewProvMap}}
        end,

    handle_or_reroute(Evt, {provider, ProviderId}, SessId, HandleLocally, ProvMap);

handle_cast(#subscription{id = SubId} = Sub, #state{event_stream_sup = EvtStmSup,
    session_id = SessId, event_streams = EvtStms, entry_to_provider_map = ProvMap} = State) ->
    HandleLocally =
        fun(_, NewProvMap, _) ->
            ?info("Adding subscription ~p to session ~p", [SubId, SessId]),
            {ok, EvtStm} = event_stream_sup:start_event_stream(EvtStmSup, self(), Sub, SessId),
            {noreply, State#state{entry_to_provider_map = NewProvMap, event_streams = maps:put(SubId, EvtStm, EvtStms)}}
        end,

    handle_or_reroute(Sub, request_to_file_entry_or_provider(Sub), SessId, HandleLocally, ProvMap);

handle_cast(#subscription_cancellation{id = SubId}, #state{
    event_streams = EvtStms, session_id = SessId} = State) ->
    ?debug("Removing subscription ~p from session ~p", [SubId, SessId]),
    {ok, EvtStm} = maps:find(SubId, EvtStms),
    erlang:exit(EvtStm, shutdown),
    {noreply, State#state{event_streams = maps:remove(SubId, EvtStms)}};

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
handle_info({'EXIT', EvtManSup, shutdown}, #state{event_manager_sup = EvtManSup} = State) ->
    {stop, normal, State};

handle_info(timeout, #state{event_manager_sup = EvtManSup, session_id = SessId} = State) ->
    {ok, EvtStmSup} = event_manager_sup:get_event_stream_sup(EvtManSup),
    {noreply, State#state{
        event_stream_sup = EvtStmSup,
        event_streams = start_event_streams(EvtStmSup, SessId)
    }};

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
    session:update(SessId, #{event_manager => undefined}).

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
%% Starts event streams for durable subscriptions.
%% @end
%%--------------------------------------------------------------------
-spec start_event_streams(EvtStmSup :: pid(), SessId :: session:id()) ->
    Stms :: streams().
start_event_streams(EvtStmSup, SessId) ->
    {ok, Docs} = subscription:list(),
    lists:foldl(fun(#document{key = SubId, value = Sub}, Stms) ->
        {ok, EvtStm} = event_stream_sup:start_event_stream(EvtStmSup, self(), Sub, SessId),
        maps:put(SubId, EvtStm, Stms)
    end, #{}, Docs).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Map given request to file-scope not if the request should be always handled locally.
%% @end
%%--------------------------------------------------------------------
-spec request_to_file_entry_or_provider(#event{} | #subscription{}) ->
    {file, fslogic_worker:file_guid_or_path()} | not_file_context.
request_to_file_entry_or_provider(#event{object = #read_event{file_uuid = FileUUID}}) ->
    {file, {guid, FileUUID}};
request_to_file_entry_or_provider(#event{object = #write_event{file_uuid = FileUUID}}) ->
    {file, {guid, FileUUID}};
request_to_file_entry_or_provider(#event{object = #update_event{}}) ->
    not_file_context;
request_to_file_entry_or_provider(#event{object = #permission_changed_event{file_uuid = _FileUUID}}) ->
    not_file_context;
request_to_file_entry_or_provider(#subscription{object = #file_attr_subscription{file_uuid = FileUUID}}) ->
    {file, {guid, FileUUID}};
request_to_file_entry_or_provider(#subscription{object = #file_location_subscription{file_uuid = FileUUID}}) ->
    {file, {guid, FileUUID}};
request_to_file_entry_or_provider(#subscription{object = #permission_changed_subscription{file_uuid = FileUUID}}) ->
    {file, {guid, FileUUID}};
request_to_file_entry_or_provider(_) ->
    not_file_context.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle request locally using given function or reroute to remote provider.
%% @end
%%--------------------------------------------------------------------
-spec handle_or_reroute(RequestMessage :: term(),
    RequestContext :: {file, fslogic_worker:file_guid_or_path()} | {provider, oneprovider:id()} | not_file_context,
    SessId :: session:id(),
    HandleLocallyFun :: fun((RequestMessage :: term(), NewProvMap :: #{},
    IsRerouted :: boolean()) -> term()), ProvMap :: #{}) -> term().
handle_or_reroute(Msg, _, undefined, HandleLocallyFun, ProvMap) ->
    HandleLocallyFun(Msg, ProvMap, false);
handle_or_reroute(#flush_events{context = Context, notify = NotifyFun} = RequestMessage,
    {provider, ProviderId}, SessId, HandleLocallyFun, ProvMap) ->
    {ok, #document{value = #session{auth = Auth, identity = #identity{}}}} = session:get(SessId),
    case oneprovider:get_provider_id() of
        ProviderId ->
            HandleLocallyFun(RequestMessage, ProvMap, false);
        _ ->
            ClientMsg =
                #client_message{
                    message_stream = #message_stream{stream_id = sequencer:term_to_stream_id(Context)},
                    message_body = RequestMessage,
                    proxy_session_id = SessId,
                    proxy_session_auth = Auth
                },
            Ref = session_manager:get_provider_session_id(outgoing, ProviderId),
            RequestTranslator = spawn(
                fun() ->
                    receive
                        #server_message{message_body = #status{}} = Msg ->
                            NotifyFun(Msg)
                    after timer:minutes(10) ->
                        ok
                    end
                end),
            provider_communicator:communicate_async(ClientMsg, Ref, RequestTranslator),
            HandleLocallyFun(RequestMessage, ProvMap, true)
    end;
handle_or_reroute(RequestMessage, {file, Entry}, SessId, HandleLocallyFun, ProvMap) ->
    {ok, #document{value = #session{auth = Auth, identity = #identity{user_id = UserId}}}} = session:get(SessId),
    UserRootDir = fslogic_uuid:user_root_dir_uuid(UserId),
    case file_meta:to_uuid(Entry) of
        {ok, UserRootDir} ->
            HandleLocallyFun(RequestMessage, ProvMap, false);
        {ok, FileUUID} ->
            CurrentTime = erlang:system_time(seconds),
            {NewProvMap, ProviderIdsFinal} =
                case maps:get(Entry, ProvMap, undefined) of
                    {CachedTime, Mapped} when CachedTime + ?ENTRY_TO_PROVIDER_MAPPING_CACHE_LIFETIME_SECONDS > CurrentTime ->
                        {ProvMap, Mapped};
                    _ ->
                        SpaceId = fslogic_spaces:get_space_id(Entry),
                        {ok, #document{value = #space_info{providers = ProviderIds}}} = space_info:get_or_fetch(SessId, SpaceId, UserId),
                        {maps:put(Entry, {erlang:system_time(seconds), ProviderIds}, ProvMap), ProviderIds}
                end,
            case {ProviderIdsFinal, lists:member(oneprovider:get_provider_id(), ProviderIdsFinal)} of
                {_, true} ->
                    case file_meta:get(FileUUID) of
                        {error, {not_found, file_meta}} ->
                            {ok, NewGuid} = file_meta:get_guid_from_phantom_file(FileUUID),
                            UpdatedRequest = change_file_in_message(RequestMessage, NewGuid),
                            handle_or_reroute(UpdatedRequest, {file, {guid, NewGuid}},
                                SessId, HandleLocallyFun, ProvMap);
                        _ ->
                            HandleLocallyFun(RequestMessage, NewProvMap, false)
                    end;
                {[H | _], false} ->
                    provider_communicator:stream(sequencer:term_to_stream_id(FileUUID), #client_message{
                        message_body = RequestMessage,
                        proxy_session_id = SessId,
                        proxy_session_auth = Auth
                    }, session_manager:get_provider_session_id(outgoing, H), 1),
                    HandleLocallyFun(RequestMessage, NewProvMap, true);
                {[], _} ->
                    throw(unsupported_space)
            end
    end;
handle_or_reroute(Msg, _, _, HandleLocallyFun, ProvMap) ->
    HandleLocallyFun(Msg, ProvMap, false).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Changes target GUID of given event
%% @end
%%--------------------------------------------------------------------
-spec change_file_in_message(any(), fslogic_worker:file_guid()) -> any().
change_file_in_message(#events{events = [#event{object = #read_event{} = Object} = Msg]}, GUID) ->
    #events{events = [Msg#event{key = GUID, object = Object#read_event{file_uuid = GUID}}]};
change_file_in_message(#events{events = [#event{object = #write_event{} = Object} = Msg]}, GUID) ->
    #events{events = [Msg#event{key = GUID, object = Object#write_event{file_uuid = GUID}}]};
change_file_in_message(#subscription{object = #file_attr_subscription{} = Object} = Sub, GUID) ->
    Sub#subscription{object = Object#file_attr_subscription{file_uuid = GUID}};
change_file_in_message(#subscription{object = #file_location_subscription{} = Object} = Sub, GUID) ->
    Sub#subscription{object = Object#file_location_subscription{file_uuid = GUID}};
change_file_in_message(#subscription{object = #permission_changed_subscription{} = Object} = Sub, GUID) ->
    Sub#subscription{object = Object#permission_changed_subscription{file_uuid = GUID}}.
