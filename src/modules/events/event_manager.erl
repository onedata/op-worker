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

-type streams() :: #{event_stream:key() => pid()}.
-type subscriptions() :: #{subscription:id() => {local, event_stream:key()} |
{remote, oneprovider:id()}}.
-type providers() :: #{file_meta:uuid() => oneprovider:id()}.
-type ctx() :: event_type:ctx() | subscription_type:ctx().

%% event manager state:
%% session_id               - ID of a session associated with this event manager
%% event_manager_sup        - pid of an event manager supervisor
%% event_stream_sup         - pid of an event stream supervisor
%% event_streams            - mapping from a subscription ID to an event stream pid
%% providers    - cache that maps file to provider that shall handle the event
-record(state, {
    session_id :: undefined | session:id(),
    manager_sup :: undefined | pid(),
    streams_sup :: undefined | pid(),
    streams = #{} :: streams(),
    subscriptions = #{} :: subscriptions(),
    providers = #{} :: providers()
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

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private @doc
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
    {ok, SessId} = session:update(SessId, #{event_manager => self()}),
    {ok, #state{manager_sup = MgrSup, session_id = SessId}, 0}.

%%--------------------------------------------------------------------
%% @private @doc
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
%% @private @doc
%% Wraps cast messages' handlers.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_cast(Request, State) ->
    try
        ProviderId = oneprovider:get_provider_id(),
        case get_provider(Request, State) of
            {ProviderId, RequestCtx, NewState} ->
                handle_locally(Request, RequestCtx, NewState);
            {RemoteProviderId, _RequestCtx, NewState} ->
                handle_remotely(Request, RemoteProviderId, NewState)
        end
    catch
        _:Reason2 ->
            ?error_stacktrace("Cannot process request ~p due to: ~p", [Request, Reason2]),
            {noreply, State}
    end.

%%--------------------------------------------------------------------
%% @private @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_info({'EXIT', MgrSup, shutdown}, #state{manager_sup = MgrSup} = State) ->
    {stop, normal, State};

handle_info(timeout, #state{manager_sup = MgrSup, session_id = SessId} = State) ->
    {ok, StmsSup} = event_manager_sup:get_event_stream_sup(MgrSup),
    {Stms, Subs} = start_event_streams(StmsSup, SessId),
    {noreply, State#state{
        streams_sup = StmsSup,
        streams = Stms,
        subscriptions = Subs
    }};

handle_info(Info, State) ->
    ?log_bad_request(Info),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private @doc
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
%% @private @doc
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
%% @private @doc
%% Returns ID of a provider responsible for request handling in given context.
%% @end
%%--------------------------------------------------------------------
-spec get_provider(Request :: term(), State :: #state{}) ->
    {ProviderId :: oneprovider:id(), Ctx :: ctx(), NewState :: #state{}} |
    no_return().
get_provider(#flush_events{provider_id = ProviderId}, State) ->
    {ProviderId, undefined, State};

get_provider(Request, #state{providers = Providers} = State) ->
    RequestCtx = get_context(Request),
    case RequestCtx of
        undefined ->
            {oneprovider:get_provider_id(), RequestCtx, State};
        {file, FileUuid} ->
            case maps:find(FileUuid, Providers) of
                {ok, Provider} ->
                    {Provider, RequestCtx, State};
                error ->
                    Provider = get_provider_for_file({guid, FileUuid}, State),
                    {Provider, RequestCtx, State#state{
                        providers = maps:put(FileUuid, Provider, Providers)
                    }}
            end
    end.

%%--------------------------------------------------------------------
%% @private @doc
%% Returns ID of a provider responsible for handling request associated with
%% a file.
%% @end
%%--------------------------------------------------------------------
-spec get_provider_for_file(Entry :: {guid, FileUuid :: file_meta:uuid()},
    State :: #state{}) -> ProviderId :: oneprovider:id() | no_return().
get_provider_for_file(Entry, #state{session_id = SessId}) ->
    {ok, UserId} = session:get_user_id(SessId),
    UserRootDir = fslogic_uuid:user_root_dir_uuid(UserId),
    case file_meta:to_uuid(Entry) of
        {ok, UserRootDir} ->
            oneprovider:get_provider_id();
        {ok, _} ->
            ProviderId = oneprovider:get_provider_id(),
            SpaceId = fslogic_spaces:get_space_id(Entry),
            {ok, #document{value = #od_space{providers = ProviderIds}}} =
                od_space:get_or_fetch(SessId, SpaceId, UserId),
            case {ProviderIds, lists:member(ProviderId, ProviderIds)} of
                {_, true} -> ProviderId;
                {[RemoteProviderId | _], _} -> RemoteProviderId;
                {[], _} -> throw(unsupported_space)
            end
    end.

%%--------------------------------------------------------------------
%% @private @doc
%% Handles request locally and if necessary changes request file context.
%% @end
%%--------------------------------------------------------------------
-spec handle_locally(Request :: term(), Ctx :: ctx(), State :: #state{}) ->
    {noreply, NewState :: #state{}}.
handle_locally(Request, undefined, State) ->
    handle_locally(Request, State);

handle_locally(Request, {file, FileGuid}, State) ->
    {ok, FileUuid} = file_meta:to_uuid({guid, FileGuid}),
    case file_meta:get(FileUuid) of
        {error, {not_found, file_meta}} ->
            case file_meta:get_guid_from_phantom_file(FileUuid) of
                {ok, NewFileGuid} ->
                    NewRequest = update_context(Request, {file, NewFileGuid}),
                    handle_cast(NewRequest, State);
                {error, {not_found, _}} ->
                    {noreply, State}
            end;
        _ ->
            handle_locally(Request, State)
    end.

%%--------------------------------------------------------------------
%% @private @doc
%% Handles request locally.
%% @end
%%--------------------------------------------------------------------
-spec handle_locally(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}}.
handle_locally({register_stream, StmKey, Stm}, #state{streams = Stms} = State) ->
    {noreply, State#state{streams = maps:put(StmKey, Stm, Stms)}};

handle_locally({unregister_stream, StmKey}, #state{streams = Stms} = State) ->
    {noreply, State#state{streams = maps:remove(StmKey, Stms)}};

handle_locally(#event{} = Evt, #state{streams = Stms} = State) ->
    StmKey = event_type:get_stream_key(Evt),
    Stm = maps:get(StmKey, Stms, undefined),
    gen_server2:cast(Stm, Evt),
    {noreply, State};

handle_locally(#flush_events{} = Request, #state{} = State) ->
    #flush_events{subscription_id = SubId, notify = NotifyFun} = Request,
    #state{streams = Stms, subscriptions = Subs} = State,
    {_, StmKey} = maps:get(SubId, Subs, {local, undefined}),
    Stm = maps:get(StmKey, Stms, undefined),
    gen_server2:cast(Stm, {flush, NotifyFun}),
    {noreply, State};

handle_locally(#subscription{id = Id} = Sub, #state{} = State) ->
    #state{
        streams_sup = StmsSup,
        streams = Stms,
        subscriptions = Subs,
        session_id = SessId
    } = State,
    StmKey = subscription_type:get_stream_key(Sub),
    NewStms = case maps:find(StmKey, Stms) of
        {ok, Stm} ->
            gen_server2:cast(Stm, {add_subscription, Sub}),
            Stms;
        error ->
            {ok, Stm} = event_stream_sup:start_stream(StmsSup, self(), Sub, SessId),
            maps:put(StmKey, Stm, Stms)
    end,
    {noreply, State#state{
        streams = NewStms,
        subscriptions = maps:put(Id, {local, StmKey}, Subs)
    }};

handle_locally(#subscription_cancellation{id = SubId} = Can, #state{} = State) ->
    #state{streams = Stms, subscriptions = Subs} = State,
    case maps:get(SubId, Subs, {local, undefined}) of
        {local, StmKey} ->
            Stm = maps:get(StmKey, Stms, undefiend),
            gen_server2:cast(Stm, {remove_subscription, SubId});
        {remote, ProviderId} ->
            handle_remotely(Can, ProviderId, State)
    end,
    {noreply, State#state{subscriptions = maps:remove(SubId, Subs)}};

handle_locally(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private @doc
%% Forwards request to the remote provider.
%% @end
%%--------------------------------------------------------------------
-spec handle_remotely(Request :: term(), ProviderId :: oneprovider:id(),
    State :: #state{}) -> {noreply, NewState :: #state{}}.
handle_remotely(#flush_events{} = Request, ProviderId, #state{} = State) ->
    #flush_events{context = Context, notify = Notify} = Request,
    #state{session_id = SessId} = State,
    {ok, Auth} = session:get_auth(SessId),
    ClientMsg = #client_message{
        message_stream = #message_stream{
            stream_id = sequencer:term_to_stream_id(Context)
        },
        message_body = Request,
        proxy_session_id = SessId,
        proxy_session_auth = Auth
    },
    Ref = session_manager:get_provider_session_id(outgoing, ProviderId),
    RequestTranslator = spawn(fun() ->
        receive
            #server_message{message_body = #status{}} = Msg ->
                Notify(Msg)
        after timer:minutes(10) ->
            ok
        end
    end),
    provider_communicator:communicate_async(ClientMsg, Ref, RequestTranslator),
    {noreply, State};

handle_remotely(#event{} = Evt, ProviderId, State) ->
    handle_remotely(#events{events = [Evt]}, ProviderId, State);

handle_remotely(Request, ProviderId, #state{session_id = SessId} = State) ->
    {file, FileUuid} = get_context(Request),
    StreamId = sequencer:term_to_stream_id(FileUuid),
    {ok, Auth} = session:get_auth(SessId),
    provider_communicator:stream(StreamId, #client_message{
        message_body = Request,
        proxy_session_id = SessId,
        proxy_session_auth = Auth
    }, session_manager:get_provider_session_id(outgoing, ProviderId), 1),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private @doc
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
%% @private @doc
%% Updates request file context.
%% @end
%%--------------------------------------------------------------------
-spec update_context(Request :: term(), Ctx :: ctx()) -> NewRequest :: term().
update_context(#event{} = Evt, Ctx) ->
    event_type:update_context(Evt, Ctx);

update_context(#subscription{} = Sub, Ctx) ->
    subscription_type:update_context(Sub, Ctx);

update_context(Request, _Ctx) ->
    Request.

%%--------------------------------------------------------------------
%% @private @doc
%% Starts event streams for durable subscriptions.
%% @end
%%--------------------------------------------------------------------
-spec start_event_streams(StmsSup :: pid(), SessId :: session:id()) ->
    {Stms :: streams(), Subs :: subscriptions()}.
start_event_streams(StmsSup, SessId) ->
    {ok, Docs} = subscription:list(),

    lists:foldl(fun(#document{value = #subscription{id = Id} = Sub}, {Stms, Subs}) ->
        StmKey = subscription_type:get_stream_key(Sub),
        {ok, Stm} = event_stream_sup:start_stream(StmsSup, self(), Sub, SessId),
        {maps:put(StmKey, Stm, Stms), maps:put(Id, {local, StmKey}, Subs)}
    end, {#{}, #{}}, Docs).
