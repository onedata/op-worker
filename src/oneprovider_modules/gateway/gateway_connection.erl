%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc gateway_connection handles a single connection (socket) to a remote
%% node. The module is responsible for sending and receiving data through the
%% socket, and completing the requests when data has been received.
%% The connection is closed after a period of inactivity.
%% @end
%% ===================================================================

-module(gateway_connection).
-author("Konrad Zemek").
-behavior(gen_server).

-include("oneproxy_pb.hrl").
-include("oneprovider_modules/dao/dao_vfs.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/gateway/registered_names.hrl").
-include("registered_names.hrl").

-record(gwcstate, {
    remote :: {inet:ip_address(), inet:port_number()},
    connection_manager :: pid(),
    socket :: inet:socket(),
    waiting_requests :: ets:tid()
}).

-define(CONNECTION_TIMEOUT, timer:seconds(10)).
-define(REQUEST_COMPLETION_TIMEOUT, timer:seconds(10)).

-export([start_link/3]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).


%% ====================================================================
%% API functions
%% ====================================================================


%% start_link/3
%% ====================================================================
%% @doc Starts gateway connection gen_server.
-spec start_link(Remote, Local, ConnectionManager) -> Result when
    Remote :: {inet:ip_address(), inet:port_number()},
    Local :: inet:ip_address(),
    ConnectionManager :: pid(),
    Result :: {ok,Pid} | ignore | {error,Error},
     Pid :: pid(),
     Error :: {already_started,Pid} | term().
%% ====================================================================
start_link(Remote, Local, ConnectionManager) ->
    gen_server:start_link(?MODULE, {Remote, Local, ConnectionManager}, []).


%% init/1
%% ====================================================================
%% @doc Initializes gateway connection, including opening sockets and initializing
%% oneproxy state.
%% @end
%% @see gen_server
-spec init({Remote, Local, ConnectionManager}) -> Result when
    Remote :: {inet:ip_address(), inet:port_number()},
    Local :: inet:ip_address(),
    ConnectionManager :: pid(),
    Result :: {ok,State} | {ok,State,Timeout} | {ok,State,hibernate}
        | {stop,Reason} | ignore,
     State :: #gwcstate{},
     Timeout :: timeout(),
     Reason :: term().
%% ====================================================================
init({Remote, Local, ConnectionManager}) ->
    process_flag(trap_exit, true),
    TID = ets:new(waiting_requests, [private]),

    {ok, GwProxyPort} = application:get_env(?APP_Name, gateway_proxy_port),
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, GwProxyPort,
        [binary, {packet, 4}, {active, once}, {ifaddr, Local}], ?CONNECTION_TIMEOUT),

    {IP, Port} = Remote,
    ProxyInit = #proxyinit{host = inet:ntoa(IP), port = integer_to_list(Port)},

    ok = gen_tcp:send(Socket, oneproxy_pb:encode_proxyinit(ProxyInit)),
    State = #gwcstate{remote = Remote, socket = Socket, connection_manager = ConnectionManager, waiting_requests = TID},
    {ok, State, ?connection_close_timeout}.


%% handle_call/3
%% ====================================================================
%% @doc Handles a call.
%% @see gen_server
-spec handle_call(Request, From, State) -> Result when
    Request :: term(),
    From :: {pid(),any()},
    State :: #gwcstate{},
    Result :: {reply,Reply,NewState} | {reply,Reply,NewState,Timeout}
        | {reply,Reply,NewState,hibernate}
        | {noreply,NewState} | {noreply,NewState,Timeout}
        | {noreply,NewState,hibernate}
        | {stop,Reason,Reply,NewState} | {stop,Reason,NewState},
     Reply :: term(),
     NewState :: term(),
     Timeout :: timeout(),
     Reason :: term().
%% ====================================================================
handle_call(_Request, _From, State) ->
    ?log_call(_Request),
    {noreply, State, ?connection_close_timeout}.


%% handle_cast/3
%% ====================================================================
%% @doc Handles a cast. The #fetch message is processed and passed to the socket.
%% @see gen_server
-spec handle_cast(Request, State) -> Result when
    Request :: term(),
    State :: #gwcstate{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
        | {noreply,NewState,hibernate}
        | {stop,Reason,NewState},
     NewState :: term(),
     Timeout :: timeout(),
     Reason :: term().
%% ====================================================================
handle_cast(#gw_fetch{} = Action, #gwcstate{socket = Socket, waiting_requests = TID} = State) ->
    #gw_fetch{offset = Offset, size = Size, file_id = FileId} = Action,
    Request = #fetchrequest{offset = Offset, size = Size, file_id = FileId},

    Data = gwproto_pb:encode_fetchrequest(Request),
    case gen_tcp:send(Socket, Data) of
        {error, Reason} -> {stop, Reason, State};
        ok ->
            Hash = gateway:compute_request_hash(Data),
            Timer = erlang:send_after(?REQUEST_COMPLETION_TIMEOUT, self(), {request_timeout, Hash}),
            ets:insert(TID, {Hash, Action, Timer}),
            {noreply, State, ?connection_close_timeout}
    end;

handle_cast(_Request, State) ->
    ?log_call(_Request),
    {noreply, State}.


%% handle_info/3
%% ====================================================================
%% @doc Handles messages. Mainly handles messages from socket in active mode.
%% @see gen_server
-spec handle_info(Info, State) -> Result when
    Info :: timeout | term(),
    State :: #gwcstate{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
        | {noreply,NewState,hibernate}
        | {stop,Reason,NewState},
     NewState :: term(),
     Timeout :: timeout(),
     Reason :: normal | term().
%% ====================================================================
handle_info({tcp, Socket, Data}, #gwcstate{waiting_requests = TID} = State) ->
    ok = inet:setopts(Socket, [{active, once}]),
    try
        Reply = gwproto_pb:decode_fetchreply(Data),
        complete_request(TID, Reply)
    catch
        Error:Reason -> ?warning_stacktrace("~p: Couldn't decode reply: {~p, ~p}", [?MODULE, Error, Reason])
    end,
    {noreply, State, ?connection_close_timeout};

handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};

handle_info({tcp_error, _Socket, Reason}, State) ->
    {stop, Reason, State};

handle_info({request_timeout, Hash}, #gwcstate{waiting_requests = TID} = State) ->
    case ets:lookup(TID, Hash) of
        [] -> ignore;
        [{_, Action, _}] ->
            ets:delete(TID, Hash),
            gateway:notify(fetch_error, timeout, Action)
    end,
    {noreply, State};

handle_info(timeout, State) ->
    {stop, normal, State};

handle_info(_Request, State) ->
    ?log_call(_Request),
    {noreply, State, ?connection_close_timeout}.


%% terminate/2
%% ====================================================================
%% @doc Cleans up any state associated with the connection.
%% @see gen_server
-spec terminate(Reason, State) -> IgnoredResult when
    Reason :: normal | shutdown | {shutdown,term()} | term(),
    State :: #gwcstate{},
    IgnoredResult :: any().
%% ====================================================================
terminate(Reason, #gwcstate{remote = Remote, socket = Socket, connection_manager = CM, waiting_requests = TID} = State) ->
    ?log_terminate(Reason, State),

    NotifyReason =
        case Reason of
            normal -> closed;
            _ ->
                gen_tcp:close(Socket),
                Reason
        end,

    gen_server:cast(CM, {connection_closed, Remote}),
    lists:foreach(
        fun({_, Action, _}) ->
            gateway:notify(fetch_error, {send_error, NotifyReason}, Action)
        end,
        ets:tab2list(TID)).


%% code_change/3
%% ====================================================================
%% @doc Performs any actions necessary on code change.
%% @see gen_server
-spec code_change(OldVsn, State, Extra) -> {ok, NewState} | {error, Reason} when
    OldVsn :: Vsn | {down, Vsn},
     Vsn :: term(),
    State :: #gwcstate{},
    Extra :: term(),
    NewState :: #gwcstate{},
    Reason :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
    {ok, State, ?connection_close_timeout}.


%% ====================================================================
%% Internal functions
%% ====================================================================


%% complete_request/2
%% ====================================================================
%% @doc Completes a fetch request, saving data to a file and notifying a process
%% that registered itself for notifications.
%% @end
-spec complete_request(TID :: ets:tid(), Reply :: #fetchreply{}) -> ok.
%% ====================================================================
complete_request(TID, #fetchreply{content = Content, request_hash = RequestHash}) ->
    case ets:lookup(TID, RequestHash) of
        [] -> ignore, ?warning("Ignored ~p", [Content]);
        [{_, #gw_fetch{} = Action, Timer}] ->
            erlang:cancel_timer(Timer),

            case Content of
                undefined -> gateway:notify(fetch_complete, 0, Action);
                _ ->
                    #gw_fetch{file_id = FileId, offset = Offset, size = RequestedSize} = Action,
                    Size = erlang:min(byte_size(Content), RequestedSize),
                    Data = binary_part(Content, 0, Size),

                    case logical_files_manager:write({uuid, FileId}, Offset, Data, no_events) of
                        {Error, Reason} -> gateway:notify(Error, Reason, Action);
                        Val -> gateway:notify(fetch_complete, Val, Action)
                    end
            end,

            ets:delete(TID, RequestHash)
    end,
    ok.
