%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% gateway_connection handles a single connection (socket) to a remote
%%% node. The module is responsible for sending and receiving data through the
%%% socket, and completing the requests when data has been received.
%%% The connection is closed after a period of inactivity.
%%% @end
%%%-------------------------------------------------------------------
-module(gateway_connection).
-author("Konrad Zemek").
-behavior(gen_server).

-include("global_definitions.hrl").
-include("modules/rtransfer/gateway.hrl").
-include("modules/rtransfer/registered_names.hrl").
-include("timeouts.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

-record(gwcstate, {
    remote :: {inet:ip_address(), inet:port_number()},
    connection_manager :: pid(),
    socket :: inet:socket(),
    waiting_requests :: ets:tid(),
    rtransfer_opts :: [rtransfer:opt()]
}).

%% How many simultaneous operations can be performed per gateway_connection.
-define(connection_load_factor,
    application:get_env(?APP_NAME, gateway_connection_load_factor, 5)).

-define(default_block_size,
    application:get_env(?APP_NAME, rtransfer_block_size, 104857600)).

-export([start_link/4]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).
-export([garbage_collect/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts gateway connection gen_server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(RtransferOpts, Remote, Local, ConnectionManager) -> Result when
    Remote :: {inet:ip_address(), inet:port_number()},
    Local :: inet:ip_address(),
    ConnectionManager :: pid(),
    RtransferOpts :: [rtransfer:opt()],
    Result :: {ok, Pid} | ignore | {error, Error},
    Pid :: pid(),
    Error :: {already_started, Pid} | term().
start_link(RtransferOpts, Remote, Local, ConnectionManager) ->
    gen_server2:start_link(?MODULE, {Remote, Local, ConnectionManager, RtransferOpts}, []).


%%--------------------------------------------------------------------
%% @doc
%% Initializes gateway connection, including opening sockets and initializing
%% oneproxy state.
%% @end
%%--------------------------------------------------------------------
-spec init({Remote, Local, ConnectionManager, RtransferOpts}) -> Result when
    Remote :: {inet:ip_address(), inet:port_number()},
    Local :: inet:ip_address(),
    ConnectionManager :: pid(),
    RtransferOpts :: [rtransfer:opt()],
    Result :: {ok, State} | {ok, State, Timeout} | {ok, State, hibernate}
    | {stop, Reason} | ignore,
    State :: #gwcstate{},
    Timeout :: timeout(),
    Reason :: term().
init({Remote, Local, ConnectionManager, RtransferOpts}) ->
    process_flag(trap_exit, true),
    TID = ets:new(waiting_requests, [private]),

    {IP, Port} = Remote,
    {ok, Socket} = gen_tcp:connect(IP, Port,
        [binary, {packet, 4}, {active, once}, {ifaddr, Local}],
        ?CONNECTION_TIMEOUT),

    State = #gwcstate{remote = Remote, socket = Socket,
        connection_manager = ConnectionManager, waiting_requests = TID,
        rtransfer_opts = RtransferOpts},

    {ok, State, ?connection_close_timeout}.


%%--------------------------------------------------------------------
%% @doc
%% Handles a call.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request, From, State) -> Result when
    Request :: term(),
    From :: {pid(), any()},
    State :: #gwcstate{},
    Result :: {reply, Reply, NewState} | {reply, Reply, NewState, Timeout}
    | {reply, Reply, NewState, hibernate}
    | {noreply, NewState} | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason, Reply, NewState} | {stop, Reason, NewState},
    Reply :: term(),
    NewState :: term(),
    Timeout :: timeout(),
    Reason :: term().
handle_call(_Request, _From, State) ->
    ?log_call(_Request),
    {noreply, State, ?connection_close_timeout}.


%%--------------------------------------------------------------------
%% @doc
%% Handles a cast. The #fetch message is processed and passed to the socket.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request, State) -> Result when
    Request :: term(),
    State :: #gwcstate{},
    Result :: {noreply, NewState} | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason, NewState},
    NewState :: term(),
    Timeout :: timeout(),
    Reason :: term().
handle_cast(#gw_fetch{} = Action, #gwcstate{socket = Socket, waiting_requests = TID} = State) ->
    #gw_fetch{offset = Offset, size = Size, file_id = FileId} = Action,
    Request = #'FetchRequest'{offset = Offset, size = Size, file_id = FileId},

    Data = messages:encode_msg(Request),
    case gen_tcp:send(Socket, Data) of
        {error, _} = Reason ->
            {stop, Reason, State};

        ok ->
            Hash = gateway:compute_request_hash(Data),
            Timer = erlang:send_after(?REQUEST_COMPLETION_TIMEOUT, self(), {request_timeout, Hash}),
            case ets:insert_new(TID, {Hash, Action, Timer}) of
                true -> ok;
                false ->
                    PrevNotify = case ets:lookup(TID, Hash) of
                        [] -> [];
                        [{_, #gw_fetch{notify = Notify}, PrevTimer}]->
                            erlang:cancel_timer(PrevTimer),
                            Notify
                    end,
                    AllNotify = sets:from_list(Action#gw_fetch.notify ++ PrevNotify),
                    MergedActions = Action#gw_fetch{notify = sets:to_list(AllNotify)},
                    ets:insert(TID, {Hash, MergedActions, Timer})
            end,
            {noreply, State, ?connection_close_timeout}
    end;

handle_cast(_Request, State) ->
    ?log_call(_Request),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @doc
%% Handles messages. Mainly handles messages from socket in active mode.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info, State) -> Result when
    Info :: timeout | term(),
    State :: #gwcstate{},
    Result :: {noreply, NewState} | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason, NewState},
    NewState :: term(),
    Timeout :: timeout(),
    Reason :: normal | term().
handle_info({tcp, Socket, Data}, #gwcstate{} = State) ->
    ok = inet:setopts(Socket, [{active, once}]),
    try
        Reply = messages:decode_msg(Data, 'FetchReply'),
        complete_request(Reply, State)
    catch
        Error:Reason ->
            ?debug_stacktrace("~p: Couldn't decode reply: {~p, ~p}", [?MODULE, Error, Reason])
    end,
    {noreply, State, ?connection_close_timeout};

handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};

handle_info({tcp_error, _Socket, Reason}, State) ->
    {stop, {error, Reason}, State};

handle_info({request_timeout, Hash}, #gwcstate{waiting_requests = TID} = State) ->
    case ets:lookup(TID, Hash) of
        [] -> ignore;
        [{_, Action, _}] ->
            ets:delete(TID, Hash),
            gateway:notify(fetch_error, timeout, Action)
    end,
    {noreply, State};

handle_info(timeout, State) ->
    {stop, timeout, State};

handle_info(_Request, State) ->
    ?log_call(_Request),
    {noreply, State, ?connection_close_timeout}.


%%--------------------------------------------------------------------
%% @doc
%% Cleans up any state associated with the connection.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason, State) -> IgnoredResult when
    Reason :: normal | shutdown | {shutdown, term()} | term(),
    State :: #gwcstate{},
    IgnoredResult :: any().
terminate(Reason, #gwcstate{remote = Remote, socket = Socket, connection_manager = CM, waiting_requests = TID} = State) ->
    ?log_terminate(Reason, State),

    NotifyReason =
        case Reason of
            normal -> normal;
            _ ->
                gen_tcp:close(Socket),
                Reason
        end,

    gen_server2:cast(CM, {connection_closed, Remote}),
    lists:foreach(
        fun({_, Action, _}) ->
            gateway:notify(fetch_error, {send_error, NotifyReason}, Action)
        end,
        ets:tab2list(TID)).


%%--------------------------------------------------------------------
%% @doc
%% Performs any actions necessary on code change.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn, State, Extra) -> {ok, NewState} | {error, Reason} when
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term(),
    State :: #gwcstate{},
    Extra :: term(),
    NewState :: #gwcstate{},
    Reason :: term().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Completes a fetch request, saving data to a file and notifying a process
%% that registered itself for notifications.
%% @end
%%--------------------------------------------------------------------
-spec complete_request(Reply :: #'FetchReply'{}, State :: #gwcstate{}) -> ok.
complete_request(#'FetchReply'{content = Content, request_hash = RequestHash}, State) ->
    TID = State#gwcstate.waiting_requests,
    RtransferOpts = State#gwcstate.rtransfer_opts,

    case ets:lookup(TID, RequestHash) of
        [] -> ignore;
        [{_, #gw_fetch{} = Action, Timer}] ->
            erlang:cancel_timer(Timer),

            case Content of
                undefined -> gateway:notify(fetch_complete, 0, Action);
                _ ->
                    OpenFun = proplists:get_value(open_fun, RtransferOpts),
                    WriteFun = proplists:get_value(write_fun, RtransferOpts),
                    CloseFun = proplists:get_value(close_fun, RtransferOpts),

                    #gw_fetch{file_id = FileId, offset = Offset, size = RequestedSize} = Action,
                    Size = erlang:min(byte_size(Content), RequestedSize),
                    Data = binary_part(Content, 0, Size),

                    case OpenFun(FileId, write) of
                        {ok, Handle} ->
                            %% TODO: loop!
                            %% TODO: {error, {storage
                            NewHandle = case WriteFun(Handle, Offset, Data) of
                                {ok, NH, Wrote} ->
                                    notify_complete(Wrote, Action),
                                    NH;

                                {error, Reason} ->
                                    notify_error(Reason, Action),
                                    Handle
                            end,

                            CloseFun(NewHandle);

                        {error, Reason} ->
                            notify_error(Reason, Action)
                    end
            end,

            ets:delete(TID, RequestHash)
    end,
    ?MODULE:garbage_collect(RtransferOpts),
    ok.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Wrapper for gateway:notify function
%% @end
%%-------------------------------------------------------------------
-spec notify_complete(Wrote :: integer(),  Action :: #gw_fetch{}) -> ok.
notify_complete(Wrote, Action) ->
    gateway:notify(fetch_complete, Wrote, Action).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Wrapper for gateway:notify function.
%% Allows to handle specially specific errors.
%% @end
%%-------------------------------------------------------------------
-spec notify_error(Reason :: term(),  Action :: #gw_fetch{}) -> ok.
notify_error(?ENOSPC, Action) ->
    gateway:notify(fetch_error, ?ENOSPC, Action#gw_fetch{retry=0});
notify_error(Reason, Action) ->
    gateway:notify(fetch_error, Reason, Action).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Does garbage collection of the process
%% @end
%%-------------------------------------------------------------------
-spec garbage_collect([rtransfer:opt()]) -> true.
garbage_collect(RtransferOpts) ->
    case application:get_env(?APP_NAME, rtransfer_gc, sync) of
        sync ->
            erlang:garbage_collect(),
            wait_gc(RtransferOpts);
        async ->
            erlang:garbage_collect();
        _ ->
            true
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Waits until garbage collection frees enough memory.
%% @end
%%-------------------------------------------------------------------
-spec wait_gc([rtransfer:opt()]) -> ok.
wait_gc(RtransferOpts) ->
    wait_gc(1, 20, RtransferOpts).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Waits until garbage collection frees enough memory.
%% @end
%%-------------------------------------------------------------------
-spec wait_gc(non_neg_integer(), non_neg_integer(), [rtransfer:opt()]) -> ok.
wait_gc(N, N, _RtransferOpts) ->
    ok;
wait_gc(N, Max, RtransferOpts) ->
    {binary, BinList} = erlang:process_info(self(), binary),
    Sum = lists:foldl(fun({_, S, _}, Acc) ->
        S + Acc
    end, 0, BinList),
    DefaultBlockSize = ?default_block_size,
    BlockSize = proplists:get_value(block_size,
        RtransferOpts, DefaultBlockSize),
    Max = BlockSize * ?connection_load_factor,
    case Sum < Max of
        true ->
            ok;
        _ ->
            timer:sleep(100 * N),
            wait_gc(N + 1, Max, RtransferOpts)
    end.
