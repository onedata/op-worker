%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc gateway_protocol_handler is responsible for handling an incoming connection
%% managed by a Ranch server. The module accepts requests from a remote node
%% and replies with a result. The connection is closed after a period of
%% inactivity.
%% @end
%% ===================================================================

-module(gateway_protocol_handler).
-author("Konrad Zemek").
-behavior(ranch_protocol).
-behavior(gen_server).

-record(gwproto_state, {
    socket,
    transport,
    ok,
    closed,
    error
}).

-include("gwproto_pb.hrl").
-include("oneprovider_modules/dao/dao_vfs.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/gateway/registered_names.hrl").

%% ranch_protocol callbacks
-export([start_link/4, init/4]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).


%% ====================================================================
%% API functions
%% ====================================================================


%% start_link/3
%% ====================================================================
%% @doc Starts gateway_protocol_handler gen_server.
start_link(Ref, Socket, Transport, Opts) ->
    proc_lib:start_link(?MODULE, init, [Ref, Socket, Transport, Opts]).


%% init/4
%% ====================================================================
%% @doc Initializes gateway_protocol_handler, accepting the connection
%% and setting socket state.
%% @end
%% @see ranch_protocol
init(Ref, Socket, Transport, _Opts) ->
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{active, once}, {packet, 4}]),
    {Ok, Closed, Error} = Transport:messages(),
    gen_server:enter_loop(?MODULE, [], #gwproto_state{
        socket = Socket,
        transport = Transport,
        ok = Ok,
        closed = Closed,
        error = Error
    }).


%% init/1
%% ====================================================================
%% @doc Initializes gateway_protocol_handler gen_server.
%% @see ranch_protocol
-spec init(State) -> Result when
    Result :: {ok,State} | {ok,State,Timeout} | {ok,State,hibernate}
    | {stop,Reason} | ignore,
    State :: #gwproto_state{},
    Timeout :: timeout(),
    Reason :: term().
init(State) ->
    {ok, State, ?connection_close_timeout}.


%% handle_call/3
%% ====================================================================
%% @doc Handles a call.
%% @see gen_server
-spec handle_call(Request, From, State) -> Result when
    Request :: term(),
    From :: {pid(),any()},
    State :: #gwproto_state{},
    Result :: {reply,Reply,NewState} | {reply,Reply,NewState,Timeout}
    | {reply,Reply,NewState,hibernate}
    | {noreply,NewState} | {noreply,NewState,Timeout}
    | {noreply,NewState,hibernate}
    | {stop,Reason,Reply,NewState} | {stop,Reason,NewState},
    Reply :: term(),
    NewState :: term(),
    Timeout :: timeout(),
    Reason :: term().
handle_call(_Request, _From, State) ->
    ?log_call(_Request),
    {noreply, State, ?connection_close_timeout}.


%% handle_cast/3
%% ====================================================================
%% @doc Handles a cast.
%% @see gen_server
-spec handle_cast(Request, State) -> Result when
    Request :: term(),
    State :: #gwproto_state{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
    | {noreply,NewState,hibernate}
    | {stop,Reason,NewState},
    NewState :: term(),
    Timeout :: timeout(),
    Reason :: term().
handle_cast(_Request, State) ->
    ?log_call(_Request),
    {noreply, State, ?connection_close_timeout}.


%% handle_info/3
%% ====================================================================
%% @doc Handle messages passed from socket in active mode. On data read,
%% processes a request and replies with the result. On error, closes the socket
%% and stops the gen_server.
%% @see gen_server
-spec handle_info(Info, State) -> Result when
    Info :: timeout | term(),
    State :: #gwproto_state{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
    | {noreply,NewState,hibernate}
    | {stop,Reason,NewState},
    NewState :: term(),
    Timeout :: timeout(),
    Reason :: normal | term().
handle_info({Ok, Socket, Data}, State) when Ok =:= State#gwproto_state.ok ->
    #gwproto_state{transport = Transport} = State,
    ok = Transport:setopts(Socket, [{active, once}]),

    #fetchrequest{file_id = FileId, offset = Offset, size = Size} = gwproto_pb:decode_fetchrequest(Data),
    Hash = gateway:compute_request_hash(Data),

    Reply =
        case logical_files_manager:read({uuid, FileId}, Offset, Size, no_events) of
            {ok, Bytes} -> #fetchreply{request_hash = Hash, content = Bytes};
            {error, file_too_small} -> #fetchreply{request_hash = Hash}
        end,

    ok = Transport:send(Socket, gwproto_pb:encode_fetchreply(Reply)),
    {noreply, State, ?connection_close_timeout};

handle_info({Closed, _Socket}, State) when Closed =:= State#gwproto_state.closed ->
    {stop, normal, State};

handle_info({Error, _Socket, Reason}, State) when Error =:= State#gwproto_state.error ->
    {stop, Reason, State};

handle_info(timeout, State) ->
    {stop, normal, State};

handle_info(_Request, State) ->
    ?log_call(_Request),
    {noreply, State, ?connection_close_timeout}.


%% terminate/2
%% ====================================================================
%% @doc Closes the socket associated with the gen_server.
%% @see gen_server
-spec terminate(Reason, State) -> IgnoredResult when
    Reason :: normal | shutdown | {shutdown,term()} | term(),
    State :: #gwproto_state{},
    IgnoredResult :: any().
terminate(_Reason, State) ->
    ?log_terminate(_Reason, State),
    #gwproto_state{transport = Transport, socket = Socket} = State,
    Transport:close(Socket).


%% code_change/3
%% ====================================================================
%% @doc Performs any actions necessary on code change.
%% @see gen_server
-spec code_change(OldVsn, State, Extra) -> {ok, NewState} | {error, Reason} when
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term(),
    State :: #gwproto_state{},
    Extra :: term(),
    NewState :: #gwproto_state{},
    Reason :: term().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================
