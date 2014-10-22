%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: @TODO: write me
%% @end
%% ===================================================================

-module(gateway_connection_manager).
-behavior(gen_server).

-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/gateway/registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

-define(CONNECTION_TIMEOUT, timer:seconds(10)).

-record(cmstate, {
    connections = #{} :: map()
}).

-export([start_link/1]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================

-spec start_link(Number) -> Result when
    Number :: non_neg_integer(),
    Result :: {ok,Pid} | ignore | {error,Error},
     Pid :: pid(),
     Error :: {already_started,Pid} | term().
start_link(Number) ->
    gen_server:start_link(?MODULE, Number, []).


-spec init(Number) -> Result when
    Number :: integer(),
    Result :: {ok,State} | {ok,State,Timeout} | {ok,State,hibernate}
        | {stop,Reason} | ignore,
     State :: #cmstate{},
     Timeout :: timeout(),
     Reason :: term().
init(Number) ->
    gen_server:cast(?GATEWAY_DISPATCHER, {register_connection_manager, Number, self()}),
    {ok, #cmstate{}}.


-spec handle_call(Request, From, State) -> Result when
    Request :: term(),
    From :: {pid(),any()},
    State :: #cmstate{},
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
    {noreply, State}.


-spec handle_cast(Request, State) -> Result when
    Request :: term(),
    State :: #cmstate{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
        | {noreply,NewState,hibernate}
        | {stop,Reason,NewState},
     NewState :: term(),
     Timeout :: timeout(),
     Reason :: term().
handle_cast({send, Data, Pid, Remote}, State) ->
    ?warning("handle_cast(~p, ~p)", [{send, Data, Pid, Remote}, State]),
    Self = self(),
    spawn_link(fun() ->
        Result = case maps:find(Remote, State#cmstate.connections) of
            error ->
                case gen_tcp:connect(Remote, ?gw_port, [binary, {packet, 4}], ?CONNECTION_TIMEOUT) of
                    {ok, Socket} ->
                        case gen_tcp:controlling_process(Socket, Self) of
                            ok -> {ok, Socket};
                            {error, Reason} ->
                                gen_tcp:close(Socket),
                                {error, Reason}
                        end;
                    Else -> Else
                end;
            Else -> Else
        end,
        gen_server:cast(Self, {send_internal, Data, Pid, Remote, Result})
    end),
    {noreply, State};

handle_cast({send_internal, Data, Pid, Remote, CreationResult}, State) ->
    ?warning("handle_cast(~p, ~p)", [{send_internal, Data, Pid, Remote, CreationResult}, State]),
    Connections = State#cmstate.connections,

    FoundSocket = maps:find(Remote, Connections),
    Socket =
        case {CreationResult, FoundSocket} of
                 {{error,  Reason}, error}           -> {error, Reason};
                 {{error, _Reason}, {ok, OldSocket}} -> OldSocket;
                 {{ok,  NewSocket}, error}           -> NewSocket;
                 {{ok,    ASocket}, {ok, ASocket}}   -> ASocket;
                 {{ok,  NewSocket}, {ok, OldSocket}} ->
                     gen_tcp:close(NewSocket),
                     OldSocket
        end,

    case Socket of
        {error, Reason1} ->
            Pid ! {error, {request_connect_error, Reason1}},
            {noreply, State};

        _ ->
            case gen_tcp:send(Socket, term_to_binary({Data, Pid})) of
                {error, Reason2} ->
                    Pid ! {error, {request_send_error, Reason2}},
                    {noreply, State#cmstate{connections = maps:remove(Remote, Connections)}};
                ok ->
                    inet:setopts(Socket, [{active, once}]),
                    {noreply, State#cmstate{connections = maps:put(Remote, Socket, Connections)}}
            end
    end;

handle_cast(_Request, State) ->
    {noreply, State}.


-spec handle_info(Info, State) -> Result when
    Info :: timeout | term(),
    State :: #cmstate{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
        | {noreply,NewState,hibernate}
        | {stop,Reason,NewState},
     NewState :: term(),
     Timeout :: timeout(),
     Reason :: normal | term().
handle_info({tcp, Socket, Data}, State) ->
    case binary_to_term(Data) of
        {ReplyType, Data, Pid} -> Pid ! {ReplyType, Data};
        _ -> ok
    end,
    inet:setopts(Socket, {active, once}),
    {noreply, State};

handle_info({tcp_closed, Socket}, State) ->
    NewConnections = remove_value(Socket, State#cmstate.connections),
    {noreply, State#cmstate{connections = NewConnections}};

handle_info({tcp_error, Socket, _Reason}, State) ->
    %% We don't know which process (if any) wanted to connect
    %% through the socket, so we just let the gateway request time
    %% out. It happens.
    gen_tcp:close(Socket),
    NewConnections = remove_value(Socket, State#cmstate.connections),
    {noreply, State#cmstate{connections = NewConnections}};

handle_info(_Info, State) ->
    {noreply, State}.


-spec terminate(Reason, State) -> IgnoredResult when
    Reason :: normal | shutdown | {shutdown,term()} | term(),
    State :: #cmstate{},
    IgnoredResult :: any().
terminate(_Reason, State) ->
    lists:foreach(fun gen_tcp:close/1, maps:values(State)).


-spec code_change(OldVsn, State, Extra) -> {ok, NewState} | {error, Reason} when
    OldVsn :: Vsn | {down, Vsn},
     Vsn :: term(),
    State :: #cmstate{},
    Extra :: term(),
    NewState :: #cmstate{},
    Reason :: term().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================


-spec remove_value(Value :: term(), Map :: map()) -> map().
remove_value(Value, Map) ->
    maps:fold(fun
        (_Key, Value_, Acc) when Value_ =:= Value -> Acc;
        (Key, Value_, Acc) -> maps:put(Key, Value_, Acc)
    end, #{}, Map).
