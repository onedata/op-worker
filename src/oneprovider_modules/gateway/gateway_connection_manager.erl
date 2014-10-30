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
-author("Konrad Zemek").
-behavior(gen_server).

-include("gwproto_pb.hrl").
-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/gateway/registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

-record(cmstate, {
    number :: non_neg_integer(), %% @TODO: printing purposes only
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
    {ok, #cmstate{number = Number}}.


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
handle_cast(#fetch{remote = Remote, notify = Notify} = Request, State) ->
    case maps:find(Remote, State#cmstate.connections) of
        error ->
            Self = self(),
            spawn(fun() ->
                case gateway_connection_supervisor:start_connection(Remote) of
                    {error, Reason} -> Notify ! {fetch_connect_error, Reason};
                    {ok, Pid} ->
                        gen_server:cast(Self, {internal, Request, {new, Pid}}),
                        {noreply, State}
                end
            end);

        {Remote, ConnectionPid} ->
            handle_cast({internal, Request, {old, ConnectionPid}}, State)
    end;

handle_cast({internal, #fetch{remote = Remote} = Request, {Type, CPid}}, State) ->
    Connections = State#cmstate.connections,
    {NewConnections, ConnectionPid} =
        case Type of
            old -> {Connections, CPid};
            new ->
                FoundSocket = maps:find(Remote, Connections),
                case FoundSocket of
                    error ->
                        {maps:put(Remote, CPid, Connections), CPid};

                    {Remote, OldPid} ->
                        supervisor:terminate_child(?GATEWAY_CONNECTION_SUPERVISOR, CPid),
                        {Connections, OldPid}
                end
        end,

    ok = gen_server:cast(ConnectionPid, Request),
    {noreply, State#cmstate{connections = NewConnections}};

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
