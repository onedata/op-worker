%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% gateway_connection_manager is responsible for managing a simple map
%%% 'remote address' -> 'gateway_connection process'. The manager takes requests
%%% and uses existing connection to fulfill it; if the connection is not open,
%%% the manager starts it.
%%% @end
%%%-------------------------------------------------------------------
-module(gateway_connection_manager).
-author("Konrad Zemek").
-behavior(gen_server).

-include("modules/rtransfer/gateway.hrl").
-include("modules/rtransfer/registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

-record(cmstate, {
    id :: term(),
    addr :: inet:ip_address(),
    connections = dict:new() :: dict:dict(inet:ip_address(), pid())
}).

-export([start_link/2]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts gateway_connection_manager gen_server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(IpAddr, Id) -> Result when
    IpAddr :: inet:ip_address(),
    Id :: term(),
    Result :: {ok,Pid} | ignore | {error,Error},
     Pid :: pid(),
     Error :: {already_started,Pid} | term().
start_link(IpAddr, Id) ->
    gen_server2:start_link(?MODULE, {IpAddr, Id}, []).


%%--------------------------------------------------------------------
%% @doc
%% Initializes a connection_manager, including registering itself with
%% a dispatcher.
%% @end
%%--------------------------------------------------------------------
-spec init({IpAddr, Id}) -> Result when
    IpAddr :: inet:ip_address(),
    Id :: term(),
    Result :: {ok,State} | {ok,State,Timeout} | {ok,State,hibernate}
        | {stop,Reason} | ignore,
     State :: #cmstate{},
     Timeout :: timeout(),
     Reason :: term().
init({IpAddr, Id}) ->
    process_flag(trap_exit, true),
    gen_server2:cast(?GATEWAY_DISPATCHER, {register_connection_manager, Id, IpAddr, self()}),
    {ok, #cmstate{id = Id, addr = IpAddr}}.


%%--------------------------------------------------------------------
%% @doc
%% Handles a call.
%% @end
%%--------------------------------------------------------------------
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
    ?log_call(_Request),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @doc
%% Handles a cast. When a request is received, connection manager first
%% acquires a connection to fulfill it, and then delegates the action to the
%% connection.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request, State) -> Result when
    Request :: term(),
    State :: #cmstate{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
        | {noreply,NewState,hibernate}
        | {stop,Reason,NewState},
     NewState :: term(),
     Timeout :: timeout(),
     Reason :: term().
handle_cast(#gw_fetch{remote = Remote} = Request, #cmstate{addr = Addr} = State) ->
    case dict:find(Remote, State#cmstate.connections) of
        error ->
            Self = self(),
            spawn(fun() ->
                case gateway_connection_supervisor:start_connection(Remote, Addr, Self) of
                    {error, Reason} ->
                        gateway:notify(fetch_error, {connection_error, Reason}, Request);
                    {ok, Pid} ->
                        gen_server2:cast(Self, {internal, Request, {new, Pid}})
                end
            end),
            {noreply, State};

        {ok, ConnectionPid} ->
            handle_cast({internal, Request, {old, ConnectionPid}}, State)
    end;

handle_cast({internal, #gw_fetch{remote = Remote} = Request, {Type, CPid}}, State) ->
    Connections = State#cmstate.connections,
    {NewConnections, ConnectionPid} =
        case Type of
            old -> {Connections, CPid};
            new ->
                case dict:find(Remote, Connections) of
                    error ->
                        {dict:store(Remote, CPid, Connections), CPid};

                    {ok, OldPid} ->
                        supervisor:terminate_child(?GATEWAY_CONNECTION_SUPERVISOR, CPid),
                        {Connections, OldPid}
                end
        end,

    ok = gen_server2:cast(ConnectionPid, Request),
    {noreply, State#cmstate{connections = NewConnections}};

handle_cast({connection_closed, Remote}, State) ->
    {noreply, State#cmstate{connections = dict:erase(Remote, State#cmstate.connections)}};

handle_cast(_Request, State) ->
    ?log_call(_Request),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @doc
%% Handles messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info, State) -> Result when
    Info :: timeout | term(),
    State :: #cmstate{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
        | {noreply,NewState,hibernate}
        | {stop,Reason,NewState},
     NewState :: term(),
     Timeout :: timeout(),
     Reason :: normal | term().
handle_info(_Info, State) ->
    ?log_call(_Info),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @doc
%% Cleans up any state associated with the connection manager.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason, State) -> IgnoredResult when
    Reason :: normal | shutdown | {shutdown,term()} | term(),
    State :: #cmstate{},
    IgnoredResult :: any().
terminate(_Reason, State) ->
    ?log_terminate(_Reason, State),
    dict:map(fun(_, Pid) ->
        supervisor:terminate_child(?GATEWAY_CONNECTION_SUPERVISOR, Pid)
    end, State#cmstate.connections).


%%--------------------------------------------------------------------
%% @doc
%% Performs any actions necessary on code change.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn, State, Extra) -> {ok, NewState} | {error, Reason} when
    OldVsn :: Vsn | {down, Vsn},
     Vsn :: term(),
    State :: #cmstate{},
    Extra :: term(),
    NewState :: #cmstate{},
    Reason :: term().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================
