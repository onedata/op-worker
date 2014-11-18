%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc gateway_dispatcher is responsible for distributing requests between
%% registered connection_managers.
%% @end
%% ===================================================================

-module(gateway_dispatcher).
-author("Konrad Zemek").
-behavior(gen_server).

-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/gateway/registered_names.hrl").

-record(cmref, {
    id :: term(),
    addr :: inet:ip_address(),
    pid :: pid()
}).

-record(gwstate, {
    connection_managers = queue:new() :: queue:queue(#cmref{})
}).

-export([start_link/1]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================


%% start_link/1
%% ====================================================================
%% @doc Starts gateway_dispatcher gen_server.
-spec start_link(NetworkInterfaces) -> Result when
    NetworkInterfaces :: [inet:ip_address()],
    Result :: {ok,Pid} | ignore | {error,Error},
     Pid :: pid(),
     Error :: {already_started,Pid} | term().
%% ====================================================================
start_link(NetworkInterfaces) ->
    gen_server:start_link({local, ?GATEWAY_DISPATCHER}, ?MODULE, NetworkInterfaces, []).


%% init/1
%% ====================================================================
%% @doc Initializes gateway dispatcher, including spinning up connection managers
%% for each entry in NetworkInterfaces list.
%% @end
%% @see gen_server
-spec init(NetworkInterfaces) -> Result when
    NetworkInterfaces :: [inet:ip_address()],
    Result :: {ok,State} | {ok,State,Timeout} | {ok,State,hibernate}
        | {stop,Reason} | ignore,
     State :: #gwstate{},
     Timeout :: timeout(),
     Reason :: term().
%% ====================================================================
init(NetworkInterfaces) ->
    process_flag(trap_exit, true),
    ConnectionManagers = lists:map(
        fun(IpAddr) ->
            CMRef = make_ref(),
            {ok, Pid} = gateway_connection_manager_supervisor:start_connection_manager(IpAddr, CMRef),
            #cmref{id = CMRef, addr = IpAddr, pid = Pid}
        end, NetworkInterfaces),
    {ok, #gwstate{connection_managers = queue:from_list(ConnectionManagers)}}.


%% handle_call/3
%% ====================================================================
%% @doc Handles a call.
%% @see gen_server
-spec handle_call(Request, From, State) -> Result when
    Request :: term(),
    From :: {pid(),any()},
    State :: #gwstate{},
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
    {noreply, State}.


%% handle_cast/3
%% ====================================================================
%% @doc Handles a cast. Connection managers register themselves with the
%% dispatcher, and requests are distributed between registered managers.
%% @see gen_server
-spec handle_cast(Request, State) -> Result when
    Request :: term(),
    State :: #gwstate{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
        | {noreply,NewState,hibernate}
        | {stop,Reason,NewState},
     NewState :: term(),
     Timeout :: timeout(),
     Reason :: term().
%% ====================================================================
handle_cast({register_connection_manager, Id, Addr, Pid}, State) ->
    FilteredManagers = queue:filter(
        fun(#cmref{id = Id1}) when Id1 =:= Id -> false;
           (_) -> true
        end, State#gwstate.connection_managers),
    NewCM = #cmref{id = Id, addr = Addr, pid = Pid},
    AugmentedConnectionsManagers = queue:in_r(NewCM, FilteredManagers),
    {noreply, State#gwstate{connection_managers = AugmentedConnectionsManagers}};

handle_cast(#gw_fetch{} = Request, State) ->
    Managers = State#gwstate.connection_managers,
    {{value, #cmref{pid = MgrPid} = Manager}, PoppedManagers} = queue:out(Managers),
    gen_server:cast(MgrPid, Request),
    {noreply, State#gwstate{connection_managers = queue:in(Manager, PoppedManagers)}};

handle_cast(_Request, State) ->
    ?log_call(_Request),
    {noreply, State}.


%% handle_info/3
%% ====================================================================
%% @doc Handles messages. Mainly handles messages from socket in active mode.
%% @see gen_server
-spec handle_info(Info, State) -> Result when
    Info :: timeout | term(),
    State :: #gwstate{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
        | {noreply,NewState,hibernate}
        | {stop,Reason,NewState},
     NewState :: term(),
     Timeout :: timeout(),
     Reason :: normal | term().
%% ====================================================================
handle_info(_Request, State) ->
    ?log_call(_Request),
    {noreply, State}.


%% terminate/2
%% ====================================================================
%% @doc Cleans up any state associated with the dispatcher, including terminating
%% connection managers.
%% @end
%% @see gen_server
-spec terminate(Reason, State) -> IgnoredResult when
    Reason :: normal | shutdown | {shutdown,term()} | term(),
    State :: #gwstate{},
    IgnoredResult :: any().
%% ====================================================================
terminate(_Reason, State) ->
    ?log_terminate(_Reason, State),
    lists:foreach(fun(#cmref{pid = Pid}) ->
        supervisor:terminate_child(?GATEWAY_CONNECTION_MANAGER_SUPERVISOR, Pid)
    end, queue:to_list(State#gwstate.connection_managers)).


%% code_change/3
%% ====================================================================
%% @doc Performs any actions necessary on code change.
%% @see gen_server
-spec code_change(OldVsn, State, Extra) -> {ok, NewState} | {error, Reason} when
    OldVsn :: Vsn | {down, Vsn},
     Vsn :: term(),
    State :: #gwstate{},
    Extra :: term(),
    NewState :: #gwstate{},
    Reason :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================
