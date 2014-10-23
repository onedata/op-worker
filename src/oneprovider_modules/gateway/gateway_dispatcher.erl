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

-module(gateway_dispatcher).
-behavior(gen_server).

-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/gateway/registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

-record(cmref, {
    number :: non_neg_integer(),
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

-spec start_link(MaxConcurrentConnections) -> Result when
    MaxConcurrentConnections :: non_neg_integer(),
    Result :: {ok,Pid} | ignore | {error,Error},
     Pid :: pid(),
     Error :: {already_started,Pid} | term().
start_link(MaxConcurrentConnections) ->
    gen_server:start_link({local, ?GATEWAY_DISPATCHER}, ?MODULE, MaxConcurrentConnections, []).


-spec init(MaxConcurrentConnections) -> Result when
    MaxConcurrentConnections :: pos_integer(),
    Result :: {ok,State} | {ok,State,Timeout} | {ok,State,hibernate}
        | {stop,Reason} | ignore,
     State :: #gwstate{},
     Timeout :: timeout(),
     Reason :: term().
init(MaxConcurrentConnections) ->
    ConnectionManagers = lists:map(
        fun(Number) ->
            {ok, Pid} = supervisor:start_child(?GATEWAY_CONNECTION_MANAGER_SUPERVISOR, [Number]),
            #cmref{number = Number, pid = Pid}
        end, lists:seq(1, MaxConcurrentConnections)),
    {ok, #gwstate{connection_managers = queue:from_list(ConnectionManagers)}}.


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
handle_call(_Request, _From, State) ->
    {noreply, State}.


-spec handle_cast(Request, State) -> Result when
    Request :: term(),
    State :: #gwstate{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
        | {noreply,NewState,hibernate}
        | {stop,Reason,NewState},
     NewState :: term(),
     Timeout :: timeout(),
     Reason :: term().
handle_cast({register_connection_manager, Number, Pid}, State) ->
    ?debug("handle_cast(~p, ~p)", [{register_connection_manager, Number, Pid}, State]),
    FilteredManagers = queue:filter(
        fun(#cmref{number = Number1}) when Number1 =:= Number -> false;
           (_) -> true
        end, State#gwstate.connection_managers),
    NewCM = #cmref{number = Number, pid = Pid},
    AugmentedConnectionsManagers = queue:in_r(NewCM, FilteredManagers),
    ?debug("handle_cast(~p, State after: ~p)", [{register_connection_manager, Number, Pid}, State#gwstate{connection_managers = AugmentedConnectionsManagers}]),
    {noreply, State#gwstate{connection_managers = AugmentedConnectionsManagers}};

handle_cast({send, _Data, _Pid, _Remote} = Request, State) ->
    ?debug("handle_cast(~p, ~p)", [Request, State]),
    Managers = State#gwstate.connection_managers,
    {{value, #cmref{pid = MgrPid} = Manager}, PoppedManagers} = queue:out(Managers),
    gen_server:cast(MgrPid, Request),
    ?debug("handle_cast(~p, State after: ~p)", [Request, State#gwstate{connection_managers = queue:in(Manager, PoppedManagers)}]),
    {noreply, State#gwstate{connection_managers = queue:in(Manager, PoppedManagers)}}.


-spec handle_info(Info, State) -> Result when
    Info :: timeout | term(),
    State :: #gwstate{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
        | {noreply,NewState,hibernate}
        | {stop,Reason,NewState},
     NewState :: term(),
     Timeout :: timeout(),
     Reason :: normal | term().
handle_info(_Request, State) ->
    {noreply, State}.


-spec terminate(Reason, State) -> IgnoredResult when
    Reason :: normal | shutdown | {shutdown,term()} | term(),
    State :: #gwstate{},
    IgnoredResult :: any().
terminate(_Reason, State) ->
    lists:foreach(
        fun(#cmref{pid = Pid}) ->
            supervisor:terminate_child(?GATEWAY_CONNECTION_MANAGER_SUPERVISOR, Pid)
        end, queue:to_list(State#gwstate.connection_managers)).


-spec code_change(OldVsn, State, Extra) -> {ok, NewState} | {error, Reason} when
    OldVsn :: Vsn | {down, Vsn},
     Vsn :: term(),
    State :: #gwstate{},
    Extra :: term(),
    NewState :: #gwstate{},
    Reason :: term().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================
