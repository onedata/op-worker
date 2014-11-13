%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc gateway_connection_manager supervises a number of connection managers
%% and a single connection_supervisor. Connection managers are started
%% on-demand by gateway_dispatcher module.
%% @end
%% ===================================================================

-module(gateway_connection_manager_supervisor).
-author("Konrad Zemek").
-behavior(supervisor).

-include("oneprovider_modules/gateway/registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

-export([start_link/0, start_connection_manager/2]).
%% supervisor callbacks
-export([init/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% start_link/0
%% ====================================================================
%% @doc Starts the supervisor.
-spec start_link() -> Result when
    Result :: {ok, pid()} | ignore | {error, Error},
     Error :: {already_started, pid()} | {shutdown, term()} | term().
%% ===================================================================
start_link() ->
    supervisor:start_link({local, ?GATEWAY_CONNECTION_MANAGER_SUPERVISOR}, ?MODULE, []).


%% start_connection_manager/2
%% ====================================================================
%% @doc Starts a connection_manager gen_server supervised by the supervisor.
-spec start_connection_manager(IpAddr :: inet:ip_address(), ManagerId :: term()) ->
    supervisor:startchild_ret().
%% ===================================================================
start_connection_manager(IpAddr, ManagerId) ->
    ChildSpec = connection_manager_spec(IpAddr, ManagerId),
    supervisor:start_child(?GATEWAY_CONNECTION_MANAGER_SUPERVISOR, ChildSpec).


%% init/1
%% ====================================================================
%% @doc Initializes supervisor parameters.
%% @see supervisor
-spec init(Args) -> Result when
    Args :: term(),
    Result :: {ok,{{RestartStrategy,MaxR,MaxT},[ChildSpec]}} | ignore,
     RestartStrategy :: supervisor:strategy(),
     MaxR :: non_neg_integer(),
     MaxT :: pos_integer(),
     ChildSpec :: supervisor:child_spec().
%% ===================================================================
init(_Args) ->
    RestartStrategy = one_for_one,
    MaxR = 3,
    MaxT = timer:minutes(1),
    {ok, {{RestartStrategy, MaxR, MaxT}, [connection_supervisor_spec()]}}.


%% ====================================================================
%% Internal functions
%% ====================================================================


%% connection_manager_spec/2
%% ====================================================================
%% @doc Creates a supervisor child_spec for a connection manager child.
-spec connection_manager_spec(IpAddr :: inet:ip_address(), ManagerId :: term()) ->
    supervisor:child_spec().
%% ===================================================================
connection_manager_spec(IpAddr, ManagerId) ->
    Module = gateway_connection_manager,
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ManagerId, {Module, start_link, [IpAddr, ManagerId]},
        Restart, ExitTimeout, Type, [Module]}.


%% connection_supervisor_spec/2
%% ====================================================================
%% @doc Creates a supervisor child_spec for a connection supervisor child.
-spec connection_supervisor_spec() -> supervisor:child_spec().
%% ===================================================================
connection_supervisor_spec() ->
    ChildId = Module = ?GATEWAY_CONNECTION_SUPERVISOR,
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = supervisor,
    {ChildId, {Module, start_link, []}, Restart, ExitTimeout, Type, [Module]}.
