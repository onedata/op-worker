%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc gateway_dispatcher_supervisor is responsible for supervising
%% a gateway dispatcher and a connection_manager supervisor. The children
%% are restarted using one_for_all strategy to ensure connection_managers
%% are always properly registered with a gateway_dispatcher.
%% @end
%% ===================================================================

-module(gateway_dispatcher_supervisor).
-author("Konrad Zemek").
-behavior(supervisor).

-include("oneprovider_modules/gateway/registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

-export([start_link/1]).
%% supervisor callbacks
-export([init/1]).

%% ====================================================================
%% API functions
%% ====================================================================


%% start_link/0
%% ====================================================================
%% @doc Starts the supervisor.
-spec start_link(NetworkInterfaces) -> Result when
    NetworkInterfaces :: [inet:ip_address()],
    Result :: {ok, pid()} | ignore | {error, Error},
     Error :: {already_started, pid()} | {shutdown, term()} | term().
%% ====================================================================
start_link(NetworkInterfaces) ->
    supervisor:start_link({local, ?GATEWAY_DISPATCHER_SUPERVISOR}, ?MODULE, NetworkInterfaces).


%% init/1
%% ====================================================================
%% @doc Initializes supervisor parameters. The NetworkInterfaces list is passed
%% to a created dispatcher.
%% @see supervisor
-spec init(NetworkInterfaces) -> Result when
    NetworkInterfaces :: [inet:ip_address()],
    Result :: {ok,{{RestartStrategy,MaxR,MaxT},[ChildSpec]}} | ignore,
     RestartStrategy :: supervisor:strategy(),
     MaxR :: non_neg_integer(),
     MaxT :: pos_integer(),
     ChildSpec :: supervisor:child_spec().
%% ====================================================================
init(NetworkInterfaces) ->
    RestartStrategy = one_for_all,
    MaxR = 3,
    MaxT = timer:minutes(1),
    {ok, {{RestartStrategy, MaxR, MaxT},
        [connection_manager_supervisor_spec(),
         dispatcher_spec(NetworkInterfaces),
         rt_priority_queue_spec(),
         rt_map_spec()]}}.


%% ====================================================================
%% Internal functions
%% ====================================================================


%% dispatcher_spec/1
%% ====================================================================
%% @doc Creates a supervisor child_spec for a dispatcher child.
-spec dispatcher_spec(NetworkInterfaces :: [inet:ip_address()]) ->
    supervisor:child_spec().
%% ====================================================================
dispatcher_spec(NetworkInterfaces) ->
    ChildId = Module = gateway_dispatcher,
    Function = {Module, start_link, [NetworkInterfaces]},
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ChildId, Function, Restart, ExitTimeout, Type, [Module]}.


%% connection_manager_supervisor_spec/0
%% ====================================================================
%% @doc Creates a supervisor child_spec for a connection manager supervisor child.
-spec connection_manager_supervisor_spec() -> supervisor:child_spec().
%% ====================================================================
connection_manager_supervisor_spec() ->
    ChildId = Module = gateway_connection_manager_supervisor,
    Function = {Module, start_link, []},
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = supervisor,
    {ChildId, Function, Restart, ExitTimeout, Type, [Module]}.


%% rt_priority_queue_spec/0
%% ====================================================================
%% @doc Creates a supervisor child_spec for a rt_containter child.
-spec rt_priority_queue_spec() -> supervisor:child_spec().
%% ====================================================================
rt_priority_queue_spec() ->
    ChildId = Module = rt_priority_queue,
    Function = {Module, new, [{local, ?GATEWAY_INCOMING_QUEUE}]},
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ChildId, Function, Restart, ExitTimeout, Type, [Module]}.


%% rt_map_spec/0
%% ====================================================================
%% @doc Creates a supervisor child_spec for a rt_containter child.
-spec rt_map_spec() -> supervisor:child_spec().
%% ====================================================================
rt_map_spec() ->
    ChildId = Module = rt_map,
    Function = {Module, new, [{local, ?GATEWAY_NOTIFY_MAP}]},
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ChildId, Function, Restart, ExitTimeout, Type, [Module]}.
