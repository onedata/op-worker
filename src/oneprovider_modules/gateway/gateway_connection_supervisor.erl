%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc gateway_connection_supervisor is responsible for supervising
%% gateway_connection gen_servers. Connections are transient, so the supervisor
%% takes a role of an easy startup and shutdown facility for connections.
%% @end
%% ===================================================================

-module(gateway_connection_supervisor).
-author("Konrad Zemek").
-behavior(supervisor).

-include("oneprovider_modules/gateway/registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

-export([start_link/0, start_connection/3]).
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
    supervisor:start_link({local, ?GATEWAY_CONNECTION_SUPERVISOR}, ?MODULE, []).


%% start_connection/3
%% ====================================================================
%% @doc Starts a connection supervised by the supervisor.
-spec start_connection(Remote :: {inet:ip_address(), inet:port_number()},
                       Local :: inet:ip_address(), ConnectionManager :: pid()) ->
    supervisor:startchild_ret().
%% ===================================================================
start_connection(Remote, Local, ConnectionManager) ->
    supervisor:start_child(?GATEWAY_CONNECTION_SUPERVISOR, [Remote, Local, ConnectionManager]).


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
    RestartStrategy = simple_one_for_one,
    MaxR = 3,
    MaxT = timer:minutes(1),
    {ok, {{RestartStrategy, MaxR, MaxT}, [connection_spec()]}}.


%% ====================================================================
%% Internal functions
%% ====================================================================


%% connection_spec/2
%% ====================================================================
%% @doc Creates a supervisor child_spec for a connection child.
-spec connection_spec() -> supervisor:child_spec().
%% ===================================================================
connection_spec() ->
    ChildId = Module = gateway_connection,
    Restart = temporary,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ChildId, {Module, start_link, []}, Restart, ExitTimeout, Type, [Module]}.
