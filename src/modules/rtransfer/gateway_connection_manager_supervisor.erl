%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% gateway_connection_manager supervises a number of connection managers
%%% and a single connection_supervisor. Connection managers are started
%%% on-demand by gateway_dispatcher module.
%%% @end
%%%-------------------------------------------------------------------
-module(gateway_connection_manager_supervisor).
-author("Konrad Zemek").
-behavior(supervisor).

-include("modules/rtransfer/registered_names.hrl").

-export([start_link/1, start_connection_manager/2]).
%% supervisor callbacks
-export([init/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_link([rtransfer:opt()]) -> Result when
    Result :: {ok, pid()} | ignore | {error, Error},
     Error :: {already_started, pid()} | {shutdown, term()} | term().
start_link(RtransferOpts) ->
    supervisor:start_link({local, ?GATEWAY_CONNECTION_MANAGER_SUPERVISOR},
        ?MODULE, RtransferOpts).


%%--------------------------------------------------------------------
%% @doc
%% Starts a connection_manager gen_server supervised by the supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_connection_manager(IpAddr :: inet:ip_address(), ManagerId :: term()) ->
    supervisor:startchild_ret().
start_connection_manager(IpAddr, ManagerId) ->
    ChildSpec = connection_manager_spec(IpAddr, ManagerId),
    supervisor:start_child(?GATEWAY_CONNECTION_MANAGER_SUPERVISOR, ChildSpec).


%%--------------------------------------------------------------------
%% @doc
%% Initializes supervisor parameters.
%% @end
%%--------------------------------------------------------------------
-spec init([rtransfer:opt()]) -> Result when
    Result :: {ok,{{RestartStrategy,MaxR,MaxT},[ChildSpec]}} | ignore,
     RestartStrategy :: supervisor:strategy(),
     MaxR :: non_neg_integer(),
     MaxT :: pos_integer(),
     ChildSpec :: supervisor:child_spec().
init(RtransferOpts) ->
    RestartStrategy = one_for_one,
    MaxR = 3,
    MaxT = timer:minutes(1),
    {ok, {{RestartStrategy, MaxR, MaxT}, [connection_supervisor_spec(RtransferOpts)]}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates a supervisor child_spec for a connection manager child.
%% @end
%%--------------------------------------------------------------------
-spec connection_manager_spec(IpAddr :: inet:ip_address(), ManagerId :: term()) ->
    supervisor:child_spec().
connection_manager_spec(IpAddr, ManagerId) ->
    Module = gateway_connection_manager,
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ManagerId, {Module, start_link, [IpAddr, ManagerId]},
        Restart, ExitTimeout, Type, [Module]}.


%%--------------------------------------------------------------------
%% @doc
%% Creates a supervisor child_spec for a connection supervisor child.
%% @end
%%--------------------------------------------------------------------
-spec connection_supervisor_spec([rtransfer:opt()]) -> supervisor:child_spec().
connection_supervisor_spec(RtransferOpts) ->
    ChildId = Module = ?GATEWAY_CONNECTION_SUPERVISOR,
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = supervisor,
    {ChildId, {Module, start_link, [RtransferOpts]}, Restart, ExitTimeout, Type, [Module]}.
