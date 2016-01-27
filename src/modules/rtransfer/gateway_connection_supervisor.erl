%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% gateway_connection_supervisor is responsible for supervising
%%% gateway_connection gen_servers. Connections are transient, so the supervisor
%%% takes a role of an easy startup and shutdown facility for connections.
%%% @end
%%%-------------------------------------------------------------------
-module(gateway_connection_supervisor).
-author("Konrad Zemek").
-behavior(supervisor).

-include("modules/rtransfer/registered_names.hrl").

-export([start_link/1, start_connection/3]).
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
-spec start_link(RtransferOpts :: [rtransfer:opt()]) -> Result when
    Result :: {ok, pid()} | ignore | {error, Error},
    Error :: {already_started, pid()} | {shutdown, term()} | term().
start_link(RtransferOpts) ->
    supervisor:start_link({local, ?GATEWAY_CONNECTION_SUPERVISOR}, ?MODULE, RtransferOpts).


%%--------------------------------------------------------------------
%% @doc
%% Starts a connection supervised by the supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_connection(Remote :: {inet:ip_address(), inet:port_number()},
                       Local :: inet:ip_address(), ConnectionManager :: pid()) ->
    supervisor:startchild_ret().
start_connection(Remote, Local, ConnectionManager) ->
    supervisor:start_child(?GATEWAY_CONNECTION_SUPERVISOR, [Remote, Local, ConnectionManager]).


%%--------------------------------------------------------------------
%% @doc
%% Initializes supervisor parameters.
%% @end
%%--------------------------------------------------------------------
-spec init(RtransferOpts :: [rtransfer:opt()]) -> Result when
    Result :: {ok,{{RestartStrategy,MaxR,MaxT},[ChildSpec]}} | ignore,
    RestartStrategy :: supervisor:strategy(),
    MaxR :: non_neg_integer(),
    MaxT :: pos_integer(),
    ChildSpec :: supervisor:child_spec().
init(RtransferOpts) ->
    RestartStrategy = simple_one_for_one,
    MaxR = 3,
    MaxT = timer:minutes(1),
    {ok, {{RestartStrategy, MaxR, MaxT}, [connection_spec(RtransferOpts)]}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates a supervisor child_spec for a connection child.
%% @end
%%--------------------------------------------------------------------
-spec connection_spec(RtransferOpts :: [rtransfer:opt()]) -> supervisor:child_spec().
connection_spec(RtransferOpts) ->
    ChildId = Module = gateway_connection,
    Restart = temporary,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ChildId, {Module, start_link, [RtransferOpts]}, Restart, ExitTimeout, Type, [Module]}.
