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

-spec start_link(MaxConcurrentConnections) -> Result when
    MaxConcurrentConnections :: pos_integer(),
    Result :: {ok, pid()} | ignore | {error, Error},
     Error :: {already_started, pid()} | {shutdown, term()} | term().
start_link(MaxConcurrentConnections) ->
    supervisor:start_link({local, ?GATEWAY_DISPATCHER_SUPERVISOR}, ?MODULE, MaxConcurrentConnections).


-spec init(MaxConcurrentConnections) -> Result when
    MaxConcurrentConnections :: pos_integer(),
    Result :: {ok,{{RestartStrategy,MaxR,MaxT},[ChildSpec]}} | ignore,
     RestartStrategy :: supervisor:strategy(),
     MaxR :: non_neg_integer(),
     MaxT :: pos_integer(),
     ChildSpec :: supervisor:child_spec().
init(MaxConcurrentConnections) ->
    RestartStrategy = one_for_all,
    MaxR = 3,
    MaxT = timer:minutes(1),
    {ok, {{RestartStrategy, MaxR, MaxT},
        [connection_manager_supervisor_spec(),
         dispatcher_spec(MaxConcurrentConnections)]}}.

%% ====================================================================
%% Internal functions
%% ====================================================================

-spec dispatcher_spec(MaxConcurrentConnections :: pos_integer()) ->
    supervisor:child_spec().
dispatcher_spec(MaxConcurrentConnections) ->
    ChildId = Module = gateway_dispatcher,
    Function = {Module, start_link, [MaxConcurrentConnections]},
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ChildId, Function, Restart, ExitTimeout, Type, [Module]}.

-spec connection_manager_supervisor_spec() -> supervisor:child_spec().
connection_manager_supervisor_spec() ->
    ChildId = Module = gateway_connection_manager_supervisor,
    Function = {Module, start_link, []},
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = supervisor,
    {ChildId, Function, Restart, ExitTimeout, Type, [Module]}.
