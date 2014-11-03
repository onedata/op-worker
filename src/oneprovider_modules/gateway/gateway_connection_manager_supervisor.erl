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

-module(gateway_connection_manager_supervisor).
-author("Konrad Zemek").
-behavior(supervisor).

-include("oneprovider_modules/gateway/registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

-export([start_link/0, start_connection_manager/1]).
%% supervisor callbacks
-export([init/1]).

%% ====================================================================
%% API functions
%% ====================================================================

-spec start_link() -> Result when
    Result :: {ok, pid()} | ignore | {error, Error},
     Error :: {already_started, pid()} | {shutdown, term()} | term().
start_link() ->
    supervisor:start_link({local, ?GATEWAY_CONNECTION_MANAGER_SUPERVISOR}, ?MODULE, []).


-spec start_connection_manager(ManagerId :: non_neg_integer()) -> supervisor:startchild_ret().
start_connection_manager(ManagerId) ->
    ChildSpec = connection_manager_spec(ManagerId),
    supervisor:start_child(?GATEWAY_CONNECTION_MANAGER_SUPERVISOR, ChildSpec).


-spec init(Args) -> Result when
    Args :: term(),
    Result :: {ok,{{RestartStrategy,MaxR,MaxT},[ChildSpec]}} | ignore,
     RestartStrategy :: supervisor:strategy(),
     MaxR :: non_neg_integer(),
     MaxT :: pos_integer(),
     ChildSpec :: supervisor:child_spec().
init(_Args) ->
    RestartStrategy = one_for_one,
    MaxR = 3,
    MaxT = timer:minutes(1),
    {ok, {{RestartStrategy, MaxR, MaxT}, [connection_supervisor_spec()]}}.


%% ====================================================================
%% Internal functions
%% ====================================================================


-spec connection_manager_spec(ManagerId :: non_neg_integer()) -> supervisor:child_spec().
connection_manager_spec(ManagerId) ->
    Module = gateway_connection_manager,
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ManagerId, {Module, start_link, [ManagerId]},
        Restart, ExitTimeout, Type, [Module]}.


-spec connection_supervisor_spec() -> supervisor:child_spec().
connection_supervisor_spec() ->
    ChildId = Module = ?GATEWAY_CONNECTION_SUPERVISOR,
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = supervisor,
    {ChildId, {Module, start_link, []}, Restart, ExitTimeout, Type, [Module]}.