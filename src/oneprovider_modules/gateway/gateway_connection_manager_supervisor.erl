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
-behavior(supervisor).

-include("oneprovider_modules/gateway/registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

-export([start_link/0]).
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


-spec init(Args) -> Result when
    Args :: term(),
    Result :: {ok,{{RestartStrategy,MaxR,MaxT},[ChildSpec]}} | ignore,
     RestartStrategy :: supervisor:strategy(),
     MaxR :: non_neg_integer(),
     MaxT :: pos_integer(),
     ChildSpec :: supervisor:child_spec().
init(_Args) ->
    RestartStrategy = simple_one_for_one,
    MaxR = 3,
    MaxT = timer:minutes(1),
    ChildId = Module = gateway_connection_manager,
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ok, {{RestartStrategy, MaxR, MaxT},
        [{ChildId, {Module, start_link, []},
         Restart, ExitTimeout, Type, [Module]}]}}.
