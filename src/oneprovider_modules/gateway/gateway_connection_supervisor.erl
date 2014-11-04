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

-spec start_link() -> Result when
    Result :: {ok, pid()} | ignore | {error, Error},
    Error :: {already_started, pid()} | {shutdown, term()} | term().
start_link() ->
    supervisor:start_link({local, ?GATEWAY_CONNECTION_SUPERVISOR}, ?MODULE, []).


-spec start_connection(Remote :: inet:ip_address(), Local :: inet:ip_address(), ConnectionManager :: pid()) ->
    supervisor:startchild_ret().
start_connection(Remote, Local, ConnectionManager) ->
    supervisor:start_child(?GATEWAY_CONNECTION_SUPERVISOR, [Remote, Local, ConnectionManager]).


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
    {ok, {{RestartStrategy, MaxR, MaxT}, [connection_spec()]}}.


%% ====================================================================
%% Internal functions
%% ====================================================================


-spec connection_spec() -> supervisor:child_spec().
connection_spec() ->
    ChildId = Module = gateway_connection,
    Restart = temporary,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ChildId, {Module, start_link, []}, Restart, ExitTimeout, Type, [Module]}.