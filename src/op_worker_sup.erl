%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc It is the main supervisor. It starts (as it child) node manager
%%% which initializes node.
%%% @end
%%%--------------------------------------------------------------------
-module(op_worker_sup).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> Result when
    Result :: {ok, pid()}
    | ignore
    | {error, Error},
    Error :: {already_started, pid()}
    | {shutdown, term()}
    | term().
start_link() ->
    supervisor:start_link({local, ?APPLICATION_SUPERVISOR_NAME}, ?MODULE, []).

%%%===================================================================
%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }} |
    ignore.
init([]) ->
    RestartStrategy = one_for_all,
    MaxRestarts = 5,
    RestartTimeWindowSecs = 10,
    {ok, {{RestartStrategy, MaxRestarts, RestartTimeWindowSecs}, [
        main_worker_sup_spec(),
        request_dispatcher_spec(),
        node_manager_spec()
    ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a supervisor child_spec for a main worker supervisor child.
%% @end
%%--------------------------------------------------------------------
-spec main_worker_sup_spec() -> supervisor:child_spec().
main_worker_sup_spec() ->
    Id = Module = main_worker_sup,
    Restart = permanent,
    Shutdown = infinity,
    Type = supervisor,
    {Id, {Module, start_link, []}, Restart, Shutdown, Type, [Module]}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a worker child_spec for a request dispatcher child.
%% @end
%%--------------------------------------------------------------------
-spec request_dispatcher_spec() -> supervisor:child_spec().
request_dispatcher_spec() ->
    Id = Module = request_dispatcher,
    Restart = permanent,
    Shutdown = timer:seconds(5),
    Type = worker,
    {Id, {Module, start_link, []}, Restart, Shutdown, Type, [Module]}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a worker child_spec for a node manager child.
%% @end
%%--------------------------------------------------------------------
-spec node_manager_spec() -> supervisor:child_spec().
node_manager_spec() ->
    Id = Module = node_manager,
    Restart = permanent,
    Shutdown = timer:seconds(5),
    Type = worker,
    {Id, {Module, start_link, []}, Restart, Shutdown, Type, [Module]}.


