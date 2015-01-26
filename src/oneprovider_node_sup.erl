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
-module(oneprovider_node_sup).
-author("Michal Wrzeszcz").

-include("registered_names.hrl").
-behaviour(supervisor).

%% API
-export([start_link/1]).

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
-spec start_link(Args :: term()) -> Result when
    Result :: {ok, pid()}
    | ignore
    | {error, Error},
    Error :: {already_started, pid()}
    | {shutdown, term()}
    | term().
start_link(NodeType) ->
    supervisor:start_link({local, ?APPLICATION_SUPERVISOR_NAME}, ?MODULE, [NodeType]).

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
-spec init(Args :: term()) -> Result when
    Result :: {ok, {SupervisionPolicy, [ChildSpec]}} | ignore,
    SupervisionPolicy :: {RestartStrategy, MaxR :: non_neg_integer(), MaxT :: pos_integer()},
    RestartStrategy :: one_for_all
    | one_for_one
    | rest_for_one
    | simple_one_for_one,
    ChildSpec :: {Id :: term(), StartFunc, RestartPolicy, Type :: worker | supervisor, Modules},
    StartFunc :: {M :: module(), F :: atom(), A :: [term()] | undefined},
    RestartPolicy :: permanent
    | transient
    | temporary,
    Modules :: [module()] | dynamic.
init([worker]) ->
    {ok, {{one_for_one, 5, 10}, [
        {main_worker_sup, {main_worker_sup, start_link, []}, permanent, infinity, supervisor, [main_worker_sup]},
        {request_dispatcher, {request_dispatcher, start_link, []}, permanent, 5000, worker, [request_dispatcher]},
        {node_manager, {node_manager, start_link, [worker]}, permanent, 5000, worker, [node_manager]}
    ]}};
init([ccm]) ->
    {ok, {{one_for_one, 5, 10}, [
        {cluster_manager, {cluster_manager, start_link, []}, permanent, 5000, worker, [cluster_manager]},
        {main_worker_sup, {main_worker_sup, start_link, []}, permanent, infinity, supervisor, [main_worker_sup]},
        {node_manager, {node_manager, start_link, [ccm]}, permanent, 5000, worker, [node_manager]},
        {request_dispatcher, {request_dispatcher, start_link, []}, permanent, 5000, worker, [request_dispatcher]}
    ]}}.
