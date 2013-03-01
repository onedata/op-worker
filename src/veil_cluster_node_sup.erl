%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: It is the main supervisor. It starts (as it child) node manager
%% which initializes node.
%% @end
%% ===================================================================

-module(veil_cluster_node_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args), {I, {I, start_link, [Args]}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

%% start_link/1
%% ====================================================================
%% @doc Starts application supervisor
-spec start_link(Args :: term()) -> Result when
	Result ::  {ok, pid()}
                | ignore
                | {error, Error},
	Error :: {already_started, pid()}
                | {shutdown, term()}
                | term().
%% ====================================================================
start_link(NodeType) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [NodeType]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/supervisor.html#Module:init-1">supervisor:init/1</a>
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
%% ====================================================================
init([NodeType]) ->
    {ok, { {one_for_one, 5, 10}, [?CHILD(node_manager, worker, NodeType)]} }.
