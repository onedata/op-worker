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

-module(oneprovider_node_sup).
-include("registered_names.hrl").
-include("supervision_macros.hrl").
-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
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
    supervisor:start_link({local, ?Supervisor_Name}, ?MODULE, [NodeType]).

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
init([worker]) ->
    {ok, {?Sup_Flags, [ %todo add dispatcher
        ?Sup_Child(node_manager, node_manager, permanent, [worker])
    ]}};
init([ccm_test]) ->
    {ok, {?Sup_Flags, [
        ?Sup_Child(node_manager, node_manager, permanent, [ccm_test])
    ]}};
init([ccm]) ->
    {ok, {?Sup_Flags, [
        ?Sup_Child(node_manager, node_manager, permanent, [ccm])
    ]}}.